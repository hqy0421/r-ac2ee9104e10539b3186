"""
Staging-table synchronization engine.

A source system lands change rows in an ordinary "staging" table. Each row
carries a business key, a version, an insert/update/delete marker and a full
after-image of the mapped columns. This module applies those rows to a target
table deterministically:

* the highest version for a business key wins, regardless of row order or the
  order batches arrive in;
* inserts and updates are full after-images -- unmapped columns are preserved,
  mapped columns are written verbatim, including NULLs;
* a delete tombstones the key, so an older update arriving later can never
  bring the row back;
* re-running the same batch rewrites nothing;
* the same latest version carrying different content is a conflict;
* target rows edited outside the sync machinery are detected (drift) and block
  the run until the change is acknowledged with a rebaseline.

All target writes, applied-version bookkeeping and tombstone states commit in
a single transaction, so any constraint failure rolls the whole batch back.
"""

import hashlib
import json
from collections import namedtuple

try:
    from sqlite_web.executor import key_decode, key_encode
except ImportError:  # Script-style execution alongside a flattened package.
    from executor import key_decode, key_encode


SYNC_CONFIG_TABLE = '_sw_sync_config'
SYNC_STATE_TABLE = '_sw_sync_state'
SYNC_RUNS_TABLE = '_sw_sync_runs'

SYNC_TABLES = (SYNC_CONFIG_TABLE, SYNC_STATE_TABLE, SYNC_RUNS_TABLE)

DEFAULT_INSERT_MARKER = 'I'
DEFAULT_UPDATE_MARKER = 'U'
DEFAULT_DELETE_MARKER = 'D'


class SyncError(Exception):
    """A configuration, conflict, drift or validation error.

    Raised inside the sync transaction so the surrounding transaction rolls
    the whole batch back.
    """


SyncConfig = namedtuple('SyncConfig', (
    'target_table', 'stage_table', 'key_columns', 'target_key_columns',
    'version_column', 'flag_column', 'insert_value', 'update_value',
    'delete_value', 'mapping'))

PlanEntry = namedtuple('PlanEntry', (
    'key_values', 'version', 'flag', 'action', 'detail'))


def is_sync_table(name):
    return name in SYNC_TABLES


def quote_ident(name):
    return '"%s"' % name.replace('"', '""')


# ---------------------------------------------------------------------------
# Value encoding / hashing
# ---------------------------------------------------------------------------

def _enc(value):
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {'b64': bytes(value).hex()}
    if value is None or isinstance(value, (int, float, str, bool)):
        return value
    return str(value)


def _canonical(payload):
    return json.dumps(payload, sort_keys=True, separators=(',', ':'),
                      default=str).encode('utf-8')


def row_hash(row):
    """Hash a full target row (column name -> native value)."""
    return hashlib.sha256(_canonical(
        {k: _enc(v) for k, v in row.items()})).hexdigest()


def _mapped_signature(mapping, stage_row):
    """Hash of the after-image a staging row contributes to the target."""
    payload = {target: _enc(stage_row.get(stage_col))
               for target, stage_col in mapping.items()}
    return hashlib.sha256(_canonical(payload)).hexdigest()


def encode_key(values):
    return key_encode(list(values))


def decode_key(token):
    return key_decode(token)


# ---------------------------------------------------------------------------
# Schema management
# ---------------------------------------------------------------------------

def ensure_sync_tables(dataset):
    """Create the bookkeeping tables once, inside the caller's transaction."""
    dataset.query(
        'CREATE TABLE IF NOT EXISTS %s ('
        'target_table TEXT PRIMARY KEY, '
        'stage_table TEXT NOT NULL, '
        'key_columns TEXT NOT NULL, '
        'target_key_columns TEXT NOT NULL DEFAULT \'[]\', '
        'version_column TEXT NOT NULL, '
        'flag_column TEXT NOT NULL, '
        'insert_value TEXT NOT NULL, '
        'update_value TEXT NOT NULL, '
        'delete_value TEXT NOT NULL, '
        'mapping TEXT NOT NULL, '
        'created_at TEXT NOT NULL DEFAULT (datetime(\'now\')), '
        'updated_at TEXT NOT NULL DEFAULT (datetime(\'now\')))'
        % quote_ident(SYNC_CONFIG_TABLE))
    dataset.query(
        'CREATE TABLE IF NOT EXISTS %s ('
        'target_table TEXT NOT NULL, '
        'key_token TEXT NOT NULL, '
        'pk_token TEXT, '
        'version_token TEXT NOT NULL, '
        'deleted INTEGER NOT NULL, '
        'row_hash TEXT, '
        'updated_at TEXT NOT NULL DEFAULT (datetime(\'now\')), '
        'PRIMARY KEY (target_table, key_token))'
        % quote_ident(SYNC_STATE_TABLE))
    dataset.query(
        'CREATE INDEX IF NOT EXISTS idx_sw_sync_state_table '
        'ON %s (target_table)' % quote_ident(SYNC_STATE_TABLE))
    dataset.query(
        'CREATE TABLE IF NOT EXISTS %s ('
        'id INTEGER PRIMARY KEY, '
        'target_table TEXT NOT NULL, '
        'started_at TEXT NOT NULL DEFAULT (datetime(\'now\')), '
        'dry_run INTEGER NOT NULL, '
        'status TEXT NOT NULL, '
        'message TEXT, '
        'stage_rows INTEGER NOT NULL DEFAULT 0, '
        'applied INTEGER NOT NULL DEFAULT 0, '
        'replayed INTEGER NOT NULL DEFAULT 0, '
        'expired INTEGER NOT NULL DEFAULT 0)'
        % quote_ident(SYNC_RUNS_TABLE))


# ---------------------------------------------------------------------------
# Configuration persistence
# ---------------------------------------------------------------------------

def _row_to_config(row):
    key_columns = json.loads(row['key_columns'])
    target_keys = json.loads(row.get('target_key_columns') or '[]')
    # Configs written before target_key_columns existed assume identically
    # named key columns on both sides.
    if not target_keys:
        target_keys = list(key_columns)
    return SyncConfig(
        target_table=row['target_table'],
        stage_table=row['stage_table'],
        key_columns=key_columns,
        target_key_columns=target_keys,
        version_column=row['version_column'],
        flag_column=row['flag_column'],
        insert_value=row['insert_value'],
        update_value=row['update_value'],
        delete_value=row['delete_value'],
        mapping=json.loads(row['mapping']))


def get_config(dataset, target_table):
    ensure_sync_tables(dataset)
    cursor = dataset.query(
        'SELECT * FROM %s WHERE target_table = ?'
        % quote_ident(SYNC_CONFIG_TABLE), (target_table,))
    row = cursor.fetchone()
    if row is None:
        return None
    cols = [d[0] for d in cursor.description]
    return _row_to_config(dict(zip(cols, row)))


def save_config(dataset, config):
    ensure_sync_tables(dataset)
    dataset.query(
        'INSERT INTO %s (target_table, stage_table, key_columns, '
        'target_key_columns, version_column, flag_column, insert_value, '
        'update_value, delete_value, mapping) '
        'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) '
        'ON CONFLICT(target_table) DO UPDATE SET '
        'stage_table=excluded.stage_table, '
        'key_columns=excluded.key_columns, '
        'target_key_columns=excluded.target_key_columns, '
        'version_column=excluded.version_column, '
        'flag_column=excluded.flag_column, '
        'insert_value=excluded.insert_value, '
        'update_value=excluded.update_value, '
        'delete_value=excluded.delete_value, '
        'mapping=excluded.mapping, '
        'updated_at=datetime(\'now\')'
        % quote_ident(SYNC_CONFIG_TABLE),
        (config.target_table, config.stage_table,
         json.dumps(config.key_columns),
         json.dumps(config.target_key_columns),
         config.version_column, config.flag_column,
         config.insert_value, config.update_value,
         config.delete_value, json.dumps(config.mapping)))


def delete_config(dataset, target_table):
    ensure_sync_tables(dataset)
    dataset.query('DELETE FROM %s WHERE target_table = ?'
                  % quote_ident(SYNC_CONFIG_TABLE), (target_table,))
    dataset.query('DELETE FROM %s WHERE target_table = ?'
                  % quote_ident(SYNC_STATE_TABLE), (target_table,))
    # Run history is retained as an audit trail.


def validate_config(dataset, config):
    """Fail early, with a readable message, on any stale configuration."""
    tables = set(dataset.cached_tables())
    if config.target_table not in tables:
        raise SyncError('Target table "%s" no longer exists.'
                        % config.target_table)
    if config.stage_table not in tables:
        raise SyncError('Staging table "%s" no longer exists. Re-point the '
                        'configuration at an existing table.'
                        % config.stage_table)
    if is_sync_table(config.stage_table):
        raise SyncError('A sync bookkeeping table cannot be used as a '
                        'staging table.')

    stage_cols = {c.name for c in dataset.get_columns(config.stage_table)}
    target_cols = {c.name for c in dataset.get_columns(config.target_table)}

    if not config.key_columns:
        raise SyncError('Select at least one business-key column.')
    target_key_columns = (config.target_key_columns
                          or list(config.key_columns))
    if len(target_key_columns) != len(config.key_columns):
        raise SyncError('Each staging business-key column needs one target '
                        'key column.')
    for col in config.key_columns:
        if col not in stage_cols:
            raise SyncError('Business-key column "%s" is missing from the '
                            'staging table.' % col)
    for col in target_key_columns:
        if col not in target_cols:
            raise SyncError('Target key column "%s" does not exist.' % col)
    for col in (config.version_column, config.flag_column):
        if col not in stage_cols:
            raise SyncError('Column "%s" is missing from the staging table.'
                            % col)
    if not config.mapping:
        raise SyncError('Map at least one target column to a staging column.')
    for target_col, stage_col in config.mapping.items():
        if target_col not in target_cols:
            raise SyncError('Mapped target column "%s" no longer exists.'
                            % target_col)
        if stage_col not in stage_cols:
            raise SyncError('Mapped staging column "%s" no longer exists.'
                            % stage_col)
    overlap = set(target_key_columns) & set(config.mapping)
    if overlap:
        raise SyncError(
            'Target key column(s) %s are used only to locate rows and cannot '
            'be mapping write columns.' % ', '.join(sorted(overlap)))

    if len(set(target_key_columns)) != len(target_key_columns):
        raise SyncError('Target key columns must be unique within the key.')
    if len(set(config.key_columns)) != len(config.key_columns):
        raise SyncError('Staging key columns must be unique within the key.')

    # Existing target rows must already be unique on the key, otherwise an
    # UPDATE or DELETE could hit more than one row.
    col_list = ', '.join(quote_ident(c) for c in target_key_columns)
    cursor = dataset.query(
        'SELECT COUNT(*) FROM (SELECT 1 FROM %s GROUP BY %s HAVING COUNT(*) > 1)'
        % (quote_ident(config.target_table), col_list))
    if cursor.fetchone()[0]:
        raise SyncError(
            'Existing rows in "%s" are not unique on target key column(s) %s; '
            'the sync needs a key that identifies a single row.'
            % (config.target_table, ', '.join(target_key_columns)))
    for marker in (config.insert_value, config.update_value,
                   config.delete_value):
        if not marker or not str(marker).strip():
            raise SyncError('Insert, update and delete markers are required.')


# ---------------------------------------------------------------------------
# Row lookup helpers
# ---------------------------------------------------------------------------

def _where(columns, values):
    clauses, params = [], []
    for col, val in zip(columns, values):
        if val is None:
            clauses.append('%s IS NULL' % quote_ident(col))
        else:
            clauses.append('%s = ?' % quote_ident(col))
            params.append(val)
    return ' AND '.join(clauses), params


def _fetch_dicts(dataset, table, sql, params=()):
    cursor = dataset.query(sql, params)
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def _pk_columns(dataset, table):
    return [c.name for c in dataset.get_columns(table) if c.primary_key]


def find_by_key(dataset, table, key_columns, key_values):
    """Return the rows matching a business key (more than one is an error)."""
    clause, params = _where(key_columns, key_values)
    rows = _fetch_dicts(
        dataset, table,
        'SELECT * FROM %s WHERE %s' % (quote_ident(table), clause), params)
    if len(rows) > 1:
        raise SyncError('Business key %s matches %d rows in "%s"; a sync '
                        'target key must identify a single row.'
                        % (tuple(key_values), len(rows), table))
    return rows[0] if rows else None


def _find_state_row(dataset, table, state, target_key_columns):
    """Locate the target row behind a state entry.

    The stored primary key is the fast path; the business key is the
    fallback, so a row whose primary key was regenerated externally is still
    found (and then reported as drift by the caller).
    """
    pk_cols = _pk_columns(dataset, table)
    if state.get('pk_token') and pk_cols:
        try:
            pk_values = key_decode(state['pk_token'])
        except Exception:
            pk_values = None
        if pk_values and len(pk_values) == len(pk_cols):
            clause, params = _where(pk_cols, pk_values)
            rows = _fetch_dicts(
                dataset, table,
                'SELECT * FROM %s WHERE %s' % (quote_ident(table), clause),
                params)
            if rows:
                return rows[0]
    return find_by_key(dataset, table, target_key_columns,
                       key_decode(state['key_token']))


def _load_states(dataset, target_table):
    cursor = dataset.query(
        'SELECT key_token, pk_token, version_token, deleted, row_hash '
        'FROM %s WHERE target_table = ?'
        % quote_ident(SYNC_STATE_TABLE), (target_table,))
    cols = [d[0] for d in cursor.description]
    return {row[0]: dict(zip(cols, row)) for row in cursor.fetchall()}


# ---------------------------------------------------------------------------
# Staging data intake
# ---------------------------------------------------------------------------

def _stage_groups(dataset, config):
    """Collapse staging rows into one winner per business key.

    Returns an ordered list of (key_values, winner, all_rows) tuples. Row
    order never affects the outcome: the maximum version wins, and equal
    versions must carry identical content or the batch is rejected.
    """
    cursor = dataset.query('SELECT * FROM %s'
                           % quote_ident(config.stage_table))
    stage_cols = [d[0] for d in cursor.description]
    raw_rows = [dict(zip(stage_cols, row)) for row in cursor.fetchall()]

    groups = {}
    order = []
    for row in raw_rows:
        key_values = [row.get(c) for c in config.key_columns]
        token = encode_key(key_values)
        if token not in groups:
            groups[token] = []
            order.append(token)
        groups[token].append(row)

    winners = []
    for token in order:
        rows = groups[token]
        def version_of(r):
            return r.get(config.version_column)
        max_version = version_of(rows[0])
        for r in rows[1:]:
            max_version = _max_version(max_version, version_of(r),
                                       config.stage_table)
        latest = [r for r in rows
                  if _version_equal(version_of(r), max_version)]
        flags = {r.get(config.flag_column) for r in latest}
        if (len({_mapped_signature(config.mapping, r) for r in latest}) > 1
                or len(flags) > 1):
            raise SyncError(
                'Conflicting staging rows for business key %s at the same '
                'latest version %r: the marker or mapped content differs.'
                % (tuple(key_decode(token)), max_version))
        winners.append((key_decode(token), latest[0], len(rows)))
    return winners, len(raw_rows)


def _version_equal(a, b):
    try:
        return a == b
    except TypeError:
        return False


def _max_version(a, b, table):
    try:
        return b if b > a else a
    except TypeError:
        raise SyncError('Version values in "%s" have incompatible types '
                        '(%r vs %r); versions for one key must be comparable.'
                        % (table, a, b))


def _cmp_version(a, b, key_values):
    try:
        return (a > b) - (a < b)
    except TypeError:
        raise SyncError('Incompatible version types for key %s: staging %r '
                        'vs already-applied %r.' % (tuple(key_values), a, b))


def _normalize_flag(config, value, key_values):
    text = '' if value is None else str(value)
    if text == str(config.delete_value):
        return 'D'
    if text == str(config.insert_value):
        return 'I'
    if text == str(config.update_value):
        return 'U'
    raise SyncError('Unknown change marker %r for business key %s; expected '
                    'one of %r, %r or %r.'
                    % (value, tuple(key_values), config.insert_value,
                       config.update_value, config.delete_value))


# ---------------------------------------------------------------------------
# State mutations
# ---------------------------------------------------------------------------

def _upsert_state(dataset, target_table, key_token, pk_token, version_value,
                  deleted, hash_value):
    dataset.query(
        'INSERT INTO %s (target_table, key_token, pk_token, version_token, '
        'deleted, row_hash, updated_at) VALUES (?, ?, ?, ?, ?, ?, '
        'datetime(\'now\')) '
        'ON CONFLICT(target_table, key_token) DO UPDATE SET '
        'pk_token=excluded.pk_token, version_token=excluded.version_token, '
        'deleted=excluded.deleted, row_hash=excluded.row_hash, '
        'updated_at=datetime(\'now\')'
        % quote_ident(SYNC_STATE_TABLE),
        (target_table, key_token, pk_token, key_encode([version_value]),
         1 if deleted else 0, hash_value))


def _insert_target(dataset, config, stage_row):
    # Mapped columns plus the target key columns, whose values come from the
    # staging business key so the new row can be located afterwards.
    cols = list(config.mapping)
    params = [stage_row.get(config.mapping[c]) for c in cols]
    for target_col, stage_col in zip(config.target_key_columns,
                                     config.key_columns):
        if target_col not in config.mapping:
            cols.append(target_col)
            params.append(stage_row.get(stage_col))
    sql = 'INSERT INTO %s (%s) VALUES (%s)' % (
        quote_ident(config.target_table),
        ', '.join(quote_ident(c) for c in cols),
        ', '.join('?' for _ in cols))
    dataset.query(sql, params)
    key_values = [stage_row.get(c) for c in config.key_columns]
    row = find_by_key(dataset, config.target_table,
                      config.target_key_columns, key_values)
    if row is None:
        raise SyncError('Inserted row for business key %s could not be '
                        're-read from "%s".'
                        % (tuple(key_values), config.target_table))
    return row


def _update_target(dataset, config, existing, stage_row):
    # Full after-image: every mapped column is assigned, NULL included,
    # while unmapped columns are left untouched.
    sets, params = [], []
    for target_col, stage_col in config.mapping.items():
        sets.append('%s = ?' % quote_ident(target_col))
        params.append(stage_row.get(stage_col))
    key_values = [existing[c] for c in config.target_key_columns]
    clause, where_params = _where(config.target_key_columns, key_values)
    params.extend(where_params)
    dataset.query(
        'UPDATE %s SET %s WHERE %s' % (
            quote_ident(config.target_table), ', '.join(sets), clause),
        params)
    refreshed = find_by_key(dataset, config.target_table,
                            config.target_key_columns, key_values)
    if refreshed is None:
        raise SyncError('Updated row for business key %s vanished after the '
                        'UPDATE.' % tuple(key_values))
    return refreshed


def _delete_target(dataset, table, target_key_columns, existing):
    key_values = [existing[c] for c in target_key_columns]
    clause, params = _where(target_key_columns, key_values)
    dataset.query('DELETE FROM %s WHERE %s'
                  % (quote_ident(table), clause), params)


def _pk_token(dataset, table, row):
    pk_cols = _pk_columns(dataset, table)
    if not pk_cols:
        return None
    return key_encode([row[c] for c in pk_cols])


# ---------------------------------------------------------------------------
# Drift detection
# ---------------------------------------------------------------------------

def _check_drift(dataset, config, states):
    """Refuse to run when the target changed outside the sync machinery.

    Every managed key is checked before any write, so drift aborts the whole
    batch before it starts.
    """
    for token, state in states.items():
        key_values = key_decode(token)
        if state['deleted']:
            existing = find_by_key(dataset, config.target_table,
                                   config.target_key_columns, key_values)
            if existing is not None:
                raise SyncError(
                    'Business key %s was deleted by sync but exists again in '
                    '"%s". The table was modified outside the sync process; '
                    'rebaseline to acknowledge it before syncing.'
                    % (tuple(key_values), config.target_table))
            continue

        existing = _find_state_row(dataset, config.target_table, state,
                                   config.target_key_columns)
        if existing is None:
            raise SyncError(
                'Previously synced row for business key %s is missing from '
                '"%s". The table was modified outside the sync process; '
                'rebaseline to acknowledge it before syncing.'
                % (tuple(key_values), config.target_table))
        # A row found through the stored primary key but carrying different
        # key columns means that key moved to another row -- also drift.
        if [existing.get(c) for c in config.target_key_columns] != key_values:
            raise SyncError(
                'Business key %s no longer matches the row stored at sync '
                'time in "%s". The table was modified outside the sync '
                'process; rebaseline to acknowledge it before syncing.'
                % (tuple(key_values), config.target_table))
        current_hash = row_hash(existing)
        if state['row_hash'] and current_hash != state['row_hash']:
            raise SyncError(
                'Row for business key %s in "%s" was edited outside the sync '
                'process (recorded hash %s, current hash %s). Rebaseline to '
                'acknowledge the change before syncing.'
                % (tuple(key_values), config.target_table,
                   state['row_hash'][:12], current_hash[:12]))


# ---------------------------------------------------------------------------
# Planning / application
# ---------------------------------------------------------------------------

def plan_and_apply(dataset, config, dry_run=False):
    """Validate, plan and (unless dry_run) apply one staging batch.

    Returns a dict of counters plus the per-key plan. Raises SyncError on
    any conflict or drift; the caller wraps this in one transaction, so the
    exception rolls everything back.
    """
    ensure_sync_tables(dataset)
    validate_config(dataset, config)
    if not dry_run:
        # Keep the bookkeeping self-contained: a direct engine call persists
        # the same configuration the web route would.
        save_config(dataset, config)

    groups, stage_row_count = _stage_groups(dataset, config)
    states = _load_states(dataset, config.target_table)
    _check_drift(dataset, config, states)
    plan = []
    counters = {'applied': 0, 'replayed': 0, 'expired': 0}

    for key_values, stage_row, _group_size in groups:
        token = encode_key(key_values)
        state = states.get(token)
        version = stage_row.get(config.version_column)
        flag = _normalize_flag(config, stage_row.get(config.flag_column),
                               key_values)

        if state is None:
            # First time the engine sees this key. The target may already
            # hold a pre-existing baseline row for it.
            existing = find_by_key(dataset, config.target_table,
                                   config.target_key_columns, key_values)
            if flag == 'D':
                if existing is None:
                    # No row anywhere, but the delete is the key's latest
                    # word: tombstone it so an older/equal insert arriving in
                    # a later batch cannot resurrect data that was meant to
                    # be gone.
                    if dry_run:
                        counters['applied'] += 1
                        plan.append(PlanEntry(
                            key_values, version, flag, 'delete',
                            'unknown key, would record a tombstone'))
                        continue
                    _upsert_state(dataset, config.target_table, token,
                                  None, version, True, None)
                    counters['applied'] += 1
                    plan.append(PlanEntry(
                        key_values, version, flag, 'delete',
                        'key tombstoned though no row existed'))
                    continue
                if dry_run:
                    counters['applied'] += 1
                    plan.append(PlanEntry(
                        key_values, version, flag, 'delete',
                        'would delete pre-existing baseline row'))
                    continue
                _delete_target(dataset, config.target_table,
                               config.target_key_columns, existing)
                _upsert_state(dataset, config.target_table, token,
                              _pk_token(dataset, config.target_table,
                                        existing),
                              version, True, None)
                counters['applied'] += 1
                plan.append(PlanEntry(
                    key_values, version, flag, 'delete',
                    'baseline row deleted, key tombstoned'))
                continue

            if existing is not None:
                # Adopt the baseline row in place: its primary key and
                # unmapped columns survive, only mapped columns change.
                if dry_run:
                    counters['applied'] += 1
                    plan.append(PlanEntry(
                        key_values, version, flag, 'update',
                        'pre-existing baseline row, would update mapped '
                        'columns'))
                    continue
                row = _update_target(dataset, config, existing, stage_row)
                _upsert_state(dataset, config.target_table, token,
                              _pk_token(dataset, config.target_table, row),
                              version, False, row_hash(row))
                counters['applied'] += 1
                plan.append(PlanEntry(
                    key_values, version, flag, 'update',
                    'baseline row adopted, mapped columns replaced'))
                continue

            if dry_run:
                counters['applied'] += 1
                plan.append(PlanEntry(key_values, version, flag, 'insert',
                                      'new key, would insert'))
                continue
            row = _insert_target(dataset, config, stage_row)
            _upsert_state(dataset, config.target_table, token,
                          _pk_token(dataset, config.target_table, row),
                          version, False, row_hash(row))
            counters['applied'] += 1
            plan.append(PlanEntry(key_values, version, flag, 'insert',
                                  'row inserted'))
            continue

        state_version = key_decode(state['version_token'])[0]
        cmpv = _cmp_version(version, state_version, key_values)

        if cmpv < 0:
            # Older than the version already in effect: order-independent
            # staleness, never written.
            counters['expired'] += 1
            plan.append(PlanEntry(key_values, version, flag, 'expired',
                                  'version %r older than applied %r'
                                  % (version, state_version)))
            continue

        if cmpv == 0:
            if state['deleted']:
                if flag == 'D':
                    counters['replayed'] += 1
                    plan.append(PlanEntry(key_values, version, flag,
                                          'replay',
                                          'delete already applied'))
                    continue
                raise SyncError(
                    'Business key %s was deleted at version %r; an '
                    'insert/update at the same version cannot resurrect it.'
                    % (tuple(key_values), version))
            if flag == 'D':
                raise SyncError(
                    'Business key %s: a delete at version %r conflicts with '
                    'the row already applied at that version.'
                    % (tuple(key_values), version))
            existing = _find_state_row(dataset, config.target_table, state,
                                       config.target_key_columns)
            projected = dict(existing)
            for target_col, stage_col in config.mapping.items():
                projected[target_col] = stage_row.get(stage_col)
            if row_hash(projected) == row_hash(existing):
                counters['replayed'] += 1
                plan.append(PlanEntry(key_values, version, flag, 'replay',
                                      'same version, identical content'))
            else:
                raise SyncError(
                    'Business key %s already applied version %r with '
                    'different content. Re-running the same latest version '
                    'with changed data is a conflict.'
                    % (tuple(key_values), version))
            continue

        # Newer version than the one in effect.
        if flag == 'D':
            if state['deleted']:
                counters['replayed'] += 1
                plan.append(PlanEntry(key_values, version, flag, 'replay',
                                      'key already tombstoned'))
                continue
            if dry_run:
                counters['applied'] += 1
                plan.append(PlanEntry(key_values, version, flag, 'delete',
                                      'would delete row'))
                continue
            existing = _find_state_row(dataset, config.target_table, state,
                                       config.target_key_columns)
            _delete_target(dataset, config.target_table,
                           config.target_key_columns, existing)
            _upsert_state(dataset, config.target_table, token,
                          state.get('pk_token'), version, True, None)
            counters['applied'] += 1
            plan.append(PlanEntry(key_values, version, flag, 'delete',
                                  'row deleted, key tombstoned'))
            continue

        if state['deleted']:
            # A newer after-image after a delete: legitimate resurrection as
            # a brand-new row; the old primary key is not reused.
            if dry_run:
                counters['applied'] += 1
                plan.append(PlanEntry(key_values, version, flag, 'insert',
                                      'newer version after delete, would '
                                      'insert a new row'))
                continue
            row = _insert_target(dataset, config, stage_row)
            _upsert_state(dataset, config.target_table, token,
                          _pk_token(dataset, config.target_table, row),
                          version, False, row_hash(row))
            counters['applied'] += 1
            plan.append(PlanEntry(key_values, version, flag, 'insert',
                                  're-inserted after earlier delete'))
            continue

        if dry_run:
            counters['applied'] += 1
            plan.append(PlanEntry(key_values, version, flag, 'update',
                                  'would update mapped columns'))
            continue
        existing = _find_state_row(dataset, config.target_table, state,
                                   config.target_key_columns)
        row = _update_target(dataset, config, existing, stage_row)
        _upsert_state(dataset, config.target_table, token,
                      _pk_token(dataset, config.target_table, row),
                      version, False, row_hash(row))
        counters['applied'] += 1
        plan.append(PlanEntry(key_values, version, flag, 'update',
                              'mapped columns replaced, others preserved'))

    if not dry_run:
        dataset.query(
            'INSERT INTO %s (target_table, dry_run, status, message, '
            'stage_rows, applied, replayed, expired) '
            'VALUES (?, 0, \'applied\', ?, ?, ?, ?, ?)'
            % quote_ident(SYNC_RUNS_TABLE),
            (config.target_table,
             'batch applied', stage_row_count, counters['applied'],
             counters['replayed'], counters['expired']))

    counters['stage_rows'] = stage_row_count
    counters['plan'] = plan
    return counters


def rebaseline(dataset, target_table):
    """Adopt the current target contents as the sync baseline.

    External edits normally block sync as drift. Rebaseline re-hashes the
    rows still present, drops bookkeeping for rows that disappeared and
    leaves tombstones untouched. It records no new versions.
    """
    ensure_sync_tables(dataset)
    config = get_config(dataset, target_table)
    if config is None:
        raise SyncError('No saved configuration exists for "%s".'
                        % target_table)
    states = _load_states(dataset, target_table)
    kept = removed = 0
    for token, state in states.items():
        key_values = key_decode(token)
        if state['deleted']:
            existing = find_by_key(dataset, target_table,
                                   config.target_key_columns, key_values)
            if existing is None:
                kept += 1
                continue
            # The key came back outside sync: adopt it as a live baseline row.
        else:
            existing = _find_state_row(dataset, target_table, state,
                                       config.target_key_columns)
            if existing is None:
                dataset.query(
                    'DELETE FROM %s WHERE target_table = ? AND key_token = ?'
                    % quote_ident(SYNC_STATE_TABLE),
                    (target_table, token))
                removed += 1
                continue
        dataset.query(
            'UPDATE %s SET row_hash = ?, pk_token = ?, deleted = 0, '
            'updated_at = datetime(\'now\') '
            'WHERE target_table = ? AND key_token = ?'
            % quote_ident(SYNC_STATE_TABLE),
            (row_hash(existing),
             _pk_token(dataset, target_table, existing),
             target_table, token))
        kept += 1
    dataset.query(
        'INSERT INTO %s (target_table, dry_run, status, message, '
        'stage_rows, applied, replayed, expired) '
        'VALUES (?, 0, \'applied\', ?, 0, 0, 0, 0)'
        % quote_ident(SYNC_RUNS_TABLE),
        (target_table,
         'rebaseline: %d key(s) adopted, %d removed' % (kept, removed)))
    return {'kept': kept, 'removed': removed}


# ---------------------------------------------------------------------------
# Read models for the UI
# ---------------------------------------------------------------------------

def state_summary(dataset, target_table):
    ensure_sync_tables(dataset)
    cursor = dataset.query(
        'SELECT COALESCE(SUM(deleted), 0), COALESCE(SUM(1 - deleted), 0), '
        'COUNT(*) FROM %s WHERE target_table = ?'
        % quote_ident(SYNC_STATE_TABLE), (target_table,))
    deleted, live, total = cursor.fetchone()
    return {'deleted': deleted, 'live': live, 'total': total}


def state_rows(dataset, target_table, page=1, page_size=50):
    ensure_sync_tables(dataset)
    cursor = dataset.query(
        'SELECT COUNT(*) FROM %s WHERE target_table = ?'
        % quote_ident(SYNC_STATE_TABLE), (target_table,))
    total = cursor.fetchone()[0]
    offset = (max(page, 1) - 1) * page_size
    cursor = dataset.query(
        'SELECT key_token, pk_token, version_token, deleted, row_hash, '
        'updated_at FROM %s WHERE target_table = ? '
        'ORDER BY updated_at DESC, key_token LIMIT ? OFFSET ?'
        % quote_ident(SYNC_STATE_TABLE),
        (target_table, page_size, offset))
    cols = [d[0] for d in cursor.description]
    rows = []
    for raw in cursor.fetchall():
        item = dict(zip(cols, raw))
        item['key_values'] = key_decode(item['key_token'])
        item['version_value'] = key_decode(item['version_token'])[0]
        rows.append(item)
    return rows, total


def recent_runs(dataset, target_table, limit=10):
    ensure_sync_tables(dataset)
    cursor = dataset.query(
        'SELECT id, started_at, dry_run, status, message, stage_rows, '
        'applied, replayed, expired FROM %s WHERE target_table = ? '
        'ORDER BY id DESC LIMIT ?'
        % quote_ident(SYNC_RUNS_TABLE), (target_table, limit))
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]
