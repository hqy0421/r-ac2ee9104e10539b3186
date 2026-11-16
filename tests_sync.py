import os
import shutil
import sqlite3
import tempfile
import unittest

from peewee import SqliteDatabase
from playhouse.dataset import DataSet

from sqlite_web import sqlite_web as sw
from sqlite_web import sync as sync_module
from sqlite_web.executor import key_encode


SCHEMA = """
CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    biz_key INTEGER UNIQUE,
    username TEXT,
    note TEXT,
    rev INTEGER);
INSERT INTO users (biz_key, username, note, rev)
    VALUES (1, 'huey', 'orig', 100);
CREATE TABLE stage (
    k INTEGER, ver INTEGER, op TEXT, username TEXT, note TEXT);
CREATE TABLE parent (pid INTEGER PRIMARY KEY);
INSERT INTO parent VALUES (1);
CREATE TABLE child (
    cid INTEGER PRIMARY KEY AUTOINCREMENT,
    bkey INTEGER UNIQUE,
    pid INTEGER REFERENCES parent(pid),
    label TEXT);
CREATE TABLE cstage (
    k INTEGER, ver INTEGER, op TEXT, pid INTEGER, label TEXT);
"""


class BaseSyncTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmp, 'app.db')
        conn = sqlite3.connect(self.db_path)
        conn.executescript(SCHEMA)
        conn.commit()
        conn.close()
        sw.datasets.clear()
        sw.initialize_app([self.db_path], foreign_keys=True)
        sw.app.config['TESTING'] = True
        self.client = sw.app.test_client()
        self.dataset = sw.datasets[os.path.realpath(self.db_path)]

    def tearDown(self):
        sw.dataset_config['foreign_keys'] = False
        shutil.rmtree(self.tmp, ignore_errors=True)

    # -- helpers -----------------------------------------------------------
    def make_config(self):
        return sync_module.SyncConfig(
            target_table='users', stage_table='stage',
            key_columns=['k'], target_key_columns=['biz_key'],
            version_column='ver', flag_column='op',
            insert_value='I', update_value='U', delete_value='D',
            mapping={'username': 'username', 'note': 'note'})

    def close_db(self):
        # Direct engine calls open a connection; the HTTP layer opens its own
        # per request, so close before mixing the two styles.
        if not self.dataset._database.is_closed():
            self.dataset.close()

    def run_engine(self, fn, *args, **kwargs):
        try:
            with self.dataset.transaction():
                return fn(*args, **kwargs)
        finally:
            self.close_db()
    def engine(self, fn, *args, **kwargs):
        """Call an engine helper directly, closing its connection afterwards
        so subsequent HTTP requests can open their own."""
        try:
            return fn(*args, **kwargs)
        finally:
            self.close_db()

    def db(self, sql, params=()):
        conn = sqlite3.connect(self.db_path)
        rows = conn.execute(sql, params).fetchall()
        conn.close()
        return rows

    def stage_rows(self, rows):
        conn = sqlite3.connect(self.db_path)
        conn.execute('DELETE FROM stage')
        conn.executemany('INSERT INTO stage VALUES (?, ?, ?, ?, ?)', rows)
        conn.commit()
        conn.close()

    def config(self, action='sync', **overrides):
        data = {
            'action': action,
            'stage_table': 'stage',
            'version_column': 'ver',
            'flag_column': 'op',
            'insert_value': 'I',
            'update_value': 'U',
            'delete_value': 'D',
            'key_columns': ['k'],
            'key_target_k': 'biz_key',
            'map_target': ['id', 'biz_key', 'username', 'note', 'rev'],
            'map_username': 'username',
            'map_note': 'note',
        }
        data.update(overrides)
        return data

    def sync(self, **overrides):
        return self.client.post('/users/sync/',
                                data=self.config(**overrides))

    def users(self):
        return self.db('SELECT biz_key, username, note, rev FROM users '
                       'ORDER BY biz_key')


class TestSyncEngine(BaseSyncTestCase):
    def test_basic_insert_update_and_state(self):
        self.stage_rows([(2, 1, 'I', 'mickey', 'n1')])
        result = self.run_engine(sync_module.plan_and_apply,
                                 self.dataset, self.make_config())
        self.assertEqual(result['applied'], 1)
        self.assertEqual(self.users(),
                         [(1, 'huey', 'orig', 100),
                          (2, 'mickey', 'n1', None)])
        summary = sync_module.state_summary(self.dataset, 'users')
        self.assertEqual(summary, {'deleted': 0, 'live': 1, 'total': 1})

    def test_baseline_row_adopted_with_primary_key_preserved(self):
        # Key 1 already exists in the target; sync adopts it in place.
        self.stage_rows([(1, 2, 'U', 'huey2', 'n2')])
        self.run_engine(sync_module.plan_and_apply, self.dataset,
                        self.make_config())
        self.assertEqual(self.db('SELECT id FROM users WHERE biz_key=1'),
                         [(1,)])
        self.assertEqual(self.users(), [(1, 'huey2', 'n2', 100)])

    def test_order_independent_max_version_wins(self):
        # Lower version arrives after the higher one in row order.
        self.stage_rows([
            (3, 10, 'I', 'ten', 'x'),
            (3, 5, 'U', 'five', 'x'),
        ])
        result = self.run_engine(sync_module.plan_and_apply, self.dataset,
                                 self.make_config())
        self.assertEqual(
            self.db("SELECT username FROM users WHERE biz_key=3"),
            [('ten',)])
        self.assertEqual(result['applied'], 1)

    def test_batch_arrival_order_independent(self):
        # v2 then v1: the stale batch must not downgrade the row.
        self.stage_rows([(4, 2, 'I', 'v2', 'a')])
        self.run_engine(sync_module.plan_and_apply, self.dataset,
                        self.make_config())
        self.stage_rows([(4, 1, 'U', 'v1', 'b')])
        result = self.run_engine(sync_module.plan_and_apply, self.dataset,
                                 self.make_config())
        self.assertEqual(result['expired'], 1)
        self.assertEqual(
            self.db("SELECT username, note FROM users WHERE biz_key=4"),
            [('v2', 'a')])

    def test_rerun_replays_without_rewriting(self):
        self.stage_rows([(2, 1, 'I', 'mickey', 'n')])
        cfg = self.make_config()
        first = self.run_engine(sync_module.plan_and_apply, self.dataset, cfg)
        before = self.users()
        second = self.run_engine(sync_module.plan_and_apply, self.dataset, cfg)
        self.assertEqual(first['applied'], 1)
        self.assertEqual(second['replayed'], 1)
        self.assertEqual(second['applied'], 0)
        self.assertEqual(self.users(), before)

    def test_same_version_content_conflict_raises(self):
        self.stage_rows([(5, 1, 'I', 'first', 'a')])
        cfg = self.make_config()
        self.run_engine(sync_module.plan_and_apply, self.dataset, cfg)
        self.stage_rows([(5, 1, 'U', 'DIFFERENT', 'a')])
        with self.assertRaises(sync_module.SyncError):
            self.run_engine(sync_module.plan_and_apply, self.dataset, cfg)
        # The conflict transaction left the row untouched.
        self.assertEqual(
            self.db("SELECT username FROM users WHERE biz_key=5"),
            [('first',)])

    def test_duplicate_latest_version_in_stage_is_conflict(self):
        self.stage_rows([
            (6, 1, 'I', 'a', 'x'),
            (6, 1, 'I', 'b', 'x'),
        ])
        with self.assertRaises(sync_module.SyncError):
            self.run_engine(sync_module.plan_and_apply, self.dataset,
                            self.make_config())

    def test_null_overwrites_old_value(self):
        self.stage_rows([(1, 2, 'U', 'huey2', None)])
        self.run_engine(sync_module.plan_and_apply, self.dataset,
                        self.make_config())
        self.assertEqual(
            self.db('SELECT note FROM users WHERE biz_key=1'), [(None,)])
        # Unmapped column untouched.
        self.assertEqual(
            self.db('SELECT rev FROM users WHERE biz_key=1'), [(100,)])

    def test_delete_tombstones_and_blocks_old_update(self):
        self.stage_rows([(1, 2, 'D', None, None)])
        self.run_engine(sync_module.plan_and_apply, self.dataset,
                        self.make_config())
        self.assertEqual(self.db('SELECT COUNT(*) FROM users WHERE biz_key=1'),
                         [(0,)])
        # An older update can never resurrect.
        self.stage_rows([(1, 1, 'U', 'zombie', 'z')])
        result = self.run_engine(sync_module.plan_and_apply, self.dataset,
                                 self.make_config())
        self.assertEqual(result['expired'], 1)
        self.assertEqual(self.db('SELECT COUNT(*) FROM users WHERE biz_key=1'),
                         [(0,)])

    def test_delete_of_unknown_key_tombstones_it(self):
        # The key exists nowhere, yet the delete is its latest word. The
        # tombstone must still be recorded at the delete's version.
        self.stage_rows([(99, 5, 'D', None, None)])
        result = self.run_engine(sync_module.plan_and_apply, self.dataset,
                                 self.make_config())
        self.assertEqual(result['applied'], 1)
        summary = sync_module.state_summary(self.dataset, 'users')
        self.assertEqual(summary['deleted'], 1)

        # An equal-version insert cannot resurrect.
        self.stage_rows([(99, 5, 'I', 'ghost', 'g')])
        with self.assertRaises(sync_module.SyncError):
            self.run_engine(sync_module.plan_and_apply, self.dataset,
                            self.make_config())
        # An older update is simply stale.
        self.stage_rows([(99, 4, 'U', 'ghost', 'g')])
        result = self.run_engine(sync_module.plan_and_apply, self.dataset,
                                 self.make_config())
        self.assertEqual(result['expired'], 1)
        self.assertEqual(
            self.db('SELECT COUNT(*) FROM users WHERE biz_key=99'), [(0,)])
        # A newer version can legitimately re-create the row.
        self.stage_rows([(99, 6, 'I', 'revived', 'r')])
        result = self.run_engine(sync_module.plan_and_apply, self.dataset,
                                 self.make_config())
        self.assertEqual(result['applied'], 1)
        self.assertEqual(
            self.db('SELECT username FROM users WHERE biz_key=99'),
            [('revived',)])

    def test_newer_version_after_delete_reinserts(self):
        self.stage_rows([(1, 2, 'D', None, None)])
        self.run_engine(sync_module.plan_and_apply, self.dataset,
                        self.make_config())
        self.stage_rows([(1, 3, 'I', 'reborn', 'r')])
        result = self.run_engine(sync_module.plan_and_apply, self.dataset,
                                 self.make_config())
        self.assertEqual(result['applied'], 1)
        self.assertEqual(
            self.db('SELECT username FROM users WHERE biz_key=1'),
            [('reborn',)])

    def test_dry_run_changes_nothing(self):
        self.stage_rows([(7, 1, 'I', 'seven', 's')])
        try:
            with self.dataset.transaction() as txn:
                result = sync_module.plan_and_apply(
                    self.dataset, self.make_config(), dry_run=True)
                txn.rollback()
        finally:
            self.close_db()
        self.assertEqual(result['applied'], 1)
        self.assertEqual(
            self.db('SELECT COUNT(*) FROM users WHERE biz_key=7'), [(0,)])
        # No run row is recorded for a rolled-back dry run.
        self.assertEqual(sync_module.recent_runs(self.dataset, 'users'), [])

    def test_unknown_marker_raises(self):
        self.stage_rows([(8, 1, 'X', 'x', 'x')])
        with self.assertRaises(sync_module.SyncError):
            self.run_engine(sync_module.plan_and_apply, self.dataset,
                            self.make_config())

    def test_drift_on_external_edit_blocks_sync(self):
        self.stage_rows([(1, 2, 'U', 'huey2', 'n2')])
        self.run_engine(sync_module.plan_and_apply, self.dataset,
                        self.make_config())
        # Edit outside the sync machinery.
        conn = sqlite3.connect(self.db_path)
        conn.execute("UPDATE users SET note='hacked' WHERE biz_key=1")
        conn.commit()
        conn.close()
        self.stage_rows([(1, 3, 'U', 'huey3', 'n3')])
        with self.assertRaises(sync_module.SyncError):
            self.run_engine(sync_module.plan_and_apply, self.dataset,
                            self.make_config())

    def test_drift_on_external_delete_blocks_sync(self):
        self.stage_rows([(1, 2, 'U', 'huey2', 'n2')])
        self.run_engine(sync_module.plan_and_apply, self.dataset,
                        self.make_config())
        conn = sqlite3.connect(self.db_path)
        conn.execute('DELETE FROM users WHERE biz_key=1')
        conn.commit()
        conn.close()
        self.stage_rows([(1, 3, 'U', 'huey3', 'n3')])
        with self.assertRaises(sync_module.SyncError):
            self.run_engine(sync_module.plan_and_apply, self.dataset,
                            self.make_config())

    def test_rebaseline_clears_drift(self):
        self.stage_rows([(1, 2, 'U', 'huey2', 'n2')])
        self.run_engine(sync_module.plan_and_apply, self.dataset,
                        self.make_config())
        conn = sqlite3.connect(self.db_path)
        conn.execute("UPDATE users SET note='hacked' WHERE biz_key=1")
        conn.commit()
        conn.close()
        self.run_engine(sync_module.rebaseline, self.dataset, 'users')
        self.stage_rows([(1, 3, 'U', 'huey3', 'n3')])
        result = self.run_engine(sync_module.plan_and_apply, self.dataset,
                                 self.make_config())
        self.assertEqual(result['applied'], 1)
        self.assertEqual(
            self.db('SELECT note FROM users WHERE biz_key=1'), [('n3',)])

    def test_rebaseline_drops_vanished_rows_keeps_tombstones(self):
        self.stage_rows([(1, 2, 'D', None, None),
                         (2, 1, 'I', 'mickey', 'm')])
        self.run_engine(sync_module.plan_and_apply, self.dataset,
                        self.make_config())
        conn = sqlite3.connect(self.db_path)
        conn.execute('DELETE FROM users WHERE biz_key=2')
        conn.commit()
        conn.close()
        result = self.run_engine(sync_module.rebaseline, self.dataset,
                                 'users')
        self.assertEqual(result['removed'], 1)
        summary = sync_module.state_summary(self.dataset, 'users')
        self.assertEqual(summary['total'], 1)
        self.assertEqual(summary['deleted'], 1)

    def test_target_key_must_be_unique(self):
        # username is not unique, so two identical values make it unusable
        # as a target business key.
        conn = sqlite3.connect(self.db_path)
        conn.execute("INSERT INTO users (biz_key, username) VALUES (8, 'dup')")
        conn.execute("INSERT INTO users (biz_key, username) VALUES (9, 'dup')")
        conn.commit()
        conn.close()
        cfg = self.make_config()._replace(target_key_columns=['username'])
        with self.assertRaises(sync_module.SyncError):
            sync_module.validate_config(self.dataset, cfg)

    def test_key_cannot_be_a_mapping_column(self):
        cfg = self.make_config()
        cfg = cfg._replace(mapping={'biz_key': 'k',
                                    'username': 'username'})
        with self.assertRaises(sync_module.SyncError):
            sync_module.validate_config(self.dataset, cfg)


class TestSyncRoutes(BaseSyncTestCase):
    def test_sync_tab_and_page_render(self):
        r = self.client.get('/users/sync/')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'Synchronize from a staging table', r.data)
        # The tab is present on ordinary table pages.
        self.assertIn(b'/users/sync/',
                      self.client.get('/users/content/').data)

    def test_internal_tables_hidden(self):
        # Create bookkeeping tables by saving a config.
        r = self.client.post('/users/sync/', data=self.config('save'))
        self.assertIn(r.status_code, (302, 303))
        page = self.client.get('/').data
        self.assertNotIn(b'_sw_sync_config', page)
        self.assertNotIn(b'_sw_sync_state', page)
        self.assertNotIn(b'_sw_sync_runs', page)
        sidebar = self.client.get('/users/content/').data
        self.assertNotIn(b'_sw_sync_state', sidebar)

    def test_save_config_persists(self):
        r = self.client.post('/users/sync/', data=self.config('save'))
        self.assertIn(r.status_code, (302, 303))
        page = self.client.get('/users/sync/').data
        self.assertIn(b'Stored configuration', page)
        self.assertIn(b'stage', page)

    def test_full_sync_workflow_through_http(self):
        self.stage_rows([
            (1, 2, 'U', 'huey2', 'n2'),
            (2, 1, 'I', 'mickey', 'm'),
        ])
        r = self.sync()
        self.assertIn(b'Sync applied 2', r.data)
        self.assertEqual(
            self.users(),
            [(1, 'huey2', 'n2', 100), (2, 'mickey', 'm', None)])
        # Run history and state render.
        page = self.client.get('/users/sync/').data
        self.assertIn(b'Run history', page)
        self.assertIn(b'tombstoned', page)
        self.assertIn(b'live', page)

    def test_conflict_flashes_and_rolls_back(self):
        self.stage_rows([(5, 1, 'I', 'first', 'a')])
        self.sync()
        self.stage_rows([(5, 1, 'U', 'DIFFERENT', 'a')])
        r = self.sync()
        self.assertIn(b'Sync stopped', r.data)
        self.assertEqual(
            self.db("SELECT username FROM users WHERE biz_key=5"),
            [('first',)])

    def test_dry_run_does_not_write(self):
        before = self.db('SELECT COUNT(*) FROM users')[0][0]
        self.stage_rows([(7, 1, 'I', 'seven', 's')])
        r = self.sync(action='dry-run')
        self.assertIn(b'would insert', r.data)
        self.assertEqual(self.db('SELECT COUNT(*) FROM users')[0][0], before)

    def test_foreign_key_failure_rolls_back_whole_batch(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute('DELETE FROM cstage')
        conn.execute("INSERT INTO cstage VALUES "
                     "(1, 1, 'I', 1, 'ok'), "
                     "(2, 1, 'I', 999, 'bad')")
        conn.commit()
        conn.close()
        data = {
            'action': 'sync', 'stage_table': 'cstage',
            'version_column': 'ver', 'flag_column': 'op',
            'insert_value': 'I', 'update_value': 'U', 'delete_value': 'D',
            'key_columns': ['k'], 'key_target_k': 'bkey',
            'map_target': ['cid', 'bkey', 'pid', 'label'],
            'map_pid': 'pid', 'map_label': 'label',
        }
        r = self.client.post('/child/sync/', data=data)
        self.assertIn(b'rolled back', r.data)
        # Even the valid first row was rolled back.
        self.assertEqual(self.db('SELECT COUNT(*) FROM child'), [(0,)])

    def test_rebaseline_route(self):
        self.stage_rows([(1, 2, 'U', 'huey2', 'n2')])
        self.sync()
        conn = sqlite3.connect(self.db_path)
        conn.execute("UPDATE users SET note='hacked' WHERE biz_key=1")
        conn.commit()
        conn.close()
        r = self.client.post('/users/sync/',
                             data={'action': 'rebaseline'},
                             follow_redirects=True)
        self.assertIn(b'Rebaselined', r.data)
        self.stage_rows([(1, 3, 'U', 'huey3', 'n3')])
        r = self.sync()
        self.assertIn(b'Sync applied 1', r.data)

    def test_delete_config_route(self):
        self.client.post('/users/sync/', data=self.config('save'))
        r = self.client.post('/users/sync/',
                             data={'action': 'delete-config'},
                             follow_redirects=True)
        self.assertIn(b'removed', r.data)
        self.assertIsNone(
            self.engine(sync_module.get_config, self.dataset, 'users'))
        # Run history retained: run a sync and confirm a run row exists.
        self.stage_rows([(2, 1, 'I', 'x', 'x')])
        self.sync()
        runs = self.engine(sync_module.recent_runs, self.dataset, 'users')
        self.assertTrue(runs)
        # The state table was repopulated only for the new run; config is
        # back because a sync persists it, but deleting again clears state.
        self.client.post('/users/sync/',
                         data={'action': 'delete-config'})
        self.assertEqual(
            self.engine(sync_module.state_summary, self.dataset,
                        'users')['total'], 0)

    def test_config_survives_dataset_reopen(self):
        self.client.post('/users/sync/', data=self.config('save'))
        sw.datasets.clear()
        sw.initialize_app([self.db_path])
        dataset = sw.datasets[os.path.realpath(self.db_path)]
        cfg = sync_module.get_config(dataset, 'users')
        self.assertIsNotNone(cfg)
        self.assertEqual(cfg.stage_table, 'stage')
        self.assertEqual(cfg.target_key_columns, ['biz_key'])

    def test_cross_database_isolation(self):
        # A second database must never see the first one's config/state.
        db2 = os.path.join(self.tmp, 'two.db')
        conn = sqlite3.connect(db2)
        conn.executescript(
            'CREATE TABLE users (id INTEGER PRIMARY KEY, biz_key INTEGER '
            'UNIQUE, username TEXT, note TEXT, rev INTEGER);'
            'CREATE TABLE stage (k INTEGER, ver INTEGER, op TEXT, '
            'username TEXT, note TEXT);')
        conn.commit()
        conn.close()
        self.stage_rows([(2, 1, 'I', 'mickey', 'm')])
        self.sync()
        sw.datasets.clear()
        sw.initialize_app([self.db_path, db2])
        second = sw.datasets[os.path.realpath(db2)]
        self.assertIsNone(sync_module.get_config(second, 'users'))
        self.assertEqual(sync_module.state_summary(second, 'users')['total'],
                         0)

    def test_read_only_disables_sync(self):
        sw.datasets.clear()
        sw.initialize_app([self.db_path], read_only=True)
        try:
            client = sw.app.test_client()
            r = client.post('/users/sync/', data=self.config('save'))
            self.assertIn(r.status_code, (302, 303))
            # Nothing was written in read-only mode.
            conn = sqlite3.connect(self.db_path)
            names = [row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")]
            conn.close()
            self.assertNotIn(sync_module.SYNC_CONFIG_TABLE, names)
        finally:
            sw.datasets.clear()
            sw.initialize_app([self.db_path])
            self.dataset = sw.datasets[os.path.realpath(self.db_path)]

    def test_sync_not_offered_for_views(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute('CREATE VIEW v_users AS SELECT * FROM users')
        conn.commit()
        conn.close()
        # Fresh process-wide dataset so the view is reflected.
        sw.datasets.clear()
        sw.initialize_app([self.db_path], foreign_keys=True)
        try:
            client = sw.app.test_client()
            r = client.get('/v_users/sync/')
            self.assertIn(r.status_code, (302, 303))
        finally:
            sw.datasets.clear()
            sw.initialize_app([self.db_path], foreign_keys=True)
            self.dataset = sw.datasets[os.path.realpath(self.db_path)]

    def test_stale_stage_table_validation(self):
        self.client.post('/users/sync/', data=self.config('save'))
        conn = sqlite3.connect(self.db_path)
        conn.execute('DROP TABLE stage')
        conn.commit()
        conn.close()
        sw.datasets.clear()
        sw.initialize_app([self.db_path], foreign_keys=True)
        try:
            client = sw.app.test_client()
            r = client.post('/users/sync/', data=self.config())
            self.assertIn(b'Sync stopped', r.data)
            self.assertIn(b'no longer exists', r.data)
        finally:
            sw.datasets.clear()
            sw.initialize_app([self.db_path], foreign_keys=True)
            self.dataset = sw.datasets[os.path.realpath(self.db_path)]

    def test_stage_table_switch_reloads_columns(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute('CREATE TABLE other_stage (kk INTEGER, vv INTEGER, '
                     'flag TEXT, name TEXT)')
        conn.commit()
        conn.close()
        r = self.client.get('/users/sync/?stage_table=other_stage')
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'other_stage', r.data)
        self.assertIn(b'name="key_target_kk"', r.data)

    def test_version_type_mismatch_raises(self):
        self.stage_rows([(1, 2, 'U', 'huey2', 'n2')])
        self.run_engine(sync_module.plan_and_apply, self.dataset,
                        self.make_config())
        # Text version against an already-applied integer version.
        self.stage_rows([(1, 'three', 'U', 'huey3', 'n3')])
        with self.assertRaises(sync_module.SyncError):
            self.run_engine(sync_module.plan_and_apply, self.dataset,
                            self.make_config())


if __name__ == '__main__':
    unittest.main()
