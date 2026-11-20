"""
零依赖夹具运行器（标准库 unittest）：在无法安装 pytest 的环境里同样可核证。
与 tests/test_cutover_fixtures.py 的夹具一一对应，全部使用本地 SQLite。

运行：python3 tests/fixtures_runner.py
"""

import os
import sqlite3
import sys
import tempfile
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

import cutover as cutmod  # noqa: E402
from cutover import (connect, initialize, cutover, close_old_windows, remap,
                     certification, CutoverError, ATOMIC, DUAL)  # noqa: E402

T0 = "2026-11-10T00:00:00+00:00"
TCUT = "2026-11-15T00:00:00+00:00"


def seed(conn, v1="m-2026.11", v2="m-2026.12",
         v1_from="2026-11-01T00:00:00+00:00"):
    conn.executescript("""
    CREATE TABLE map_versions(id INTEGER PRIMARY KEY AUTOINCREMENT,
      version TEXT UNIQUE NOT NULL, status TEXT NOT NULL, effective_from TEXT NOT NULL);
    CREATE TABLE mappings(id INTEGER PRIMARY KEY AUTOINCREMENT,
      version TEXT NOT NULL, old_key TEXT NOT NULL, new_key TEXT NOT NULL, kind TEXT NOT NULL);
    CREATE TABLE conflicts(id INTEGER PRIMARY KEY AUTOINCREMENT,
      old_key TEXT NOT NULL, reason TEXT NOT NULL, status TEXT NOT NULL);
    CREATE TABLE formal_entities(id INTEGER PRIMARY KEY AUTOINCREMENT,
      entity_key TEXT NOT NULL, mapped_from TEXT NOT NULL, version TEXT NOT NULL, created_at TEXT NOT NULL);
    """)
    conn.execute("INSERT INTO map_versions VALUES (NULL,?,?,?)", (v1, "生效", v1_from))
    conn.execute("INSERT INTO map_versions VALUES (NULL,?,?,?)", (v2, "待切版", "2026-11-20T00:00:00+00:00"))
    conn.executemany("INSERT INTO mappings VALUES (NULL,?,?,?,?)", [
        (v1, "OLD-A1", "NEW-100", "一对一"),
        (v1, "OLD-B1", "NEW-200", "多对一"),
        (v2, "OLD-A1", "NEW-900", "一对一"),
        (v2, "OLD-B1", "NEW-200", "多对一"),
    ])
    conn.commit()


class FixtureBase(unittest.TestCase):
    def fresh(self, mode, max_overlap=3600, *, old_from="2026-11-01T00:00:00+00:00"):
        d = tempfile.mkdtemp()
        db = Path(d) / f"cut_{mode}.db"
        c = connect(db); seed(c, v1_from=old_from); c.close()
        initialize(db, mode, max_overlap_seconds=max_overlap)
        return db


class AtomicFixtures(FixtureBase):
    def test_cut_after_only_new_version_remappable(self):
        db = self.fresh(ATOMIC)
        with connect(db) as c:
            pre = remap(c, "OLD-A1", T0)
            self.assertEqual((pre["version"], pre["entity_key"]), ("m-2026.11", "NEW-100"))
            info = cutover(c, "m-2026.12", effective_at=TCUT)
            self.assertTrue(info["closed_window_ids"])
            opens = c.execute("SELECT * FROM effective_windows WHERE effective_to IS NULL").fetchall()
            self.assertEqual(len(opens), 1)
            self.assertEqual(opens[0]["version"], "m-2026.12")
            old = c.execute("SELECT * FROM effective_windows WHERE version='m-2026.11'").fetchone()
            new = c.execute("SELECT * FROM effective_windows WHERE version='m-2026.12'").fetchone()
            self.assertEqual(old["effective_to"], new["effective_from"])
            post = remap(c, "OLD-A1", "2026-11-16T00:00:00+00:00")
            self.assertEqual((post["version"], post["entity_key"]), ("m-2026.12", "NEW-900"))
            stale = c.execute(
                "SELECT COUNT(*) c FROM formal_entities_v2 WHERE version='m-2026.11' AND created_at>=?",
                (TCUT,)).fetchone()["c"]
            self.assertEqual(stale, 0)
            self.assertTrue(certification(c)["checks"]["all_green"])

    def test_cannot_keep_old_window_open(self):
        db = self.fresh(ATOMIC)
        with connect(db) as c:
            with self.assertRaisesRegex(CutoverError, "禁止保留旧窗"):
                cutover(c, "m-2026.12", effective_at=TCUT, close_old=False)
            self.assertEqual(c.execute("SELECT COUNT(*) c FROM effective_windows").fetchone()["c"], 1)
            self.assertEqual(c.execute("SELECT COUNT(*) c FROM cutover_ledger").fetchone()["c"], 0)

    def test_failed_cut_rolls_back_both_states(self):
        db = self.fresh(ATOMIC)
        with connect(db) as c:
            with self.assertRaises(CutoverError):
                cutover(c, "m-4040.01", effective_at=TCUT)
            old = c.execute("SELECT * FROM effective_windows WHERE version='m-2026.11'").fetchone()
            self.assertIsNone(old["effective_to"])
            self.assertEqual(c.execute("SELECT COUNT(*) c FROM effective_windows").fetchone()["c"], 1)
            self.assertEqual(remap(c, "OLD-A1", T0)["version"], "m-2026.11")


class DualFixtures(FixtureBase):
    # 双窗夹具：旧窗在切版前 30 分钟开窗，切版后进入 30 分钟有界重叠
    OLD_FROM = "2026-11-14T23:30:00+00:00"

    def test_event_time_falls_into_unique_window_with_snapshot(self):
        db = self.fresh(DUAL, old_from=self.OLD_FROM)
        with connect(db) as c:
            info = cutover(c, "m-2026.12", effective_at=TCUT, close_old=False)
            # 重叠被物化：旧窗拿到计划收口时刻（at+cap），当前只新版开窗
            self.assertEqual(c.execute(
                "SELECT COUNT(*) c FROM effective_windows WHERE effective_to IS NULL").fetchone()["c"], 1)
            old = c.execute("SELECT * FROM effective_windows WHERE version='m-2026.11'").fetchone()
            self.assertIsNotNone(old["effective_to"])
            self.assertEqual(info["overlap_seconds"], 3600)
            # 但在重叠区间的事件时刻，两窗都可被命中（按时刻裁决）
            self.assertEqual(len(cutmod._open_windows(c, TCUT)), 2)
            before = remap(c, "OLD-A1", "2026-11-14T23:59:59+00:00")
            after = remap(c, "OLD-A1", TCUT)
            self.assertEqual(before["entity_key"], "NEW-100")
            self.assertEqual(after["entity_key"], "NEW-900")
            rows = c.execute("SELECT * FROM gate_decisions WHERE decision='accept'").fetchall()
            self.assertTrue(all(r["window_hit"] for r in rows))
            self.assertEqual({r["hit_version"] for r in rows}, {"m-2026.11", "m-2026.12"})
            cert = certification(c)
            self.assertTrue(cert["checks"]["no_unbounded_dual_active"])
            # 历史重叠区间被核证为「有界重叠」
            self.assertTrue(any("有界重叠" in x["detail"] for x in cert["continuity"]))

    def test_rejected_event_zero_formal_growth(self):
        db = self.fresh(DUAL, old_from=self.OLD_FROM)
        with connect(db) as c:
            cutover(c, "m-2026.12", effective_at=TCUT, close_old=False)
            n0 = c.execute("SELECT COUNT(*) c FROM formal_entities_v2").fetchone()["c"]
            with self.assertRaises(CutoverError):
                remap(c, "OLD-GHOST", TCUT)
            self.assertEqual(c.execute("SELECT COUNT(*) c FROM formal_entities_v2").fetchone()["c"], n0)
            d = c.execute("SELECT * FROM gate_decisions WHERE decision='reject'").fetchone()
            self.assertIsNone(d["formal_id"]); self.assertIsNone(d["new_key"])
            close_old_windows(c, "2026-11-18T00:00:00+00:00")
            # 把新窗也收口后，2027 年的事件时刻无任何窗可落 => 拒收且正式零增
            c.execute(
                "UPDATE effective_windows SET effective_to=? WHERE version='m-2026.12'",
                ("2026-11-19T00:00:00+00:00",)); c.commit()
            with self.assertRaises(CutoverError):
                remap(c, "OLD-A1", "2027-01-01T00:00:00+00:00")
            self.assertEqual(c.execute("SELECT COUNT(*) c FROM formal_entities_v2").fetchone()["c"], n0)

    def test_unbounded_overlap_red_light_and_blocks(self):
        # 直接构造缺收口时刻的并存开窗（无界双生效）=> 核证红灯且再切被拒
        db = self.fresh(DUAL, max_overlap=60, old_from=self.OLD_FROM)
        with connect(db) as c:
            cutover(c, "m-2026.12", effective_at=TCUT, close_old=False)
            # 再登记一版并手工插入无收口点的开窗，模拟无边界并存
            # （正常切版入口不可能产生这种行，只有绕过闸口才会出现）
            c.execute(
                "INSERT INTO map_versions(version,status,effective_from) VALUES (?,?,?)",
                ("m-2027.01", "待切版", "2026-11-15T00:30:00+00:00"))
            c.execute(
                "INSERT INTO effective_windows(version,effective_from,effective_to,"
                "cut_id,opened_event) VALUES (?,?,?,?,?)",
                ("m-2027.01", "2026-11-15T00:30:00+00:00", None, None, "seed"))
            c.commit()
            cert = certification(c)
            self.assertFalse(cert["checks"]["no_unbounded_dual_active"])
            self.assertFalse(cert["checks"]["all_green"])
            self.assertTrue(cert["overlap_violations"])
            # 红灯状态下走正规入口再切一个未开窗的新版本，必须先被无界检查拦下
            c.execute(
                "INSERT INTO map_versions(version,status,effective_from) VALUES (?,?,?)",
                ("m-2027.02", "待切版", "2027-02-01T00:00:00+00:00")); c.commit()
            with self.assertRaisesRegex(CutoverError, "无边界"):
                cutover(c, "m-2027.02", effective_at="2026-11-15T01:00:00+00:00", close_old=False)

    def test_tied_windows_reject_not_guess(self):
        db = self.fresh(DUAL, old_from=self.OLD_FROM)
        with connect(db) as c:
            cutover(c, "m-2026.12", effective_at=TCUT, close_old=False)
            c.execute("UPDATE effective_windows SET effective_from=? WHERE version='m-2026.11'",
                      (TCUT,)); c.commit()
            with self.assertRaisesRegex(CutoverError, "并列生效窗"):
                remap(c, "OLD-A1", TCUT)
            self.assertEqual(c.execute("SELECT COUNT(*) c FROM formal_entities_v2").fetchone()["c"], 0)


class ConcurrencyFixtures(FixtureBase):
    def _double_cut(self, mode, close_old):
        db = self.fresh(mode)
        errors = []

        def worker():
            try:
                c = connect(db)
                try:
                    cutover(c, "m-2026.12", effective_at=TCUT, close_old=close_old)
                finally:
                    c.close()
            except Exception as e:
                errors.append(e)

        ts = [threading.Thread(target=worker) for _ in range(2)]
        for t in ts: t.start()
        for t in ts: t.join()
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], CutoverError)
        with connect(db) as c:
            self.assertEqual(c.execute(
                "SELECT COUNT(*) c FROM effective_windows WHERE version='m-2026.12'").fetchone()["c"], 1)
            self.assertEqual(c.execute(
                "SELECT COUNT(*) c FROM cutover_ledger WHERE result='committed'").fetchone()["c"], 1)

    def test_atomic_double_cut_one_winner(self):
        self._double_cut(ATOMIC, True)

    def test_dual_double_cut_one_winner(self):
        self._double_cut(DUAL, False)


class FreezeAndCrossValidation(FixtureBase):
    def test_mode_frozen_no_mix(self):
        db = self.fresh(ATOMIC)
        with self.assertRaisesRegex(CutoverError, "禁止混"):
            initialize(db, DUAL)
        with connect(db) as c:
            with self.assertRaises(sqlite3.IntegrityError):
                c.execute("UPDATE cutover_config SET mode='dual'")
            c.rollback()
            with self.assertRaises(sqlite3.IntegrityError):
                c.execute("DELETE FROM cutover_config")
            c.rollback()
            cutover(c, "m-2026.12", effective_at=TCUT)
            with self.assertRaises(sqlite3.IntegrityError):
                c.execute("UPDATE effective_windows SET effective_to=NULL WHERE version='m-2026.11'")
            c.rollback()

    def test_cross_validation_card(self):
        db = self.fresh(ATOMIC)
        with connect(db) as c:
            remap(c, "OLD-A1", T0)
            cutover(c, "m-2026.12", effective_at=TCUT)
            remap(c, "OLD-A1", "2026-11-16T00:00:00+00:00")
            cert = certification(c)
            # 窗内无静默分叉
            self.assertEqual(cert["cross_validation_card"], [])
            # 跨边界换键被单独留痕，可逐窗追溯
            change = [x for x in cert["boundary_changes"] if x["mapped_from"] == "OLD-A1"]
            self.assertEqual(change[0]["distinct_new_keys"], 2)
            self.assertTrue(cert["checks"]["cross_validation_ok"])
            self.assertTrue(cert["checks"]["every_formal_traceable_to_window"])
            self.assertTrue(cert["checks"]["boundary_continuity_ok"])
            self.assertTrue(cert["checks"]["all_green"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
