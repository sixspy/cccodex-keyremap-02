"""
本地 SQLite 夹具（pytest 版，与 tests/fixtures_runner.py 同一套事实）：

  1. 原子架构（甲）切版后仅新版可映，旧版零命中进正式；失败两版状态皆回滚
  2. 双窗架构（乙）按事件时刻落入唯一窗，window_hit 快照可查，失败笔正式零增
  3. 并发双切同一目标版，BEGIN IMMEDIATE 写锁串行化后只留争用，无无锁双写
  4. 切版边界核证只读接口全绿 / 故意构造违例时亮红灯
无 pytest 环境时可运行：python3 tests/fixtures_runner.py
"""

import sqlite3
import threading

import pytest

import cutover as cutmod
from cutover import (connect, initialize, cutover, close_old_windows, remap,
                     certification, CutoverError, ATOMIC, DUAL)

T0 = "2026-11-10T00:00:00+00:00"
TCUT = "2026-11-15T00:00:00+00:00"
OLD_FROM_DUAL = "2026-11-14T23:30:00+00:00"  # 切版前 30 分钟开窗，构成有界短重叠


def _seed_map_versions(conn, old_from="2026-11-01T00:00:00+00:00",
                       v1="m-2026.11", v2="m-2026.12"):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS map_versions(
      id INTEGER PRIMARY KEY AUTOINCREMENT, version TEXT UNIQUE NOT NULL,
      status TEXT NOT NULL, effective_from TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS mappings(
      id INTEGER PRIMARY KEY AUTOINCREMENT, version TEXT NOT NULL,
      old_key TEXT NOT NULL, new_key TEXT NOT NULL, kind TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS conflicts(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      old_key TEXT NOT NULL, reason TEXT NOT NULL, status TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS formal_entities(
      id INTEGER PRIMARY KEY AUTOINCREMENT, entity_key TEXT NOT NULL,
      mapped_from TEXT NOT NULL, version TEXT NOT NULL, created_at TEXT NOT NULL);
    """)
    conn.execute("INSERT INTO map_versions(version,status,effective_from) VALUES (?,?,?)",
                 (v1, "生效", old_from))
    conn.execute("INSERT INTO map_versions(version,status,effective_from) VALUES (?,?,?)",
                 (v2, "待切版", "2026-11-20T00:00:00+00:00"))
    # 同一旧键两版映到不同新键：正是「同旧键映出两套新键」的危险场景
    conn.executemany("INSERT INTO mappings(version,old_key,new_key,kind) VALUES (?,?,?,?)", [
        (v1, "OLD-A1", "NEW-100", "一对一"),
        (v1, "OLD-B1", "NEW-200", "多对一"),
        (v2, "OLD-A1", "NEW-900", "一对一"),
        (v2, "OLD-B1", "NEW-200", "多对一"),
    ])
    conn.commit()


@pytest.fixture
def atomic_db(tmp_path):
    db = tmp_path / "cutover_atomic.db"
    c = connect(db); _seed_map_versions(c); c.close()
    initialize(db, ATOMIC)
    return db


@pytest.fixture
def dual_db(tmp_path):
    db = tmp_path / "cutover_dual.db"
    c = connect(db); _seed_map_versions(c, old_from=OLD_FROM_DUAL); c.close()
    initialize(db, DUAL, max_overlap_seconds=3600)
    return db


# --------------------------------------------------------------------------- #
# 夹具一：原子架构，切后仅新版可映；旧版不可能再被命中
# --------------------------------------------------------------------------- #

def test_atomic_cutover_only_new_version_remappable(atomic_db):
    with connect(atomic_db) as c:
        pre = remap(c, "OLD-A1", T0)
        assert pre["version"] == "m-2026.11" and pre["entity_key"] == "NEW-100"

        info = cutover(c, "m-2026.12", effective_at=TCUT)
        assert info["mode"] == ATOMIC and info["closed_window_ids"]

        opens = c.execute(
            "SELECT * FROM effective_windows WHERE effective_to IS NULL").fetchall()
        assert len(opens) == 1 and opens[0]["version"] == "m-2026.12"
        old = c.execute(
            "SELECT * FROM effective_windows WHERE version='m-2026.11'").fetchone()
        new = c.execute(
            "SELECT * FROM effective_windows WHERE version='m-2026.12'").fetchone()
        assert old["effective_to"] == new["effective_from"] == TCUT

        post = remap(c, "OLD-A1", "2026-11-16T00:00:00+00:00")
        assert post["version"] == "m-2026.12" and post["entity_key"] == "NEW-900"
        assert post["window_id"] == new["id"]
        assert '"m-2026.12"' in post["window_hit"]

        stale = c.execute(
            "SELECT COUNT(*) c FROM formal_entities_v2 "
            "WHERE version='m-2026.11' AND created_at>=?", (TCUT,)).fetchone()["c"]
        assert stale == 0

        cert = certification(c)
        assert cert["checks"]["all_green"] is True
        assert cert["checks"]["atomic_single_open_ok"] is True


def test_atomic_cannot_keep_old_window_open(atomic_db):
    with connect(atomic_db) as c:
        with pytest.raises(CutoverError, match="禁止保留旧窗"):
            cutover(c, "m-2026.12", effective_at=TCUT, close_old=False)
        assert c.execute("SELECT COUNT(*) c FROM effective_windows").fetchone()["c"] == 1
        assert c.execute("SELECT COUNT(*) c FROM cutover_ledger").fetchone()["c"] == 0


def test_atomic_failed_cutover_rolls_back_both_states(atomic_db):
    with connect(atomic_db) as c:
        with pytest.raises(CutoverError):
            cutover(c, "m-4040.01", effective_at=TCUT)
        old = c.execute(
            "SELECT * FROM effective_windows WHERE version='m-2026.11'").fetchone()
        assert old["effective_to"] is None
        assert c.execute("SELECT COUNT(*) c FROM effective_windows").fetchone()["c"] == 1
        assert c.execute(
            "SELECT COUNT(*) c FROM cutover_ledger WHERE result='committed'").fetchone()["c"] == 0
        out = remap(c, "OLD-A1", T0)
        assert out["version"] == "m-2026.11"


# --------------------------------------------------------------------------- #
# 夹具二：双窗架构，按事件时刻落入唯一窗，window_hit 可查
# --------------------------------------------------------------------------- #

def test_dual_first_cut_dynamic_clause_safe(tmp_path):
    # 边界：initialize 已物化唯一 seed 窗，双窗首切走重叠分支，
    # 动态 IN 子句只含一个窗 id，不得构造出非法 SQL
    db = tmp_path / "first.db"
    c = connect(db)
    _seed_map_versions(c, old_from=OLD_FROM_DUAL)
    c.close()
    initialize(db, DUAL)
    with connect(db) as c:
        info = cutover(c, "m-2026.12", effective_at=TCUT, close_old=False)
        assert info["new_window_id"] >= 1 and info["overlap_seconds"] == 3600
        out = remap(c, "OLD-A1", TCUT)
        assert out["version"] == "m-2026.12"
        assert certification(c)["checks"]["all_green"] is True


def test_dual_window_event_time_arbitration(dual_db):
    with connect(dual_db) as c:
        info = cutover(c, "m-2026.12", effective_at=TCUT, close_old=False)
        # 重叠被物化：旧窗拿到计划收口时刻 TCUT+cap，当前只新版开窗
        assert info["overlap_seconds"] == 3600
        assert c.execute(
            "SELECT COUNT(*) c FROM effective_windows WHERE effective_to IS NULL").fetchone()["c"] == 1
        old = c.execute(
            "SELECT * FROM effective_windows WHERE version='m-2026.11'").fetchone()
        assert old["effective_to"] is not None
        # 重叠区间内按事件时刻两窗皆可命中，裁决唯一
        assert len(cutmod._open_windows(c, TCUT)) == 2

        before = remap(c, "OLD-A1", "2026-11-14T23:59:59+00:00")
        after = remap(c, "OLD-A1", TCUT)
        assert before["version"] == "m-2026.11" and before["entity_key"] == "NEW-100"
        assert after["version"] == "m-2026.12" and after["entity_key"] == "NEW-900"

        rows = c.execute(
            "SELECT * FROM gate_decisions WHERE decision='accept'").fetchall()
        assert all(r["window_hit"] for r in rows)
        assert {r["hit_version"] for r in rows} == {"m-2026.11", "m-2026.12"}

        cert = certification(c)
        assert cert["checks"]["no_unbounded_dual_active"] is True
        assert any("有界重叠" in x["detail"] for x in cert["continuity"])


def test_repeated_rejects_on_same_connection_keep_reasons(dual_db):
    # 拒收先留痕提交再抛业务错；同一连接连续拒收不得冒出 "no transaction" 噪音，
    # 且每次拒收都正式零增、决策流水各自留痕
    with connect(dual_db) as c:
        cutover(c, "m-2026.12", effective_at=TCUT, close_old=False)
        n0 = c.execute("SELECT COUNT(*) c FROM formal_entities_v2").fetchone()["c"]
        for key in ("OLD-GHOST", "OLD-GHOST2", "OLD-GHOST3"):
            with pytest.raises(CutoverError) as ei:
                remap(c, key, TCUT)
            assert "无旧键" in str(ei.value)
        assert c.execute("SELECT COUNT(*) c FROM formal_entities_v2").fetchone()["c"] == n0
        assert c.execute(
            "SELECT COUNT(*) c FROM gate_decisions WHERE decision='reject'").fetchone()["c"] == 3
        # 拒收后连接仍健康，正常笔可继续提交
        ok = remap(c, "OLD-A1", TCUT)
        assert ok["entity_key"] == "NEW-900"


def test_dual_rejected_event_zero_formal_growth(dual_db):
    with connect(dual_db) as c:
        cutover(c, "m-2026.12", effective_at=TCUT, close_old=False)
        n0 = c.execute("SELECT COUNT(*) c FROM formal_entities_v2").fetchone()["c"]

        with pytest.raises(CutoverError):  # 新版无此旧键 -> 拒收，正式零增
            remap(c, "OLD-GHOST", TCUT)
        assert c.execute("SELECT COUNT(*) c FROM formal_entities_v2").fetchone()["c"] == n0
        d = c.execute("SELECT * FROM gate_decisions WHERE decision='reject'").fetchone()
        assert d["formal_id"] is None and d["new_key"] is None
        assert d["hit_version"] == "m-2026.12"
        assert any(r["old_key"] == "OLD-GHOST"
                   for r in c.execute("SELECT * FROM conflicts").fetchall())

        # 全部收口后，窗外时刻的事件同样零增
        close_old_windows(c, "2026-11-18T00:00:00+00:00")
        c.execute("UPDATE effective_windows SET effective_to=? WHERE version='m-2026.12'",
                  ("2026-11-19T00:00:00+00:00",)); c.commit()
        with pytest.raises(CutoverError):
            remap(c, "OLD-A1", "2027-01-01T00:00:00+00:00")
        assert c.execute("SELECT COUNT(*) c FROM formal_entities_v2").fetchone()["c"] == n0


def test_dual_unbounded_overlap_flagged_and_blocks(dual_db):
    with connect(dual_db) as c:
        cutover(c, "m-2026.12", effective_at=TCUT, close_old=False)
        # 绕过闸口手工插入一条无收口点的并存开窗，模拟无边界双生效
        c.execute(
            "INSERT INTO map_versions(version,status,effective_from) VALUES (?,?,?)",
            ("m-2027.01", "待切版", "2026-11-15T00:30:00+00:00"))
        c.execute(
            "INSERT INTO effective_windows(version,effective_from,effective_to,"
            "cut_id,opened_event) VALUES (?,?,?,?,?)",
            ("m-2027.01", "2026-11-15T00:30:00+00:00", None, None, "seed"))
        c.commit()
        cert = certification(c)
        assert cert["checks"]["no_unbounded_dual_active"] is False
        assert cert["checks"]["all_green"] is False
        assert cert["overlap_violations"]

        c.execute(
            "INSERT INTO map_versions(version,status,effective_from) VALUES (?,?,?)",
            ("m-2027.02", "待切版", "2027-02-01T00:00:00+00:00")); c.commit()
        with pytest.raises(CutoverError, match="无边界"):
            cutover(c, "m-2027.02", effective_at="2026-11-15T01:00:00+00:00", close_old=False)


def test_dual_tied_windows_reject_not_guess(dual_db):
    with connect(dual_db) as c:
        cutover(c, "m-2026.12", effective_at=TCUT, close_old=False)
        c.execute("UPDATE effective_windows SET effective_from=? WHERE version='m-2026.11'",
                  (TCUT,)); c.commit()
        with pytest.raises(CutoverError, match="并列生效窗"):
            remap(c, "OLD-A1", TCUT)
        assert c.execute("SELECT COUNT(*) c FROM formal_entities_v2").fetchone()["c"] == 0


# --------------------------------------------------------------------------- #
# 夹具三：并发双切同一目标版 -> 只留争用，无锁双写
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("mode,close_old", [(ATOMIC, True), (DUAL, False)])
def test_concurrent_double_cut_survives_one_winner(tmp_path, mode, close_old):
    db = tmp_path / f"cc_{mode}.db"
    c = connect(db)
    _seed_map_versions(c, old_from=OLD_FROM_DUAL); c.close()
    initialize(db, mode, max_overlap_seconds=3600)
    errors: list[Exception] = []

    def worker():
        try:
            cc = connect(db)
            try:
                cutover(cc, "m-2026.12", effective_at=TCUT, close_old=close_old)
            finally:
                cc.close()
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads: t.start()
    for t in threads: t.join()

    assert len(errors) == 1 and isinstance(errors[0], CutoverError)
    with connect(db) as c:
        assert c.execute(
            "SELECT COUNT(*) c FROM effective_windows WHERE version='m-2026.12'").fetchone()["c"] == 1
        assert c.execute(
            "SELECT COUNT(*) c FROM cutover_ledger WHERE result='committed'").fetchone()["c"] == 1
        assert c.execute(
            "SELECT COUNT(*) c FROM effective_windows WHERE version='m-2026.12' "
            "AND effective_to IS NULL").fetchone()["c"] == 1


# --------------------------------------------------------------------------- #
# 口径锁定：一个库只能贯彻一种写法，禁止混用
# --------------------------------------------------------------------------- #

def test_mode_is_frozen_and_cannot_mix(atomic_db):
    with pytest.raises(CutoverError, match="禁止混"):
        initialize(atomic_db, DUAL)
    with connect(atomic_db) as c:
        assert cutmod.get_mode(c) == ATOMIC
        with pytest.raises(sqlite3.IntegrityError):
            c.execute("UPDATE cutover_config SET mode='dual'")
        c.rollback()
        with pytest.raises(sqlite3.IntegrityError):
            c.execute("DELETE FROM cutover_config")
        c.rollback()
        with pytest.raises(sqlite3.IntegrityError):
            c.execute("DELETE FROM effective_windows")
        c.rollback()
        cutover(c, "m-2026.12", effective_at=TCUT)
        with pytest.raises(sqlite3.IntegrityError):
            c.execute("UPDATE effective_windows SET effective_to=NULL WHERE version='m-2026.11'")
        c.rollback()


# --------------------------------------------------------------------------- #
# 互证卡：窗内静默分叉才算失败；跨边界换键单独留痕可核证
# --------------------------------------------------------------------------- #

def test_cross_validation_card_across_boundary(atomic_db):
    with connect(atomic_db) as c:
        remap(c, "OLD-A1", T0)
        cutover(c, "m-2026.12", effective_at=TCUT)
        remap(c, "OLD-A1", "2026-11-16T00:00:00+00:00")
        cert = certification(c)
        assert cert["cross_validation_card"] == []  # 窗内无静默分叉
        change = [x for x in cert["boundary_changes"] if x["mapped_from"] == "OLD-A1"]
        assert change and change[0]["distinct_new_keys"] == 2
        assert cert["checks"]["every_formal_traceable_to_window"] is True
        assert cert["checks"]["boundary_continuity_ok"] is True
        assert cert["checks"]["all_green"] is True


# --------------------------------------------------------------------------- #
# FastAPI 层冒烟（登录、重映闸、核证只读接口、管理员权限）
# --------------------------------------------------------------------------- #

def test_api_smoke_with_cutover(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    import app as appmod

    monkeypatch.setattr(appmod, "DB_PATH", tmp_path / "api.db")
    appmod.init_db()
    client = TestClient(appmod.app)
    assert client.get("/api/health").json()["cutover_mode"] == ATOMIC

    tok = client.post("/api/login", json={"username": "admin", "password": "123456"}).json()["token"]
    h = {"Authorization": f"Bearer {tok}"}

    r = client.post("/api/map-versions", headers=h, json={
        "version": "m-2026.12",
        "mappings": [{"old_key": "OLD-B2", "new_key": "NEW-299", "kind": "一对一"}]})
    assert r.status_code == 200
    r = client.post("/api/cutover", headers=h, json={"to_version": "m-2026.12"})
    assert r.status_code == 200 and r.json()["cut_id"] >= 1

    out = client.post("/api/remap", headers=h, json={"old_key": "OLD-B2"}).json()
    assert out["entity_key"] == "NEW-299" and "window_hit" in out

    assert client.post("/api/remap", headers=h, json={"old_key": "OLD-ZZ"}).status_code == 409

    cert = client.get("/api/certification", headers=h).json()
    assert cert["checks"]["all_green"] is True
    gate_items = client.get("/api/gate-decisions", headers=h).json()["items"]
    assert any(g["decision"] == "reject" and g["formal_id"] is None for g in gate_items)

    vtok = client.post("/api/login",
                       json={"username": "viewer", "password": "view123"}).json()["token"]
    vh = {"Authorization": f"Bearer {vtok}"}
    assert client.post("/api/cutover", headers=vh,
                       json={"to_version": "m-2026.12"}).status_code == 403
    assert client.get("/api/certification", headers=vh).status_code == 200
