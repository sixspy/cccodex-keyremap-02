"""
本地 SQLite 夹具生成器：在 fixtures/generated/ 下产出两个可直接 sqlite3 打开核查的库。

  atomic_cutover.db  —— 甲路径（原子单窗）：切版后仅新版可映，旧窗同事务关闭
  dual_window.db     —— 乙路径（双窗按事件时刻裁）：有界短重叠，window_hit 快照可查

运行：python3 -I fixtures/build_fixtures.py
核查：sqlite3 fixtures/generated/atomic_cutover.db \
        "SELECT * FROM effective_windows; SELECT * FROM cutover_ledger;"
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

import cutover as cutmod  # noqa: E402
from cutover import connect, initialize, cutover, remap, certification, ATOMIC, DUAL  # noqa: E402

OUT = ROOT / "fixtures" / "generated"
TCUT = "2026-11-15T00:00:00+00:00"
OLD_FROM = "2026-11-14T23:30:00+00:00"

SEED_SQL = """
CREATE TABLE map_versions(id INTEGER PRIMARY KEY AUTOINCREMENT,
  version TEXT UNIQUE NOT NULL, status TEXT NOT NULL, effective_from TEXT NOT NULL);
CREATE TABLE mappings(id INTEGER PRIMARY KEY AUTOINCREMENT,
  version TEXT NOT NULL, old_key TEXT NOT NULL, new_key TEXT NOT NULL, kind TEXT NOT NULL);
CREATE TABLE conflicts(id INTEGER PRIMARY KEY AUTOINCREMENT,
  old_key TEXT NOT NULL, reason TEXT NOT NULL, status TEXT NOT NULL);
CREATE TABLE formal_entities(id INTEGER PRIMARY KEY AUTOINCREMENT,
  entity_key TEXT NOT NULL, mapped_from TEXT NOT NULL, version TEXT NOT NULL, created_at TEXT NOT NULL);
"""
SEED_ROWS_VERSIONS = [
    ("m-2026.11", "生效", "2026-11-01T00:00:00+00:00"),
    ("m-2026.12", "待切版", "2026-11-20T00:00:00+00:00"),
]
SEED_ROWS_MAPPINGS = [
    ("m-2026.11", "OLD-A1", "NEW-100", "一对一"),
    ("m-2026.11", "OLD-B1", "NEW-200", "多对一"),
    ("m-2026.12", "OLD-A1", "NEW-900", "一对一"),
    ("m-2026.12", "OLD-B1", "NEW-200", "多对一"),
]


def _seed(conn, old_from):
    conn.executescript(SEED_SQL)
    v1, v2 = SEED_ROWS_VERSIONS
    conn.execute("INSERT INTO map_versions(version,status,effective_from) VALUES (?,?,?)",
                 (v1[0], v1[1], old_from))
    conn.execute("INSERT INTO map_versions(version,status,effective_from) VALUES (?,?,?)", v2)
    conn.executemany(
        "INSERT INTO mappings(version,old_key,new_key,kind) VALUES (?,?,?,?)",
        SEED_ROWS_MAPPINGS)
    conn.commit()


def build_atomic(db: Path) -> dict:
    c = connect(db); _seed(c, "2026-11-01T00:00:00+00:00"); c.close()
    initialize(db, ATOMIC)
    with connect(db) as c:
        remap(c, "OLD-A1", "2026-11-10T00:00:00+00:00")
        cut = cutover(c, "m-2026.12", effective_at=TCUT)          # 同事务关旧开新
        after = remap(c, "OLD-A1", "2026-11-16T00:00:00+00:00")  # 只可能命中新版
        cert = certification(c)
    return {"cut": cut, "post_cut_remap": after,
            "open_windows": cert["checks"]["open_window_count"],
            "all_green": cert["checks"]["all_green"]}


def build_dual(db: Path) -> dict:
    c = connect(db); _seed(c, OLD_FROM); c.close()
    initialize(db, DUAL, max_overlap_seconds=3600)
    with connect(db) as c:
        cut = cutover(c, "m-2026.12", effective_at=TCUT, close_old=False)
        before = remap(c, "OLD-A1", "2026-11-14T23:59:59+00:00")  # 落旧窗
        after = remap(c, "OLD-A1", TCUT)                          # 落新窗
        try:
            remap(c, "OLD-GHOST", TCUT)                            # 新版无映射 -> 零增
        except cutmod.CutoverError:
            pass
        cert = certification(c)
        hits = [dict(r) for r in c.execute(
            "SELECT decision_id,old_key,hit_version,window_hit,decision,formal_id "
            "FROM gate_decisions ORDER BY decision_id")]
    return {"cut": cut, "before_cut_hit": before, "at_cut_hit": after,
            "gate_decisions": hits, "all_green": cert["checks"]["all_green"],
            "overlap_violations": cert["overlap_violations"]}


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for name in ("atomic_cutover.db", "dual_window.db"):
        p = OUT / name
        if p.exists():
            p.unlink()
    summary = {
        "atomic_cutover.db": build_atomic(OUT / "atomic_cutover.db"),
        "dual_window.db": build_dual(OUT / "dual_window.db"),
    }
    (OUT / "fixture_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    print(f"\n夹具库与摘要已写入 {OUT}")


if __name__ == "__main__":
    main()
