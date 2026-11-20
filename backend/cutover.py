"""
version_cutover —— 映射版切版的物化生效边界与唯一命中重映闸。

两条正确性口径（每个库初始化时二选一，落库后不可变，禁止两边混用）：

  甲 / atomic 原子单窗：
    切版在同一 BEGIN IMMEDIATE 事务内关掉旧版生效窗、打开新版生效窗，
    任一步失败整体回滚，两版状态皆不变。任意时刻至多一个开窗，
    重映闸只可能命中唯一窗；旧版在切版后不可能再被命中进正式区。

  乙 / dual 双窗按事件时刻裁：
    允许新旧版有界短重叠，每笔重映按事件时刻落入唯一窗
    （重叠期确定性裁决：effective_from 更晚者胜，再以 cut_id 决胜），
    并把命中窗物化进 gate_decisions 的 window_hit 快照；
    无法唯一命中或映射缺失则该笔正式零增（只留拒收决策，不写正式区）。
    开窗重叠超过 max_overlap_seconds 即构成「无边界双生效」违例，
    核证接口必然亮红灯，且再次切版会被拒绝。

硬约束落点：
  * 无边界双生效长期并存 —— 甲：partial UNIQUE 开窗索引；乙：违例视图 + 切版前置拒绝
  * 原子架构下旧版仍被命中进正式 —— 甲：旧窗同事务关闭 + 闸口开窗计数断言
  * 两路同时点切同一目标版无锁双写 —— BEGIN IMMEDIATE 写锁 + cutover_ledger.to_version 唯一
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

ATOMIC = "atomic"          # 甲路径：原子单窗
DUAL = "dual"              # 乙路径：双窗按事件时刻裁
MODES = (ATOMIC, DUAL)

DEFAULT_MAX_OVERLAP = 3600  # 乙路径允许的有界重叠上限（秒）


class CutoverError(Exception):
    """切版/重映的业务级失败（拒收、回滚、争用落败都走这里）。"""


def _rollback_safely(conn: sqlite3.Connection) -> None:
    try:
        conn.rollback()
    except sqlite3.Error:
        pass


def _as_conflict(e: Exception, prefix: str) -> CutoverError:
    """锁等待超时/约束冲突统一归一化为争用失败，调用方保证已回滚。"""
    msg = str(e).lower()
    if "locked" in msg or "busy" in msg:
        return CutoverError(f"{prefix}写锁争用落败，未产生任何生效窗：{e}")
    return CutoverError(f"{prefix}约束冲突，已整体回滚：{e}")


# --------------------------------------------------------------------------- #
# 时间：统一 UTC ISO-8601，半开区间 [effective_from, effective_to)，字典序可比
# --------------------------------------------------------------------------- #

def now_ts() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def iso(epoch_seconds: float) -> str:
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).isoformat(timespec="microseconds")


# --------------------------------------------------------------------------- #
# 连接与模式锁定
# --------------------------------------------------------------------------- #

def connect(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def get_mode(conn: sqlite3.Connection) -> str:
    row = conn.execute("SELECT mode FROM cutover_config LIMIT 1").fetchone()
    if row is None:
        raise CutoverError("cutover_config 未初始化：库未选定正确性口径")
    return row["mode"]


def init_schema(conn: sqlite3.Connection, mode: str | None = None,
                max_overlap_seconds: int = DEFAULT_MAX_OVERLAP) -> str:
    """建表并一次性锁定口径。对已初始化的库传错 mode 直接拒绝，杜绝混用。"""
    if mode is not None and mode not in MODES:
        raise CutoverError(f"未知口径 {mode!r}，只允许 {MODES}")

    conn.executescript("""
    CREATE TABLE IF NOT EXISTS cutover_config(
      id INTEGER PRIMARY KEY CHECK (id=1),
      mode TEXT NOT NULL CHECK (mode IN ('atomic','dual')),
      max_overlap_seconds INTEGER NOT NULL,
      locked_at TEXT NOT NULL
    );

    -- 生效窗登记表（物化的可核证生效边界；行不可变，关窗只许 NULL->时刻一次）
    CREATE TABLE IF NOT EXISTS effective_windows(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      version TEXT NOT NULL,
      effective_from TEXT NOT NULL,
      effective_to TEXT,
      cut_id INTEGER,
      opened_event TEXT NOT NULL CHECK (opened_event IN ('seed','cutover_open')),
      FOREIGN KEY(version) REFERENCES map_versions(version),
      FOREIGN KEY(cut_id) REFERENCES cutover_ledger(cut_id)
    );

    -- 切版审计台账
    CREATE TABLE IF NOT EXISTS cutover_ledger(
      cut_id INTEGER PRIMARY KEY AUTOINCREMENT,
      cut_at TEXT NOT NULL,
      from_version TEXT,
      to_version TEXT NOT NULL,
      mode TEXT NOT NULL CHECK (mode IN ('atomic','dual')),
      old_window_id INTEGER,
      new_window_id INTEGER,
      close_old INTEGER NOT NULL CHECK (close_old IN (0,1)),
      overlap_seconds INTEGER,
      result TEXT NOT NULL CHECK (result IN ('committed','rejected')),
      note TEXT
    );

    -- 重映闸决策流水（含 window_hit 物化快照）
    CREATE TABLE IF NOT EXISTS gate_decisions(
      decision_id INTEGER PRIMARY KEY AUTOINCREMENT,
      event_time TEXT NOT NULL,
      old_key TEXT NOT NULL,
      hit_window_id INTEGER,
      hit_version TEXT,
      window_hit TEXT,
      decision TEXT NOT NULL CHECK (decision IN ('accept','reject')),
      reason TEXT NOT NULL,
      new_key TEXT,
      formal_id INTEGER
    );

    CREATE TABLE IF NOT EXISTS formal_entities_v2(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      entity_key TEXT NOT NULL,
      mapped_from TEXT NOT NULL,
      version TEXT NOT NULL,
      window_id INTEGER NOT NULL,
      decision_id INTEGER NOT NULL,
      created_at TEXT NOT NULL,
      FOREIGN KEY(window_id) REFERENCES effective_windows(id),
      FOREIGN KEY(decision_id) REFERENCES gate_decisions(decision_id)
    );
    """)

    locked = conn.execute("SELECT mode FROM cutover_config WHERE id=1").fetchone()
    if locked is not None:
        if mode is not None and locked["mode"] != mode:
            raise CutoverError(
                f"本库口径已锁定为 {locked['mode']!r}，禁止混入 {mode!r} 写法")
        return locked["mode"]
    if mode is None:
        raise CutoverError("新库必须显式选定口径：atomic 或 dual")

    conn.execute(
        "INSERT INTO cutover_config(id,mode,max_overlap_seconds,locked_at) VALUES (1,?,?,?)",
        (mode, max_overlap_seconds, now_ts()))
    # 配置行不可变：禁止改口径、禁止删配置
    conn.execute("""CREATE TRIGGER IF NOT EXISTS trg_config_freeze_upd
        BEFORE UPDATE ON cutover_config
        BEGIN SELECT RAISE(ABORT,'cutover_config 不可变：正确性口径一经选定禁止分叉'); END""")
    conn.execute("""CREATE TRIGGER IF NOT EXISTS trg_config_freeze_del
        BEFORE DELETE ON cutover_config
        BEGIN SELECT RAISE(ABORT,'cutover_config 不可删除'); END""")
    # 生效窗不可删、不可重开（effective_to 只许 NULL->非 NULL 一次）
    conn.execute("""CREATE TRIGGER IF NOT EXISTS trg_window_no_delete
        BEFORE DELETE ON effective_windows
        BEGIN SELECT RAISE(ABORT,'生效窗登记表只追加不可删'); END""")
    conn.execute("""CREATE TRIGGER IF NOT EXISTS trg_window_close_once
        BEFORE UPDATE OF effective_to ON effective_windows
        WHEN OLD.effective_to IS NOT NULL
        BEGIN SELECT RAISE(ABORT,'生效窗已关闭，禁止重开/改写边界'); END""")
    if mode == ATOMIC:
        # 甲路径硬保证：同一时刻至多一个开窗（对 effective_to IS NULL 的行，
        # 生成列恒为 1，UNIQUE 索引即「开窗数 ≤ 1」约束）
        conn.execute("""CREATE UNIQUE INDEX IF NOT EXISTS idx_one_open_window
            ON effective_windows((1)) WHERE effective_to IS NULL""")
    # 乙路径也不允许同一版本重复开窗
    conn.execute("""CREATE UNIQUE INDEX IF NOT EXISTS idx_window_version_open
        ON effective_windows(version) WHERE effective_to IS NULL""")
    conn.execute("""CREATE UNIQUE INDEX IF NOT EXISTS idx_ledger_to_version
        ON cutover_ledger(to_version) WHERE result='committed'""")
    conn.commit()
    return mode


def initialize(db_path: str | Path, mode: str | None = None,
               max_overlap_seconds: int = DEFAULT_MAX_OVERLAP) -> str:
    """建表 + 锁口径 + 为既有 map_versions 补登首个生效窗（幂等）。"""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = connect(db_path)
    try:
        locked_mode = init_schema(conn, mode, max_overlap_seconds)
        n = conn.execute("SELECT COUNT(*) c FROM effective_windows").fetchone()["c"]
        if n == 0:
            # 兼容旧库：把现存「生效」徽标版本物化为首个开窗，边界从此可核证
            rows = conn.execute(
                "SELECT version, effective_from FROM map_versions ORDER BY id").fetchall()
            if not rows:
                raise CutoverError("map_versions 为空，无法登记首个生效窗")
            first = rows[0]
            wid = conn.execute(
                "INSERT INTO effective_windows(version,effective_from,effective_to,"
                "cut_id,opened_event) VALUES (?,?,?,?,?)",
                (first["version"], first["effective_from"], None, None, "seed")).lastrowid
            # 回填切版前已存在的正式区历史行：挂到 seed 窗并补 accept 决策快照，
            # 保证「正式区每笔可回指命中窗」对旧库数据同样成立。
            legacy = conn.execute("SELECT * FROM formal_entities").fetchall()
            for r in legacy:
                dcur = conn.execute(
                    "INSERT INTO gate_decisions(event_time,old_key,hit_window_id,hit_version,"
                    "window_hit,decision,reason,new_key,formal_id) VALUES (?,?,?,?,?,?,?,?,?)",
                    (r["created_at"], r["mapped_from"], wid, first["version"],
                     json.dumps({"window_id": wid, "version": first["version"],
                                 "effective_from": first["effective_from"],
                                 "effective_to": None, "cut_id": None,
                                 "opened_event": "seed-migrated"}, ensure_ascii=False),
                     "accept", "历史行迁移回填", r["entity_key"], None))
                fid = conn.execute(
                    "INSERT INTO formal_entities_v2(entity_key,mapped_from,version,"
                    "window_id,decision_id,created_at) VALUES (?,?,?,?,?,?)",
                    (r["entity_key"], r["mapped_from"], r["version"], wid,
                     dcur.lastrowid, r["created_at"])).lastrowid
                conn.execute("UPDATE gate_decisions SET formal_id=? WHERE decision_id=?",
                             (fid, dcur.lastrowid))
            conn.commit()
        return locked_mode
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# 窗查询
# --------------------------------------------------------------------------- #

def _open_windows(conn: sqlite3.Connection, at: str) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM effective_windows "
        "WHERE effective_from<=? AND (effective_to IS NULL OR effective_to>?) "
        "ORDER BY effective_from DESC, id DESC", (at, at)).fetchall()


def active_window(conn: sqlite3.Connection) -> sqlite3.Row | None:
    """当前开窗。甲路径至多一行；乙路径可能多行（有界重叠期）。"""
    rows = conn.execute(
        "SELECT * FROM effective_windows WHERE effective_to IS NULL "
        "ORDER BY effective_from DESC, id DESC").fetchall()
    return rows[0] if rows else None


def resolve_window(conn: sqlite3.Connection, event_time: str) -> tuple[sqlite3.Row | None, str]:
    """按事件时刻求唯一命中窗，返回 (窗, 拒收原因/空串)。"""
    mode = get_mode(conn)
    hits = _open_windows(conn, event_time)
    if not hits:
        return None, "事件时刻无生效窗"
    if len(hits) == 1:
        return hits[0], ""
    if mode == ATOMIC:
        # 甲路径出现多窗即架构违例，宁拒不猜
        return None, f"原子单窗架构下出现 {len(hits)} 个开窗，拒绝猜测命中"
    # 乙路径：重叠期确定性裁决，最晚 effective_from 者胜（排序已保证），仍并列则拒
    win = hits[0]
    tied = [h for h in hits if h["effective_from"] == win["effective_from"]]
    if len(tied) > 1:
        return None, f"事件时刻 {event_time} 存在 {len(tied)} 个并列生效窗，无法唯一命中"
    return win, ""


# --------------------------------------------------------------------------- #
# 切版
# --------------------------------------------------------------------------- #

def _overlap_violations(conn: sqlite3.Connection, at: str | None = None) -> list[dict]:
    """
    乙路径无界双生效判定（只读核证用；at 保留给调用点语义对齐，当前不参与过滤）。
    两窗的并存区间 = [max(from), min(to))；关键看这个并存区间是否有终点：
      * 两窗 effective_to 皆为 NULL => 并存永不结束，无界
      * 并存区间有终点但时长超过 max_overlap_seconds => 超界
      * 其中一窗（通常是当前生效新版）单独无限开窗、旧窗已带收口点 => 合法
    已完全成为历史的重叠也按其时长复核，防止事后无法发现超界。
    """
    cap = conn.execute(
        "SELECT max_overlap_seconds FROM cutover_config").fetchone()["max_overlap_seconds"]
    rows = conn.execute("SELECT * FROM effective_windows ORDER BY id").fetchall()
    bad: list[dict] = []
    for i in range(len(rows)):
        for j in range(i + 1, len(rows)):
            a, b = rows[i], rows[j]
            lo = max(a["effective_from"], b["effective_from"])
            if a["effective_to"] is not None and a["effective_to"] <= lo:
                continue  # 两窗实际不相交
            if b["effective_to"] is not None and b["effective_to"] <= lo:
                continue
            if a["effective_to"] is None and b["effective_to"] is None:
                bad.append({"versions": sorted([a["version"], b["version"]]),
                            "overlap_seconds": None, "cap": cap,
                            "reason": "两窗均无收口时刻，并存区间无边界"})
                continue
            # 至少一窗有终点 => 并存区间在较早的收口点结束
            hi = min(x["effective_to"] for x in (a, b) if x["effective_to"] is not None)
            if hi <= lo:
                continue
            span = (datetime.fromisoformat(hi) - datetime.fromisoformat(lo)).total_seconds()
            if span > cap:
                bad.append({"versions": sorted([a["version"], b["version"]]),
                            "overlap_seconds": int(span), "cap": cap,
                            "reason": "并存区间时长超过有界上限"})
    return bad


def cutover(conn: sqlite3.Connection, to_version: str, *,
            effective_at: str | None = None, close_old: bool | None = None,
            note: str = "") -> dict:
    """
    切版入口。甲路径强制同事务关旧窗；乙路径默认允许有界重叠，
    close_old=True 可在同事务关旧窗（收口）。失败抛 CutoverError，事务回滚。
    """
    mode = get_mode(conn)
    at = effective_at or now_ts()
    if close_old is None:
        close_old = (mode == ATOMIC)
    if mode == ATOMIC and not close_old:
        raise CutoverError("原子单窗口径禁止保留旧窗：切版必须同事务关闭旧版生效窗")

    # BEGIN IMMEDIATE：立刻拿 SQLite 库级写锁，并发双切在此排队，
    # 后面的唯一索引/唯一台账再兜底，确保无锁双写不可能发生。
    conn.execute("BEGIN IMMEDIATE")
    try:
        target = conn.execute(
            "SELECT version FROM map_versions WHERE version=?", (to_version,)).fetchone()
        if target is None:
            raise CutoverError(f"目标版本 {to_version!r} 不在映射册，切版拒绝")
        dup = conn.execute(
            "SELECT 1 FROM effective_windows WHERE version=? AND effective_to IS NULL",
            (to_version,)).fetchone()
        if dup:
            raise CutoverError(f"版本 {to_version!r} 已有开窗，禁止重复切版")

        olds = conn.execute(
            "SELECT * FROM effective_windows WHERE effective_to IS NULL ORDER BY id").fetchall()

        if mode == DUAL and not close_old:
            # 双窗口径允许短重叠，但边界必须物化：切版同事务给每个仍开着的旧窗
            # 写死「计划收口时刻」at+cap，使重叠恒为有界，杜绝长期双生效。
            existing = _open_windows(conn, at)
            cap = conn.execute(
                "SELECT max_overlap_seconds c FROM cutover_config").fetchone()["c"]
            planned_end = iso(datetime.fromisoformat(at).timestamp() + cap)
            for w in existing:
                if w["effective_to"] is not None:
                    remain = (datetime.fromisoformat(w["effective_to"])
                              - datetime.fromisoformat(at)).total_seconds()
                    if remain > cap:
                        raise CutoverError(
                            f"切版后与旧窗 {w['version']} 还将重叠 {int(remain)}s "
                            f"> 上限 {cap}s，构成无边界双生效，切版拒绝；请先 close_old 收口")
                else:
                    conn.execute(
                        "UPDATE effective_windows SET effective_to=? WHERE id=?",
                        (planned_end, w["id"]))
            violations = _overlap_violations(conn, at)
            if violations:
                raise CutoverError(f"已存在无边界双生效违例 {violations}，须先收口再切版")
            # 后续 olds 已被改写，刷新本地快照（首切无旧窗时 existing 为空，跳过）
            if existing:
                placeholders = ",".join("?" * len(existing))
                olds = conn.execute(
                    f"SELECT * FROM effective_windows WHERE id IN ({placeholders}) ORDER BY id",
                    [w["id"] for w in existing]).fetchall()

        cur = conn.execute(
            "INSERT INTO cutover_ledger(cut_at,from_version,to_version,mode,"
            "old_window_id,new_window_id,close_old,overlap_seconds,result,note) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (at, olds[-1]["version"] if olds else None, to_version, mode,
             olds[-1]["id"] if olds else None, None, 1 if close_old else 0, None,
             "committed", note))
        cut_id = cur.lastrowid

        # 甲路径必须先关旧窗再开新窗，否则「至多一开窗」唯一索引会拦下新窗；
        # 两步同处一个事务，任一步失败整体回滚，两版状态皆不变。
        overlap_seconds = None
        if close_old and olds:
            for w in olds:
                conn.execute(
                    "UPDATE effective_windows SET effective_to=? WHERE id=? AND effective_to IS NULL",
                    (at, w["id"]))
            overlap_seconds = 0

        new_id = conn.execute(
            "INSERT INTO effective_windows(version,effective_from,effective_to,"
            "cut_id,opened_event) VALUES (?,?,?,?,?)",
            (to_version, at, None, cut_id, "cutover_open")).lastrowid

        if not close_old and olds:
            # 有界重叠秒数：旧窗计划收口时刻 - 切版时刻（上面已物化收口点）
            tail = olds[-1]
            overlap_seconds = None if tail["effective_to"] is None else int(
                (datetime.fromisoformat(tail["effective_to"])
                 - datetime.fromisoformat(at)).total_seconds())

        conn.execute("UPDATE cutover_ledger SET new_window_id=?, overlap_seconds=? WHERE cut_id=?",
                     (new_id, overlap_seconds, cut_id))
        conn.commit()
        return {"cut_id": cut_id, "mode": mode, "to_version": to_version,
                "effective_at": at, "new_window_id": new_id,
                "closed_window_ids": [w["id"] for w in olds] if close_old else [],
                "overlap_seconds": overlap_seconds}
    except (sqlite3.IntegrityError, sqlite3.OperationalError) as e:
        _rollback_safely(conn)
        raise _as_conflict(e, "切版争用落败或") from e
    except CutoverError:
        _rollback_safely(conn)
        raise
    except Exception as e:
        _rollback_safely(conn)
        raise CutoverError(f"切版异常，两版状态皆回滚：{e}") from e


def close_old_windows(conn: sqlite3.Connection, effective_at: str | None = None) -> dict:
    """乙路径收口：显式关闭除最新开窗外的全部开窗。"""
    mode = get_mode(conn)
    if mode != DUAL:
        raise CutoverError("close_old_windows 仅双窗口径使用；原子口径切版即自动关窗")
    at = effective_at or now_ts()
    conn.execute("BEGIN IMMEDIATE")
    try:
        olds = conn.execute(
            "SELECT * FROM effective_windows WHERE effective_to IS NULL ORDER BY id").fetchall()
        if len(olds) <= 1:
            conn.rollback()
            return {"closed": [], "note": "无并列开窗，无需收口"}
        victims = olds[:-1]
        for w in victims:
            conn.execute(
                "UPDATE effective_windows SET effective_to=? WHERE id=?", (at, w["id"]))
        conn.commit()
        return {"closed": [w["id"] for w in victims], "at": at}
    except (sqlite3.IntegrityError, sqlite3.OperationalError) as e:
        _rollback_safely(conn)
        raise _as_conflict(e, "收口失败") from e
    except CutoverError:
        _rollback_safely(conn)
        raise


# --------------------------------------------------------------------------- #
# 重映闸：唯一命中 + window_hit 快照 + 失败正式零增
# --------------------------------------------------------------------------- #

def remap(conn: sqlite3.Connection, old_key: str, event_time: str | None = None) -> dict:
    at = event_time or now_ts()
    conn.execute("BEGIN IMMEDIATE")
    # 拒收也必须留痕（gate_decisions + conflicts），故拒收分支先 commit 再抛业务错；
    # finalized=True 时外层异常处理不得再 rollback 一个已结束的事务。
    finalized = False
    try:
        win, reason = resolve_window(conn, at)
        if win is None:
            conn.execute(
                "INSERT INTO gate_decisions(event_time,old_key,hit_window_id,hit_version,"
                "window_hit,decision,reason,new_key,formal_id) VALUES (?,?,?,?,?,?,?,?,?)",
                (at, old_key, None, None, None, "reject", reason, None, None))
            conn.execute(
                "INSERT INTO conflicts(old_key,reason,status) VALUES (?,?,?)",
                (old_key, reason, "挂起"))
            conn.commit(); finalized = True
            raise CutoverError(reason)

        m = conn.execute(
            "SELECT * FROM mappings WHERE version=? AND old_key=?",
            (win["version"], old_key)).fetchone()
        if m is None:
            reason = f"命中窗版本 {win['version']} 内无旧键 {old_key} 映射"
            conn.execute(
                "INSERT INTO gate_decisions(event_time,old_key,hit_window_id,hit_version,"
                "window_hit,decision,reason,new_key,formal_id) VALUES (?,?,?,?,?,?,?,?,?)",
                (at, old_key, win["id"], win["version"], _hit_snapshot(win),
                 "reject", reason, None, None))
            conn.execute(
                "INSERT INTO conflicts(old_key,reason,status) VALUES (?,?,?)",
                (old_key, reason, "挂起"))
            conn.commit(); finalized = True
            raise CutoverError(reason)

        dcur = conn.execute(
            "INSERT INTO gate_decisions(event_time,old_key,hit_window_id,hit_version,"
            "window_hit,decision,reason,new_key,formal_id) VALUES (?,?,?,?,?,?,?,?,?)",
            (at, old_key, win["id"], win["version"], _hit_snapshot(win),
             "accept", "唯一窗命中", m["new_key"], None))
        decision_id = dcur.lastrowid
        fcur = conn.execute(
            "INSERT INTO formal_entities_v2(entity_key,mapped_from,version,window_id,"
            "decision_id,created_at) VALUES (?,?,?,?,?,?)",
            (m["new_key"], old_key, win["version"], win["id"], decision_id, at))
        formal_id = fcur.lastrowid
        conn.execute("UPDATE gate_decisions SET formal_id=? WHERE decision_id=?",
                     (formal_id, decision_id))
        # 同步旧正式区表，保持既有页面/接口可读
        conn.execute(
            "INSERT INTO formal_entities(entity_key,mapped_from,version,created_at) "
            "VALUES (?,?,?,?)", (m["new_key"], old_key, win["version"], at))
        conn.commit()
        return {"entity_key": m["new_key"], "version": win["version"],
                "window_id": win["id"], "decision_id": decision_id,
                "formal_id": formal_id, "window_hit": _hit_snapshot(win)}
    except (sqlite3.IntegrityError, sqlite3.OperationalError) as e:
        if not finalized:
            _rollback_safely(conn)
        raise _as_conflict(e, "重映闸") from e
    except CutoverError:
        # 拒收留痕已 commit；若是事务中途的业务错则回滚
        if not finalized:
            _rollback_safely(conn)
        raise
    except Exception as e:
        if not finalized:
            _rollback_safely(conn)
        raise CutoverError(f"重映异常，该笔正式零增：{e}") from e


def _hit_snapshot(win: sqlite3.Row) -> str:
    return json.dumps({
        "window_id": win["id"], "version": win["version"],
        "effective_from": win["effective_from"],
        "effective_to": win["effective_to"], "cut_id": win["cut_id"],
        "opened_event": win["opened_event"]}, ensure_ascii=False, sort_keys=True)


# --------------------------------------------------------------------------- #
# 只读核证
# --------------------------------------------------------------------------- #

def certification(conn: sqlite3.Connection) -> dict:
    """切版边界核证只读接口的数据源：映射册/重映闸/正式区/互证卡/审计页共用。"""
    mode = get_mode(conn)
    cfg = conn.execute("SELECT * FROM cutover_config").fetchone()
    boundaries = [dict(r) for r in conn.execute(
        "SELECT w.*, c.cut_at, c.result AS cut_result "
        "FROM effective_windows w LEFT JOIN cutover_ledger c ON c.cut_id=w.cut_id "
        "ORDER BY w.id").fetchall()]
    open_wins = conn.execute(
        "SELECT COUNT(*) c FROM effective_windows WHERE effective_to IS NULL").fetchone()["c"]
    cuts = [dict(r) for r in conn.execute(
        "SELECT * FROM cutover_ledger ORDER BY cut_id").fetchall()]
    decisions = [dict(r) for r in conn.execute(
        "SELECT * FROM gate_decisions ORDER BY decision_id").fetchall()]

    # 互证卡（失败口径）：同一旧键在【同一个生效窗内】映出两套新键 = 静默分叉，互证对不上。
    # 跨已核证边界的换键是切版的预期结果，单列 boundary_changes 供核证，不算失败。
    cross = [dict(r) for r in conn.execute("""
        SELECT mapped_from, version, window_id,
               COUNT(DISTINCT entity_key) AS distinct_new_keys,
               GROUP_CONCAT(DISTINCT entity_key) AS new_keys,
               COUNT(*) AS formal_rows
        FROM formal_entities_v2
        GROUP BY mapped_from, window_id
        HAVING COUNT(DISTINCT entity_key)>1
        ORDER BY mapped_from""").fetchall()]

    boundary_changes = [dict(r) for r in conn.execute("""
        SELECT mapped_from,
               COUNT(DISTINCT entity_key) AS distinct_new_keys,
               GROUP_CONCAT(DISTINCT entity_key) AS new_keys,
               GROUP_CONCAT(DISTINCT version) AS versions,
               COUNT(*) AS formal_rows
        FROM formal_entities_v2 GROUP BY mapped_from
        HAVING COUNT(DISTINCT entity_key)>1 OR COUNT(DISTINCT version)>1
        ORDER BY mapped_from""").fetchall()]

    # 窗连续性：前窗 effective_to 必须等于后窗 effective_from（甲路径恒等；乙路径允许有界重叠）
    continuity = []
    for a, b in zip(boundaries, boundaries[1:]):
        if a["effective_to"] is None:
            continuity.append({"prev_window": a["id"], "next_window": b["id"],
                               "ok": False, "detail": "前窗仍开，边界未闭合"})
        elif mode == ATOMIC and a["effective_to"] != b["effective_from"]:
            continuity.append({"prev_window": a["id"], "next_window": b["id"],
                               "ok": False, "detail": "原子口径要求关窗时刻==开窗时刻"})
        else:
            continuity.append({"prev_window": a["id"], "next_window": b["id"], "ok": True,
                               "detail": "边界衔接" + ("（有界重叠）" if mode == DUAL else "")})

    overlap_bad = _overlap_violations(conn) if mode == DUAL else []
    rejected = [d for d in decisions if d["decision"] == "reject"]
    # 正式区每一笔都必须能回指命中窗快照
    untraceable = conn.execute(
        "SELECT COUNT(*) c FROM formal_entities_v2 WHERE window_id IS NULL "
        "OR decision_id IS NULL").fetchone()["c"]

    checks = {
        "mode_locked": mode,
        "open_window_count": open_wins,
        "atomic_single_open_ok": (mode != ATOMIC or open_wins <= 1),
        "no_unbounded_dual_active": (mode != DUAL or not overlap_bad),
        "boundary_continuity_ok": all(x["ok"] for x in continuity),
        "cross_validation_ok": len(cross) == 0,
        "every_formal_traceable_to_window": untraceable == 0,
        "rejected_zero_formal_growth": all(
            d["formal_id"] is None and d["new_key"] is None for d in rejected),
    }
    checks["all_green"] = all(checks[k] for k in (
        "atomic_single_open_ok", "no_unbounded_dual_active", "boundary_continuity_ok",
        "cross_validation_ok", "every_formal_traceable_to_window",
        "rejected_zero_formal_growth"))
    return {
        "mode": mode,
        "config": dict(cfg),
        "checks": checks,
        "boundaries": boundaries,
        "continuity": continuity,
        "overlap_violations": overlap_bad,
        "cross_validation_card": cross,
        "boundary_changes": boundary_changes,
        "cut_audit": cuts,
        "gate_decisions": decisions,
    }
