
import hashlib, secrets, sqlite3
from pathlib import Path
from fastapi import Header, HTTPException
from pydantic import BaseModel

import cutover
from cutover import (connect, initialize as cutover_initialize, cutover as do_cutover,
                     close_old_windows, remap as gate_remap, certification,
                     active_window, CutoverError, ATOMIC, DUAL)

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "static" if (ROOT / "static").exists() else ROOT / "frontend"
SESSIONS: dict[str, str] = {}

# 本部署贯彻的正确性口径：甲=原子单窗（默认）。改口径只能换新库，
# 已初始化的库被 cutover_config 触发器冻结，传另一口径会直接报错。
CUTOVER_MODE = ATOMIC

def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()

def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))

class LoginIn(BaseModel):
    username: str
    password: str

def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u

def require_admin(authorization: str | None) -> str:
    u = require_user(authorization)
    with connect(DB_PATH) as c:
        role = c.execute("SELECT role FROM users WHERE username=?", (u,)).fetchone()["role"]
    if role != "管理员":
        raise HTTPException(403, "切版仅管理员可操作")
    return u

from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

PRODUCT = "实体键桥"
SKELETON = "实体键映射生效后入站先重映再进正式区，冲突进队列，映射版本与正式区对账"

app = FastAPI(title=PRODUCT)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"])

def init_db(mode: str | None = None):
    """建表、锁口径、补登首个生效窗（幂等）。"""
    DATA.mkdir(parents=True, exist_ok=True)
    chosen = mode or CUTOVER_MODE
    with connect(DB_PATH) as conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS map_versions(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          version TEXT UNIQUE NOT NULL,
          status TEXT NOT NULL,
          effective_from TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS mappings(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          version TEXT NOT NULL,
          old_key TEXT NOT NULL,
          new_key TEXT NOT NULL,
          kind TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS formal_entities(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          entity_key TEXT NOT NULL,
          mapped_from TEXT NOT NULL,
          version TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS conflicts(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          old_key TEXT NOT NULL,
          reason TEXT NOT NULL,
          status TEXT NOT NULL
        );
        """)
        if conn.execute("SELECT COUNT(*) c FROM map_versions").fetchone()["c"] == 0:
            conn.execute(
                "INSERT INTO map_versions(version,status,effective_from) VALUES (?,?,?)",
                ("m-2026.11", "生效", "2026-11-01T00:00:00+00:00"))
            conn.executemany(
                "INSERT INTO mappings(version,old_key,new_key,kind) VALUES (?,?,?,?)", [
                    ("m-2026.11", "OLD-A1", "NEW-100", "一对一"),
                    ("m-2026.11", "OLD-B1", "NEW-200", "多对一"),
                    ("m-2026.11", "OLD-B2", "NEW-200", "多对一"),
                ])
            conn.execute(
                "INSERT INTO conflicts(old_key,reason,status) VALUES (?,?,?)",
                ("OLD-C9", "无映射", "挂起"))
        conn.commit()
    cutover_initialize(DB_PATH, chosen)


@app.on_event("startup")
def _s(): init_db()

@app.get("/api/health")
def health():
    with connect(DB_PATH) as c:
        n = c.execute("SELECT COUNT(*) c FROM mappings").fetchone()["c"]
        mode = c.execute("SELECT mode FROM cutover_config").fetchone()
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "mappings": n,
            "cutover_mode": mode["mode"] if mode else None, "db": str(DB_PATH)}

@app.post("/api/login")
def login(body: LoginIn):
    with connect(DB_PATH) as c:
        row = c.execute(
            "SELECT username,role FROM users WHERE username=? AND password_sha=?",
            (body.username, sha(body.password))).fetchone()
    if not row: raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16); SESSIONS[tok] = row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}

@app.get("/api/map-versions")
def versions(authorization: str | None = Header(default=None)):
    """映射册：status 由生效窗登记表派生，徽标不再是真相源。"""
    require_user(authorization)
    with connect(DB_PATH) as c:
        items = [dict(r) for r in c.execute("""
            SELECT v.id, v.version,
                   CASE WHEN w.effective_to IS NULL THEN '生效' ELSE '已切出' END AS status,
                   v.effective_from,
                   w.id AS window_id, w.effective_from AS window_from,
                   w.effective_to AS window_to, w.cut_id
            FROM map_versions v
            LEFT JOIN effective_windows w ON w.version=v.version
            ORDER BY v.id""").fetchall()]
    return {"items": items}

@app.get("/api/mappings")
def mappings(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect(DB_PATH) as c:
        return {"items": [dict(r) for r in c.execute(
            "SELECT * FROM mappings ORDER BY id").fetchall()]}

class VersionIn(BaseModel):
    version: str
    effective_from: str | None = None
    mappings: list[dict] | None = None  # [{old_key,new_key,kind}]

@app.post("/api/map-versions")
def register_version(body: VersionIn, authorization: str | None = Header(default=None)):
    """新映射册版本登记（注册不等于生效；生效只经由 /api/cutover 物化开窗）。"""
    require_admin(authorization)
    with connect(DB_PATH) as c:
        try:
            c.execute(
                "INSERT INTO map_versions(version,status,effective_from) VALUES (?,?,?)",
                (body.version, "待切版", body.effective_from or cutover.now_ts()))
            if body.mappings:
                c.executemany(
                    "INSERT INTO mappings(version,old_key,new_key,kind) VALUES (?,?,?,?)",
                    [(body.version, m["old_key"], m["new_key"], m.get("kind", "一对一"))
                     for m in body.mappings])
            c.commit()
        except sqlite3.IntegrityError:
            raise HTTPException(409, f"版本 {body.version} 已存在")
    return {"registered": body.version, "note": "登记完成，须经切版开窗后方可命中"}

class CutoverIn(BaseModel):
    to_version: str
    effective_at: str | None = None
    close_old: bool | None = None
    note: str | None = ""

@app.post("/api/cutover")
def cut(body: CutoverIn, authorization: str | None = Header(default=None)):
    """切版：物化生效窗边界。甲路径同事务关旧开新；乙路径可短重叠。"""
    require_admin(authorization)
    with connect(DB_PATH) as c:
        try:
            result = do_cutover(c, body.to_version, effective_at=body.effective_at,
                                close_old=body.close_old, note=body.note or "")
        except CutoverError as e:
            raise HTTPException(409, str(e))
    return result

class CloseIn(BaseModel):
    effective_at: str | None = None

@app.post("/api/cutover/close-old")
def close_old(body: CloseIn, authorization: str | None = Header(default=None)):
    """乙路径专用：关闭并列旧窗收口。"""
    require_admin(authorization)
    with connect(DB_PATH) as c:
        try:
            return close_old_windows(c, body.effective_at)
        except CutoverError as e:
            raise HTTPException(409, str(e))

class RemapIn(BaseModel):
    old_key: str
    event_time: str | None = None

@app.post("/api/remap")
def remap(body: RemapIn, authorization: str | None = Header(default=None)):
    """重映闸：按事件时刻唯一命中生效窗，写 window_hit 快照；失败正式零增。"""
    require_user(authorization)
    with connect(DB_PATH) as c:
        try:
            out = gate_remap(c, body.old_key, body.event_time)
        except CutoverError as e:
            raise HTTPException(409, str(e))
    return out

@app.get("/api/formal")
def formal(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect(DB_PATH) as c:
        items = [dict(r) for r in c.execute("""
            SELECT f.*, fv.window_id, fv.decision_id
            FROM formal_entities f
            LEFT JOIN formal_entities_v2 fv
              ON fv.entity_key=f.entity_key AND fv.mapped_from=f.mapped_from
             AND fv.created_at=f.created_at
            ORDER BY f.id DESC""").fetchall()]
    return {"items": items}

@app.get("/api/conflicts")
def conflicts(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect(DB_PATH) as c:
        return {"items": [dict(r) for r in c.execute(
            "SELECT * FROM conflicts ORDER BY id DESC").fetchall()]}

@app.get("/api/certification")
def certify(authorization: str | None = Header(default=None)):
    """切版边界核证只读接口：映射册/重映闸/正式区/互证卡/切版审计页协同。"""
    require_user(authorization)
    with connect(DB_PATH) as c:
        return certification(c)

@app.get("/api/gate-decisions")
def gate(authorization: str | None = Header(default=None)):
    """重映闸流水：window_hit 快照可逐笔回查。"""
    require_user(authorization)
    with connect(DB_PATH) as c:
        return {"items": [dict(r) for r in c.execute(
            "SELECT * FROM gate_decisions ORDER BY decision_id DESC").fetchall()]}

@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")
app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
