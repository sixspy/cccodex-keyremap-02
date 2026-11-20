# 实体键桥

> 遵守 `ops/底座生成前必读_效率与技术栈.md`。FastAPI + Vue 3 + SQLite，交付须 docker compose 一键起；库用 SQLite，不挂外置 Postgres。  
> **禁止同构换皮**：本仓主链路、表结构、接口与页面均独立，不得与其它仓共用同一套 `/api/items` 壳。

## 骨架一句话

实体键映射生效后入站先重映再进正式区，冲突进队列，映射版本与正式区对账

## 基本信息

| 项 | 值 |
|----|-----|
| 产品名（首页） | 实体键桥 |
| 目录 | `07-keyremap` |
| 领域建议 | 数仓与数据湖 |
| 接口 | http://127.0.0.1:10912 |
| 库 | `data/app.db` |
| 账号 | admin / 123456 |

## 为何能出多题

一对多/多对一、生效窗、旧键只读、重映重放

## 避开

别做成 Feed 冲突归并；正式区不是 staging 换皮

## 互斥切口候选

- 冲突拒收整批 vs 进冲突区后重放
- 废弃旧键只读保留 vs 重写正式键
- 生效窗原子切换 vs 双窗按事件时裁

## 切版生效边界（version_cutover）

映射版切换不再靠页面「生效」徽标，而是物化可核证的生效窗边界，落在 `backend/cutover.py`：

- **生效窗登记表** `effective_windows(version, effective_from, effective_to, cut_id)`（行不可变，关窗只许 NULL→时刻一次，触发器禁删禁重开）+ 切版审计台账 `cutover_ledger` + 重映闸流水 `gate_decisions`（含 `window_hit` JSON 快照）。
- **口径二选一且全库锁定**：`cutover_config.mode` 一经初始化不可变（触发器禁 UPDATE/DELETE），`atomic`（甲，原子单窗，默认）与 `dual`（乙，双窗按事件时刻裁）禁止混用。
  - 甲：切版同事务 `BEGIN IMMEDIATE` 先关旧窗再开新窗，任一失败两版状态皆回滚；部分唯一索引保证任意时刻开窗数 ≤ 1，旧版切后不可能再命中进正式区。
  - 乙：允许有界短重叠，切版同事务给旧窗物化计划收口点（`at + max_overlap_seconds`），每笔重映按事件时刻落入唯一窗并写 `window_hit`；并列窗宁拒不猜，失败笔正式零增（只留拒收决策+冲突队列）。
- **并发双切**：写锁串行化 + `cutover_ledger(to_version)` 唯一约束，同一目标版只有一切胜出，落败方收到明确争用错误，无无锁双写。
- **只读核证** `GET /api/certification`：六项检查（单窗/无界双生效/边界连续性/窗内互证分叉/正式区可回指窗/拒收零增），映射册、重映闸、正式区、互证卡、切版审计页共用同一数据源。
- 重映 `POST /api/remap` 只认生效窗唯一命中，不再读 `map_versions.status` 徽标；`GET /api/map-versions` 的 status 由窗派生。

接口：`POST /api/map-versions`（登记新版，管理员）、`POST /api/cutover`（切版开窗，管理员）、
`POST /api/cutover/close-old`（乙路径收口）、`POST /api/remap`（重映闸）、
`GET /api/gate-decisions`（闸流水）、`GET /api/certification`（只读核证）。

### 本地 SQLite 夹具

```powershell
python -m pytest tests/test_cutover_fixtures.py -q   # 需 pytest 环境
python tests/fixtures_runner.py                      # 零依赖，标准库 unittest
python fixtures/build_fixtures.py                    # 产出 fixtures/generated/*.db 可直接核查
```

夹具覆盖：原子切后仅新版可映且失败整体回滚；双窗按事件时刻落唯一窗且 window_hit 可查、失败零增；
并发双切只留争用；无界双生效核证红灯并阻断后续切版；口径锁定禁混用。

## 启动（一键）

`powershell
cd projects/07-keyremap
docker compose up -d --build
`

浏览器：http://127.0.0.1:10912/  
健康检查：GET http://127.0.0.1:10912/api/health

本机开发（可选）：

`powershell
python -m pip install -r requirements.txt
python -m uvicorn app:app --app-dir backend --host 127.0.0.1 --port 10604
python -m pytest -q
`
