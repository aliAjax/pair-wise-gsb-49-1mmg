# 再保险合约与巨灾暴露管理

纯Python标准库实现的再保险合约与巨灾暴露管理原型，使用SQLite持久化，HTTP接口由`http.server`提供。

## 模块结构

- `app.py`：命令行参数、依赖组装和服务启动。
- `src/domain.py`：领域数据类型、错误和基础校验。
- `src/rules.py`：状态转换、分层摊回、赔偿限额和恢复保费和冲突检查。
- `src/repository.py`：SQLite建表、事务和查询。
- `src/service.py`：用例编排、权限检查、乐观并发和审计。
- `src/ledger_rules.py`：共享容量账纯规则（合约版本、预占分层、恢复次数、差额）。
- `src/ledger_repository.py`：容量账存储（合约/版本/赔案/占用/批次/审计/待复核/保留输入）。
- `src/ledger_service.py`：容量账用例编排、批次幂等、并发冲突与失败恢复。
- `src/http_api.py`：HTTP路由与统一错误响应。
- `src/audit.py`：事件时间线。
- `static/index.html`：最小演示页面。
- `tests/`：完整流程、规则计算和失败场景测试。

## 共享容量账（再保合约 / 赔案 / 占用 / 审计共用一份账）

容量账解决“按单张赔案各算各的摊回、容量与恢复次数对不上、晚到赔案冲掉已结算依据”的问题：

- **先预占、后结算**：赔案 `calculate` 时按当前合约版本预占保额，占用行带版本号与条款快照；`settle` 只认可这笔 `reserved` 占用，预占失效后无法结算。
- **多赔案共用一本容量账**：单层容量 = 层宽 × 分出比例；每恢复一次得到一个同等容量层。各赔案按顺序逐层占用，单层放不下或超过最大恢复次数即返回 `capacity_exceeded`。
- **拒赔 / 金额改小**：未结算的旧占用置 `stale`、释放保额，新预占生效；结算只认新占用。
- **合约版本变化**：未结算占用全部失效、赔案进入 `recalculate_required` 重算；已结算占用保留当时条款依据，新旧差额写入**待复核**（`ledger_reviews`），只能确认关闭、不覆盖已结算金额。
- **两窗口并发**：写操作在批次上串行，先到版本生效；后到者收到 `409 optimistic_conflict`，其原始输入写入**保留输入**（`ledger_retained_inputs`），刷新后可调重放接口重提交。
- **批次化 + 幂等**：每个写请求带 `request_id`，同号重放只返回首次结果，不重复占额、不追加审计。入库中断后，启动时（及每次写之前）自动把残留 `open` 批次撤回到最近完整批次；提交前失败整体回滚，提交后失败凭 `request_id` 拿回结果。
- **旧赔案回填**：缺占用批次号的旧赔案按现有金额回填预占；金额算不清或容量不足时进入 `pending_verification` 并生成待复核，不强行入账。

### 容量账接口（均需 X-User-Id / X-Role，写请求建议带 request_id）

- `POST /api/ledger/contracts`：创建合约（v1 条款），`{"reference","request_id","data":{attachment,limit,cession_pct,reinstatement_pct,max_reinstatements}}`
- `GET  /api/ledger/contracts` / `GET /api/ledger/contracts/{id}`：合约与容量账汇总（各层占用、已用恢复次数、余额）。
- `POST /api/ledger/contracts/{id}/adjust`：调整合约生成新版本，`{"expected_version","data":{条款}}`。
- `POST /api/ledger/contracts/{id}/claims`：提交赔案，`{"data":{"claim_number"}}`。
- `GET  /api/ledger/contracts/{id}/claims` / `.../occupations` / `.../audit`：赔案列表 / 占用行 / 合约审计。
- `POST /api/ledger/claims/{id}/calculate|settle|reject`：预占 / 结算 / 拒赔，body 带 `expected_version`（赔案版本）。
- `GET  /api/ledger/claims/{id}` / `.../audit`：赔案详情（含历史占用）/ 赔案审计。
- `GET  /api/ledger/reviews?status=open`：待复核列表；`POST /api/ledger/reviews/{id}/resolve` 处理（`recalculate` 或 `dismiss`）。
- `GET  /api/ledger/retained`：冲突保留输入；`POST /api/ledger/retained/{id}/replay` 按当前版本重放。
- `POST /api/ledger/claims/import-legacy`：导入旧赔案（可带 `legacy_batch_no`、`approved_loss`，缺金额即待核）。
- `POST /api/ledger/admin/backfill-legacy`：把可计算的旧赔案按现有金额回填容量账（幂等）。
- `POST /api/ledger/admin/recover`：手动触发未完成批次恢复。


## 启动

```bash
python3 app.py --db ./data.db --port 8325
```

默认端口为`8325`，默认数据库位于项目目录。服务启动时自动建表。

## 主要接口

- `GET /health`：健康检查。
- `GET /`：演示页面。
- `GET /api/records`：记录列表，可带`state`和`limit`参数。
- `GET /api/records/{id}`：记录详情。
- `GET /api/records/{id}/audit`：审计时间线。
- `GET /api/stats`：状态统计。
- `POST /api/records`：创建记录，请求体为`{"reference":"...","data":{...}}`。
- `POST /api/records/{id}/actions/{action}`：执行业务动作，请求体为`{"expected_version":1,"data":{...}}`。

除`/health`和`/`外，请求需提供`X-User-Id`、`X-Role`，可选`X-Org`。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

测试覆盖完整流程、规则计算、重复引用、权限拒绝和版本冲突。
