# 再保险合约容量账

纯 Python 标准库实现的再保险合约与赔案摊回管理原型，SQLite 持久化，HTTP 接口由 `http.server` 提供。

核心是一份**统一容量账**：合约版本（条款）、赔案、摊回容量占用、待复核与审计记录共用同一批数字，
所有变更在同一个批次事务里落库。

## 容量账语义

- **合约版本化**：`adjust` 生成新版本（`contract_terms.version` 递增），旧版本置 `superseded` 但保留，
  层宽、分出比例、聚合容量（`layer_capacity × (1 + max_reinstatements)`）与恢复次数上限按版本固化。
- **先预占，后结算**：赔案 `calculate` 时按当时的合约版本计算依据快照（approved_loss、起赔点、层宽、
  分出比例、摊回金额、占用的恢复次数），写入 `capacity_ledger`（状态 `reserved`）；
  `settle` 只认可赔案当前指向的那笔预占（归属、状态、版本必须一致），把同一行转为 `settled`。
- **未结算占用可失效重算**：拒赔（`reject`）释放 `reserved` 占用、容量立即归还；
  金额改小/改大（`adjust_claim`）旧预占置 `superseded`，按最新合约版本重新预占；
  合约版本变化时，所有 `reserved` 赔案按新版本重算——新版本放不下的退回 `submitted` 并进待复核。
- **已结算依据永久保留**：合约版本变化不重算 `settled` 占用；新老依据差额写入 `review_items`
  （`settled_diff`），容量账继续按结算当时的金额计数，差额只作待复核，不冲销历史。
- **并发先到先得**：合约调整带 `expected_version`，赔案计算可带 `expected_term_version`。
  两个窗口同时提交时，先到者拿到写锁生效，后到者收到 409（`conflict` 上下文里带当前版本号），
  输入保留在客户端，按最新版本重提即可。
- **批次与幂等**：每次写操作分配 `batch_no`（可用请求体 `data.batch_no` 自带）。
  占用、待复核、审计、批次完成标记在同一事务提交；入库失败整批回滚。
  进程启动和 `POST /api/admin/recover` 会：①回滚所有未完成批次及其占用/审计；
  ②从最近完整批次继续；③对缺占用批次号的旧赔案按现有金额回填——
  可核算的补记 `reserved/settled` 占用（批次号 `legacy-backfill`），
  金额或依据缺失、或回填将超容量的未结算赔案进 `review_items` 待核，
  已结算赔案即使超过当前容量也保留依据并挂差额待核。同批次号重试只回放首次结果，不重复占额、不追加审计。

## 模块结构

- `app.py`：参数解析、依赖组装、启动时自动恢复。
- `src/domain.py`：基础类型、错误（含容量超额与冲突上下文）、批次号生成。
- `src/rules.py`：条款归一化、摊回依据计算、容量/恢复次数求和与校验、状态转换与版本检查。
- `src/repository.py`：表结构、`Batch` 事务（占用/待复核/审计/批次同事务）、恢复与旧赔案回填。
- `src/service.py`：用例编排（创建/绑定/调整合约，提交/计算/调整/结算/拒赔，待复核，恢复）。
- `src/http_api.py`：HTTP 路由与统一错误响应（冲突详情在 `conflict` 字段）。
- `src/audit.py`：合约/赔案审计时间线。
- `tests/`：规则计算、完整流程、版本冲突、失败注入与恢复回填测试。

## 启动

```bash
python3 app.py --db ./data.db --port 8325
```

服务启动时自动建表并执行一次恢复（回滚残留批次、回填旧赔案）。

## 主要接口

除 `/health` 和 `/` 外均需 `X-User-Id`、`X-Role` 头。

合约：

- `POST /api/contracts`：`{"reference":"RI-1","data":{event_id,attachment,limit,cession_pct,reinstatement_pct,max_reinstatements}}`
- `POST /api/contracts/{ref}/actions/bind` / `actions/adjust`：`{"expected_version":1,"data":{...条款...}}`
- `GET /api/contracts`、`GET /api/contracts/{ref}`、`GET /api/contracts/{ref}/audit`
- `GET /api/contracts/{ref}/ledger`：统一容量账（容量、已用、可用、恢复次数、全部占用）
- `GET /api/contracts/{ref}/claims`

赔案：

- `POST /api/contracts/{ref}/claims`：`{"data":{"claim_number":"CLM-1"}}`
- `POST /api/claims/{id}/actions/calculate`：`{"expected_version":1,"data":{"approved_loss":3000000,"expected_term_version":2}}`
- `POST /api/claims/{id}/actions/adjust_claim`：修改核定金额，旧预占失效重算
- `POST /api/claims/{id}/actions/settle` / `actions/reject`
- `GET /api/claims/{id}`、`GET /api/claims/{id}/audit`

待复核与运维：

- `GET /api/reviews?status=open`、`POST /api/reviews/{id}/resolve`
- `POST /api/admin/recover`（仅 admin）
- `GET /api/stats`、`GET /health`

所有写操作可在 `data.batch_no` 携带客户端幂等键；响应中的 `batch_no` 即占用与审计共用的批次号，
`replayed:true` 表示这是同批次重试的回放结果。

## 测试

```bash
python3 -m unittest discover -s tests -v
```
