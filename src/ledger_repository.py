"""共享容量账的 SQLite 存储：合约、合约版本、赔案、占用、批次、审计、待复核、保留输入。

设计要点：
- 任何写入都属于一个 ledger_batches 批次（open -> complete / abandoned）。
- 容量视图只认 complete 批次里的 reserved/settled 占用；open 批次里的行对容量账不可见。
- 批次内每条 insert/update 都写入 ledger_batch_mutations；入库中断后，
  recover_open_batches() 逆序撤回最近未完成批次，回到最近完整批次的状态。
"""
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .domain import Conflict, NotFound

CONTRACTS = "ledger_contracts"
VERSIONS = "ledger_contract_versions"
CLAIMS = "ledger_claims"
OCCUPATIONS = "ledger_occupations"
BATCHES = "ledger_batches"
AUDIT = "ledger_audit"
REVIEWS = "ledger_reviews"
RETAINED = "ledger_retained_inputs"
MUTATIONS = "ledger_batch_mutations"

LEDGER_TABLES = (CONTRACTS, VERSIONS, CLAIMS, OCCUPATIONS, AUDIT, REVIEWS, RETAINED)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class LedgerRepository:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        # 服务进程内的写串行化：先到版本生效，后到者按版本冲突处理。
        self._pending_crash: Optional[str] = None
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=15, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 15000")
        return connection

    def _init_schema(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS ledger_batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    request_id TEXT NOT NULL UNIQUE,
                    batch_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    result_json TEXT,
                    created_at TEXT NOT NULL,
                    completed_at TEXT
                );
                CREATE TABLE IF NOT EXISTS ledger_contracts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    reference TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL DEFAULT '',
                    active_version INTEGER NOT NULL DEFAULT 0,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ledger_contract_versions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    contract_id INTEGER NOT NULL REFERENCES ledger_contracts(id),
                    version_no INTEGER NOT NULL,
                    attachment REAL NOT NULL,
                    limit_amount REAL NOT NULL,
                    layer_width REAL NOT NULL,
                    cession_pct REAL NOT NULL,
                    reinstatement_pct REAL NOT NULL,
                    max_reinstatements INTEGER NOT NULL,
                    created_batch_id INTEGER REFERENCES ledger_batches(id),
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(contract_id, version_no)
                );
                CREATE TABLE IF NOT EXISTS ledger_claims (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    contract_id INTEGER NOT NULL REFERENCES ledger_contracts(id),
                    claim_number TEXT NOT NULL UNIQUE,
                    state TEXT NOT NULL,
                    approved_loss REAL,
                    claim_version INTEGER NOT NULL DEFAULT 1,
                    current_occupation_id INTEGER,
                    is_legacy INTEGER NOT NULL DEFAULT 0,
                    legacy_batch_no TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ledger_occupations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    contract_id INTEGER NOT NULL REFERENCES ledger_contracts(id),
                    claim_id INTEGER NOT NULL REFERENCES ledger_claims(id),
                    batch_id INTEGER NOT NULL REFERENCES ledger_batches(id),
                    status TEXT NOT NULL,
                    amount REAL NOT NULL,
                    reinstatement_premium REAL NOT NULL,
                    basis_json TEXT NOT NULL,
                    superseded_by INTEGER,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ledger_audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id INTEGER REFERENCES ledger_batches(id),
                    contract_id INTEGER REFERENCES ledger_contracts(id),
                    claim_id INTEGER REFERENCES ledger_claims(id),
                    action TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    details_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ledger_reviews (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    contract_id INTEGER REFERENCES ledger_contracts(id),
                    claim_id INTEGER REFERENCES ledger_claims(id),
                    occupation_id INTEGER REFERENCES ledger_occupations(id),
                    review_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    details_json TEXT NOT NULL,
                    resolution_json TEXT,
                    created_batch_id INTEGER,
                    resolved_batch_id INTEGER,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    resolved_at TEXT
                );
                CREATE TABLE IF NOT EXISTS ledger_retained_inputs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_type TEXT NOT NULL,
                    entity_id INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    input_json TEXT NOT NULL,
                    expected_version INTEGER,
                    current_version INTEGER,
                    actor_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    created_batch_id INTEGER,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ledger_batch_mutations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id INTEGER NOT NULL REFERENCES ledger_batches(id),
                    kind TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    row_id INTEGER NOT NULL,
                    before_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_lc_occ_contract ON ledger_occupations(contract_id, status);
                CREATE INDEX IF NOT EXISTS idx_lc_claims_contract ON ledger_claims(contract_id);
                CREATE INDEX IF NOT EXISTS idx_lc_audit_contract ON ledger_audit(contract_id, id);
                CREATE INDEX IF NOT EXISTS idx_lc_audit_claim ON ledger_audit(claim_id, id);
                CREATE INDEX IF NOT EXISTS idx_lc_reviews_status ON ledger_reviews(status);
                CREATE INDEX IF NOT EXISTS idx_lc_mutations_batch ON ledger_batch_mutations(batch_id, id);
                """
            )

    # ---------- 基础工具 ----------
    @staticmethod
    def _parse_json_fields(row: sqlite3.Row, fields: tuple) -> Dict[str, Any]:
        item = dict(row)
        for field in fields:
            if item.get(field) is not None:
                item[field] = json.loads(item[field])
        return item

    def _crash_point(self, marker: str) -> None:
        """测试用：在指定写入点之后模拟入库失败，留下 open 批次。"""
        if self._pending_crash == marker:
            self._pending_crash = None
            from .domain import StorageFailure
            raise StorageFailure("入库中断（模拟）：批次未完成，需从最近完整批次恢复")

    def _log_mutation(self, conn: sqlite3.Connection, batch_id: int, kind: str, table: str, row_id: int, before: Dict[str, Any]) -> None:
        conn.execute(
            "INSERT INTO %s(batch_id,kind,table_name,row_id,before_json,created_at) VALUES(?,?,?,?,?,?)" % MUTATIONS,
            (batch_id, kind, table, row_id, json.dumps(before, ensure_ascii=False, sort_keys=True), _now()),
        )

    def insert_row(self, conn: sqlite3.Connection, batch_id: int, table: str, fields: Dict[str, Any]) -> int:
        columns = list(fields.keys())
        sql = "INSERT INTO %s(%s) VALUES(%s)" % (table, ",".join(columns), ",".join("?" for _ in columns))
        cursor = conn.execute(sql, tuple(fields[col] for col in columns))
        row_id = int(cursor.lastrowid)
        if batch_id:
            self._log_mutation(conn, batch_id, "insert", table, row_id, {})
        return row_id

    def update_row(self, conn: sqlite3.Connection, batch_id: int, table: str, row_id: int, changes: Dict[str, Any]) -> None:
        current = conn.execute("SELECT * FROM %s WHERE id=?" % table, (row_id,)).fetchone()
        if current is None:
            raise NotFound("%s#%s不存在" % (table, row_id))
        before = {key: current[key] for key in changes}
        self._log_mutation(conn, batch_id, "update", table, row_id, before)
        sets = ",".join("%s=?" % key for key in changes)
        conn.execute("UPDATE %s SET %s WHERE id=?" % (table, sets), tuple(changes.values()) + (row_id,))

    # ---------- 批次生命周期 ----------
    # 两阶段：先独立提交一行 open 意图（崩溃后仍在），业务写入再进 BEGIN IMMEDIATE
    # 事务。事务崩溃整体回滚，恢复时把残留 open 批次标记 abandoned；已 complete
    # 的批次凭 request_id 幂等返回原结果。
    def find_batch(self, request_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM %s WHERE request_id=?" % BATCHES, (request_id,)).fetchone()
        if row is None:
            return None
        return self._parse_json_fields(row, ("result_json",))

    def open_batch(self, request_id: str, batch_type: str, actor_id: str, reuse_id: Optional[int] = None) -> tuple:
        conn = self._connect()  # autocommit
        now = _now()
        if reuse_id is None:
            cursor = conn.execute(
                "INSERT INTO %s(request_id,batch_type,status,actor_id,created_at) VALUES(?,?,?,?,?)" % BATCHES,
                (request_id, batch_type, "open", actor_id, now),
            )
            batch_id = int(cursor.lastrowid)
        else:
            conn.execute(
                "UPDATE %s SET status='open',result_json=NULL,batch_type=?,actor_id=?,completed_at=NULL WHERE id=?" % BATCHES,
                (batch_type, actor_id, reuse_id),
            )
            batch_id = reuse_id
        conn.execute("BEGIN IMMEDIATE")  # 行级写锁：同库并发批次在此排队
        return batch_id, conn

    def complete_batch(self, conn: sqlite3.Connection, batch_id: int, result: Dict[str, Any], crash_marker: str = "") -> Dict[str, Any]:
        conn.execute(
            "UPDATE %s SET status='complete',result_json=?,completed_at=? WHERE id=?" % BATCHES,
            (json.dumps(result, ensure_ascii=False, sort_keys=True), _now(), batch_id),
        )
        if crash_marker == "before_commit":
            # 事务提交前模拟入库失败：连接关闭即整体回滚，只留下 open 意图行。
            self.trigger_crash("before_commit")
        conn.commit()
        if crash_marker == "after_commit":
            # 已提交但响应丢失：重试必须凭 request_id 拿回同一结果。
            self.trigger_crash("after_commit")
        conn.close()
        return result

    def trigger_crash(self, marker: str) -> None:
        if self._pending_crash == marker:
            self._pending_crash = None
            from .domain import StorageFailure
            raise StorageFailure("入库中断（模拟）：批次未完成，需从最近完整批次恢复")

    def abandon_in_progress(self, conn: sqlite3.Connection, batch_id: int) -> None:
        """业务冲突主动放弃：回滚事务写入，仅把意图行标为 abandoned。"""
        conn.rollback()
        conn.execute("UPDATE %s SET status='abandoned',completed_at=? WHERE id=?" % BATCHES, (_now(), batch_id))
        conn.close()

    def close_failed(self, conn: sqlite3.Connection) -> None:
        """异常出口：确保连接关闭，未提交事务随连接回滚。"""
        try:
            conn.close()
        except sqlite3.Error:
            pass

    def _undo_batch(self, conn: sqlite3.Connection, batch_id: int) -> None:
        """按变更日志逆序撤回一个批次残留的已落库写入，不追加任何审计。"""
        mutations = conn.execute(
            "SELECT * FROM %s WHERE batch_id=? ORDER BY id DESC" % MUTATIONS, (batch_id,)
        ).fetchall()
        for mutation in mutations:
            table = mutation["table_name"]
            row_id = mutation["row_id"]
            if mutation["kind"] == "insert":
                conn.execute("DELETE FROM %s WHERE id=?" % table, (row_id,))
            else:
                before = json.loads(mutation["before_json"])
                sets = ",".join("%s=?" % key for key in before)
                conn.execute("UPDATE %s SET %s WHERE id=?" % (table, sets), tuple(before.values()) + (row_id,))

    def recover_open_batches(self) -> List[Dict[str, Any]]:
        """把所有残留 open 批次撤回到最近完整批次。幂等，启动/每次写之前调用。"""
        recovered: List[Dict[str, Any]] = []
        conn = self._connect()
        rows = conn.execute("SELECT * FROM %s WHERE status='open' ORDER BY id" % BATCHES).fetchall()
        for row in rows:
            self._undo_batch(conn, int(row["id"]))
            conn.execute(
                "UPDATE %s SET status='abandoned',completed_at=? WHERE id=?" % BATCHES,
                (_now(), int(row["id"])),
            )
            recovered.append({"batch_id": int(row["id"]), "request_id": row["request_id"], "batch_type": row["batch_type"]})
        conn.close()
        return recovered

    # ---------- 合约 / 版本 ----------
    def insert_contract(self, conn: sqlite3.Connection, batch_id: int, reference: str, name: str, actor_id: str) -> int:
        now = _now()
        return self.insert_row(conn, batch_id, CONTRACTS, {
            "reference": reference, "name": name, "active_version": 0,
            "created_by": actor_id, "created_at": now, "updated_at": now,
        })

    def insert_version(self, conn: sqlite3.Connection, batch_id: int, contract_id: int, version_no: int, terms: Dict[str, Any], actor_id: str) -> int:
        return self.insert_row(conn, batch_id, VERSIONS, {
            "contract_id": contract_id, "version_no": version_no,
            "attachment": terms["attachment"], "limit_amount": terms["limit"],
            "layer_width": terms["layer_width"], "cession_pct": terms["cession_pct"],
            "reinstatement_pct": terms["reinstatement_pct"], "max_reinstatements": terms["max_reinstatements"],
            "created_batch_id": batch_id, "created_by": actor_id, "created_at": _now(),
        })

    def get_contract(self, contract_id: int) -> Dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM %s WHERE id=?" % CONTRACTS, (contract_id,)).fetchone()
        if row is None:
            raise NotFound("合约不存在")
        return dict(row)

    def get_contract_by_reference(self, reference: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM %s WHERE reference=?" % CONTRACTS, (reference,)).fetchone()
        return dict(row) if row else None

    def list_contracts(self, limit: int = 100) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM %s ORDER BY id DESC LIMIT ?" % CONTRACTS, (limit,)).fetchall()
        return [dict(row) for row in rows]

    def get_version(self, contract_id: int, version_no: int) -> Dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM %s WHERE contract_id=? AND version_no=?" % VERSIONS, (contract_id, version_no)
            ).fetchone()
        if row is None:
            raise NotFound("合约版本v%s不存在" % version_no)
        return dict(row)

    def list_versions(self, contract_id: int) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM %s WHERE contract_id=? ORDER BY version_no" % VERSIONS, (contract_id,)
            ).fetchall()
        return [dict(row) for row in rows]

    def version_terms(self, version_row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "contract_version": int(version_row["version_no"]),
            "attachment": float(version_row["attachment"]),
            "limit": float(version_row["limit_amount"]),
            "layer_width": float(version_row["layer_width"]),
            "cession_pct": float(version_row["cession_pct"]),
            "reinstatement_pct": float(version_row["reinstatement_pct"]),
            "max_reinstatements": int(version_row["max_reinstatements"]),
        }

    # ---------- 赔案 ----------
    def insert_claim(self, conn, batch_id: int, contract_id: int, claim_number: str, actor_id: str,
                     approved_loss: Optional[float], is_legacy: bool, legacy_batch_no: Optional[str],
                     state: str = "submitted") -> int:
        now = _now()
        return self.insert_row(conn, batch_id, CLAIMS, {
            "contract_id": contract_id, "claim_number": claim_number, "state": state,
            "approved_loss": approved_loss, "claim_version": 1, "current_occupation_id": None,
            "is_legacy": 1 if is_legacy else 0, "legacy_batch_no": legacy_batch_no,
            "created_by": actor_id, "created_at": now, "updated_at": now,
        })

    def get_claim(self, claim_id: int) -> Dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM %s WHERE id=?" % CLAIMS, (claim_id,)).fetchone()
        if row is None:
            raise NotFound("赔案不存在")
        return dict(row)

    def get_claim_by_number(self, claim_number: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM %s WHERE claim_number=?" % CLAIMS, (claim_number,)).fetchone()
        return dict(row) if row else None

    def list_claims(self, contract_id: Optional[int] = None, limit: int = 100) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        with self._connect() as conn:
            if contract_id is None:
                rows = conn.execute("SELECT * FROM %s ORDER BY id DESC LIMIT ?" % CLAIMS, (limit,)).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM %s WHERE contract_id=? ORDER BY id DESC LIMIT ?" % CLAIMS, (contract_id, limit)
                ).fetchall()
        return [dict(row) for row in rows]

    def legacy_claims_needing_backfill(self, conn) -> List[Dict[str, Any]]:
        # 已待核的赔案已有待复核单，等人工处理；回填只捞尚无占用且未待核的旧赔案。
        rows = conn.execute(
            "SELECT * FROM %s WHERE is_legacy=1 AND state NOT IN "
            "('rejected','settled','pending_verification') AND current_occupation_id IS NULL ORDER BY id"
            % CLAIMS
        ).fetchall()
        return [dict(row) for row in rows]

    def update_loss_direct(self, claim_id: int, approved_loss: float) -> None:
        """人工补录旧赔案的核定金额（待核处理入口）。"""
        with self._connect() as conn:
            conn.execute("UPDATE %s SET approved_loss=?,updated_at=? WHERE id=?" % CLAIMS,
                         (float(approved_loss), _now(), claim_id))

    # ---------- 占用（容量账行） ----------
    def insert_occupation(self, conn, batch_id: int, contract_id: int, claim_id: int, status: str,
                          amount: float, premium: float, basis: Dict[str, Any]) -> int:
        return self.insert_row(conn, batch_id, OCCUPATIONS, {
            "contract_id": contract_id, "claim_id": claim_id, "batch_id": batch_id,
            "status": status, "amount": amount, "reinstatement_premium": premium,
            "basis_json": json.dumps(basis, ensure_ascii=False, sort_keys=True),
            "superseded_by": None, "created_at": _now(),
        })

    def conn_get_occupation(self, conn, occupation_id: int) -> Dict[str, Any]:
        row = conn.execute("SELECT * FROM %s WHERE id=?" % OCCUPATIONS, (occupation_id,)).fetchone()
        if row is None:
            raise NotFound("占用不存在")
        return self._parse_json_fields(row, ("basis_json",))

    def get_occupation(self, occupation_id: int) -> Dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM %s WHERE id=?" % OCCUPATIONS, (occupation_id,)).fetchone()
        if row is None:
            raise NotFound("占用不存在")
        return self._parse_json_fields(row, ("basis_json",))

    def active_occupations(self, contract_id: int, exclude_claim_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """容量账口径（读侧）：只统计 complete 批次里 reserved/settled 的占用。"""
        sql = (
            "SELECT o.* FROM %s o JOIN %s b ON b.id=o.batch_id "
            "WHERE o.contract_id=? AND b.status='complete' AND o.status IN ('reserved','settled')"
        ) % (OCCUPATIONS, BATCHES)
        params: List[Any] = [contract_id]
        if exclude_claim_id is not None:
            sql += " AND o.claim_id<>?"
            params.append(exclude_claim_id)
        sql += " ORDER BY o.id"
        with self._connect() as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
        return [self._parse_json_fields(row, ("basis_json",)) for row in rows]

    def conn_active_occupations(self, conn, contract_id: int) -> List[Dict[str, Any]]:
        """写侧口径：同一事务连接可见已提交占用和本批次刚预占的占用；
        其它 open 批次未提交的行天然不可见。"""
        rows = conn.execute(
            "SELECT * FROM %s WHERE contract_id=? AND status IN ('reserved','settled') ORDER BY id"
            % OCCUPATIONS,
            (contract_id,),
        ).fetchall()
        return [self._parse_json_fields(row, ("basis_json",)) for row in rows]

    def conn_all_occupations(self, conn, contract_id: int) -> List[Dict[str, Any]]:
        rows = conn.execute("SELECT * FROM %s WHERE contract_id=? ORDER BY id" % OCCUPATIONS, (contract_id,)).fetchall()
        return [self._parse_json_fields(row, ("basis_json",)) for row in rows]

    def conn_get(self, conn, table: str, row_id: int) -> Dict[str, Any]:
        row = conn.execute("SELECT * FROM %s WHERE id=?" % table, (row_id,)).fetchone()
        if row is None:
            raise NotFound("%s#%s不存在" % (table, row_id))
        return dict(row)

    def conn_get_version(self, conn, contract_id: int, version_no: int) -> Dict[str, Any]:
        row = conn.execute(
            "SELECT * FROM %s WHERE contract_id=? AND version_no=?" % VERSIONS, (contract_id, version_no)
        ).fetchone()
        if row is None:
            raise NotFound("合约版本v%s不存在" % version_no)
        return dict(row)

    def get_claim_by_number_conn(self, conn, claim_number: str) -> Optional[Dict[str, Any]]:
        row = conn.execute("SELECT * FROM %s WHERE claim_number=?" % CLAIMS, (claim_number,)).fetchone()
        return dict(row) if row else None

    def list_occupations(self, contract_id: int) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT o.*, b.request_id AS batch_request_id, b.status AS batch_status FROM %s o "
                "JOIN %s b ON b.id=o.batch_id WHERE o.contract_id=? ORDER BY o.id" % (OCCUPATIONS, BATCHES),
                (contract_id,),
            ).fetchall()
        return [self._parse_json_fields(row, ("basis_json",)) for row in rows]

    # ---------- 审计 ----------
    def insert_audit(self, conn, batch_id: int, action: str, actor_id: str, details: Dict[str, Any],
                     contract_id: Optional[int] = None, claim_id: Optional[int] = None) -> int:
        return self.insert_row(conn, batch_id, AUDIT, {
            "batch_id": batch_id, "contract_id": contract_id, "claim_id": claim_id,
            "action": action, "actor_id": actor_id,
            "details_json": json.dumps(details, ensure_ascii=False, sort_keys=True), "created_at": _now(),
        })

    def audit_timeline(self, contract_id: Optional[int] = None, claim_id: Optional[int] = None) -> List[Dict[str, Any]]:
        sql = (
            "SELECT a.*, b.request_id AS batch_request_id, b.batch_type AS batch_type FROM %s a "
            "LEFT JOIN %s b ON b.id=a.batch_id WHERE 1=1"
        ) % (AUDIT, BATCHES)
        params: List[Any] = []
        if contract_id is not None:
            sql += " AND a.contract_id=?"
            params.append(contract_id)
        if claim_id is not None:
            sql += " AND a.claim_id=?"
            params.append(claim_id)
        sql += " ORDER BY a.id"
        with self._connect() as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
        return [self._parse_json_fields(row, ("details_json",)) for row in rows]

    # ---------- 待复核 ----------
    def insert_review(self, conn, batch_id: int, review_type: str, details: Dict[str, Any], actor_id: str,
                      contract_id: Optional[int] = None, claim_id: Optional[int] = None,
                      occupation_id: Optional[int] = None) -> int:
        return self.insert_row(conn, batch_id, REVIEWS, {
            "contract_id": contract_id, "claim_id": claim_id, "occupation_id": occupation_id,
            "review_type": review_type, "status": "open",
            "details_json": json.dumps(details, ensure_ascii=False, sort_keys=True),
            "resolution_json": None, "created_batch_id": batch_id, "resolved_batch_id": None,
            "created_by": actor_id, "created_at": _now(), "resolved_at": None,
        })

    def get_review(self, review_id: int) -> Dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM %s WHERE id=?" % REVIEWS, (review_id,)).fetchone()
        if row is None:
            raise NotFound("待复核记录不存在")
        return self._parse_json_fields(row, ("details_json", "resolution_json"))

    def list_reviews(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            if status:
                rows = conn.execute("SELECT * FROM %s WHERE status=? ORDER BY id" % REVIEWS, (status,)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM %s ORDER BY id" % REVIEWS).fetchall()
        return [self._parse_json_fields(row, ("details_json", "resolution_json")) for row in rows]

    # ---------- 冲突时保留输入 ----------
    def insert_retained_row(self, conn, batch_id: Optional[int], entity_type: str, entity_id: int, action: str,
                            data: Dict[str, Any], expected_version: Optional[int], current_version: int,
                            actor_id: str, reason: str) -> int:
        return self.insert_row(conn, batch_id if batch_id is not None else 0, RETAINED, {
            "entity_type": entity_type, "entity_id": entity_id, "action": action,
            "input_json": json.dumps(data or {}, ensure_ascii=False),
            "expected_version": expected_version, "current_version": current_version,
            "actor_id": actor_id, "status": "retained", "reason": reason,
            "created_batch_id": batch_id, "created_at": _now(),
        })

    def insert_retained(self, entity_type: str, entity_id: int, action: str, data: Dict[str, Any],
                        expected_version: Optional[int], current_version: int, actor_id: str, reason: str,
                        batch_id: Optional[int] = None) -> int:
        """独立落库：业务批次撤回后，后到者的输入依然保留。"""
        with self._connect() as conn:
            return self.insert_retained_row(
                conn, batch_id, entity_type, entity_id, action, data,
                expected_version, current_version, actor_id, reason,
            )

    def get_retained(self, retained_id: int) -> Dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM %s WHERE id=?" % RETAINED, (retained_id,)).fetchone()
        if row is None:
            raise NotFound("保留输入不存在")
        return self._parse_json_fields(row, ("input_json",))

    def list_retained(self, status: str = "retained") -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM %s WHERE status=? ORDER BY id" % RETAINED, (status,)).fetchall()
        return [self._parse_json_fields(row, ("input_json",)) for row in rows]

    def mark_retained(self, conn, batch_id: int, retained_id: int, status: str) -> None:
        self.update_row(conn, batch_id, RETAINED, retained_id, {"status": status})
