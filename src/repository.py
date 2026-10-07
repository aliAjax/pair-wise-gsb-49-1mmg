"""SQLite 表结构、批次事务与容量账访问。

所有会改变容量账的用例都通过一个 op_batch（批次）完成：
占用（capacity_ledger）、待复核、审计与批次完成标记在同一个事务里提交，
因此“入库失败”时整批回滚，不会出现占了额却没有审计、或有审计却没占额的半批。
重试同一批次号只回放首次结果，不重复占额、不追加审计。
"""
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .domain import Conflict, NotFound

LEGACY_BACKFILL_BATCH = "legacy-backfill"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _loads(value: Any, default: Any = None) -> Any:
    if value is None or value == "":
        return default
    return json.loads(value)


class Batch:
    """一次用例事务；同一批次号重放时 conn 为 None，所有写操作短路。"""

    def __init__(self, repository: "Repository", conn: Optional[sqlite3.Connection], batch_no: str,
                 op_type: str, replay: Optional[Dict[str, Any]] = None) -> None:
        self.repository = repository
        self.conn = conn
        self.batch_no = batch_no
        self.op_type = op_type
        self.replay = replay

    def __enter__(self) -> "Batch":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.conn is not None:
            if exc_type is None:
                # 未显式 finish 视为异常中断，整批回滚
                self.abort()
            else:
                self.abort()

    def q(self, sql: str, params: Sequence[Any] = ()) -> sqlite3.Cursor:
        return self.conn.execute(sql, params)

    # ---- 合约 ----
    def insert_contract(self, reference: str, event_id: str, version: int, state: str,
                        terms: Dict[str, Any], actor_id: str, contract_pk: Optional[int] = None) -> int:
        now = _now()
        cursor = self.conn.execute(
            "INSERT INTO contract_terms(contract_pk,reference,event_id,version,state,terms,created_by,updated_by,created_at,updated_at)"
            " VALUES(?,?,?,?,?,?,?,?,?,?)",
            (contract_pk, reference, event_id, version, state,
             json.dumps(terms, ensure_ascii=False, sort_keys=True), actor_id, actor_id, now, now),
        )
        term_id = int(cursor.lastrowid)
        if contract_pk is None:
            # 首个版本自引用为合约主键
            self.conn.execute("UPDATE contract_terms SET contract_pk=? WHERE id=?", (term_id, term_id))
        return term_id

    def mark_contract_active(self, term_id: int, actor_id: str) -> None:
        self.conn.execute(
            "UPDATE contract_terms SET state=?,updated_by=?,updated_at=? WHERE id=?",
            ("active", actor_id, _now(), term_id),
        )

    def supersede_prior_terms(self, contract_pk: int, actor_id: str, keep_id: Optional[int] = None) -> None:
        if keep_id is None:
            self.conn.execute(
                "UPDATE contract_terms SET state=?,updated_by=?,updated_at=? WHERE contract_pk=? AND state=?",
                ("superseded", actor_id, _now(), contract_pk, "active"),
            )
        else:
            self.conn.execute(
                "UPDATE contract_terms SET state=?,updated_by=?,updated_at=? WHERE contract_pk=? AND state=? AND id<>?",
                ("superseded", actor_id, _now(), contract_pk, "active", keep_id),
            )

    # ---- 赔案 ----
    def insert_claim(self, contract_pk: int, claim_number: str, event_id: str, actor_id: str) -> int:
        now = _now()
        cursor = self.conn.execute(
            "INSERT INTO claims(contract_pk,claim_number,event_id,state,version,created_by,updated_by,created_at,updated_at)"
            " VALUES(?,?,?,?,1,?,?,?,?)",
            (contract_pk, claim_number, event_id, "submitted", actor_id, actor_id, now, now),
        )
        return int(cursor.lastrowid)

    def update_claim(self, claim_id: int, actor_id: str, **fields: Any) -> None:
        allowed = {
            "state", "approved_loss", "recovery", "reinstatement_premium", "reinstatements_used",
            "term_id", "term_version", "occupation_id", "basis", "rejection_reason",
            "payment_reference", "version",
        }
        keys = [key for key in fields if key in allowed]
        if not keys:
            return
        assignments = ",".join("%s=?" % key for key in keys)
        params: List[Any] = []
        for key in keys:
            value = fields[key]
            if key == "basis" and value is not None:
                value = json.dumps(value, ensure_ascii=False, sort_keys=True)
            params.append(value)
        params.extend([actor_id, _now(), claim_id])
        self.conn.execute(
            "UPDATE claims SET %s,updated_by=?,updated_at=? WHERE id=?" % assignments, params
        )

    # ---- 容量账 ----
    def insert_occupation(self, contract_pk: int, claim_id: Optional[int], term_id: int, term_version: int,
                          status: str, occupied_amount: float, reinstatements_used: float,
                          reinstatement_premium: float, basis: Dict[str, Any], actor_id: str,
                          claim_number: str = "") -> int:
        now = _now()
        settled_at = now if status == "settled" else None
        cursor = self.conn.execute(
            "INSERT INTO capacity_ledger(op_batch_no,contract_pk,claim_id,claim_number,term_id,term_version,"
            "status,occupied_amount,reinstatements_used,reinstatement_premium,basis,settled_at,created_by,created_at)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (self.batch_no, contract_pk, claim_id, claim_number, term_id, term_version, status,
             round(float(occupied_amount), 2), float(reinstatements_used), round(float(reinstatement_premium), 2),
             json.dumps(basis, ensure_ascii=False, sort_keys=True), settled_at, actor_id, now),
        )
        return int(cursor.lastrowid)

    def update_occupation(self, occ_id: int, status: str, actor_id: str, superseded_by: Optional[int] = None,
                          released_reason: Optional[str] = None) -> None:
        self.conn.execute(
            "UPDATE capacity_ledger SET status=?,superseded_by=COALESCE(?,superseded_by),"
            "released_reason=COALESCE(?,released_reason) WHERE id=?",
            (status, superseded_by, released_reason, occ_id),
        )

    def settle_occupation(self, occ_id: int, actor_id: str) -> None:
        self.conn.execute(
            "UPDATE capacity_ledger SET status='settled',settled_at=? WHERE id=?",
            (_now(), occ_id),
        )

    # ---- 待复核 / 审计 ----
    def insert_review(self, kind: str, contract_pk: Optional[int], claim_id: Optional[int],
                      occupation_id: Optional[int], details: Dict[str, Any], actor_id: str,
                      from_term_version: Optional[int] = None, to_term_version: Optional[int] = None,
                      old_amount: Optional[float] = None, new_amount: Optional[float] = None,
                      diff_amount: Optional[float] = None) -> int:
        now = _now()
        cursor = self.conn.execute(
            "INSERT INTO review_items(op_batch_no,kind,status,contract_pk,claim_id,occupation_id,"
            "from_term_version,to_term_version,old_amount,new_amount,diff_amount,details,created_by,created_at)"
            " VALUES(?,?, 'open',?,?,?,?,?,?,?,?,?,?,?)",
            (self.batch_no, kind, contract_pk, claim_id, occupation_id, from_term_version, to_term_version,
             old_amount, new_amount, diff_amount,
             json.dumps(details, ensure_ascii=False, sort_keys=True), actor_id, now),
        )
        return int(cursor.lastrowid)

    def audit(self, subject_type: str, subject_id: int, actor_id: str, action: str,
              version: int, details: Dict[str, Any]) -> None:
        self.conn.execute(
            "INSERT INTO audit_events(batch_no,subject_type,subject_id,action,actor_id,version,details,created_at)"
            " VALUES(?,?,?,?,?,?,?,?)",
            (self.batch_no, subject_type, subject_id, action, actor_id, int(version),
             json.dumps(details, ensure_ascii=False, sort_keys=True), _now()),
        )

    def finish(self, result: Dict[str, Any]) -> None:
        if self.conn is None:
            return
        now = _now()
        try:
            if self.repository.fail_next_commit:
                self.repository.fail_next_commit = False
                raise sqlite3.OperationalError("injected commit failure")
            self.conn.execute(
                "UPDATE op_batches SET status='complete',result=?,completed_at=?,updated_at=? WHERE batch_no=?",
                (json.dumps(result, ensure_ascii=False, sort_keys=True), now, now, self.batch_no),
            )
            self.conn.commit()
        except Exception:
            # 提交失败：整批回滚（占用/审计/批次行都不落库），连接必须关闭
            self.abort()
            raise
        self.conn.close()
        self.conn = None

    def abort(self) -> None:
        if self.conn is not None:
            self.conn.rollback()
            self.conn.close()
            self.conn = None


class Repository:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.fail_next_commit = False
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=15)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 15000")
        return connection

    def _init_schema(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS contract_terms (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    contract_pk INTEGER,
                    reference TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    terms TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    updated_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(contract_pk, version)
                );
                CREATE TABLE IF NOT EXISTS claims (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    contract_pk INTEGER NOT NULL REFERENCES contract_terms(id),
                    claim_number TEXT NOT NULL UNIQUE,
                    event_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    approved_loss REAL,
                    recovery REAL,
                    reinstatement_premium REAL,
                    reinstatements_used REAL,
                    term_id INTEGER,
                    term_version INTEGER,
                    occupation_id INTEGER,
                    basis TEXT,
                    rejection_reason TEXT,
                    payment_reference TEXT,
                    created_by TEXT NOT NULL,
                    updated_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS capacity_ledger (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    op_batch_no TEXT NOT NULL,
                    contract_pk INTEGER NOT NULL,
                    claim_id INTEGER,
                    claim_number TEXT NOT NULL DEFAULT '',
                    term_id INTEGER,
                    term_version INTEGER,
                    status TEXT NOT NULL,
                    occupied_amount REAL NOT NULL,
                    reinstatements_used REAL NOT NULL DEFAULT 0,
                    reinstatement_premium REAL NOT NULL DEFAULT 0,
                    basis TEXT,
                    superseded_by INTEGER,
                    released_reason TEXT,
                    settled_at TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS review_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    op_batch_no TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open',
                    contract_pk INTEGER,
                    claim_id INTEGER,
                    occupation_id INTEGER,
                    from_term_version INTEGER,
                    to_term_version INTEGER,
                    old_amount REAL,
                    new_amount REAL,
                    diff_amount REAL,
                    details TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    resolved_by TEXT,
                    resolution_note TEXT,
                    created_at TEXT NOT NULL,
                    resolved_at TEXT
                );
                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_no TEXT NOT NULL,
                    subject_type TEXT NOT NULL,
                    subject_id INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    details TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS op_batches (
                    batch_no TEXT PRIMARY KEY,
                    op_type TEXT NOT NULL,
                    subject_ref TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL,
                    result TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    completed_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_contracts_pk_ver ON contract_terms(contract_pk, version);
                CREATE INDEX IF NOT EXISTS idx_contracts_ref ON contract_terms(reference);
                CREATE INDEX IF NOT EXISTS idx_claims_contract ON claims(contract_pk, state);
                CREATE INDEX IF NOT EXISTS idx_ledger_contract_status ON capacity_ledger(contract_pk, status);
                CREATE INDEX IF NOT EXISTS idx_ledger_claim ON capacity_ledger(claim_id);
                CREATE INDEX IF NOT EXISTS idx_reviews_status ON review_items(status);
                CREATE INDEX IF NOT EXISTS idx_audit_subject ON audit_events(subject_type, subject_id, id);
                CREATE INDEX IF NOT EXISTS idx_batches_status ON op_batches(status);
                """
            )

    # ---- 行映射 ----
    @staticmethod
    def _contract_row(row: sqlite3.Row) -> Dict[str, Any]:
        item = dict(row)
        item["terms"] = json.loads(item["terms"])
        return item

    @staticmethod
    def _claim_row(row: sqlite3.Row) -> Dict[str, Any]:
        item = dict(row)
        item["basis"] = _loads(item.get("basis"))
        return item

    @staticmethod
    def _occ_row(row: sqlite3.Row) -> Dict[str, Any]:
        item = dict(row)
        item["basis"] = _loads(item.get("basis"))
        return item

    @staticmethod
    def _review_row(row: sqlite3.Row) -> Dict[str, Any]:
        item = dict(row)
        item["details"] = _loads(item.get("details"), {})
        return item

    @staticmethod
    def _audit_row(row: sqlite3.Row) -> Dict[str, Any]:
        item = dict(row)
        item["details"] = json.loads(item["details"])
        return item

    @staticmethod
    def _batch_row(row: sqlite3.Row) -> Dict[str, Any]:
        item = dict(row)
        item["result"] = _loads(item.get("result"))
        return item

    # ---- 批次 ----
    def start_batch(self, batch_no: str, op_type: str, subject_ref: str = "") -> Batch:
        conn = self._connect()
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT * FROM op_batches WHERE batch_no=?", (batch_no,)).fetchone()
        if row is not None:
            if row["status"] != "complete":
                conn.rollback()
                conn.close()
                raise Conflict("批次%s中断未恢复，请先调用recovery" % batch_no, {"batch_no": batch_no})
            replay = self._batch_row(row)
            conn.rollback()
            conn.close()
            return Batch(self, None, batch_no, op_type, replay=replay)
        now = _now()
        conn.execute(
            "INSERT INTO op_batches(batch_no,op_type,subject_ref,status,created_at,updated_at) VALUES(?,?,?, 'open',?,?)",
            (batch_no, op_type, subject_ref, now, now),
        )
        return Batch(self, conn, batch_no, op_type)

    def latest_batch(self) -> Optional[Dict[str, Any]]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM op_batches WHERE status='complete' ORDER BY completed_at DESC, rowid DESC LIMIT 1"
            ).fetchone()
        return self._batch_row(row) if row else None

    def incomplete_batches(self) -> List[Dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM op_batches WHERE status!='complete' ORDER BY rowid").fetchall()
        return [self._batch_row(row) for row in rows]

    # ---- 合约读 ----
    def _contract_pk(self, connection: sqlite3.Connection, reference: str) -> Optional[int]:
        row = connection.execute(
            "SELECT contract_pk FROM contract_terms WHERE reference=? ORDER BY version DESC LIMIT 1",
            (reference,),
        ).fetchone()
        return None if row is None else int(row["contract_pk"])

    def get_contract(self, reference: str, connection: Optional[sqlite3.Connection] = None) -> Dict[str, Any]:
        own = connection is None
        connection = connection or self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM contract_terms WHERE reference=? ORDER BY version DESC LIMIT 1", (reference,)
            ).fetchone()
        finally:
            if own:
                connection.close()
        if row is None:
            raise NotFound("合约不存在")
        return self._contract_row(row)

    def get_contract_by_pk(self, contract_pk: int, connection: Optional[sqlite3.Connection] = None) -> Dict[str, Any]:
        own = connection is None
        connection = connection or self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM contract_terms WHERE contract_pk=? ORDER BY version DESC LIMIT 1", (contract_pk,)
            ).fetchone()
        finally:
            if own:
                connection.close()
        if row is None:
            raise NotFound("合约不存在")
        return self._contract_row(row)

    def get_term(self, term_id: int, connection: Optional[sqlite3.Connection] = None) -> Dict[str, Any]:
        own = connection is None
        connection = connection or self._connect()
        try:
            row = connection.execute("SELECT * FROM contract_terms WHERE id=?", (term_id,)).fetchone()
        finally:
            if own:
                connection.close()
        if row is None:
            raise NotFound("合约版本不存在")
        return self._contract_row(row)

    def list_contracts(self, state: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM contract_terms ORDER BY contract_pk DESC, version DESC"
            ).fetchall()
        latest: Dict[int, Dict[str, Any]] = {}
        for row in rows:
            item = self._contract_row(row)
            latest.setdefault(int(item["contract_pk"]), item)
        items = list(latest.values())
        if state:
            items = [item for item in items if item["state"] == state]
        return items[:limit]

    # ---- 赔案读 ----
    def get_claim(self, claim_id: int, connection: Optional[sqlite3.Connection] = None) -> Dict[str, Any]:
        own = connection is None
        connection = connection or self._connect()
        try:
            row = connection.execute("SELECT * FROM claims WHERE id=?", (claim_id,)).fetchone()
        finally:
            if own:
                connection.close()
        if row is None:
            raise NotFound("赔案不存在")
        return self._claim_row(row)

    def list_claims(self, contract_pk: Optional[int] = None, state: Optional[str] = None,
                    limit: int = 100, connection: Optional[sqlite3.Connection] = None) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        own = connection is None
        connection = connection or self._connect()
        try:
            sql = "SELECT * FROM claims"
            where = []
            params: List[Any] = []
            if contract_pk is not None:
                where.append("contract_pk=?")
                params.append(contract_pk)
            if state:
                where.append("state=?")
                params.append(state)
            if where:
                sql += " WHERE " + " AND ".join(where)
            sql += " ORDER BY id DESC LIMIT ?"
            params.append(limit)
            rows = connection.execute(sql, params).fetchall()
        finally:
            if own:
                connection.close()
        return [self._claim_row(row) for row in rows]

    # ---- 容量账读 ----
    def occupations(self, contract_pk: int, active_only: bool = False,
                    connection: Optional[sqlite3.Connection] = None) -> List[Dict[str, Any]]:
        own = connection is None
        connection = connection or self._connect()
        try:
            sql = "SELECT * FROM capacity_ledger WHERE contract_pk=?"
            if active_only:
                sql += " AND status IN ('reserved','settled')"
            sql += " ORDER BY id"
            rows = connection.execute(sql, (contract_pk,)).fetchall()
        finally:
            if own:
                connection.close()
        return [self._occ_row(row) for row in rows]

    def get_occupation(self, occ_id: int, connection: Optional[sqlite3.Connection] = None) -> Dict[str, Any]:
        own = connection is None
        connection = connection or self._connect()
        try:
            row = connection.execute("SELECT * FROM capacity_ledger WHERE id=?", (occ_id,)).fetchone()
        finally:
            if own:
                connection.close()
        if row is None:
            raise NotFound("容量占用不存在")
        return self._occ_row(row)

    # ---- 待复核 ----
    def list_reviews(self, status: Optional[str] = None, contract_pk: Optional[int] = None,
                     claim_id: Optional[int] = None, limit: int = 100) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        sql = "SELECT * FROM review_items"
        where = []
        params: List[Any] = []
        if status:
            where.append("status=?")
            params.append(status)
        if contract_pk is not None:
            where.append("contract_pk=?")
            params.append(contract_pk)
        if claim_id is not None:
            where.append("claim_id=?")
            params.append(claim_id)
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        with self._connect() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [self._review_row(row) for row in rows]

    def resolve_review(self, review_id: int, actor_id: str, note: str) -> Dict[str, Any]:
        now = _now()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT * FROM review_items WHERE id=?", (review_id,)).fetchone()
            if row is None:
                connection.rollback()
                raise NotFound("待复核项不存在")
            item = self._review_row(row)
            if item["status"] != "open":
                connection.rollback()
                raise Conflict("待复核项已处理")
            connection.execute(
                "UPDATE review_items SET status='resolved',resolved_by=?,resolution_note=?,resolved_at=? WHERE id=?",
                (actor_id, note, now, review_id),
            )
            connection.execute(
                "INSERT INTO audit_events(batch_no,subject_type,subject_id,action,actor_id,version,details,created_at)"
                " VALUES(?,?, 'review',?,?,?,?,?)",
                ("review-%s" % review_id, review_id, "review_resolved", actor_id, 0,
                 json.dumps({"resolution_note": note}, ensure_ascii=False), now),
            )
            result = connection.execute("SELECT * FROM review_items WHERE id=?", (review_id,)).fetchone()
            connection.commit()
        return self._review_row(result)

    # ---- 审计 ----
    def audit_timeline(self, subject_type: str, subject_id: int) -> List[Dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM audit_events WHERE subject_type=? AND subject_id=? ORDER BY id",
                (subject_type, subject_id),
            ).fetchall()
        return [self._audit_row(row) for row in rows]

    # ---- 统计 / 健康 ----
    def stats(self) -> Dict[str, Any]:
        with self._connect() as connection:
            contract_rows = connection.execute(
                "SELECT state, COUNT(DISTINCT contract_pk) AS total FROM contract_terms GROUP BY state"
            ).fetchall()
            claim_rows = connection.execute("SELECT state, COUNT(*) AS total FROM claims GROUP BY state").fetchall()
            review_rows = connection.execute(
                "SELECT status, COUNT(*) AS total FROM review_items GROUP BY status"
            ).fetchall()
        return {
            "contracts": {str(row["state"]): int(row["total"]) for row in contract_rows},
            "claims": {str(row["state"]): int(row["total"]) for row in claim_rows},
            "reviews": {str(row["status"]): int(row["total"]) for row in review_rows},
        }

    def health(self) -> bool:
        try:
            with self._connect() as connection:
                connection.execute("SELECT 1").fetchone()
            return True
        except sqlite3.Error:
            return False

    # ---- 恢复：回滚不完整批次 → 从最近完整批次继续 → 回填旧赔案 ----
    def recover(self) -> Dict[str, Any]:
        latest = self.latest_batch()
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            open_rows = conn.execute("SELECT batch_no FROM op_batches WHERE status!='complete'").fetchall()
            open_batches = [str(row["batch_no"]) for row in open_rows]
            removed = {"batches": len(open_batches), "occupations": 0, "reviews": 0, "audits": 0}
            if open_batches:
                placeholders = ",".join("?" for _ in open_batches)
                cur = conn.execute(
                    "DELETE FROM capacity_ledger WHERE op_batch_no IN (%s)" % placeholders, open_batches
                )
                removed["occupations"] = cur.rowcount
                cur = conn.execute("DELETE FROM review_items WHERE op_batch_no IN (%s)" % placeholders, open_batches)
                removed["reviews"] = cur.rowcount
                cur = conn.execute(
                    "DELETE FROM audit_events WHERE batch_no IN (%s)" % placeholders, open_batches
                )
                removed["audits"] = cur.rowcount
                conn.execute("DELETE FROM op_batches WHERE status!='complete'")
            # 游离占用：批次号不在任何已完成批次中（旧库/异常残留）
            orphan = conn.execute(
                "SELECT DISTINCT op_batch_no FROM capacity_ledger WHERE op_batch_no NOT IN ("
                "SELECT batch_no FROM op_batches WHERE status='complete')"
            ).fetchall()
            orphan_batches = [str(row["op_batch_no"]) for row in orphan]
            if orphan_batches:
                placeholders = ",".join("?" for _ in orphan_batches)
                cur = conn.execute(
                    "DELETE FROM capacity_ledger WHERE op_batch_no IN (%s)" % placeholders, orphan_batches
                )
                removed["occupations"] += cur.rowcount
            backfill = self._backfill_legacy(conn)
            conn.commit()
        finally:
            conn.close()
        return {
            "latest_complete_batch": latest["batch_no"] if latest else None,
            "removed": removed,
            "backfill": backfill,
        }

    def _backfill_legacy(self, conn: sqlite3.Connection) -> Dict[str, Any]:
        """缺占用批次号的旧赔案：按现有金额回填占用；算不清的进待核。按赔案幂等。"""
        now = _now()
        existed = conn.execute(
            "SELECT 1 FROM op_batches WHERE batch_no=?", (LEGACY_BACKFILL_BATCH,)
        ).fetchone()
        if existed is None:
            conn.execute(
                "INSERT INTO op_batches(batch_no,op_type,subject_ref,status,created_at,updated_at)"
                " VALUES(?, 'legacy_backfill','legacy','open',?,?)",
                (LEGACY_BACKFILL_BATCH, now, now),
            )
        legacy = conn.execute(
            "SELECT * FROM claims WHERE occupation_id IS NULL AND state IN ('reserved','settled') ORDER BY id"
        ).fetchall()
        summary = {"backfilled": 0, "pending_review": 0}
        for row in legacy:
            claim = self._claim_row(row)
            contract = self.get_contract_by_pk(int(claim["contract_pk"]), connection=conn)
            terms = contract["terms"]
            basis = claim.get("basis")
            amount = claim.get("recovery")
            loss = claim.get("approved_loss")
            parseable = (
                isinstance(basis, dict) and isinstance(amount, (int, float))
                and isinstance(loss, (int, float)) and claim.get("term_id")
            )
            if not parseable:
                already = conn.execute(
                    "SELECT 1 FROM review_items WHERE claim_id=? AND kind IN ('legacy_unverifiable','backfill_overrun')"
                    " LIMIT 1",
                    (claim["id"],),
                ).fetchone()
                if already is None:
                    conn.execute(
                        "INSERT INTO review_items(op_batch_no,kind,status,contract_pk,claim_id,details,created_by,created_at)"
                        " VALUES(?, 'legacy_unverifiable','open',?,?,?,'system',?)",
                        (LEGACY_BACKFILL_BATCH, int(claim["contract_pk"]), claim["id"],
                         json.dumps({"reason": "缺少占用依据或金额，无法回填", "claim_number": claim["claim_number"]},
                                    ensure_ascii=False), now),
                    )
                    conn.execute(
                        "INSERT INTO audit_events(batch_no,subject_type,subject_id,action,actor_id,version,details,created_at)"
                        " VALUES(?, 'claim',?,?, 'legacy_pending_review','system',?,?)",
                        (LEGACY_BACKFILL_BATCH, claim["id"], claim["version"],
                         json.dumps({"reason": "legacy_unverifiable"}, ensure_ascii=False), now),
                    )
                    summary["pending_review"] += 1
                continue
            occ_rows = conn.execute(
                "SELECT * FROM capacity_ledger WHERE contract_pk=? AND status IN ('reserved','settled') ORDER BY id",
                (int(claim["contract_pk"]),),
            ).fetchall()
            occs = [self._occ_row(r) for r in occ_rows]
            used_amount = round(sum(float(o["occupied_amount"]) for o in occs), 2)
            used_reinst = round(sum(float(o.get("reinstatements_used", 0.0)) for o in occs), 6)
            capacity = float(terms["aggregate_capacity"])
            max_reinst = 1.0 + float(terms["max_reinstatements"])
            amount = round(float(amount), 2)
            reinst = float(basis.get("reinstatements_used", 0.0))
            overrun = used_amount + amount > capacity + 0.01 or used_reinst + reinst > max_reinst + 1e-6
            if claim["state"] == "reserved" and overrun:
                already = conn.execute(
                    "SELECT 1 FROM review_items WHERE claim_id=? AND kind='backfill_overrun' LIMIT 1",
                    (claim["id"],),
                ).fetchone()
                if already is None:
                    conn.execute(
                        "INSERT INTO review_items(op_batch_no,kind,status,contract_pk,claim_id,details,created_by,created_at)"
                        " VALUES(?, 'backfill_overrun','open',?,?,?,'system',?)",
                        (LEGACY_BACKFILL_BATCH, int(claim["contract_pk"]), claim["id"],
                         json.dumps({"reason": "旧赔案按现有金额回填将超容量，先待核", "claim_number": claim["claim_number"],
                                     "used_amount": used_amount, "amount": amount, "capacity": capacity},
                                    ensure_ascii=False), now),
                    )
                    conn.execute(
                        "INSERT INTO audit_events(batch_no,subject_type,subject_id,action,actor_id,version,details,created_at)"
                        " VALUES(?, 'claim',?,?, 'legacy_pending_review','system',?,?)",
                        (LEGACY_BACKFILL_BATCH, claim["id"], claim["version"],
                         json.dumps({"reason": "backfill_overrun"}, ensure_ascii=False), now),
                    )
                    summary["pending_review"] += 1
                continue
            occ_id = int(conn.execute(
                "INSERT INTO capacity_ledger(op_batch_no,contract_pk,claim_id,claim_number,term_id,term_version,"
                "status,occupied_amount,reinstatements_used,reinstatement_premium,basis,settled_at,created_by,created_at)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (LEGACY_BACKFILL_BATCH, int(claim["contract_pk"]), claim["id"], claim["claim_number"],
                 claim["term_id"], claim.get("term_version"),
                 "settled" if claim["state"] == "settled" else "reserved",
                 amount, reinst, float(claim.get("reinstatement_premium") or basis.get("reinstatement_premium", 0.0)),
                 json.dumps(basis, ensure_ascii=False, sort_keys=True),
                 now if claim["state"] == "settled" else None, "system", now),
            ).lastrowid)
            conn.execute("UPDATE claims SET occupation_id=? WHERE id=?", (occ_id, claim["id"]))
            if overrun:
                # 已结算保留当时依据，容量差额转待核
                conn.execute(
                    "INSERT INTO review_items(op_batch_no,kind,status,contract_pk,claim_id,occupation_id,details,"
                    "created_by,created_at) VALUES(?, 'backfill_overrun','open',?,?,?,?,'system',?)",
                    (LEGACY_BACKFILL_BATCH, int(claim["contract_pk"]), claim["id"], occ_id,
                     json.dumps({"reason": "已结算旧赔案回填后超出当前容量", "claim_number": claim["claim_number"],
                                 "used_amount": round(used_amount + amount, 2), "capacity": capacity},
                                ensure_ascii=False), now),
                )
                summary["pending_review"] += 1
            conn.execute(
                "INSERT INTO audit_events(batch_no,subject_type,subject_id,action,actor_id,version,details,created_at)"
                " VALUES(?, 'claim',?, 'legacy_backfilled','system',?,?,?)",
                (LEGACY_BACKFILL_BATCH, claim["id"], claim["version"],
                 json.dumps({"occupation_id": occ_id, "amount": amount, "overrun": overrun}, ensure_ascii=False), now),
            )
            summary["backfilled"] += 1
        conn.execute(
            "UPDATE op_batches SET status='complete',result=?,completed_at=?,updated_at=? WHERE batch_no=?",
            (json.dumps(summary, ensure_ascii=False), now, now, LEGACY_BACKFILL_BATCH),
        )
        return summary

    # ---- 测试支撑：制造旧赔案 / 游离批次 ----
    def seed(self, sql: str, params: Sequence[Any] = ()) -> None:
        with self._connect() as connection:
            connection.execute(sql, params)
