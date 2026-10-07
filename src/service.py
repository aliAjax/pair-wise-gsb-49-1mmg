"""用例编排：合约版本、赔案预占/结算、容量账、待复核与恢复共用同一批次事务。"""
from typing import Any, Dict, List, Optional

from .audit import AuditRecorder
from .domain import (
    Actor,
    Conflict,
    PermissionDenied,
    ValidationError,
    new_batch_no,
    number,
    optional_integer,
    text,
)
from .repository import Repository
from .rules import (
    CLAIM_REJECTED,
    CLAIM_RESERVED,
    CLAIM_SETTLED,
    CLAIM_SUBMITTED,
    CONTRACT_ACTIVE,
    CONTRACT_DRAFT,
    OCC_RELEASED,
    OCC_RESERVED,
    OCC_SETTLED,
    OCC_SUPERSEDED,
    REVIEW_BACKFILL_OVERRUN,
    REVIEW_LEGACY_UNVERIFIABLE,
    REVIEW_RECALC_SHORTFALL,
    REVIEW_SETTLED_DIFF,
    DomainRules,
)


class Service:
    def __init__(self, repository: Repository, rules: DomainRules, audit: AuditRecorder = None,
                 auto_recover: bool = True) -> None:
        self.repository = repository
        self.rules = rules
        self.audit = audit or AuditRecorder(repository)
        # 启动即从最近完整批次恢复，并回填缺占用批次号的旧赔案
        if auto_recover:
            repository.recover()

    # ---- 通用 ----
    @staticmethod
    def _actor(actor: Actor) -> Actor:
        if actor is None or not actor.user_id.strip() or not actor.role.strip():
            raise PermissionDenied("缺少调用身份")
        return actor

    def _ensure_known_role(self, actor: Actor) -> None:
        if not self.rules.known_role(actor.role):
            raise PermissionDenied("角色无权访问该服务")

    @staticmethod
    def _batch_no(data: Dict[str, Any]) -> str:
        value = (data or {}).get("batch_no")
        if value is None:
            return new_batch_no()
        if not isinstance(value, str) or not value.strip():
            raise ValidationError("batch_no必须是非空字符串")
        return value.strip()

    def _run(self, batch_no: str, op_type: str, subject_ref: str, fn) -> Dict[str, Any]:
        batch = self.repository.start_batch(batch_no, op_type, subject_ref)
        if batch.replay is not None:
            # 同批次号重试：只回放首次结果，不重复占额、不追加审计
            result = dict(batch.replay.get("result") or {})
            result["batch_no"] = batch_no
            result["replayed"] = True
            return result
        with batch:
            result = fn(batch)
            batch.finish(result)
        result["batch_no"] = batch_no
        result["replayed"] = False
        return result

    # ---- 合约 ----
    def create_contract(self, actor: Actor, reference: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        if not self.rules.role_can_create_contract(actor.role):
            raise PermissionDenied("角色无权创建合约")
        reference = text({"reference": reference}, "reference")
        prepared = self.rules.validate_contract_create(payload or {})
        batch_no = self._batch_no(payload or {})
        event_id = prepared["event_id"]

        def work(batch):
            exists = batch.q(
                "SELECT id FROM contract_terms WHERE reference=? ORDER BY version DESC LIMIT 1", (reference,)
            ).fetchone()
            if exists is not None:
                raise Conflict("合约reference已存在", {"reference": reference})
            term_id = batch.insert_contract(reference, event_id, 1, CONTRACT_DRAFT, prepared, actor.user_id)
            batch.audit("contract", term_id, actor.user_id, "created", 1,
                        {"state": CONTRACT_DRAFT, "version": 1, "terms": prepared})
            contract = self.repository._contract_row(batch.q(
                "SELECT * FROM contract_terms WHERE id=?", (term_id,)
            ).fetchone())
            return {"contract": contract}

        return self._run(batch_no, "create_contract", reference, work)

    def bind_contract(self, actor: Actor, reference: str, expected_version: Optional[int],
                      data: Dict[str, Any]) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        if not self.rules.role_can_contract_action(actor.role, "bind"):
            raise PermissionDenied("角色无权绑定合约")
        underwriter_id = text(data or {}, "underwriter_id")
        batch_no = self._batch_no(data or {})

        def work(batch):
            row = batch.q(
                "SELECT * FROM contract_terms WHERE reference=? ORDER BY version DESC LIMIT 1", (reference,)
            ).fetchone()
            if row is None:
                from .domain import NotFound
                raise NotFound("合约不存在")
            contract = self.repository._contract_row(row)
            self.rules.assert_expected_version(
                contract["version"], expected_version, {"reference": reference}
            )
            self.rules.require_contract_transition(contract, "bind")
            batch.mark_contract_active(contract["id"], actor.user_id)
            batch.audit("contract", contract["contract_pk"], actor.user_id, "bind", contract["version"],
                        {"underwriter_id": underwriter_id, "version": contract["version"]})
            latest = self.repository._contract_row(batch.q(
                "SELECT * FROM contract_terms WHERE id=?", (contract["id"],)
            ).fetchone())
            return {"contract": latest}

        return self._run(batch_no, "bind_contract", reference, work)

    def adjust_contract(self, actor: Actor, reference: str, expected_version: Optional[int],
                        data: Dict[str, Any]) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        if not self.rules.role_can_contract_action(actor.role, "adjust"):
            raise PermissionDenied("角色无权调整合约")
        data = dict(data or {})
        new_terms = self.rules.normalize_terms(data)
        note = str(data.get("note") or "")
        batch_no = self._batch_no(data)

        def work(batch):
            row = batch.q(
                "SELECT * FROM contract_terms WHERE reference=? ORDER BY version DESC LIMIT 1", (reference,)
            ).fetchone()
            if row is None:
                from .domain import NotFound
                raise NotFound("合约不存在")
            contract = self.repository._contract_row(row)
            self.rules.assert_expected_version(
                contract["version"], expected_version, {"reference": reference}
            )
            self.rules.require_contract_transition(contract, "adjust")
            contract_pk = int(contract["contract_pk"])
            new_version = int(contract["version"]) + 1
            event_id = contract["event_id"]
            new_id = batch.insert_contract(reference, event_id, new_version, CONTRACT_ACTIVE,
                                           new_terms, actor.user_id, contract_pk=contract_pk)
            batch.supersede_prior_terms(contract_pk, actor.user_id, keep_id=new_id)
            new_term_row = self.repository._contract_row(batch.q(
                "SELECT * FROM contract_terms WHERE id=?", (new_id,)
            ).fetchone())

            reviews: List[Dict[str, Any]] = []
            claim_rows = batch.q(
                "SELECT * FROM claims WHERE contract_pk=? AND state IN (?, ?) ORDER BY id",
                (contract_pk, CLAIM_RESERVED, CLAIM_SETTLED),
            ).fetchall()
            settled_diffs = 0
            shortfalls = 0
            rebased = 0
            for claim_row in claim_rows:
                claim = self.repository._claim_row(claim_row)
                occ = None
                if claim.get("occupation_id"):
                    occ_row = batch.q(
                        "SELECT * FROM capacity_ledger WHERE id=?", (claim["occupation_id"],)
                    ).fetchone()
                    if occ_row is not None:
                        occ = self.repository._occ_row(occ_row)
                if occ is None or occ["status"] not in (OCC_RESERVED, OCC_SETTLED):
                    # 旧赔案无有效占用：交给恢复/回填流程，调整动作不臆算
                    continue
                loss = float(claim["approved_loss"])
                new_basis = self.rules.recovery_basis(new_terms, loss)
                if occ["status"] == OCC_SETTLED:
                    # 已结算：永久保留当时依据，差额只进待复核
                    old_amount = round(float(occ["occupied_amount"]), 2)
                    diff = round(new_basis["recoverable_amount"] - old_amount, 2)
                    if abs(diff) > 0.01:
                        rid = batch.insert_review(
                            REVIEW_SETTLED_DIFF, contract_pk, claim["id"], occ["id"],
                            {"reason": "合约版本变化，已结算摊回保留原依据", "claim_number": claim["claim_number"],
                             "new_basis": new_basis, "old_basis": occ.get("basis")},
                            actor.user_id, from_term_version=int(occ["term_version"]),
                            to_term_version=new_version, old_amount=old_amount,
                            new_amount=new_basis["recoverable_amount"], diff_amount=diff,
                        )
                        reviews.append(self.repository._review_row(
                            batch.q("SELECT * FROM review_items WHERE id=?", (rid,)).fetchone()))
                        settled_diffs += 1
                    continue
                # reserved：旧占用失效，按新版本重算并重新预占
                active_rows = batch.q(
                    "SELECT * FROM capacity_ledger WHERE contract_pk=? AND status IN ('reserved','settled')"
                    " AND id<>? ORDER BY id",
                    (contract_pk, occ["id"]),
                ).fetchall()
                others = [self.repository._occ_row(r) for r in active_rows]
                fits = True
                try:
                    self.rules.assert_capacity(
                        new_terms, others, new_basis["recoverable_amount"], new_basis["reinstatements_used"],
                        context={"claim_id": claim["id"], "claim_number": claim["claim_number"]},
                    )
                except Conflict:
                    fits = False
                if not fits:
                    batch.update_occupation(occ["id"], OCC_RELEASED, actor.user_id,
                                            released_reason="contract_adjust_shortfall")
                    batch.update_claim(
                        claim["id"], actor.user_id, state=CLAIM_SUBMITTED, version=int(claim["version"]) + 1,
                        approved_loss=None, recovery=None, reinstatement_premium=None,
                        reinstatements_used=None, term_id=new_id, term_version=new_version,
                        occupation_id=None, basis=None,
                    )
                    rid = batch.insert_review(
                        REVIEW_RECALC_SHORTFALL, contract_pk, claim["id"], occ["id"],
                        {"reason": "合约版本变化后新版本容量不足，旧预占失效，赔案退回待计算",
                         "claim_number": claim["claim_number"], "new_basis": new_basis},
                        actor.user_id, from_term_version=int(occ["term_version"]),
                        to_term_version=new_version, old_amount=round(float(occ["occupied_amount"]), 2),
                        new_amount=new_basis["recoverable_amount"],
                        diff_amount=round(new_basis["recoverable_amount"] - float(occ["occupied_amount"]), 2),
                    )
                    reviews.append(self.repository._review_row(
                        batch.q("SELECT * FROM review_items WHERE id=?", (rid,)).fetchone()))
                    batch.audit("claim", claim["id"], actor.user_id, "occupation_invalidated",
                                int(claim["version"]) + 1,
                                {"reason": "contract_adjust_shortfall", "occupation_id": occ["id"],
                                 "review_id": rid, "to_term_version": new_version})
                    shortfalls += 1
                    continue
                new_occ_id = batch.insert_occupation(
                    contract_pk, claim["id"], new_id, new_version,
                    OCC_RESERVED, new_basis["recoverable_amount"], new_basis["reinstatements_used"],
                    new_basis["reinstatement_premium"], new_basis, actor.user_id,
                    claim_number=claim["claim_number"],
                )
                batch.update_occupation(occ["id"], OCC_SUPERSEDED, actor.user_id, superseded_by=new_occ_id)
                batch.update_claim(
                    claim["id"], actor.user_id, state=CLAIM_RESERVED, version=int(claim["version"]) + 1,
                    approved_loss=new_basis["approved_loss"], recovery=new_basis["recoverable_amount"],
                    reinstatement_premium=new_basis["reinstatement_premium"],
                    reinstatements_used=new_basis["reinstatements_used"],
                    term_id=new_id, term_version=new_version, occupation_id=new_occ_id, basis=new_basis,
                )
                batch.audit("claim", claim["id"], actor.user_id, "recalculated_on_adjust",
                            int(claim["version"]) + 1,
                            {"from_term_version": int(occ["term_version"]), "to_term_version": new_version,
                             "old_occupation_id": occ["id"], "occupation_id": new_occ_id,
                             "recoverable_amount": new_basis["recoverable_amount"]})
                rebased += 1
            batch.audit("contract", contract_pk, actor.user_id, "adjust", new_version,
                        {"from_version": contract["version"], "to_version": new_version, "note": note,
                         "settled_diffs": settled_diffs, "recalc_shortfalls": shortfalls, "rebased": rebased,
                         "terms": new_terms})
            return {"contract": new_term_row, "reviews": reviews,
                    "summary": {"settled_diffs": settled_diffs, "recalc_shortfalls": shortfalls,
                                "rebased": rebased}}

        return self._run(batch_no, "adjust_contract", reference, work)

    def get_contract(self, actor: Actor, reference: str) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        return self.repository.get_contract(reference)

    def list_contracts(self, actor: Actor, state: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        return self.repository.list_contracts(state=state, limit=limit)

    def contract_ledger(self, actor: Actor, reference: str) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        contract = self.repository.get_contract(reference)
        occs = self.repository.occupations(int(contract["contract_pk"]))
        used_amount, used_reinst = self.rules.capacity_usage(occs)
        terms = contract["terms"]
        by_status: Dict[str, int] = {}
        for occ in occs:
            by_status[occ["status"]] = by_status.get(occ["status"], 0) + 1
        return {
            "contract_ref": reference,
            "term_version": contract["version"],
            "capacity": terms["aggregate_capacity"],
            "max_reinstatements": 1.0 + float(terms["max_reinstatements"]),
            "used_amount": used_amount,
            "used_reinstatements": used_reinst,
            "available_amount": round(float(terms["aggregate_capacity"]) - used_amount, 2),
            "occupation_count": by_status,
            "occupations": occs,
        }

    # ---- 赔案 ----
    def submit_claim(self, actor: Actor, reference: str, data: Dict[str, Any]) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        if not self.rules.role_can_claim_action(actor.role, "submit_claim"):
            raise PermissionDenied("角色无权提交赔案")
        data = dict(data or {})
        claim_number = text(data, "claim_number")
        batch_no = self._batch_no(data)

        def work(batch):
            row = batch.q(
                "SELECT * FROM contract_terms WHERE reference=? ORDER BY version DESC LIMIT 1", (reference,)
            ).fetchone()
            if row is None:
                from .domain import NotFound
                raise NotFound("合约不存在")
            contract = self.repository._contract_row(row)
            if contract["state"] != CONTRACT_ACTIVE:
                raise Conflict("合约尚未生效，不能提交赔案")
            event_id = str(data.get("event_id") or contract["event_id"])
            dup = batch.q("SELECT id FROM claims WHERE claim_number=?", (claim_number,)).fetchone()
            if dup is not None:
                raise Conflict("赔案号已存在", {"claim_number": claim_number})
            claim_id = batch.insert_claim(int(contract["contract_pk"]), claim_number, event_id, actor.user_id)
            batch.audit("claim", claim_id, actor.user_id, "submit_claim", 1,
                        {"claim_number": claim_number, "event_id": event_id,
                         "contract_ref": reference})
            claim = self.repository._claim_row(batch.q(
                "SELECT * FROM claims WHERE id=?", (claim_id,)
            ).fetchone())
            return {"claim": claim}

        return self._run(batch_no, "submit_claim", "claim:%s" % claim_number, work)

    def calculate_claim(self, actor: Actor, claim_id: int, expected_version: Optional[int],
                        data: Dict[str, Any], action: str = "calculate") -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        if not self.rules.role_can_claim_action(actor.role, "calculate"):
            raise PermissionDenied("角色无权计算摊回")
        data = dict(data or {})
        approved_loss = number(data, "approved_loss", 0)
        expected_term_version = optional_integer(data, "expected_term_version")
        batch_no = self._batch_no(data)

        def work(batch):
            claim_row = batch.q("SELECT * FROM claims WHERE id=?", (claim_id,)).fetchone()
            if claim_row is None:
                from .domain import NotFound
                raise NotFound("赔案不存在")
            claim = self.repository._claim_row(claim_row)
            self.rules.assert_expected_version(
                claim["version"], expected_version, {"claim_id": claim_id}
            )
            self.rules.require_claim_transition(claim, "calculate")
            term_row = batch.q(
                "SELECT * FROM contract_terms WHERE contract_pk=? ORDER BY version DESC LIMIT 1",
                (int(claim["contract_pk"]),),
            ).fetchone()
            term = self.repository._contract_row(term_row)
            self.rules.assert_expected_term_version(
                int(term["version"]), expected_term_version, {"claim_id": claim_id}
            )
            # 最新版本即当前生效口径；旧版本保留为已结算占用的历史依据
            if term["state"] not in (CONTRACT_ACTIVE, "superseded"):
                raise Conflict("合约当前版本未生效")
            result = self._reserve(batch, claim, term, approved_loss, actor.user_id, action)
            return result

        return self._run(batch_no, action, "claim:%s" % claim_id, work)

    def adjust_claim(self, actor: Actor, claim_id: int, expected_version: Optional[int],
                     data: Dict[str, Any]) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        if not self.rules.role_can_claim_action(actor.role, "adjust_claim"):
            raise PermissionDenied("角色无权调整赔案")
        data = dict(data or {})
        approved_loss = number(data, "approved_loss", 0)
        expected_term_version = optional_integer(data, "expected_term_version")
        batch_no = self._batch_no(data)

        def work(batch):
            claim_row = batch.q("SELECT * FROM claims WHERE id=?", (claim_id,)).fetchone()
            if claim_row is None:
                from .domain import NotFound
                raise NotFound("赔案不存在")
            claim = self.repository._claim_row(claim_row)
            self.rules.assert_expected_version(
                claim["version"], expected_version, {"claim_id": claim_id}
            )
            self.rules.require_claim_transition(claim, "adjust_claim")
            if not claim.get("occupation_id"):
                raise Conflict("赔案缺少有效预占，请先计算")
            latest = self.repository._contract_row(batch.q(
                "SELECT * FROM contract_terms WHERE contract_pk=? ORDER BY version DESC LIMIT 1",
                (int(claim["contract_pk"]),),
            ).fetchone())
            self.rules.assert_expected_term_version(
                int(latest["version"]), expected_term_version, {"claim_id": claim_id}
            )
            # 金额改小/改大一律按最新合约版本重算：旧预占失效，新预占入账
            result = self._reserve(batch, claim, latest, approved_loss, actor.user_id, "adjust_claim")
            return result

        return self._run(batch_no, "adjust_claim", "claim:%s" % claim_id, work)

    def _reserve(self, batch, claim: Dict[str, Any], term: Dict[str, Any], approved_loss: float,
                 actor_id: str, action: str) -> Dict[str, Any]:
        """容量账核心：按合约版本计算依据、校验容量、预占（旧预占同时失效）。"""
        basis = self.rules.recovery_basis(term["terms"], approved_loss)
        old_occ = None
        if claim.get("occupation_id"):
            old_row = batch.q(
                "SELECT * FROM capacity_ledger WHERE id=? AND claim_id=?",
                (claim["occupation_id"], claim["id"]),
            ).fetchone()
            if old_row is not None:
                old_occ = self.repository._occ_row(old_row)
                if old_occ["status"] != OCC_RESERVED:
                    raise Conflict("原预占已失效，请重新计算", {"occupation_id": old_occ["id"]})
        rows = batch.q(
            "SELECT * FROM capacity_ledger WHERE contract_pk=? AND status IN ('reserved','settled')"
            + (" AND id<>?" if old_occ else "") + " ORDER BY id",
            (int(claim["contract_pk"]), old_occ["id"]) if old_occ else (int(claim["contract_pk"]),),
        ).fetchall()
        others = [self.repository._occ_row(r) for r in rows]
        self.rules.assert_capacity(
            term["terms"], others, basis["recoverable_amount"], basis["reinstatements_used"],
            context={"claim_id": claim["id"], "claim_number": claim["claim_number"],
                     "requested_amount": basis["recoverable_amount"]},
        )
        occ_id = batch.insert_occupation(
            int(claim["contract_pk"]), int(claim["id"]), int(term["id"]), int(term["version"]),
            OCC_RESERVED, basis["recoverable_amount"], basis["reinstatements_used"],
            basis["reinstatement_premium"], basis, actor_id,
            claim_number=claim["claim_number"],
        )
        new_version = int(claim["version"]) + 1
        if old_occ is not None:
            batch.update_occupation(old_occ["id"], OCC_SUPERSEDED, actor_id, superseded_by=occ_id)
        batch.update_claim(
            claim["id"], actor_id, state=CLAIM_RESERVED, version=new_version,
            approved_loss=basis["approved_loss"], recovery=basis["recoverable_amount"],
            reinstatement_premium=basis["reinstatement_premium"],
            reinstatements_used=basis["reinstatements_used"],
            term_id=int(term["id"]), term_version=int(term["version"]),
            occupation_id=occ_id, basis=basis,
        )
        batch.audit("claim", claim["id"], actor_id, action, new_version,
                    {"occupation_id": occ_id, "term_version": int(term["version"]),
                     "old_occupation_id": old_occ["id"] if old_occ else None,
                     "approved_loss": basis["approved_loss"],
                     "recoverable_amount": basis["recoverable_amount"],
                     "reinstatements_used": basis["reinstatements_used"]})
        new_claim = self.repository._claim_row(batch.q(
            "SELECT * FROM claims WHERE id=?", (claim["id"],)
        ).fetchone())
        new_occ = self.repository._occ_row(batch.q(
            "SELECT * FROM capacity_ledger WHERE id=?", (occ_id,)
        ).fetchone())
        all_rows = batch.q(
            "SELECT * FROM capacity_ledger WHERE contract_pk=? AND status IN ('reserved','settled') ORDER BY id",
            (int(claim["contract_pk"]),),
        ).fetchall()
        used_amount, used_reinst = self.rules.capacity_usage(
            [self.repository._occ_row(r) for r in all_rows]
        )
        capacity_view = {
            "capacity": round(float(term["terms"]["aggregate_capacity"]), 2),
            "used_amount": used_amount,
            "available_amount": round(float(term["terms"]["aggregate_capacity"]) - used_amount, 2),
            "max_reinstatements": 1.0 + float(term["terms"]["max_reinstatements"]),
            "used_reinstatements": used_reinst,
            "projected_amount": used_amount,
            "projected_reinstatements": used_reinst,
        }
        return {"claim": new_claim, "occupation": new_occ, "capacity": capacity_view}

    def settle_claim(self, actor: Actor, claim_id: int, expected_version: Optional[int],
                     data: Dict[str, Any]) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        if not self.rules.role_can_claim_action(actor.role, "settle"):
            raise PermissionDenied("角色无权结算")
        data = dict(data or {})
        payment_reference = text(data, "payment_reference")
        batch_no = self._batch_no(data)

        def work(batch):
            claim_row = batch.q("SELECT * FROM claims WHERE id=?", (claim_id,)).fetchone()
            if claim_row is None:
                from .domain import NotFound
                raise NotFound("赔案不存在")
            claim = self.repository._claim_row(claim_row)
            self.rules.assert_expected_version(claim["version"], expected_version, {"claim_id": claim_id})
            self.rules.require_claim_transition(claim, "settle")
            if not claim.get("occupation_id"):
                raise Conflict("没有可结算的预占，请先计算", {"claim_id": claim_id})
            occ_row = batch.q("SELECT * FROM capacity_ledger WHERE id=?", (claim["occupation_id"],)).fetchone()
            if occ_row is None:
                raise Conflict("预占记录不存在，请重新计算")
            occ = self.repository._occ_row(occ_row)
            # 结算只认可这笔预占：归属、状态、版本必须对得上
            if occ["claim_id"] != claim["id"] or occ["status"] != OCC_RESERVED:
                raise Conflict("预占已失效，结算被拒绝，请重新计算", {"occupation_id": occ["id"]})
            if occ["term_id"] != claim["term_id"]:
                raise Conflict("预占依据的合约版本与赔案不一致，请重新计算",
                               {"occupation_term": occ["term_version"], "claim_term": claim["term_version"]})
            batch.settle_occupation(occ["id"], actor.user_id)
            new_version = int(claim["version"]) + 1
            batch.update_claim(claim["id"], actor.user_id, state=CLAIM_SETTLED, version=new_version,
                               payment_reference=payment_reference)
            batch.audit("claim", claim["id"], actor.user_id, "settle", new_version,
                        {"occupation_id": occ["id"], "payment_reference": payment_reference,
                         "recoverable_amount": occ["occupied_amount"],
                         "term_version": occ["term_version"], "basis": occ.get("basis")})
            new_claim = self.repository._claim_row(batch.q(
                "SELECT * FROM claims WHERE id=?", (claim["id"],)
            ).fetchone())
            settled_occ = self.repository._occ_row(batch.q(
                "SELECT * FROM capacity_ledger WHERE id=?", (occ["id"],)
            ).fetchone())
            return {"claim": new_claim, "occupation": settled_occ}

        return self._run(batch_no, "settle_claim", "claim:%s" % claim_id, work)

    def reject_claim(self, actor: Actor, claim_id: int, expected_version: Optional[int],
                     data: Dict[str, Any]) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        if not self.rules.role_can_claim_action(actor.role, "reject"):
            raise PermissionDenied("角色无权拒赔")
        data = dict(data or {})
        reason = text(data, "reject_reason")
        batch_no = self._batch_no(data)

        def work(batch):
            claim_row = batch.q("SELECT * FROM claims WHERE id=?", (claim_id,)).fetchone()
            if claim_row is None:
                from .domain import NotFound
                raise NotFound("赔案不存在")
            claim = self.repository._claim_row(claim_row)
            self.rules.assert_expected_version(claim["version"], expected_version, {"claim_id": claim_id})
            self.rules.require_claim_transition(claim, "reject")
            released_occ = None
            if claim.get("occupation_id"):
                occ_row = batch.q("SELECT * FROM capacity_ledger WHERE id=?", (claim["occupation_id"],)).fetchone()
                if occ_row is not None:
                    occ = self.repository._occ_row(occ_row)
                    if occ["status"] == OCC_RESERVED:
                        # 未结算占用随拒赔失效，容量立刻归还
                        batch.update_occupation(occ["id"], OCC_RELEASED, actor.user_id,
                                                released_reason="claim_rejected")
                        released_occ = self.repository._occ_row(batch.q(
                            "SELECT * FROM capacity_ledger WHERE id=?", (occ["id"],)
                        ).fetchone())
                    elif occ["status"] == OCC_SETTLED:
                        raise Conflict("赔案已结算，不能拒赔")
            new_version = int(claim["version"]) + 1
            batch.update_claim(claim["id"], actor.user_id, state=CLAIM_REJECTED, version=new_version,
                               rejection_reason=reason)
            batch.audit("claim", claim["id"], actor.user_id, "reject", new_version,
                        {"reject_reason": reason,
                         "released_occupation_id": released_occ["id"] if released_occ else None})
            new_claim = self.repository._claim_row(batch.q(
                "SELECT * FROM claims WHERE id=?", (claim["id"],)
            ).fetchone())
            out = {"claim": new_claim}
            if released_occ is not None:
                out["occupation"] = released_occ
            return out

        return self._run(batch_no, "reject_claim", "claim:%s" % claim_id, work)

    def get_claim(self, actor: Actor, claim_id: int) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        return self.repository.get_claim(claim_id)

    def list_claims(self, actor: Actor, reference: Optional[str] = None, state: Optional[str] = None,
                    limit: int = 100) -> List[Dict[str, Any]]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        contract_pk = None
        if reference:
            contract_pk = int(self.repository.get_contract(reference)["contract_pk"])
        return self.repository.list_claims(contract_pk=contract_pk, state=state, limit=limit)

    # ---- 待复核 ----
    def list_reviews(self, actor: Actor, status: Optional[str] = None, reference: Optional[str] = None,
                     limit: int = 100) -> List[Dict[str, Any]]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        contract_pk = None
        if reference:
            contract_pk = int(self.repository.get_contract(reference)["contract_pk"])
        return self.repository.list_reviews(status=status, contract_pk=contract_pk, limit=limit)

    def resolve_review(self, actor: Actor, review_id: int, data: Dict[str, Any]) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        if actor.role not in ("admin", "finance", "underwriter"):
            raise PermissionDenied("角色无权处理待复核")
        note = text(data or {}, "resolution_note")
        return self.repository.resolve_review(review_id, actor.user_id, note)

    # ---- 审计 ----
    def timeline(self, actor: Actor, subject_type: str, subject_id: int) -> List[Dict[str, Any]]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        return self.audit.timeline(subject_type, subject_id)

    # ---- 恢复 ----
    def recover(self, actor: Actor) -> Dict[str, Any]:
        actor = self._actor(actor)
        if actor.role != "admin":
            raise PermissionDenied("仅管理员可触发恢复")
        return self.repository.recover()

    # ---- 统计 ----
    def stats(self, actor: Actor) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        return self.repository.stats()
