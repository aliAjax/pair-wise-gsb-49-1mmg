"""共享容量账用例编排。

合约、赔案、占用、审计、待复核共用一份容量账：
- calculate 先按合约版本预占保额，settle 只认可这笔预占；
- 拒赔 / 金额改小 / 合约版本变化：未结算占用失效重算，已结算保留当时依据、差额进待复核；
- 两个窗口同时写：先到版本生效，后到者乐观冲突，输入保留、可重放；
- 所有写入批次化；request_id 幂等；入库中断后从最近完整批次恢复，重试不重复占额/审计。
"""
import threading
import uuid
from typing import Any, Callable, Dict, List, Optional

from . import ledger_rules as rules
from .domain import (
    Actor, Conflict, OptimisticConflict, PermissionDenied, ValidationError, text,
)
from .ledger_repository import (
    CLAIMS, CONTRACTS, OCCUPATIONS, REVIEWS, LedgerRepository,
)


ROLE_CREATE_CONTRACT = {"underwriter", "admin"}
ROLE_ADJUST_CONTRACT = {"underwriter", "admin"}
ROLE_CLAIMS = {"claims_officer", "admin"}
ROLE_SETTLE = {"finance", "admin"}
ROLE_REJECT = {"finance", "claims_officer", "admin"}
ROLE_REVIEW = {"finance", "claims_officer", "admin"}
ROLE_ANY = {"underwriter", "claims_officer", "finance", "admin"}
KNOWN_ROLES = ROLE_ANY

CONTRACT_ENTITY = "contract"
CLAIM_ENTITY = "claim"


class _WorkerCtx:
    def __init__(self, repo: LedgerRepository, conn, batch_id: int, actor: Actor):
        self.repo = repo
        self.conn = conn
        self.batch_id = batch_id
        self.actor = actor

    # 常用 worker 内快捷方法
    def get_contract(self, contract_id: int) -> Dict[str, Any]:
        return self.repo.conn_get(self.conn, CONTRACTS, contract_id)

    def get_claim(self, claim_id: int) -> Dict[str, Any]:
        return self.repo.conn_get(self.conn, CLAIMS, claim_id)

    def active_terms(self, contract: Dict[str, Any]) -> Dict[str, Any]:
        version_no = int(contract["active_version"])
        row = self.repo.conn_get_version(self.conn, contract["id"], version_no)
        return self.repo.version_terms(row)

    def occupations(self, contract_id: int) -> List[Dict[str, Any]]:
        return self.repo.conn_all_occupations(self.conn, contract_id)

    def audit(self, action: str, details: Dict[str, Any], contract_id: Optional[int] = None,
              claim_id: Optional[int] = None) -> None:
        self.repo.insert_audit(
            self.conn, self.batch_id, action, self.actor.user_id, details,
            contract_id=contract_id, claim_id=claim_id,
        )


class LedgerService:
    def __init__(self, repository: LedgerRepository) -> None:
        self.repository = repository
        self._write_lock = threading.Lock()
        # 启动即从最近完整批次恢复，清理任何残留 open 批次。
        self.recover()

    # ---------- 身份与请求 ----------
    @staticmethod
    def _actor(actor: Actor) -> Actor:
        if actor is None or not actor.user_id.strip() or not actor.role.strip():
            raise PermissionDenied("缺少调用身份")
        return actor

    def _ensure_role(self, actor: Actor, allowed: set) -> None:
        if actor.role not in allowed:
            raise PermissionDenied("角色无权执行该操作")

    def recover(self) -> List[Dict[str, Any]]:
        with self._write_lock:
            return self.repository.recover_open_batches()

    def arm_crash(self, marker: str) -> None:
        """测试钩子：在下一次批次提交点模拟入库中断。"""
        self.repository._pending_crash = marker

    def _run_batch(self, request_id: str, batch_type: str, actor: Actor,
                   worker: Callable[[_WorkerCtx], Dict[str, Any]],
                   crash_marker: str = "", reuse_id: Optional[int] = None) -> Dict[str, Any]:
        request_id = request_id or uuid.uuid4().hex
        with self._write_lock:
            # 测试钩子：未显式指定提交点时，采用已布防的崩溃标记。
            if not crash_marker:
                crash_marker = self.repository._pending_crash or ""
            # 写前恢复：把上一轮遗留的 open 批次撤回，容量账回到最近完整批次。
            self.repository.recover_open_batches()
            existing = self.repository.find_batch(request_id)
            if existing is not None:
                if existing["status"] == "complete":
                    # 幂等：同一请求重放，直接返回上次结果，不重复占额也不追加审计。
                    result = dict(existing.get("result_json") or {})
                    result["replayed"] = True
                    result["batch_id"] = int(existing["id"])
                    return result
                # abandoned（重试一个未完成的请求）：复用该意图行重新跑。
                reuse_id = int(existing["id"])
            batch_id, conn = self.repository.open_batch(request_id, batch_type, actor.user_id, reuse_id=reuse_id)
            ctx = _WorkerCtx(self.repository, conn, batch_id, actor)
            try:
                result = worker(ctx)
            except OptimisticConflict as exc:
                # 后到版本冲突：保留输入随批次提交，再把冲突抛回调用方。
                conflict_result = {"conflict": True, "retained_id": 0}
                conflict_result.update(exc.payload or {})
                self.repository.complete_batch(conn, batch_id, conflict_result)
                raise
            except Exception:
                self.repository.abandon_in_progress(conn, batch_id)
                raise
            try:
                result = dict(result)
                result["batch_id"] = batch_id
                result["request_id"] = request_id
                return self.repository.complete_batch(conn, batch_id, result, crash_marker=crash_marker)
            except Exception:
                self.repository.close_failed(conn)
                raise

    @staticmethod
    def _retained_conflict(ctx: _WorkerCtx, entity_type: str, entity_id: int, action: str,
                           data: Dict[str, Any], expected_version: Optional[int], current_version: int) -> OptimisticConflict:
        reason = "版本依据已失效，请刷新后重放"
        retained_id = ctx.repo.insert_retained_row(
            ctx.conn, ctx.batch_id, entity_type, entity_id, action, data,
            int(expected_version) if expected_version is not None else None,
            int(current_version), ctx.actor.user_id, reason,
        )
        return OptimisticConflict(
            reason,
            {"retained_id": retained_id, "entity_type": entity_type, "entity_id": entity_id,
             "action": action, "current_version": int(current_version),
             "expected_version": int(expected_version) if expected_version is not None else None},
        )

    # ---------- 合约 ----------
    def create_contract(self, actor: Actor, reference: str, payload: Dict[str, Any],
                        request_id: str = "") -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_role(actor, ROLE_CREATE_CONTRACT)
        reference = text({"reference": reference}, "reference")
        terms = rules.validate_contract_terms(payload or {})
        name = (payload or {}).get("name")
        name = name.strip() if isinstance(name, str) else ""
        if self.repository.get_contract_by_reference(reference):
            raise Conflict("合约编号已存在")

        def worker(ctx: _WorkerCtx) -> Dict[str, Any]:
            contract_id = ctx.repo.insert_contract(ctx.conn, ctx.batch_id, reference, name, actor.user_id)
            ctx.repo.insert_version(ctx.conn, ctx.batch_id, contract_id, 1, terms, actor.user_id)
            ctx.repo.update_row(ctx.conn, ctx.batch_id, CONTRACTS, contract_id,
                                {"active_version": 1, "updated_at": _now_text()})
            ctx.audit("contract_created", {"summary": "合约已创建", "reference": reference, "version": 1},
                      contract_id=contract_id)
            contract = ctx.get_contract(contract_id)
            contract["terms"] = dict(terms, contract_version=1)
            return {"contract": contract, "terms": dict(terms, contract_version=1), "version": 1}

        return self._run_batch(request_id, "create_contract", actor, worker)

    def adjust_contract(self, actor: Actor, contract_id: int, expected_version: int, payload: Dict[str, Any],
                        request_id: str = "") -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_role(actor, ROLE_ADJUST_CONTRACT)
        terms = rules.validate_contract_terms(payload or {})

        def worker(ctx: _WorkerCtx) -> Dict[str, Any]:
            contract = ctx.get_contract(contract_id)
            current_version = int(contract["active_version"])
            if int(expected_version) != current_version:
                raise self._retained_conflict(ctx, CONTRACT_ENTITY, contract_id, "adjust_contract",
                                              payload, expected_version, current_version)
            new_version = current_version + 1
            terms["contract_version"] = new_version
            ctx.repo.insert_version(ctx.conn, ctx.batch_id, contract_id, new_version, terms, actor.user_id)
            ctx.repo.update_row(ctx.conn, ctx.batch_id, CONTRACTS, contract_id,
                                {"active_version": new_version, "updated_at": _now_text()})
            stale_claims: List[int] = []
            reviews_created: List[int] = []
            for occupation in ctx.occupations(contract_id):
                if occupation["status"] not in rules.ACTIVE_OCCUPATIONS:
                    continue
                claim = ctx.get_claim(int(occupation["claim_id"]))
                if occupation["status"] == rules.OCCUPATION_RESERVED:
                    # 未结算占用一律失效，等赔案按新版本重算。
                    ctx.repo.update_row(ctx.conn, ctx.batch_id, OCCUPATIONS, occupation["id"],
                                        {"status": rules.OCCUPATION_STALE})
                    ctx.repo.update_row(ctx.conn, ctx.batch_id, CLAIMS, claim["id"], {
                        "state": "recalculate_required", "current_occupation_id": None,
                        "claim_version": int(claim["claim_version"]) + 1, "updated_at": _now_text(),
                    })
                    stale_claims.append(int(claim["id"]))
                    ctx.audit("occupation_invalidated", {
                        "summary": "合约版本变化，未结算预占失效，需按v%s重算" % new_version,
                        "occupation_id": occupation["id"], "old_version": current_version,
                        "new_version": new_version,
                    }, contract_id=contract_id, claim_id=int(claim["id"]))
                else:
                    # 已结算：保留当时依据，仅把新旧差额放进待复核。
                    loss = claim.get("approved_loss")
                    difference = rules.diff_basis(float(loss), occupation["basis_json"], terms)
                    if abs(difference["difference"]) > rules.MONEY_EPSILON:
                        review_id = ctx.repo.insert_review(
                            ctx.conn, ctx.batch_id, "version_diff",
                            {"summary": "已结算摊回在新版本下存在差额", **difference},
                            actor.user_id, contract_id=contract_id, claim_id=int(claim["id"]),
                            occupation_id=occupation["id"],
                        )
                        reviews_created.append(review_id)
                        ctx.audit("settled_basis_preserved", {
                            "summary": "已结算依据保留(v%s)，差额%s进入待复核" % (
                                difference["old_version"], difference["difference"]),
                            "occupation_id": occupation["id"], "review_id": review_id,
                            "difference": difference["difference"],
                        }, contract_id=contract_id, claim_id=int(claim["id"]))
            ctx.audit("contract_adjusted", {
                "summary": "合约已调整为v%s" % new_version,
                "from_version": current_version, "to_version": new_version,
                "stale_claims": stale_claims, "reviews": reviews_created,
            }, contract_id=contract_id)
            return {"contract": ctx.get_contract(contract_id), "terms": terms, "version": new_version,
                    "invalidated_claims": stale_claims, "review_ids": reviews_created}

        return self._run_batch(request_id, "adjust_contract", actor, worker)

    def get_contract_detail(self, actor: Actor, contract_id: int) -> Dict[str, Any]:
        actor = self._actor(actor)
        contract = self.repository.get_contract(contract_id)
        return self._contract_detail(contract_id, contract)

    def _contract_detail(self, contract_id: int, contract: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        contract = contract or self.repository.get_contract(contract_id)
        version_row = self.repository.get_version(contract_id, int(contract["active_version"]))
        terms = self.repository.version_terms(version_row)
        active = self.repository.active_occupations(contract_id)
        amounts = [float(item["amount"]) for item in active]
        projected = rules.summarize_layers(amounts, terms)
        result = dict(contract)
        result["terms"] = terms
        result["versions"] = [self.repository.version_terms(v) for v in self.repository.list_versions(contract_id)]
        result["ledger"] = {
            "total_reserved": projected["total_reserved"],
            "capacity": projected["capacity"],
            "base_capacity": projected["base_capacity"],
            "available": projected["available"],
            "layers": projected["layers"],
            "over_capacity": projected["over_capacity"],
            "reinstatements_used": projected["reinstatements_used"],
            "max_reinstatements": projected["max_reinstatements"],
            "active_occupation_count": len(amounts),
        }
        return result

    def list_contracts(self, actor: Actor, limit: int = 100) -> List[Dict[str, Any]]:
        actor = self._actor(actor)
        return [self._contract_detail(int(item["id"]), item) for item in self.repository.list_contracts(limit=limit)]

    # ---------- 赔案 ----------
    def submit_claim(self, actor: Actor, contract_id: int, data: Dict[str, Any], request_id: str = "") -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_role(actor, ROLE_CLAIMS)
        claim_number = text(data or {}, "claim_number")
        if self.repository.get_claim_by_number(claim_number):
            raise Conflict("赔案号已存在")

        def worker(ctx: _WorkerCtx) -> Dict[str, Any]:
            contract = ctx.get_contract(contract_id)  # 合约必须存在
            claim_id = ctx.repo.insert_claim(ctx.conn, ctx.batch_id, contract_id, claim_number,
                                             actor.user_id, approved_loss=None, is_legacy=False,
                                             legacy_batch_no=None, state="submitted")
            ctx.audit("claim_submitted", {"summary": "赔案已提交，等待计算预占", "claim_number": claim_number},
                      contract_id=contract_id, claim_id=claim_id)
            return {"claim": ctx.get_claim(claim_id)}

        return self._run_batch(request_id, "submit_claim", actor, worker)

    def calculate(self, actor: Actor, claim_id: int, expected_version: int, data: Dict[str, Any],
                  request_id: str = "", crash_marker: str = "") -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_role(actor, ROLE_CLAIMS)
        approved_loss = rules.validate_approved_loss(data or {})

        def worker(ctx: _WorkerCtx) -> Dict[str, Any]:
            claim = ctx.get_claim(claim_id)
            if int(claim["claim_version"]) != int(expected_version):
                raise self._retained_conflict(ctx, CLAIM_ENTITY, claim_id, "calculate",
                                              {"approved_loss": approved_loss, **(data or {})},
                                              expected_version, int(claim["claim_version"]))
            if not rules.claim_can_calculate(claim["state"]):
                raise Conflict("当前赔案状态不允许计算：%s" % claim["state"])
            contract = ctx.get_contract(int(claim["contract_id"]))
            terms = ctx.active_terms(contract)
            recovery = rules.recovery_for(approved_loss, terms)
            if recovery <= 0:
                raise ValidationError("核定损失未超过起赔点，无可预占摊回")
            active = ctx.repo.conn_active_occupations(ctx.conn, int(claim["contract_id"]))
            others = [float(item["amount"]) for item in active if int(item["claim_id"]) != claim_id]
            projected = rules.project_occupations(terms, others, recovery)
            basis = rules.basis_of(recovery, terms)
            basis["approved_loss"] = rules.money(approved_loss)
            basis["reinstatement_layer"] = projected["new_layer"]
            basis["layers_after"] = projected["layers"]
            occupation_id = ctx.repo.insert_occupation(
                ctx.conn, ctx.batch_id, int(claim["contract_id"]), claim_id,
                rules.OCCUPATION_RESERVED, recovery, basis["reinstatement_premium"], basis,
            )
            old_occupation_id = claim.get("current_occupation_id")
            if old_occupation_id:
                # 金额改小后重算：旧的未结算预占失效，结算只认可新的这笔。
                old = ctx.repo.conn_get(ctx.conn, OCCUPATIONS, int(old_occupation_id))
                if old["status"] == rules.OCCUPATION_RESERVED:
                    ctx.repo.update_row(ctx.conn, ctx.batch_id, OCCUPATIONS, int(old_occupation_id),
                                        {"status": rules.OCCUPATION_STALE, "superseded_by": occupation_id})
            ctx.repo.update_row(ctx.conn, ctx.batch_id, CLAIMS, claim_id, {
                "state": "calculated", "approved_loss": rules.money(approved_loss),
                "current_occupation_id": occupation_id,
                "claim_version": int(claim["claim_version"]) + 1, "updated_at": _now_text(),
            })
            ctx.audit("capacity_reserved", {
                "summary": "按合约v%s预占保额%s" % (terms["contract_version"], recovery),
                "occupation_id": occupation_id, "superseded": int(old_occupation_id) if old_occupation_id else None,
                "basis": basis, "approved_loss": rules.money(approved_loss),
                "total_reserved": projected["total_reserved"], "available": projected["available"],
                "reinstatements_used": projected["reinstatements_used"],
            }, contract_id=int(claim["contract_id"]), claim_id=claim_id)
            return {
                "claim": ctx.get_claim(claim_id), "occupation_id": occupation_id,
                "basis": basis, "ledger_after": projected,
            }

        return self._run_batch(request_id, "calculate", actor, worker, crash_marker=crash_marker)

    def settle(self, actor: Actor, claim_id: int, expected_version: int, data: Dict[str, Any],
               request_id: str = "") -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_role(actor, ROLE_SETTLE)
        payment_reference = text(data or {}, "payment_reference")

        def worker(ctx: _WorkerCtx) -> Dict[str, Any]:
            claim = ctx.get_claim(claim_id)
            if int(claim["claim_version"]) != int(expected_version):
                raise self._retained_conflict(ctx, CLAIM_ENTITY, claim_id, "settle",
                                              {"payment_reference": payment_reference},
                                              expected_version, int(claim["claim_version"]))
            if not rules.claim_can_settle(claim["state"]):
                raise Conflict("只有已预占待结算的赔案可以结算")
            occupation_id = claim.get("current_occupation_id")
            if not occupation_id:
                raise Conflict("赔案缺少有效预占，结算无依据")
            occupation = ctx.repo.conn_get(ctx.conn, OCCUPATIONS, int(occupation_id))
            if occupation["status"] != rules.OCCUPATION_RESERVED:
                raise Conflict("预占已失效，结算只认可当前这笔reserved占用")
            basis = json_load(occupation["basis_json"])
            ctx.repo.update_row(ctx.conn, ctx.batch_id, OCCUPATIONS, int(occupation_id),
                                {"status": rules.OCCUPATION_SETTLED})
            ctx.repo.update_row(ctx.conn, ctx.batch_id, CLAIMS, claim_id, {
                "state": "settled", "claim_version": int(claim["claim_version"]) + 1,
                "updated_at": _now_text(),
            })
            ctx.audit("recovery_settled", {
                "summary": "摊回已结算，认可预占#%s（合约v%s）" % (
                    occupation_id, basis.get("contract_version")),
                "occupation_id": int(occupation_id), "payment_reference": payment_reference,
                "settled_amount": occupation["amount"], "basis": basis,
            }, contract_id=int(claim["contract_id"]), claim_id=claim_id)
            updated_occupation = ctx.repo.conn_get_occupation(ctx.conn, int(occupation_id))
            return {"claim": ctx.get_claim(claim_id), "occupation": updated_occupation,
                    "payment_reference": payment_reference}

        return self._run_batch(request_id, "settle", actor, worker)

    def reject(self, actor: Actor, claim_id: int, expected_version: int, data: Dict[str, Any],
               request_id: str = "") -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_role(actor, ROLE_REJECT)
        reason = text(data or {}, "reject_reason")

        def worker(ctx: _WorkerCtx) -> Dict[str, Any]:
            claim = ctx.get_claim(claim_id)
            if int(claim["claim_version"]) != int(expected_version):
                raise self._retained_conflict(ctx, CLAIM_ENTITY, claim_id, "reject",
                                              {"reject_reason": reason},
                                              expected_version, int(claim["claim_version"]))
            if not rules.claim_can_reject(claim["state"]):
                raise Conflict("当前赔案状态不允许拒赔：%s" % claim["state"])
            occupation_id = claim.get("current_occupation_id")
            released = 0.0
            if occupation_id:
                occupation = ctx.repo.conn_get(ctx.conn, OCCUPATIONS, int(occupation_id))
                if occupation["status"] == rules.OCCUPATION_RESERVED:
                    ctx.repo.update_row(ctx.conn, ctx.batch_id, OCCUPATIONS, int(occupation_id),
                                        {"status": rules.OCCUPATION_VOIDED})
                    released = float(occupation["amount"])
            ctx.repo.update_row(ctx.conn, ctx.batch_id, CLAIMS, claim_id, {
                "state": "rejected", "current_occupation_id": None,
                "claim_version": int(claim["claim_version"]) + 1, "updated_at": _now_text(),
            })
            ctx.audit("claim_rejected", {
                "summary": "赔案已拒赔，未结算预占失效，释放保额%s" % rules.money(released),
                "occupation_id": int(occupation_id) if occupation_id else None,
                "released_amount": rules.money(released), "reject_reason": reason,
            }, contract_id=int(claim["contract_id"]), claim_id=claim_id)
            return {"claim": ctx.get_claim(claim_id), "released_amount": rules.money(released)}

        return self._run_batch(request_id, "reject", actor, worker)

    def get_claim_detail(self, actor: Actor, claim_id: int) -> Dict[str, Any]:
        actor = self._actor(actor)
        claim = self.repository.get_claim(claim_id)
        occupations = [item for item in self.repository.list_occupations(int(claim["contract_id"]))
                       if int(item["claim_id"]) == claim_id]
        result = dict(claim)
        result["occupations"] = occupations
        if claim.get("current_occupation_id"):
            result["current_occupation"] = self.repository.get_occupation(int(claim["current_occupation_id"]))
        return result

    def list_claims(self, actor: Actor, contract_id: Optional[int] = None, limit: int = 100) -> List[Dict[str, Any]]:
        actor = self._actor(actor)
        return self.repository.list_claims(contract_id=contract_id, limit=limit)

    # ---------- 旧赔案导入与回填 ----------
    def import_legacy_claims(self, actor: Actor, contract_id: int, claims: List[Dict[str, Any]],
                             request_id: str = "") -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_role(actor, ROLE_CLAIMS)
        if not isinstance(claims, list) or not claims:
            raise ValidationError("claims必须是非空列表")
        normalized: List[Dict[str, Any]] = []
        for index, item in enumerate(claims):
            if not isinstance(item, dict):
                raise ValidationError("claims[%s]必须是对象" % index)
            claim_number = text(item, "claim_number")
            legacy_batch_no = item.get("legacy_batch_no")
            if legacy_batch_no is not None and not isinstance(legacy_batch_no, str):
                raise ValidationError("legacy_batch_no必须是文本")
            loss = item.get("approved_loss")
            if loss is not None and (isinstance(loss, bool) or not isinstance(loss, (int, float)) or float(loss) < 0):
                raise ValidationError("approved_loss必须是非负数字")
            normalized.append({"claim_number": claim_number,
                               "approved_loss": None if loss is None else rules.money(float(loss)),
                               "legacy_batch_no": (legacy_batch_no or "").strip() or None})

        def worker(ctx: _WorkerCtx) -> Dict[str, Any]:
            contract = ctx.get_contract(contract_id)  # 合约存在性
            imported: List[int] = []
            pending: List[int] = []
            for item in normalized:
                if ctx.repo.get_claim_by_number_conn(ctx.conn, item["claim_number"]):
                    raise Conflict("赔案号已存在：%s" % item["claim_number"])
                loss = item["approved_loss"]
                state = "pending_verification" if loss is None else "submitted"
                claim_id = ctx.repo.insert_claim(
                    ctx.conn, ctx.batch_id, contract_id, item["claim_number"], actor.user_id,
                    approved_loss=loss, is_legacy=True, legacy_batch_no=item["legacy_batch_no"],
                    state=state,
                )
                imported.append(claim_id)
                if state == "pending_verification":
                    review_id = ctx.repo.insert_review(
                        ctx.conn, ctx.batch_id, "legacy_unclear",
                        {"summary": "旧赔案缺少可计算依据，待核", "reason": "缺少approved_loss"},
                        actor.user_id, contract_id=contract_id, claim_id=claim_id,
                    )
                    pending.append(claim_id)
                    ctx.audit("legacy_claim_pending", {
                        "summary": "旧赔案导入，算不清先待核", "review_id": review_id,
                        "reason": "缺少approved_loss",
                        "legacy_batch_no": item["legacy_batch_no"],
                    }, contract_id=contract_id, claim_id=claim_id)
                else:
                    ctx.audit("legacy_claim_imported", {
                        "summary": "旧赔案已导入，等待按现有金额回填占用",
                        "legacy_batch_no": item["legacy_batch_no"],
                    }, contract_id=contract_id, claim_id=claim_id)
            return {"imported": imported, "pending_verification": pending,
                    "imported_count": len(imported), "pending_count": len(pending)}

        return self._run_batch(request_id, "import_legacy_claims", actor, worker)

    def backfill_legacy(self, actor: Actor, request_id: str = "", crash_marker: str = "") -> Dict[str, Any]:
        """旧赔案缺占用批次号：按现有金额回填；算不清的先待核。单批次幂等。"""
        actor = self._actor(actor)
        self._ensure_role(actor, ROLE_CLAIMS)

        def worker(ctx: _WorkerCtx) -> Dict[str, Any]:
            backfilled: List[Dict[str, Any]] = []
            pending: List[Dict[str, Any]] = []
            # 按合约分组处理，容量账逐笔过账。
            rows = ctx.repo.legacy_claims_needing_backfill(ctx.conn)
            by_contract: Dict[int, List[Dict[str, Any]]] = {}
            for row in rows:
                by_contract.setdefault(int(row["contract_id"]), []).append(row)
            for contract_id, claim_rows in by_contract.items():
                contract = ctx.get_contract(contract_id)
                terms = ctx.active_terms(contract)
                active_amounts = [
                    float(item["amount"])
                    for item in ctx.repo.conn_active_occupations(ctx.conn, contract_id)
                ]
                for claim in claim_rows:
                    unclear = rules.looks_unclear_legacy(claim, terms)
                    if unclear:
                        review_id = ctx.repo.insert_review(
                            ctx.conn, ctx.batch_id, "legacy_unclear",
                            {"summary": "旧赔案无法回填，先待核", "reason": unclear},
                            actor.user_id, contract_id=contract_id, claim_id=int(claim["id"]),
                        )
                        ctx.repo.update_row(ctx.conn, ctx.batch_id, CLAIMS, claim["id"],
                                            {"state": "pending_verification"})
                        pending.append({"claim_id": int(claim["id"]), "review_id": review_id, "reason": unclear})
                        ctx.audit("legacy_backfill_pending", {
                            "summary": "旧赔案算不清，先待核", "reason": unclear,
                        }, contract_id=contract_id, claim_id=int(claim["id"]))
                        continue
                    loss = float(claim["approved_loss"])
                    recovery = rules.recovery_for(loss, terms)
                    try:
                        projected = rules.project_occupations(terms, active_amounts, recovery)
                    except Conflict as exc:
                        review_id = ctx.repo.insert_review(
                            ctx.conn, ctx.batch_id, "capacity_shortfall",
                            {"summary": "旧赔案回填时容量不足，先待核", "error": str(exc),
                             **(getattr(exc, "payload", {}) or {})},
                            actor.user_id, contract_id=contract_id, claim_id=int(claim["id"]),
                        )
                        ctx.repo.update_row(ctx.conn, ctx.batch_id, CLAIMS, claim["id"],
                                            {"state": "pending_verification"})
                        pending.append({"claim_id": int(claim["id"]), "review_id": review_id,
                                        "reason": "容量不足"})
                        ctx.audit("legacy_backfill_pending", {
                            "summary": "旧赔案回填容量不足，先待核", "review_id": review_id,
                        }, contract_id=contract_id, claim_id=int(claim["id"]))
                        continue
                    basis = rules.basis_of(recovery, terms)
                    basis["approved_loss"] = rules.money(loss)
                    basis["backfill"] = True
                    occupation_id = ctx.repo.insert_occupation(
                        ctx.conn, ctx.batch_id, contract_id, int(claim["id"]),
                        rules.OCCUPATION_RESERVED, recovery, basis["reinstatement_premium"], basis,
                    )
                    ctx.repo.update_row(ctx.conn, ctx.batch_id, CLAIMS, claim["id"], {
                        "state": "calculated", "current_occupation_id": occupation_id,
                        "claim_version": int(claim["claim_version"]) + 1, "updated_at": _now_text(),
                    })
                    active_amounts.append(recovery)
                    backfilled.append({"claim_id": int(claim["id"]), "occupation_id": occupation_id,
                                       "amount": recovery})
                    ctx.audit("legacy_backfilled", {
                        "summary": "旧赔案按现有金额回填占用（原批次号%s）" % claim.get("legacy_batch_no"),
                        "occupation_id": occupation_id, "amount": recovery, "basis": basis,
                        "legacy_batch_no": claim.get("legacy_batch_no"),
                    }, contract_id=contract_id, claim_id=int(claim["id"]))
            return {"backfilled": backfilled, "pending": pending,
                    "backfilled_count": len(backfilled), "pending_count": len(pending)}

        return self._run_batch(request_id, "backfill_legacy", actor, worker, crash_marker=crash_marker)

    # ---------- 待复核 ----------
    def list_reviews(self, actor: Actor, status: Optional[str] = None) -> List[Dict[str, Any]]:
        actor = self._actor(actor)
        return self.repository.list_reviews(status=status)

    def resolve_review(self, actor: Actor, review_id: int, data: Dict[str, Any], request_id: str = "") -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_role(actor, ROLE_REVIEW)
        action = text(data or {}, "action")
        if action not in ("recalculate", "dismiss"):
            raise ValidationError("action只能是recalculate/dismiss")
        note = (data or {}).get("note")
        note = note.strip() if isinstance(note, str) else ""

        def worker(ctx: _WorkerCtx) -> Dict[str, Any]:
            review = ctx.repo.conn_get(ctx.conn, REVIEWS, review_id)
            if review["status"] != "open":
                raise Conflict("待复核记录已处理")
            claim_id = review["claim_id"]
            resolution: Dict[str, Any] = {"action": action, "note": note}
            if action == "recalculate":
                if review["review_type"] == "version_diff":
                    raise Conflict("已结算依据不可重算，只能确认关闭（dismiss）")
                if not claim_id:
                    raise ValidationError("该待复核记录没有关联赔案，无法重算")
                claim = ctx.get_claim(int(claim_id))
                contract = ctx.get_contract(int(claim["contract_id"]))
                terms = ctx.active_terms(contract)
                unclear = rules.looks_unclear_legacy(claim, terms)
                if unclear:
                    raise Conflict("赔案仍算不清：%s" % unclear)
                loss = float(claim["approved_loss"])
                recovery = rules.recovery_for(loss, terms)
                others = [float(item["amount"]) for item in
                          ctx.repo.conn_active_occupations(ctx.conn, int(claim["contract_id"]))
                          if int(item["claim_id"]) != int(claim_id)]
                projected = rules.project_occupations(terms, others, recovery)
                basis = rules.basis_of(recovery, terms)
                basis["approved_loss"] = rules.money(loss)
                occupation_id = ctx.repo.insert_occupation(
                    ctx.conn, ctx.batch_id, int(claim["contract_id"]), int(claim_id),
                    rules.OCCUPATION_RESERVED, recovery, basis["reinstatement_premium"], basis,
                )
                ctx.repo.update_row(ctx.conn, ctx.batch_id, CLAIMS, int(claim_id), {
                    "state": "calculated", "current_occupation_id": occupation_id,
                    "claim_version": int(claim["claim_version"]) + 1, "updated_at": _now_text(),
                })
                resolution["occupation_id"] = occupation_id
                resolution["ledger_after"] = projected
                ctx.audit("review_recalculated", {
                    "summary": "待复核已处理：按当前版本重新预占%s" % recovery,
                    "review_id": review_id, "occupation_id": occupation_id,
                }, contract_id=int(claim["contract_id"]), claim_id=int(claim_id))
            else:
                ctx.audit("review_dismissed", {
                    "summary": "待复核已关闭（保留原依据）", "review_id": review_id, "note": note,
                }, contract_id=review["contract_id"], claim_id=claim_id)
            ctx.repo.update_row(ctx.conn, ctx.batch_id, REVIEWS, review_id, {
                "status": "resolved", "resolution_json": json_dump(resolution),
                "resolved_batch_id": ctx.batch_id, "resolved_at": _now_text(),
            })
            return {"review_id": review_id, "resolution": resolution}

        return self._run_batch(request_id, "resolve_review", actor, worker)

    # ---------- 冲突保留输入 ----------
    def list_retained(self, actor: Actor, status: str = "retained") -> List[Dict[str, Any]]:
        actor = self._actor(actor)
        return self.repository.list_retained(status=status)

    def replay_retained(self, actor: Actor, retained_id: int, data: Optional[Dict[str, Any]] = None,
                        request_id: str = "") -> Dict[str, Any]:
        """后到窗口看清冲突后重放：可用最新输入覆盖；仍按当时 action 走完整批次。"""
        actor = self._actor(actor)
        retained = self.repository.get_retained(retained_id)
        if retained["status"] != "retained":
            raise Conflict("保留输入已处理")
        import json as _json
        payload = dict(retained["input_json"])
        if data:
            payload.update(data)
        action = retained["action"]

        def worker(ctx: _WorkerCtx) -> Dict[str, Any]:
            if retained["entity_type"] == CONTRACT_ENTITY:
                contract = ctx.get_contract(int(retained["entity_id"]))
                expected = int(contract["active_version"])
                outcome = self._worker_adjust(ctx, int(retained["entity_id"]), expected, payload)
            else:
                claim = ctx.get_claim(int(retained["entity_id"]))
                expected = int(claim["claim_version"])
                outcome = self._worker_claim_action(ctx, action, int(retained["entity_id"]), expected, payload)
            ctx.repo.mark_retained(ctx.conn, ctx.batch_id, retained_id, "replayed")
            outcome["retained_id"] = retained_id
            return outcome

        return self._run_batch(request_id, "replay_%s" % action, actor, worker)

    def _worker_adjust(self, ctx: _WorkerCtx, contract_id: int, expected_version: int,
                       payload: Dict[str, Any]) -> Dict[str, Any]:
        terms = rules.validate_contract_terms(payload)
        contract = ctx.get_contract(contract_id)
        current_version = int(contract["active_version"])
        if expected_version != current_version:
            raise Conflict("合约在重放期间又被调整，请基于v%s再次重放" % current_version)
        new_version = current_version + 1
        terms["contract_version"] = new_version
        ctx.repo.insert_version(ctx.conn, ctx.batch_id, contract_id, new_version, terms, ctx.actor.user_id)
        ctx.repo.update_row(ctx.conn, ctx.batch_id, CONTRACTS, contract_id,
                            {"active_version": new_version, "updated_at": _now_text()})
        stale_claims: List[int] = []
        review_ids: List[int] = []
        for occupation in ctx.occupations(contract_id):
            if occupation["status"] not in rules.ACTIVE_OCCUPATIONS:
                continue
            claim = ctx.get_claim(int(occupation["claim_id"]))
            if occupation["status"] == rules.OCCUPATION_RESERVED:
                ctx.repo.update_row(ctx.conn, ctx.batch_id, OCCUPATIONS, occupation["id"],
                                    {"status": rules.OCCUPATION_STALE})
                ctx.repo.update_row(ctx.conn, ctx.batch_id, CLAIMS, claim["id"], {
                    "state": "recalculate_required", "current_occupation_id": None,
                    "claim_version": int(claim["claim_version"]) + 1, "updated_at": _now_text(),
                })
                stale_claims.append(int(claim["id"]))
            else:
                difference = rules.diff_basis(float(claim.get("approved_loss")), occupation["basis_json"], terms)
                if abs(difference["difference"]) > rules.MONEY_EPSILON:
                    review_id = ctx.repo.insert_review(
                        ctx.conn, ctx.batch_id, "version_diff",
                        {"summary": "已结算摊回在新版本下存在差额", **difference},
                        ctx.actor.user_id, contract_id=contract_id, claim_id=int(claim["id"]),
                        occupation_id=occupation["id"],
                    )
                    review_ids.append(review_id)
        ctx.audit("contract_adjusted", {"summary": "合约已调整为v%s（重放）" % new_version,
                                        "from_version": current_version, "to_version": new_version,
                                        "stale_claims": stale_claims, "reviews": review_ids},
                  contract_id=contract_id)
        return {"contract": ctx.get_contract(contract_id), "terms": terms, "version": new_version,
                "invalidated_claims": stale_claims, "review_ids": review_ids}

    def _worker_claim_action(self, ctx: _WorkerCtx, action: str, claim_id: int, expected_version: int,
                             payload: Dict[str, Any]) -> Dict[str, Any]:
        if action == "calculate":
            approved_loss = rules.validate_approved_loss(payload)
            claim = ctx.get_claim(claim_id)
            if int(claim["claim_version"]) != expected_version:
                raise Conflict("赔案在重放期间又被更新，请基于v%s再次重放" % claim["claim_version"])
            if not rules.claim_can_calculate(claim["state"]):
                raise Conflict("当前赔案状态不允许计算：%s" % claim["state"])
            contract = ctx.get_contract(int(claim["contract_id"]))
            terms = ctx.active_terms(contract)
            recovery = rules.recovery_for(approved_loss, terms)
            if recovery <= 0:
                raise ValidationError("核定损失未超过起赔点，无可预占摊回")
            others = [float(item["amount"]) for item in
                      ctx.repo.conn_active_occupations(ctx.conn, int(claim["contract_id"]))
                      if int(item["claim_id"]) != claim_id]
            projected = rules.project_occupations(terms, others, recovery)
            basis = rules.basis_of(recovery, terms)
            basis["approved_loss"] = rules.money(approved_loss)
            occupation_id = ctx.repo.insert_occupation(
                ctx.conn, ctx.batch_id, int(claim["contract_id"]), claim_id,
                rules.OCCUPATION_RESERVED, recovery, basis["reinstatement_premium"], basis,
            )
            old_occupation_id = claim.get("current_occupation_id")
            if old_occupation_id:
                old = ctx.repo.conn_get(ctx.conn, OCCUPATIONS, int(old_occupation_id))
                if old["status"] == rules.OCCUPATION_RESERVED:
                    ctx.repo.update_row(ctx.conn, ctx.batch_id, OCCUPATIONS, int(old_occupation_id),
                                        {"status": rules.OCCUPATION_STALE, "superseded_by": occupation_id})
            ctx.repo.update_row(ctx.conn, ctx.batch_id, CLAIMS, claim_id, {
                "state": "calculated", "approved_loss": rules.money(approved_loss),
                "current_occupation_id": occupation_id,
                "claim_version": int(claim["claim_version"]) + 1, "updated_at": _now_text(),
            })
            ctx.audit("capacity_reserved", {
                "summary": "按合约v%s预占保额%s（重放）" % (terms["contract_version"], recovery),
                "occupation_id": occupation_id, "basis": basis, "ledger_after": projected,
            }, contract_id=int(claim["contract_id"]), claim_id=claim_id)
            return {"claim": ctx.get_claim(claim_id), "occupation_id": occupation_id, "basis": basis}
        if action == "settle":
            payment_reference = text(payload, "payment_reference")
            claim = ctx.get_claim(claim_id)
            if int(claim["claim_version"]) != expected_version:
                raise Conflict("赔案在重放期间又被更新，请基于v%s再次重放" % claim["claim_version"])
            occupation_id = claim.get("current_occupation_id")
            occupation = ctx.repo.conn_get(ctx.conn, OCCUPATIONS, int(occupation_id))
            if occupation["status"] != rules.OCCUPATION_RESERVED:
                raise Conflict("预占已失效，无法结算")
            basis = json_load(occupation["basis_json"])
            ctx.repo.update_row(ctx.conn, ctx.batch_id, OCCUPATIONS, int(occupation_id),
                                {"status": rules.OCCUPATION_SETTLED})
            ctx.repo.update_row(ctx.conn, ctx.batch_id, CLAIMS, claim_id, {
                "state": "settled", "claim_version": int(claim["claim_version"]) + 1,
                "updated_at": _now_text(),
            })
            ctx.audit("recovery_settled", {
                "summary": "摊回已结算（重放），认可预占#%s" % occupation_id,
                "occupation_id": int(occupation_id), "payment_reference": payment_reference,
                "basis": basis,
            }, contract_id=int(claim["contract_id"]), claim_id=claim_id)
            return {"claim": ctx.get_claim(claim_id), "occupation_id": int(occupation_id),
                    "payment_reference": payment_reference}
        if action == "reject":
            reason = text(payload, "reject_reason")
            claim = ctx.get_claim(claim_id)
            if int(claim["claim_version"]) != expected_version:
                raise Conflict("赔案在重放期间又被更新，请基于v%s再次重放" % claim["claim_version"])
            occupation_id = claim.get("current_occupation_id")
            released = 0.0
            if occupation_id:
                occupation = ctx.repo.conn_get(ctx.conn, OCCUPATIONS, int(occupation_id))
                if occupation["status"] == rules.OCCUPATION_RESERVED:
                    ctx.repo.update_row(ctx.conn, ctx.batch_id, OCCUPATIONS, int(occupation_id),
                                        {"status": rules.OCCUPATION_VOIDED})
                    released = float(occupation["amount"])
            ctx.repo.update_row(ctx.conn, ctx.batch_id, CLAIMS, claim_id, {
                "state": "rejected", "current_occupation_id": None,
                "claim_version": int(claim["claim_version"]) + 1, "updated_at": _now_text(),
            })
            ctx.audit("claim_rejected", {
                "summary": "赔案已拒赔（重放），释放保额%s" % rules.money(released),
                "released_amount": rules.money(released), "reject_reason": reason,
            }, contract_id=int(claim["contract_id"]), claim_id=claim_id)
            return {"claim": ctx.get_claim(claim_id), "released_amount": rules.money(released)}
        raise ValidationError("不支持重放的动作：%s" % action)

    # ---------- 查询 / 审计 ----------
    def audit_timeline(self, actor: Actor, contract_id: Optional[int] = None,
                       claim_id: Optional[int] = None) -> List[Dict[str, Any]]:
        actor = self._actor(actor)
        return self.repository.audit_timeline(contract_id=contract_id, claim_id=claim_id)

    def occupations(self, actor: Actor, contract_id: int) -> List[Dict[str, Any]]:
        actor = self._actor(actor)
        self.repository.get_contract(contract_id)
        return self.repository.list_occupations(contract_id)


def _now_text() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


def json_load(value: str) -> Dict[str, Any]:
    import json
    return json.loads(value)


def json_dump(value: Dict[str, Any]) -> str:
    import json
    return json.dumps(value, ensure_ascii=False, sort_keys=True)
