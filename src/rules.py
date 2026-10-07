"""合约版本、赔案状态、分层摊回与容量账校验规则。

容量账语义（合约、赔案、占用、审计共用同一套数字口径）：

- 合约每次调整生成新版本，层宽/分出比例/聚合容量/恢复次数上限按版本固化。
- 赔案进入计算（calculate）时，按其当时引用的合约版本预占容量，
  占用记录保存版本与计算依据快照；结算只认可同一笔预占。
- reserved（已预占未结算）在拒赔、金额改小、合约版本变化后释放失效并重算；
  settled（已结算）的占用永久保留当时依据，新老依据的差额进入待复核。
"""
from typing import Any, Dict, List, Optional, Tuple

from .domain import Actor, CapacityExceeded, Conflict, ValidationError, integer, number, optional_integer, text

# 合约状态
CONTRACT_DRAFT = "draft"
CONTRACT_ACTIVE = "active"
CONTRACT_SUSPENDED = "suspended"

# 赔案状态
CLAIM_SUBMITTED = "submitted"
CLAIM_RESERVED = "reserved"
CLAIM_SETTLED = "settled"
CLAIM_REJECTED = "rejected"

# 容量占用状态
OCC_RESERVED = "reserved"
OCC_SETTLED = "settled"
OCC_RELEASED = "released"
OCC_SUPERSEDED = "superseded"

ACTIVE_OCC_STATES = (OCC_RESERVED, OCC_SETTLED)

# 待复核类型
REVIEW_SETTLED_DIFF = "settled_diff"
REVIEW_RECALC_SHORTFALL = "recalc_shortfall"
REVIEW_LEGACY_UNVERIFIABLE = "legacy_unverifiable"
REVIEW_BACKFILL_OVERRUN = "backfill_overrun"
REVIEW_OPEN = "open"
REVIEW_RESOLVED = "resolved"

CREATE_ROLES = {"underwriter"}
CONTRACT_ACTION_ROLES = {
    "bind": {"underwriter"},
    "adjust": {"underwriter"},
}
CLAIM_ACTION_ROLES = {
    "submit_claim": {"claims_officer"},
    "calculate": {"claims_officer"},
    "adjust_claim": {"claims_officer"},
    "settle": {"finance"},
    "reject": {"finance", "claims_officer"},
}

CONTRACT_TRANSITIONS = {
    "bind": {CONTRACT_DRAFT: CONTRACT_ACTIVE},
    "adjust": {CONTRACT_ACTIVE: CONTRACT_ACTIVE},
}
CLAIM_TRANSITIONS = {
    "submit_claim": {},  # 创建即 submitted
    "calculate": {CLAIM_SUBMITTED: CLAIM_RESERVED, CLAIM_RESERVED: CLAIM_RESERVED},
    "adjust_claim": {CLAIM_RESERVED: CLAIM_RESERVED},
    "settle": {CLAIM_RESERVED: CLAIM_SETTLED},
    "reject": {CLAIM_SUBMITTED: CLAIM_REJECTED, CLAIM_RESERVED: CLAIM_REJECTED},
}

EPS = 0.01


class DomainRules:
    # ---- 角色 ----
    def known_role(self, role: str) -> bool:
        all_roles = set(CREATE_ROLES)
        for roles in CONTRACT_ACTION_ROLES.values():
            all_roles.update(roles)
        for roles in CLAIM_ACTION_ROLES.values():
            all_roles.update(roles)
        return role == "admin" or role in all_roles

    def role_can_create_contract(self, role: str) -> bool:
        return role == "admin" or role in CREATE_ROLES

    def role_can_contract_action(self, role: str, action: str) -> bool:
        return role == "admin" or role in CONTRACT_ACTION_ROLES.get(action, set())

    def role_can_claim_action(self, role: str, action: str) -> bool:
        return role == "admin" or role in CLAIM_ACTION_ROLES.get(action, set())

    # ---- 合约版本条款 ----
    def normalize_terms(self, data: Dict[str, Any]) -> Dict[str, Any]:
        p = dict(data or {})
        attachment = number(p, "attachment", 0)
        limit = number(p, "limit", 0)
        cession_pct = number(p, "cession_pct", 0, 1)
        reinstatement_pct = number(p, "reinstatement_pct", 0, 1)
        max_reinstatements = integer(p, "max_reinstatements", 0, 100)
        if limit <= attachment:
            raise ValidationError("赔款限额必须高于起赔点")
        width = round(limit - attachment, 2)
        layer_capacity = round(width * cession_pct, 2)
        # 每次恢复填满一层容量；恢复次数上限即有效层数
        aggregate_capacity = round(layer_capacity * (1 + max_reinstatements), 2)
        return {
            "attachment": round(attachment, 2),
            "limit": round(limit, 2),
            "layer_width": width,
            "cession_pct": cession_pct,
            "reinstatement_pct": reinstatement_pct,
            "max_reinstatements": max_reinstatements,
            "layer_capacity": layer_capacity,
            "aggregate_capacity": aggregate_capacity,
        }

    def validate_contract_create(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        p = dict(payload or {})
        text(p, "event_id")
        terms = self.normalize_terms(p)
        out = {"event_id": text(p, "event_id"), "note": str(p.get("note") or "")}
        out.update(terms)
        return out

    # ---- 摊回计算（依据快照的唯一来源） ----
    @staticmethod
    def recovery_basis(terms: Dict[str, Any], approved_loss: float) -> Dict[str, Any]:
        width = float(terms["layer_width"])
        cession = float(terms["cession_pct"])
        retained = max(0.0, round(approved_loss - float(terms["attachment"]), 2))
        recovery = round(min(retained, width) * cession, 2)
        reinstatements_used = 0.0
        layer_capacity = float(terms["layer_capacity"])
        if recovery > 0 and layer_capacity > 0:
            reinstatements_used = round(recovery / layer_capacity, 6)
        premium_rate = float(terms.get("reinstatement_pct", 0.0))
        premium = round(recovery * premium_rate, 2)
        return {
            "approved_loss": round(float(approved_loss), 2),
            "attachment": float(terms["attachment"]),
            "layer_width": width,
            "cession_pct": cession,
            "retained_loss": retained,
            "recoverable_amount": recovery,
            "reinstatements_used": reinstatements_used,
            "reinstatement_premium": premium,
        }

    # ---- 容量账（对 reserved + settled 占用统一求和） ----
    @staticmethod
    def capacity_usage(occupations: List[Dict[str, Any]]) -> Tuple[float, float]:
        used_amount = 0.0
        used_reinst = 0.0
        for occ in occupations:
            if occ["status"] not in ACTIVE_OCC_STATES:
                continue
            used_amount += float(occ["occupied_amount"])
            used_reinst += float(occ.get("reinstatements_used", 0.0))
        return round(used_amount, 2), round(used_reinst, 6)

    def assert_capacity(self, terms: Dict[str, Any], occupations: List[Dict[str, Any]], extra_amount: float = 0.0,
                        extra_reinst: float = 0.0, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        used_amount, used_reinst = self.capacity_usage(occupations)
        capacity = float(terms["aggregate_capacity"])
        max_reinst = 1.0 + float(terms["max_reinstatements"])
        projected_amount = round(used_amount + extra_amount, 2)
        projected_reinst = round(used_reinst + extra_reinst, 6)
        result = {
            "capacity": round(capacity, 2),
            "used_amount": used_amount,
            "available_amount": round(max(0.0, capacity - used_amount), 2),
            "max_reinstatements": max_reinst,
            "used_reinstatements": used_reinst,
            "projected_amount": projected_amount,
            "projected_reinstatements": projected_reinst,
        }
        if projected_amount > capacity + EPS:
            raise CapacityExceeded("累计摊回超过合约聚合容量", context or result)
        if projected_reinst > max_reinst + 1e-6:
            raise CapacityExceeded("累计恢复次数超过合约上限", context or result)
        return result

    # ---- 状态转换 ----
    def require_contract_transition(self, contract: Dict[str, Any], action: str) -> str:
        allowed = CONTRACT_TRANSITIONS.get(action, {}).get(contract["state"])
        if allowed is None:
            raise Conflict("当前合约状态不允许执行%s" % action)
        return allowed

    def require_claim_transition(self, claim: Dict[str, Any], action: str) -> str:
        allowed = CLAIM_TRANSITIONS.get(action, {}).get(claim["state"])
        if allowed is None:
            raise Conflict("当前赔案状态不允许执行%s" % action)
        return allowed

    def assert_expected_version(self, current_version: int, expected_version: Optional[int], context: Dict[str, Any]) -> None:
        if expected_version is not None and int(expected_version) != int(current_version):
            ctx = dict(context)
            ctx.update({"current_version": current_version, "expected_version": expected_version})
            raise Conflict("版本已变化，请基于最新版本重试", ctx)

    def assert_expected_term_version(self, current: int, expected: Optional[int], context: Dict[str, Any]) -> None:
        if expected is not None and int(expected) != int(current):
            ctx = dict(context)
            ctx.update({"current_term_version": current, "expected_term_version": expected})
            raise Conflict("合约版本已变化，请基于最新版本重试", ctx)
