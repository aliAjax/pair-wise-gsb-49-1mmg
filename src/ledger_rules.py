"""共享容量账的纯领域规则：合约版本、预占金额、容量余额、恢复次数。

账本上每一笔占用都带着它预占时的合约版本号和条款快照；本模块只负责
算清楚金额与余额，不碰数据库，也不产生副作用。
"""
from typing import Any, Dict, List, Optional, Tuple

from .domain import CapacityConflict, ValidationError, integer, number, text


# 赔案生命周期：进入 calculate 即预占，settle 只认可同一笔预占。
CLAIM_STATES = ("submitted", "calculated", "settled", "rejected", "recalculate_required", "pending_verification")
CLAIM_STATE_LABELS = {
    "submitted": "已提交",
    "calculated": "已预占待结算",
    "settled": "已结算",
    "rejected": "已拒赔",
    "recalculate_required": "合约版本变化待重算",
    "pending_verification": "待核",
}

# 占用状态：预占成立(reserved)、结算认可(settled)、失效重算(stale)、拒赔释放(voided)。
OCCUPATION_RESERVED = "reserved"
OCCUPATION_SETTLED = "settled"
OCCUPATION_STALE = "stale"
OCCUPATION_VOIDED = "voided"
ACTIVE_OCCUPATIONS = (OCCUPATION_RESERVED, OCCUPATION_SETTLED)

REVIEW_OPEN = "open"
REVIEW_RESOLVED = "resolved"
REVIEW_TYPES = ("version_diff", "capacity_shortfall", "legacy_unclear")

MONEY_EPSILON = 0.01


def money(value: float) -> float:
    return round(float(value), 2)


def validate_contract_terms(payload: Dict[str, Any]) -> Dict[str, float]:
    """校验合约分层条款，返回标准化后的数值字段。"""
    p = dict(payload or {})
    attachment = number(p, "attachment", 0)
    limit = number(p, "limit", 0)
    cession_pct = number(p, "cession_pct", 0, 1)
    reinstatement_pct = number(p, "reinstatement_pct", 0, 1)
    max_reinstatements = integer(p, "max_reinstatements", 0, 100)
    if limit <= attachment:
        raise ValidationError("赔款限额必须高于起赔点")
    return {
        "attachment": money(attachment),
        "limit": money(limit),
        "layer_width": money(limit - attachment),
        "cession_pct": cession_pct,
        "reinstatement_pct": reinstatement_pct,
        "max_reinstatements": max_reinstatements,
    }


def recovery_for(loss_amount: float, terms: Dict[str, Any]) -> float:
    """按某一版本条款计算单张赔案的摊回：分层内损失 × 分出比例。"""
    width = float(terms["layer_width"])
    retained = max(0.0, float(loss_amount) - float(terms["attachment"]))
    return money(min(retained, width) * float(terms["cession_pct"]))


def reinstatement_premium_for(recovery: float, terms: Dict[str, Any]) -> float:
    return money(recovery * float(terms["reinstatement_pct"]))


def basis_of(recovery: float, terms: Dict[str, Any]) -> Dict[str, Any]:
    """一笔占用的“当时依据”：条款快照 + 算出的金额。"""
    return {
        "contract_version": int(terms["contract_version"]),
        "attachment": money(terms["attachment"]),
        "limit": money(terms["limit"]),
        "layer_width": money(terms["layer_width"]),
        "cession_pct": float(terms["cession_pct"]),
        "reinstatement_pct": float(terms["reinstatement_pct"]),
        "max_reinstatements": int(terms["max_reinstatements"]),
        "recoverable_amount": money(recovery),
        "reinstatement_premium": reinstatement_premium_for(recovery, terms),
    }


def base_capacity(terms: Dict[str, Any]) -> float:
    """单层保额（容量账每层的容量）。"""
    return money(float(terms["layer_width"]) * float(terms["cession_pct"]))


def aggregate_capacity(terms: Dict[str, Any]) -> float:
    """总容量 = 基础层 + 每次恢复各一层，共 (1 + 最大恢复次数) 层。"""
    return money(base_capacity(terms) * (1 + int(terms["max_reinstatements"])))


def allocate_layers(active_amounts: List[float], new_amount: float,
                    terms: Dict[str, Any], replacing_amount: float = 0.0) -> Dict[str, Any]:
    """把已有占用和新占用按发生顺序逐层装入容量账。

    - 第 0 层是原始保障，第 k 层(k>=1)消耗第 k 次恢复，最多 max_reinstatements 次；
    - 单笔占用不得超过一层容量（赔案不能靠跨层拿到超过单层的摊回）；
    - 每个占用紧贴层顺序放置（先填低层剩余，溢出进入下一层）。

    返回每层占用、新占用落在的层、已用恢复次数和容量余额。
    """
    base = base_capacity(terms)
    max_layers = 1 + int(terms["max_reinstatements"])
    new_amount = money(new_amount)
    if new_amount > base + MONEY_EPSILON:
        raise CapacityConflict(
            "单笔摊回%s超过单层容量%s" % (new_amount, base),
            {"requested": new_amount, "base_capacity": base},
        )
    layers = [0.0] * max_layers

    def place(amount: float) -> int:
        for index in range(max_layers):
            if layers[index] + amount <= base + MONEY_EPSILON:
                layers[index] = money(layers[index] + amount)
                return index
        raise CapacityConflict(
            "容量层已满，再开恢复层将超过最大恢复次数%s" % int(terms["max_reinstatements"]),
            {"base_capacity": base, "max_reinstatements": int(terms["max_reinstatements"]),
             "layers": layers, "requested": amount},
        )

    new_layer: Optional[int] = None
    for amount in active_amounts:
        amount = money(float(amount))
        if amount > MONEY_EPSILON:
            place(amount)
    if new_amount > MONEY_EPSILON:
        new_layer = place(new_amount)
    total = money(sum(layers))
    used = max((index for index, value in enumerate(layers) if value > MONEY_EPSILON), default=0)
    return {
        "total_reserved": total,
        "capacity": money(base * max_layers),
        "base_capacity": base,
        "layers": layers,
        "new_layer": new_layer,
        "reinstatements_used": used,
        "available": money(base * max_layers - total),
    }


def reinstatements_used(active_amounts: List[float], terms: Dict[str, Any]) -> int:
    """已生效占用实际用到的最高恢复层编号（0 表示只用初始层）。"""
    if not active_amounts:
        return 0
    return allocate_layers(active_amounts, 0.0, terms)["reinstatements_used"]


def summarize_layers(active_amounts: List[float], terms: Dict[str, Any]) -> Dict[str, Any]:
    """只读汇总容量账，不抛异常：容量缩小导致超占时用 over_capacity 标记。"""
    base = base_capacity(terms)
    max_layers = 1 + int(terms["max_reinstatements"])
    layers = [0.0] * max_layers
    over = 0.0
    for raw in active_amounts:
        amount = money(float(raw))
        if amount <= MONEY_EPSILON:
            continue
        remaining = amount
        for index in range(max_layers):
            room = money(base - layers[index])
            if room > MONEY_EPSILON:
                take = min(room, remaining)
                layers[index] = money(layers[index] + take)
                remaining = money(remaining - take)
                if remaining <= MONEY_EPSILON:
                    break
        if remaining > MONEY_EPSILON:
            over = money(over + remaining)
    total = money(sum(layers))
    capacity = money(base * max_layers)
    used = max((index for index, value in enumerate(layers) if value > MONEY_EPSILON), default=0)
    return {
        "total_reserved": total,
        "capacity": capacity,
        "base_capacity": base,
        "layers": layers,
        "reinstatements_used": used,
        "max_reinstatements": int(terms["max_reinstatements"]),
        "available": money(max(0.0, capacity - total)),
        "over_capacity": over,
    }


def project_occupations(
    terms: Dict[str, Any],
    active_amounts: List[float],
    new_amount: float,
    replacing_amount: float = 0.0,
) -> Dict[str, Any]:
    """模拟把 new_amount 预占进去后的容量账；容量不足直接抛 CapacityConflict。"""
    amounts = [float(amount) for amount in active_amounts if float(amount) > MONEY_EPSILON]
    return allocate_layers(amounts, float(new_amount), terms, replacing_amount=float(replacing_amount))


def validate_approved_loss(data: Dict[str, Any]) -> float:
    return number(data or {}, "approved_loss", 0)


def diff_basis(loss_amount: float, old_basis: Dict[str, Any], new_terms: Dict[str, Any]) -> Dict[str, Any]:
    """已结算占用在新版本条款下重算的差额。差额>容差才需要进待复核。"""
    old_amount = money(float(old_basis.get("recoverable_amount", 0.0)))
    new_amount = recovery_for(float(loss_amount), new_terms)
    return {
        "settled_amount": old_amount,
        "would_be_amount": new_amount,
        "difference": money(new_amount - old_amount),
        "old_version": int(old_basis.get("contract_version", 0)),
        "new_version": int(new_terms["contract_version"]),
    }


def claim_can_calculate(state: str) -> bool:
    return state in ("submitted", "calculated", "recalculate_required")


def claim_can_reject(state: str) -> bool:
    return state in ("submitted", "calculated", "recalculate_required", "pending_verification")


def claim_can_settle(state: str) -> bool:
    return state == "calculated"


def looks_unclear_legacy(claim: Dict[str, Any], terms: Optional[Dict[str, Any]]) -> Optional[str]:
    """旧赔案回填前的可计算性判断，返回 None 表示可以按现有金额回填。"""
    if terms is None:
        return "找不到对应合约版本条款"
    try:
        loss = float(claim.get("approved_loss"))
    except (TypeError, ValueError):
        return "缺少已核定金额approved_loss"
    if loss < 0:
        return "approved_loss不能为负"
    recovery_for(loss, terms)
    return None
