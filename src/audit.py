"""审计事件封装，时间线查询保持只读。审计与占用共用同一批次号落库。"""
from typing import Any, Dict, List


class AuditRecorder:
    def __init__(self, repository: Any) -> None:
        self.repository = repository

    def timeline(self, subject_type: str, subject_id: int) -> List[Dict[str, Any]]:
        return self.repository.audit_timeline(subject_type, subject_id)

    def note(self, subject_type: str, subject_id: int, actor_id: str, action: str,
             details: Dict[str, Any]) -> None:
        # 独立审计注释（不走容量账批次的场景）
        self.repository.add_audit(subject_type, subject_id, actor_id, action, details)
