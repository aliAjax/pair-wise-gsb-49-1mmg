"""HTTP 路由与统一错误输出。"""
import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict
from urllib.parse import parse_qs, urlparse

from .domain import Actor, DomainError, PermissionDenied, ValidationError


RECORD_RE = re.compile(r"^/api/records/(\d+)$")
ACTION_RE = re.compile(r"^/api/records/(\d+)/actions/([a-z_]+)$")
AUDIT_RE = re.compile(r"^/api/records/(\d+)/audit$")

# ---- 共享容量账路由 ----
LC_CONTRACT_RE = re.compile(r"^/api/ledger/contracts/(\d+)$")
LC_CONTRACT_CLAIMS_RE = re.compile(r"^/api/ledger/contracts/(\d+)/claims$")
LC_CONTRACT_OCC_RE = re.compile(r"^/api/ledger/contracts/(\d+)/occupations$")
LC_CONTRACT_ADJUST_RE = re.compile(r"^/api/ledger/contracts/(\d+)/adjust$")
LC_CONTRACT_AUDIT_RE = re.compile(r"^/api/ledger/contracts/(\d+)/audit$")
LC_CLAIM_RE = re.compile(r"^/api/ledger/claims/(\d+)$")
LC_CLAIM_ACTION_RE = re.compile(r"^/api/ledger/claims/(\d+)/(calculate|settle|reject)$")
LC_CLAIM_AUDIT_RE = re.compile(r"^/api/ledger/claims/(\d+)/audit$")
LC_REVIEW_RE = re.compile(r"^/api/ledger/reviews/(\d+)/resolve$")
LC_RETAINED_RE = re.compile(r"^/api/ledger/retained/(\d+)/replay$")


def make_handler(service: Any, static_dir: Path, ledger: Any = None):
    class Handler(BaseHTTPRequestHandler):
        server_version = "reinsurance-exposure/1.0"

        def log_message(self, fmt: str, *args: Any) -> None:
            return

        def _actor(self) -> Actor:
            user_id = self.headers.get("X-User-Id", "").strip()
            role = self.headers.get("X-Role", "").strip()
            if not user_id or not role:
                raise PermissionDenied("缺少X-User-Id或X-Role")
            return Actor(user_id=user_id, role=role, organization=self.headers.get("X-Org", ""))

        def _body(self) -> Dict[str, Any]:
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError as exc:
                raise ValidationError("Content-Length无效") from exc
            if length > 1024 * 1024:
                raise ValidationError("请求体过大")
            raw = self.rfile.read(length) if length else b"{}"
            try:
                data = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValidationError("请求体必须是JSON") from exc
            if not isinstance(data, dict):
                raise ValidationError("JSON顶层必须是对象")
            return data

        def _send(self, status: int, payload: Any, content_type: str = "application/json; charset=utf-8") -> None:
            if content_type.startswith("application/json"):
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            else:
                body = payload
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _handle_error(self, exc: Exception) -> None:
            if isinstance(exc, DomainError):
                payload = {"error": exc.code, "message": str(exc)}
                payload.update(getattr(exc, "payload", {}) or {})
                self._send(exc.status, payload)
            else:
                self._send(500, {"error": "internal_error", "message": "服务内部错误"})

        def do_GET(self) -> None:
            try:
                parsed = urlparse(self.path)
                if parsed.path == "/health":
                    self._send(200, {"status": "ok", "service": "reinsurance-exposure", "database": service.repository.health()})
                    return
                if parsed.path == "/":
                    page = (static_dir / "index.html").read_bytes()
                    self._send(200, page, "text/html; charset=utf-8")
                    return
                if parsed.path == "/api/records":
                    query = parse_qs(parsed.query)
                    records = service.list_records(self._actor(), state=query.get("state", [None])[0], limit=int(query.get("limit", ["100"])[0]))
                    self._send(200, {"items": records})
                    return
                match = RECORD_RE.match(parsed.path)
                if match:
                    self._send(200, service.get_record(self._actor(), int(match.group(1))))
                    return
                match = AUDIT_RE.match(parsed.path)
                if match:
                    self._send(200, {"items": service.timeline(self._actor(), int(match.group(1)))})
                    return
                if parsed.path == "/api/stats":
                    self._send(200, service.stats(self._actor()))
                    return
                if ledger is not None:
                    if self._ledger_get(parsed):
                        return
                self._send(404, {"error": "not_found", "message": "路径不存在"})
            except Exception as exc:
                self._handle_error(exc)

        def do_POST(self) -> None:
            try:
                parsed = urlparse(self.path)
                body = self._body()
                if parsed.path == "/api/records":
                    record = service.create(self._actor(), body.get("reference", ""), body.get("data", {}))
                    self._send(201, record)
                    return
                match = ACTION_RE.match(parsed.path)
                if match:
                    version = body.get("expected_version")
                    if not isinstance(version, int):
                        raise ValidationError("expected_version必须是整数")
                    record = service.act(self._actor(), int(match.group(1)), version, match.group(2), body.get("data", {}))
                    self._send(200, record)
                    return
                if ledger is not None and self._ledger_post(parsed, body):
                    return
                self._send(404, {"error": "not_found", "message": "路径不存在"})
            except Exception as exc:
                self._handle_error(exc)

        # ---------- 共享容量账 ----------
        def _request_id(self, body: Dict[str, Any]) -> str:
            value = body.get("request_id")
            if value is None:
                return ""
            if not isinstance(value, str) or not value.strip():
                raise ValidationError("request_id必须是非空文本")
            return value.strip()

        @staticmethod
        def _version(body: Dict[str, Any]) -> int:
            version = body.get("expected_version")
            if not isinstance(version, int):
                raise ValidationError("expected_version必须是整数")
            return version

        def _ledger_get(self, parsed) -> bool:
            path = parsed.path
            query = parse_qs(parsed.query)
            limit = int(query.get("limit", ["100"])[0])
            if path == "/api/ledger/contracts":
                self._send(200, {"items": ledger.list_contracts(self._actor(), limit=limit)})
                return True
            if path == "/api/ledger/claims":
                self._send(200, {"items": ledger.list_claims(self._actor(), limit=limit)})
                return True
            if path == "/api/ledger/reviews":
                status = query.get("status", [None])[0]
                self._send(200, {"items": ledger.list_reviews(self._actor(), status=status)})
                return True
            if path == "/api/ledger/retained":
                self._send(200, {"items": ledger.list_retained(self._actor())})
                return True
            match = LC_CONTRACT_RE.match(path)
            if match:
                self._send(200, ledger.get_contract_detail(self._actor(), int(match.group(1))))
                return True
            match = LC_CONTRACT_CLAIMS_RE.match(path)
            if match:
                self._send(200, {"items": ledger.list_claims(self._actor(), contract_id=int(match.group(1)), limit=limit)})
                return True
            match = LC_CONTRACT_OCC_RE.match(path)
            if match:
                self._send(200, {"items": ledger.occupations(self._actor(), int(match.group(1)))})
                return True
            match = LC_CONTRACT_AUDIT_RE.match(path)
            if match:
                self._send(200, {"items": ledger.audit_timeline(self._actor(), contract_id=int(match.group(1)))})
                return True
            match = LC_CLAIM_RE.match(path)
            if match:
                self._send(200, ledger.get_claim_detail(self._actor(), int(match.group(1))))
                return True
            match = LC_CLAIM_AUDIT_RE.match(path)
            if match:
                self._send(200, {"items": ledger.audit_timeline(self._actor(), claim_id=int(match.group(1)))})
                return True
            return False

        def _ledger_post(self, parsed, body: Dict[str, Any]) -> bool:
            path = parsed.path
            request_id = self._request_id(body)
            if path == "/api/ledger/contracts":
                result = ledger.create_contract(
                    self._actor(), body.get("reference", ""), body.get("data", {}), request_id=request_id
                )
                self._send(201, result)
                return True
            match = LC_CONTRACT_CLAIMS_RE.match(path)
            if match:
                result = ledger.submit_claim(
                    self._actor(), int(match.group(1)), body.get("data", {}), request_id=request_id
                )
                self._send(201, result)
                return True
            if path == "/api/ledger/claims/import-legacy":
                contract_id = body.get("contract_id")
                if not isinstance(contract_id, int):
                    raise ValidationError("contract_id必须是整数")
                result = ledger.import_legacy_claims(
                    self._actor(), contract_id, body.get("claims", []), request_id=request_id
                )
                self._send(200, result)
                return True
            if path == "/api/ledger/admin/recover":
                self._send(200, {"recovered": ledger.recover()})
                return True
            if path == "/api/ledger/admin/backfill-legacy":
                self._send(200, ledger.backfill_legacy(self._actor(), request_id=request_id))
                return True
            match = LC_CONTRACT_ADJUST_RE.match(path)
            if match:
                self._send(200, ledger.adjust_contract(
                    self._actor(), int(match.group(1)), self._version(body), body.get("data", {}),
                    request_id=request_id,
                ))
                return True
            match = LC_CLAIM_ACTION_RE.match(path)
            if match:
                claim_id = int(match.group(1))
                action = match.group(2)
                version = self._version(body)
                if action == "calculate":
                    result = ledger.calculate(self._actor(), claim_id, version, body.get("data", {}),
                                              request_id=request_id)
                elif action == "settle":
                    result = ledger.settle(self._actor(), claim_id, version, body.get("data", {}),
                                           request_id=request_id)
                else:
                    result = ledger.reject(self._actor(), claim_id, version, body.get("data", {}),
                                           request_id=request_id)
                self._send(200, result)
                return True
            match = LC_REVIEW_RE.match(path)
            if match:
                self._send(200, ledger.resolve_review(
                    self._actor(), int(match.group(1)), body.get("data", {}), request_id=request_id,
                ))
                return True
            match = LC_RETAINED_RE.match(path)
            if match:
                self._send(200, ledger.replay_retained(
                    self._actor(), int(match.group(1)), body.get("data"), request_id=request_id,
                ))
                return True
            return False

    return Handler


def create_server(host: str, port: int, service: Any, static_dir: Path, ledger: Any = None) -> ThreadingHTTPServer:
    return ThreadingHTTPServer((host, port), make_handler(service, static_dir, ledger))
