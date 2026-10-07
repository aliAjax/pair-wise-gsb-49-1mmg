"""HTTP 路由与统一错误输出。"""
import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .domain import Actor, DomainError, PermissionDenied, ValidationError


CONTRACT_REF_RE = r"([A-Za-z0-9._-]+)"
CONTRACT_RE = re.compile(r"^/api/contracts/%s$" % CONTRACT_REF_RE)
CONTRACT_LEDGER_RE = re.compile(r"^/api/contracts/%s/ledger$" % CONTRACT_REF_RE)
CONTRACT_CLAIMS_RE = re.compile(r"^/api/contracts/%s/claims$" % CONTRACT_REF_RE)
CONTRACT_ACTION_RE = re.compile(r"^/api/contracts/%s/actions/(bind|adjust)$" % CONTRACT_REF_RE)
CONTRACT_AUDIT_RE = re.compile(r"^/api/contracts/%s/audit$" % CONTRACT_REF_RE)
CLAIM_RE = re.compile(r"^/api/claims/(\d+)$")
CLAIM_AUDIT_RE = re.compile(r"^/api/claims/(\d+)/audit$")
CLAIM_ACTION_RE = re.compile(r"^/api/claims/(\d+)/actions/(calculate|settle|reject|adjust_claim)$")
REVIEW_RE = re.compile(r"^/api/reviews$")
REVIEW_RESOLVE_RE = re.compile(r"^/api/reviews/(\d+)/resolve$")
RECOVER_RE = re.compile(r"^/api/admin/recover$")


def make_handler(service: Any, static_dir: Path):
    class Handler(BaseHTTPRequestHandler):
        server_version = "reinsurance-capacity/2.0"

        def log_message(self, fmt: str, *args: Any) -> None:
            return

        def _actor(self) -> Actor:
            user_id = self.headers.get("X-User-Id", "").strip()
            role = self.headers.get("X-Role", "").strip()
            if not user_id or not role:
                raise PermissionDenied("缺少X-User-Id或X-Role")
            return Actor(user_id=user_id, role=role, organization=self.headers.get("X-Org", ""))

        def _body(self) -> dict:
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

        @staticmethod
        def _expected_version(body: dict):
            version = body.get("expected_version")
            if version is None:
                return None
            if not isinstance(version, int):
                raise ValidationError("expected_version必须是整数")
            return version

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
                if getattr(exc, "context", None):
                    payload["conflict"] = exc.context
                self._send(exc.status, payload)
            else:
                self._send(500, {"error": "internal_error", "message": "服务内部错误"})

        def do_GET(self) -> None:
            try:
                parsed = urlparse(self.path)
                query = parse_qs(parsed.query)
                path = parsed.path
                if path == "/health":
                    self._send(200, {"status": "ok", "service": "reinsurance-capacity",
                                     "database": service.repository.health()})
                    return
                if path == "/":
                    page = (static_dir / "index.html").read_bytes()
                    self._send(200, page, "text/html; charset=utf-8")
                    return
                if path == "/api/stats":
                    self._send(200, service.stats(self._actor()))
                    return
                if REVIEW_RE.match(path):
                    self._send(200, {"items": service.list_reviews(
                        self._actor(), status=query.get("status", [None])[0],
                        reference=query.get("contract", [None])[0],
                        limit=int(query.get("limit", ["100"])[0]))})
                    return
                match = CONTRACT_RE.match(path)
                if match:
                    self._send(200, service.get_contract(self._actor(), match.group(1)))
                    return
                match = CONTRACT_LEDGER_RE.match(path)
                if match:
                    self._send(200, service.contract_ledger(self._actor(), match.group(1)))
                    return
                match = CONTRACT_CLAIMS_RE.match(path)
                if match:
                    self._send(200, {"items": service.list_claims(
                        self._actor(), reference=match.group(1),
                        state=query.get("state", [None])[0],
                        limit=int(query.get("limit", ["100"])[0]))})
                    return
                match = CONTRACT_AUDIT_RE.match(path)
                if match:
                    contract = service.get_contract(self._actor(), match.group(1))
                    self._send(200, {"items": service.timeline(
                        self._actor(), "contract", int(contract["contract_pk"]))})
                    return
                match = CLAIM_RE.match(path)
                if match:
                    self._send(200, service.get_claim(self._actor(), int(match.group(1))))
                    return
                match = CLAIM_AUDIT_RE.match(path)
                if match:
                    self._send(200, {"items": service.timeline(
                        self._actor(), "claim", int(match.group(1)))})
                    return
                if path == "/api/contracts":
                    self._send(200, {"items": service.list_contracts(
                        self._actor(), state=query.get("state", [None])[0],
                        limit=int(query.get("limit", ["100"])[0]))})
                    return
                self._send(404, {"error": "not_found", "message": "路径不存在"})
            except Exception as exc:
                self._handle_error(exc)

        def do_POST(self) -> None:
            try:
                parsed = urlparse(self.path)
                path = parsed.path
                body = self._body()
                if path == "/api/contracts":
                    result = service.create_contract(
                        self._actor(), body.get("reference", ""), body.get("data", {}))
                    self._send(201, result)
                    return
                match = CONTRACT_ACTION_RE.match(path)
                if match:
                    result = self._contract_action(match.group(2), match.group(1), body)
                    self._send(200, result)
                    return
                match = CONTRACT_CLAIMS_RE.match(path)
                if match:
                    result = service.submit_claim(self._actor(), match.group(1), body.get("data", {}))
                    self._send(201, result)
                    return
                match = CLAIM_ACTION_RE.match(path)
                if match:
                    result = self._claim_action(match.group(2), int(match.group(1)), body)
                    self._send(200, result)
                    return
                match = REVIEW_RESOLVE_RE.match(path)
                if match:
                    result = service.resolve_review(self._actor(), int(match.group(1)), body.get("data", {}))
                    self._send(200, result)
                    return
                if RECOVER_RE.match(path):
                    result = service.recover(self._actor())
                    self._send(200, result)
                    return
                self._send(404, {"error": "not_found", "message": "路径不存在"})
            except Exception as exc:
                self._handle_error(exc)

        def _contract_action(self, action: str, reference: str, body: dict) -> Any:
            version = self._expected_version(body)
            data = body.get("data", {})
            if action == "bind":
                return service.bind_contract(self._actor(), reference, version, data)
            return service.adjust_contract(self._actor(), reference, version, data)

        def _claim_action(self, action: str, claim_id: int, body: dict) -> Any:
            version = self._expected_version(body)
            data = body.get("data", {})
            if action == "calculate":
                return service.calculate_claim(self._actor(), claim_id, version, data)
            if action == "adjust_claim":
                return service.adjust_claim(self._actor(), claim_id, version, data)
            if action == "settle":
                return service.settle_claim(self._actor(), claim_id, version, data)
            return service.reject_claim(self._actor(), claim_id, version, data)

    return Handler


def create_server(host: str, port: int, service: Any, static_dir: Path) -> ThreadingHTTPServer:
    return ThreadingHTTPServer((host, port), make_handler(service, static_dir))
