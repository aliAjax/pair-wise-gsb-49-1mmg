import datetime
import tempfile
import unittest
from pathlib import Path

from app import build_service
from src.domain import Actor, Conflict


TERMS = {"event_id": "CAT-2026-01", "attachment": 1_000_000.0, "limit": 5_000_000.0,
         "cession_pct": 0.4, "reinstatement_pct": 0.15, "max_reinstatements": 1}
BASIS = ('{"recoverable_amount":400000,"reinstatements_used":0.25,'
         '"approved_loss":2000000,"attachment":1000000,"layer_width":4000000,'
         '"cession_pct":0.4,"reinstatement_premium":60000}')


def _now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


class RecoveryTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.temp.name) / "test.db")
        self.service = build_service(self.db_path)
        self.actor_u = Actor("uw", "underwriter")
        self.actor_c = Actor("clm", "claims_officer")
        self.actor_f = Actor("fin", "finance")
        self.actor_a = Actor("root", "admin")
        self.service.create_contract(self.actor_u, "RI-R", TERMS)
        self.service.bind_contract(self.actor_u, "RI-R", 1, {"underwriter_id": "UW-8"})
        self.contract = self.service.get_contract(self.actor_c, "RI-R")

    def tearDown(self):
        self.temp.cleanup()

    def _legacy_claim(self, number, state, parseable=True):
        conn = self.service.repository._connect()
        try:
            if parseable:
                conn.execute(
                    "INSERT INTO claims(contract_pk,claim_number,event_id,state,version,approved_loss,"
                    "recovery,reinstatement_premium,reinstatements_used,term_id,term_version,basis,"
                    "created_by,updated_by,created_at,updated_at)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (self.contract["contract_pk"], number, "CAT-2026-01", state, 1, 2_000_000,
                     400_000, 60_000, 0.25, self.contract["id"], 1, BASIS,
                     "legacy", "legacy", _now(), _now()),
                )
            else:
                conn.execute(
                    "INSERT INTO claims(contract_pk,claim_number,event_id,state,version,"
                    "created_by,updated_by,created_at,updated_at)"
                    " VALUES(?,?,?,?,?,?,?,?,?)",
                    (self.contract["contract_pk"], number, "CAT-2026-01", state, 1,
                     "legacy", "legacy", _now(), _now()),
                )
            conn.commit()
        finally:
            conn.close()

    def test_failed_commit_rolls_back_and_retry_is_idempotent(self):
        claim = self.service.submit_claim(self.actor_c, "RI-R", {"claim_number": "CLM-F"})
        self.service.repository.fail_next_commit = True
        with self.assertRaises(Exception):
            self.service.calculate_claim(
                self.actor_c, claim["claim"]["id"], 1,
                {"approved_loss": 3_000_000, "batch_no": "batch-1"},
            )
        # 提交失败整批回滚（批次行、占用、审计都不存在）；同号重试直接重新执行
        result = self.service.calculate_claim(
            self.actor_c, claim["claim"]["id"], 1,
            {"approved_loss": 3_000_000, "batch_no": "batch-1"},
        )
        self.assertEqual(result["claim"]["recovery"], 800_000.0)
        # 管理员恢复报告无残留
        report = self.service.recover(self.actor_a)
        self.assertEqual(report["removed"]["occupations"], 0)
        replay = self.service.calculate_claim(
            self.actor_c, claim["claim"]["id"], result["claim"]["version"],
            {"approved_loss": 999, "batch_no": "batch-1"},
        )
        self.assertTrue(replay["replayed"])
        self.assertEqual(replay["claim"]["recovery"], 800_000.0)
        ledger = self.service.contract_ledger(self.actor_c, "RI-R")
        self.assertEqual(ledger["occupation_count"], {"reserved": 1})
        timeline = self.service.timeline(self.actor_c, "claim", claim["claim"]["id"])
        self.assertEqual([e["action"] for e in timeline], ["submit_claim", "calculate"])

    def test_zombie_batch_is_rolled_back_on_restart(self):
        conn = self.service.repository._connect()
        try:
            conn.execute(
                "INSERT INTO op_batches(batch_no,op_type,status,created_at,updated_at)"
                " VALUES('zombie','calculate','open',?,?)", (_now(), _now()))
            conn.execute(
                "INSERT INTO capacity_ledger(op_batch_no,contract_pk,claim_number,status,"
                "occupied_amount,reinstatements_used,basis,created_by,created_at)"
                " VALUES('zombie',?,'Z','reserved',999999,5.0,'{}','x',?)",
                (self.contract["contract_pk"], _now()),
            )
            conn.commit()
        finally:
            conn.close()
        # 新进程启动：自动回滚未完成批次，容量账干净
        service2 = build_service(self.db_path)
        ledger = service2.contract_ledger(self.actor_c, "RI-R")
        self.assertEqual(ledger["used_amount"], 0.0)
        self.assertEqual(ledger["occupation_count"], {})
        self.assertEqual(service2.repository.incomplete_batches(), [])

    def test_legacy_claims_backfilled_or_pending_review(self):
        self._legacy_claim("OLD-OK", "reserved", parseable=True)
        self._legacy_claim("OLD-SETTLED", "settled", parseable=True)
        self._legacy_claim("OLD-FUZZY", "settled", parseable=False)
        service2 = build_service(self.db_path)
        claims = {c["claim_number"]: c for c in service2.repository.list_claims(limit=50)}
        self.assertIsNotNone(claims["OLD-OK"]["occupation_id"])
        self.assertIsNotNone(claims["OLD-SETTLED"]["occupation_id"])
        self.assertIsNone(claims["OLD-FUZZY"]["occupation_id"])
        ledger = service2.contract_ledger(self.actor_c, "RI-R")
        self.assertEqual(ledger["used_amount"], 800_000.0)
        reviews = service2.list_reviews(self.actor_a)
        self.assertTrue(any(r["kind"] == "legacy_unverifiable"
                            and r["claim_id"] == claims["OLD-FUZZY"]["id"]
                            for r in reviews))
        # 回填的占用可以直接结算
        settled = service2.settle_claim(
            self.actor_f, claims["OLD-OK"]["id"], 1, {"payment_reference": "OLD-PAY"})
        self.assertEqual(settled["occupation"]["op_batch_no"], "legacy-backfill")
        # 重复恢复不重复占额、不重复挂待复核
        service2.recover(self.actor_a)
        service2.recover(self.actor_a)
        ledger2 = service2.contract_ledger(self.actor_c, "RI-R")
        self.assertEqual(ledger2["occupation_count"], {"settled": 2})

    def test_legacy_reserved_over_capacity_stays_pending(self):
        # 单层 400k、无恢复的新合约
        self.service.create_contract(self.actor_u, "RI-SMALL", {
            "event_id": "E2", "attachment": 1_000_000, "limit": 2_000_000,
            "cession_pct": 0.4, "reinstatement_pct": 0.15, "max_reinstatements": 0})
        self.service.bind_contract(self.actor_u, "RI-SMALL", 1, {"underwriter_id": "UW-9"})
        small = self.service.get_contract(self.actor_c, "RI-SMALL")
        conn = self.service.repository._connect()
        try:
            conn.execute(
                "INSERT INTO claims(contract_pk,claim_number,event_id,state,version,approved_loss,"
                "recovery,reinstatement_premium,reinstatements_used,term_id,term_version,basis,"
                "created_by,updated_by,created_at,updated_at)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (small["contract_pk"], "OLD-BIG", "E2", "reserved", 1, 2_250_000, 500_000,
                 75_000, 1.25, small["id"], 1,
                 '{"recoverable_amount":500000,"reinstatements_used":1.25}', "legacy",
                 "legacy", _now(), _now()),
            )
            conn.commit()
        finally:
            conn.close()
        service2 = build_service(self.db_path)
        claim = [c for c in service2.repository.list_claims(limit=50)
                 if c["claim_number"] == "OLD-BIG"][0]
        self.assertIsNone(claim["occupation_id"])
        reviews = service2.list_reviews(self.actor_a)
        self.assertTrue(any(r["kind"] == "backfill_overrun" for r in reviews))
        self.assertEqual(service2.contract_ledger(self.actor_c, "RI-SMALL")["used_amount"], 0.0)


if __name__ == "__main__":
    unittest.main()
