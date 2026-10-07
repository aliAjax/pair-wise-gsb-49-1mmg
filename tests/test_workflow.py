import tempfile
import unittest
from pathlib import Path

from app import build_service
from src.domain import Actor, Conflict


TERMS = {"event_id": "CAT-2026-01", "attachment": 1_000_000.0, "limit": 5_000_000.0,
         "cession_pct": 0.4, "reinstatement_pct": 0.15, "max_reinstatements": 1}


class CapacityLedgerWorkflowTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.service = build_service(str(Path(self.temp.name) / "test.db"))
        self.actor_u = Actor("uw", "underwriter")
        self.actor_c = Actor("clm", "claims_officer")
        self.actor_f = Actor("fin", "finance")

    def tearDown(self):
        self.temp.cleanup()

    def _contract(self, reference="RI-25001"):
        self.service.create_contract(self.actor_u, reference, TERMS)
        self.service.bind_contract(self.actor_u, reference, 1, {"underwriter_id": "UW-8"})
        return reference

    def _claim(self, reference, number, loss):
        created = self.service.submit_claim(self.actor_c, reference, {"claim_number": number})
        result = self.service.calculate_claim(
            self.actor_c, created["claim"]["id"], 1, {"approved_loss": loss}
        )
        return result

    def test_complete_flow_uses_single_capacity_account(self):
        ref = self._contract()
        result = self._claim(ref, "CLM-88", 3_000_000)
        self.assertEqual(result["claim"]["state"], "reserved")
        self.assertEqual(result["occupation"]["status"], "reserved")
        self.assertEqual(result["occupation"]["term_version"], 1)
        # 预占后容量视图立即反映同一笔占用
        self.assertEqual(result["capacity"]["used_amount"], 800_000.0)
        settled = self.service.settle_claim(
            self.actor_f, result["claim"]["id"], result["claim"]["version"],
            {"payment_reference": "PAY-1"},
        )
        self.assertEqual(settled["claim"]["state"], "settled")
        # 结算只认可这笔预占：occupation id 不变、状态变 settled、依据保留
        self.assertEqual(settled["occupation"]["id"], result["occupation"]["id"])
        self.assertEqual(settled["occupation"]["status"], "settled")
        self.assertEqual(settled["occupation"]["occupied_amount"], 800_000.0)
        ledger = self.service.contract_ledger(self.actor_c, ref)
        self.assertEqual(ledger["used_amount"], 800_000.0)
        self.assertEqual(ledger["occupation_count"], {"settled": 1})
        timeline = self.service.timeline(self.actor_c, "claim", result["claim"]["id"])
        self.assertEqual([e["action"] for e in timeline],
                         ["submit_claim", "calculate", "settle"])
        # 占用批次号与计算审计批次号一致
        self.assertEqual(settled["occupation"]["op_batch_no"], timeline[1]["batch_no"])

    def test_late_claim_cannot_overwrite_settled_basis(self):
        ref = self._contract()
        first = self._claim(ref, "CLM-1", 3_000_000)
        self.service.settle_claim(self.actor_f, first["claim"]["id"],
                                  first["claim"]["version"], {"payment_reference": "P1"})
        # 后到赔案各算各的，统一在同一容量账上校验
        second = self._claim(ref, "CLM-2", 5_000_000)
        self.assertEqual(second["claim"]["recovery"], 1_600_000.0)
        third = self.service.submit_claim(self.actor_c, ref, {"claim_number": "CLM-3"})
        with self.assertRaises(Conflict) as ctx:
            self.service.calculate_claim(self.actor_c, third["claim"]["id"], 1,
                                         {"approved_loss": 5_000_000})
        self.assertEqual(ctx.exception.code, "capacity_exceeded")
        # 已结算依据不被后到赔案冲掉
        kept = self.service.get_claim(self.actor_c, first["claim"]["id"])
        self.assertEqual(kept["recovery"], 800_000.0)

    def test_reject_releases_unsettled_occupation(self):
        ref = self._contract()
        result = self._claim(ref, "CLM-R", 3_000_000)
        rejected = self.service.reject_claim(
            self.actor_c, result["claim"]["id"], result["claim"]["version"],
            {"reject_reason": "duplicate"},
        )
        self.assertEqual(rejected["claim"]["state"], "rejected")
        self.assertEqual(rejected["occupation"]["status"], "released")
        ledger = self.service.contract_ledger(self.actor_c, ref)
        self.assertEqual(ledger["used_amount"], 0.0)
        # 容量归还后新赔案可以占满
        again = self._claim(ref, "CLM-R2", 5_000_000)
        self.assertEqual(again["occupation"]["occupied_amount"], 1_600_000.0)

    def test_amount_change_supersedes_old_occupation(self):
        ref = self._contract()
        result = self._claim(ref, "CLM-A", 3_000_000)
        adjusted = self.service.adjust_claim(
            self.actor_c, result["claim"]["id"], result["claim"]["version"],
            {"approved_loss": 2_000_000},
        )
        self.assertEqual(adjusted["claim"]["recovery"], 400_000.0)
        self.assertNotEqual(adjusted["occupation"]["id"], result["occupation"]["id"])
        ledger = self.service.contract_ledger(self.actor_c, ref)
        # 旧预占 superseded 不再计容量
        self.assertEqual(ledger["used_amount"], 400_000.0)
        self.assertEqual(ledger["occupation_count"], {"reserved": 1, "superseded": 1})

    def test_settle_rejects_occupation_mismatch(self):
        ref = self._contract()
        result = self._claim(ref, "CLM-S", 3_000_000)
        # 金额调整使旧预占失效，拿旧版本直接结算必须被拒
        self.service.adjust_claim(self.actor_c, result["claim"]["id"],
                                  result["claim"]["version"], {"approved_loss": 1_500_000})
        with self.assertRaises(Conflict):
            self.service.settle_claim(self.actor_f, result["claim"]["id"],
                                      result["claim"]["version"], {"payment_reference": "PX"})


if __name__ == "__main__":
    unittest.main()
