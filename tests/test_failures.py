import tempfile
import unittest
from pathlib import Path

from app import build_service
from src.domain import Actor, Conflict, PermissionDenied


TERMS = {"event_id": "CAT-2026-01", "attachment": 1_000_000.0, "limit": 5_000_000.0,
         "cession_pct": 0.4, "reinstatement_pct": 0.15, "max_reinstatements": 1}


class VersionConflictTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.service = build_service(str(Path(self.temp.name) / "test.db"))
        self.actor_u = Actor("uw", "underwriter")
        self.actor_c = Actor("clm", "claims_officer")

    def tearDown(self):
        self.temp.cleanup()

    def _setup(self):
        self.service.create_contract(self.actor_u, "RI-X", TERMS)
        self.service.bind_contract(self.actor_u, "RI-X", 1, {"underwriter_id": "UW-8"})
        claim = self.service.submit_claim(self.actor_c, "RI-X", {"claim_number": "CLM-X"})
        return claim

    def test_stale_contract_version_on_adjust(self):
        self._setup()
        self.service.adjust_contract(self.actor_u, "RI-X", 1,
                                     {"attachment": 1_200_000, "limit": 5_000_000,
                                      "cession_pct": 0.4, "reinstatement_pct": 0.15,
                                      "max_reinstatements": 1})
        with self.assertRaises(Conflict) as ctx:
            self.service.adjust_contract(self.actor_u, "RI-X", 1,
                                         {"attachment": 1_300_000, "limit": 5_000_000,
                                          "cession_pct": 0.4, "reinstatement_pct": 0.15,
                                          "max_reinstatements": 1})
        self.assertEqual(ctx.exception.context["current_version"], 2)

    def test_stale_term_version_on_calculate(self):
        claim = self._setup()
        self.service.adjust_contract(self.actor_u, "RI-X", 1,
                                     {"attachment": 1_200_000, "limit": 5_000_000,
                                      "cession_pct": 0.4, "reinstatement_pct": 0.15,
                                      "max_reinstatements": 1})
        with self.assertRaises(Conflict) as ctx:
            self.service.calculate_claim(self.actor_c, claim["claim"]["id"], 1,
                                         {"approved_loss": 3_000_000,
                                          "expected_term_version": 1})
        self.assertEqual(ctx.exception.context["current_term_version"], 2)
        # 后到者保留输入，改用新版本重提即成功
        result = self.service.calculate_claim(self.actor_c, claim["claim"]["id"], 1,
                                              {"approved_loss": 3_000_000,
                                               "expected_term_version": 2})
        self.assertEqual(result["claim"]["term_version"], 2)

    def test_contract_adjust_keeps_settled_and_recalculates_reserved(self):
        claim = self._setup()
        settled = self.service.calculate_claim(self.actor_c, claim["claim"]["id"], 1,
                                               {"approved_loss": 3_000_000})
        self.service.settle_claim(Actor("fin", "finance"), settled["claim"]["id"],
                                  settled["claim"]["version"], {"payment_reference": "P"})
        other = self.service.submit_claim(self.actor_c, "RI-X", {"claim_number": "CLM-Y"})
        reserved = self.service.calculate_claim(self.actor_c, other["claim"]["id"], 1,
                                                {"approved_loss": 2_000_000})
        result = self.service.adjust_contract(self.actor_u, "RI-X", 1,
                                              {"attachment": 1_500_000, "limit": 5_000_000,
                                               "cession_pct": 0.4, "reinstatement_pct": 0.15,
                                               "max_reinstatements": 0})
        self.assertEqual(result["summary"]["settled_diffs"], 1)
        self.assertGreaterEqual(result["summary"]["rebased"], 1)
        # 已结算：依据保留在 v1，差额入待复核
        kept = self.service.get_claim(self.actor_c, settled["claim"]["id"])
        self.assertEqual(kept["recovery"], 800_000.0)
        self.assertEqual(kept["term_version"], 1)
        reviews = self.service.list_reviews(self.actor_u)
        self.assertTrue(any(r["kind"] == "settled_diff" and r["diff_amount"] == -200_000.0
                            for r in reviews))
        # 未结算：按新版本重算（2,000,000 损失 - 1,500,000 起赔 = 500,000 × 0.4）
        rebased = self.service.get_claim(self.actor_c, reserved["claim"]["id"])
        self.assertEqual(rebased["recovery"], 200_000.0)
        self.assertEqual(rebased["term_version"], 2)

    def test_adjust_shortfall_returns_claim_and_opens_review(self):
        claim = self._setup()
        settled = self.service.calculate_claim(self.actor_c, claim["claim"]["id"], 1,
                                               {"approved_loss": 5_000_000})
        self.service.settle_claim(Actor("fin", "finance"), settled["claim"]["id"],
                                  settled["claim"]["version"], {"payment_reference": "P"})
        other = self.service.submit_claim(self.actor_c, "RI-X", {"claim_number": "CLM-Z"})
        reserved = self.service.calculate_claim(self.actor_c, other["claim"]["id"], 1,
                                                {"approved_loss": 2_000_000})
        # 新版本单层 400k 容量，已结算占用 1,600k：未结算赔案在新版本放不下
        self.service.adjust_contract(self.actor_u, "RI-X", 1,
                                     {"attachment": 2_000_000, "limit": 3_000_000,
                                      "cession_pct": 0.4, "reinstatement_pct": 0.15,
                                      "max_reinstatements": 0})
        claim_now = self.service.get_claim(self.actor_c, reserved["claim"]["id"])
        self.assertEqual(claim_now["state"], "submitted")
        self.assertIsNone(claim_now["occupation_id"])
        kinds = [r["kind"] for r in self.service.list_reviews(self.actor_u)]
        self.assertIn("recalc_shortfall", kinds)

    def test_permission_denied(self):
        with self.assertRaises(PermissionDenied):
            self.service.create_contract(Actor("outsider", "outsider"), "RI-X", TERMS)
        with self.assertRaises(PermissionDenied):
            self.service.recover(Actor("outsider", "underwriter"))

    def test_duplicate_contract_reference(self):
        self._setup()
        with self.assertRaises(Conflict):
            self.service.create_contract(self.actor_u, "RI-X", TERMS)


if __name__ == "__main__":
    unittest.main()
