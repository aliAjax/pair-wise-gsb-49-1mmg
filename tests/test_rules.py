import unittest

from src.rules import DomainRules


TERMS_V1 = {
    "attachment": 1_000_000.0, "limit": 5_000_000.0, "cession_pct": 0.4,
    "reinstatement_pct": 0.15, "max_reinstatements": 1,
    "layer_width": 4_000_000.0, "layer_capacity": 1_600_000.0,
    "aggregate_capacity": 3_200_000.0,
}


class RulesTest(unittest.TestCase):
    def setUp(self):
        self.rules = DomainRules()

    def test_normalize_terms_capacity_and_reinstatements(self):
        terms = self.rules.normalize_terms(
            {"attachment": 1_000_000, "limit": 5_000_000, "cession_pct": 0.4,
             "reinstatement_pct": 0.15, "max_reinstatements": 1}
        )
        self.assertEqual(terms["layer_width"], 4_000_000.0)
        self.assertEqual(terms["layer_capacity"], 1_600_000.0)
        # 一次恢复填满一层：容量 = 层容量 × (1 + 恢复次数)
        self.assertEqual(terms["aggregate_capacity"], 3_200_000.0)

    def test_recovery_basis(self):
        basis = self.rules.recovery_basis(TERMS_V1, 3_000_000)
        self.assertEqual(basis["retained_loss"], 2_000_000.0)
        self.assertEqual(basis["recoverable_amount"], 800_000.0)
        self.assertEqual(basis["reinstatement_premium"], 120_000.0)
        self.assertEqual(basis["reinstatements_used"], 0.5)

    def test_recovery_basis_below_attachment_is_zero(self):
        basis = self.rules.recovery_basis(TERMS_V1, 500_000)
        self.assertEqual(basis["recoverable_amount"], 0.0)
        self.assertEqual(basis["reinstatements_used"], 0.0)

    def test_capacity_account_counts_reserved_and_settled_together(self):
        occs = [
            {"status": "reserved", "occupied_amount": 800_000, "reinstatements_used": 0.5},
            {"status": "settled", "occupied_amount": 800_000, "reinstatements_used": 0.5},
            {"status": "released", "occupied_amount": 800_000, "reinstatements_used": 0.5},
            {"status": "superseded", "occupied_amount": 800_000, "reinstatements_used": 0.5},
        ]
        used_amount, used_reinst = self.rules.capacity_usage(occs)
        self.assertEqual(used_amount, 1_600_000.0)
        self.assertEqual(used_reinst, 1.0)

    def test_capacity_overrun_and_reinstatement_cap(self):
        from src.domain import CapacityExceeded
        occs = [{"status": "settled", "occupied_amount": 3_000_000, "reinstatements_used": 1.875}]
        with self.assertRaises(CapacityExceeded):
            self.rules.assert_capacity(TERMS_V1, occs, 300_000, 0.2)
        # 金额没超，但恢复次数超过 1 + max_reinstatements
        occs2 = [{"status": "settled", "occupied_amount": 100_000, "reinstatements_used": 1.9}]
        with self.assertRaises(CapacityExceeded):
            self.rules.assert_capacity(TERMS_V1, occs2, 100_000, 0.2)

    def test_fits_within_capacity(self):
        occs = [{"status": "reserved", "occupied_amount": 800_000, "reinstatements_used": 0.5}]
        view = self.rules.assert_capacity(TERMS_V1, occs, 800_000, 0.5)
        self.assertEqual(view["used_amount"], 800_000)
        self.assertEqual(view["available_amount"], 2_400_000.0)


if __name__ == "__main__":
    unittest.main()
