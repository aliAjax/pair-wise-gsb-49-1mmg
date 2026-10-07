import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from app import build_ledger_service
from src import ledger_rules as rules
from src.domain import Actor, CapacityConflict, Conflict, OptimisticConflict, PermissionDenied
from src.ledger_repository import BATCHES, OCCUPATIONS, LedgerRepository
from src.ledger_service import LedgerService


TERMS = {"attachment": 1_000_000.0, "limit": 5_000_000.0, "cession_pct": 0.4,
         "reinstatement_pct": 0.15, "max_reinstatements": 1}
# 单层容量 = 400万 × 0.4 = 160万；1次恢复 => 两层共320万
UW = Actor("uw-1", "underwriter")
CO = Actor("co-1", "claims_officer")
FIN = Actor("fin-1", "finance")


class LedgerCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.temp.name) / "ledger.db")
        self.service = build_ledger_service(self.db)

    def tearDown(self):
        self.temp.cleanup()

    def create_contract(self, reference="T-1", terms=None):
        result = self.service.create_contract(UW, reference, dict(terms or TERMS))
        return int(result["contract"]["id"])

    def claim(self, contract_id, number):
        return int(self.service.submit_claim(CO, contract_id, {"claim_number": number})["claim"]["id"])

    def calculate(self, claim_id, version, loss):
        return self.service.calculate(CO, claim_id, version, {"approved_loss": loss})

    def assert_amounts(self, contract_id, expected):
        occupations = self.service.repository.active_occupations(contract_id)
        self.assertEqual([float(o["amount"]) for o in occupations], expected)


class SharedLedgerTest(LedgerCase):
    def test_multi_claims_share_one_capacity_book_and_reinstatements(self):
        cid = self.create_contract()
        c1 = self.claim(cid, "CLM-1")
        r1 = self.calculate(c1, 1, 3_000_000)  # 摊回80万，层0
        self.assertEqual(r1["basis"]["recoverable_amount"], 800_000.0)
        self.assertEqual(r1["ledger_after"]["reinstatements_used"], 0)
        # 结算只认可这笔预占
        settled = self.service.settle(FIN, c1, r1["claim"]["claim_version"], {"payment_reference": "P-1"})
        self.assertEqual(settled["occupation"]["status"], "settled")
        self.assertEqual(settled["occupation"]["basis_json"]["recoverable_amount"], 800_000.0)

        c2 = self.claim(cid, "CLM-2")
        r2 = self.calculate(c2, 1, 5_000_000)  # 摊回160万 => 层0余80万 + 层1开80万
        self.assertEqual(r2["basis"]["reinstatement_layer"], 1)
        self.assertEqual(r2["ledger_after"]["reinstatements_used"], 1)

        # 晚到赔案：只能看到账本剩余，不能冲掉已结算依据
        c3 = self.claim(cid, "CLM-3")
        r3 = self.calculate(c3, 1, 3_000_000)  # 摊回80万，恰好填入层0剩余
        self.assertEqual(r3["ledger_after"]["layers"], [1_600_000.0, 1_600_000.0])
        detail = self.service.get_contract_detail(CO, cid)
        self.assertEqual(detail["ledger"]["available"], 0.0)

        c4 = self.claim(cid, "CLM-4")
        with self.assertRaises(CapacityConflict):  # 第3层 => 超过1次恢复
            self.calculate(c4, 1, 1_100_000)

        settled_again = self.service.get_claim_detail(FIN, c1)
        self.assertEqual(settled_again["current_occupation"]["status"], "settled")
        self.assertEqual(settled_again["current_occupation"]["basis_json"]["recoverable_amount"], 800_000.0)

    def test_reject_releases_unsettled_occupation(self):
        cid = self.create_contract()
        c1 = self.claim(cid, "CLM-1")
        r1 = self.calculate(c1, 1, 3_000_000)
        rejected = self.service.reject(FIN, c1, r1["claim"]["claim_version"], {"reject_reason": "重复报案"})
        self.assertEqual(rejected["claim"]["state"], "rejected")
        self.assertEqual(rejected["released_amount"], 800_000.0)
        self.assert_amounts(cid, [])
        # 容量完全回来：再开赔案仍只用初始层
        c2 = self.claim(cid, "CLM-2")
        r2 = self.calculate(c2, 1, 5_000_000)
        self.assertEqual(r2["ledger_after"]["reinstatements_used"], 0)
        self.assertEqual(r2["basis"]["recoverable_amount"], 1_600_000.0)

    def test_smaller_amount_recomputes_and_voids_old_reservation(self):
        cid = self.create_contract()
        c1 = self.claim(cid, "CLM-1")
        r1 = self.calculate(c1, 1, 5_000_000)  # 160万
        v = r1["claim"]["claim_version"]
        r2 = self.calculate(c1, v, 2_500_000)    # 60万：旧预占失效
        self.assertEqual(r2["basis"]["recoverable_amount"], 600_000.0)
        self.assertEqual(r2["ledger_after"]["total_reserved"], 600_000.0)
        detail = self.service.get_claim_detail(CO, c1)
        statuses = sorted(o["status"] for o in detail["occupations"])
        self.assertEqual(statuses, ["reserved", "stale"])
        self.assert_amounts(cid, [600_000.0])
        # 结算只认新的预占
        settled = self.service.settle(FIN, c1, r2["claim"]["claim_version"], {"payment_reference": "P"})
        self.assertEqual(settled["claim"]["state"], "settled")

    def test_settle_requires_matching_reservation(self):
        cid = self.create_contract()
        c1 = self.claim(cid, "CLM-1")
        r1 = self.calculate(c1, 1, 5_000_000)
        # 金额改小后，拿旧版本结算被拒（输入保留为乐观冲突）
        stale_version = r1["claim"]["claim_version"]
        r2 = self.calculate(c1, stale_version, 2_500_000)
        with self.assertRaises(OptimisticConflict):
            self.service.settle(FIN, c1, stale_version, {"payment_reference": "STALE"})
        settled = self.service.settle(FIN, c1, r2["claim"]["claim_version"], {"payment_reference": "P"})
        self.assertEqual(settled["claim"]["state"], "settled")

    def test_contract_adjustment_keeps_settled_basis_and_queues_diff(self):
        cid = self.create_contract()
        c1 = self.claim(cid, "CLM-1")
        r1 = self.calculate(c1, 1, 5_000_000)  # 触顶：摊回160万（v1）
        self.service.settle(FIN, c1, r1["claim"]["claim_version"], {"payment_reference": "P-1"})
        c2 = self.claim(cid, "CLM-2")
        r2 = self.calculate(c2, 1, 3_000_000)  # 另一笔80万预占（初始层）

        new_terms = dict(TERMS, cession_pct=0.5)  # 单层变200万：已结算160万 -> 新版应摊回200万，有差额
        adjusted = self.service.adjust_contract(UW, cid, 1, new_terms)
        self.assertEqual(adjusted["version"], 2)
        # 未结算(c2)失效待重算；已结算(c1)保留
        self.assertEqual(adjusted["invalidated_claims"], [c2])
        self.assertEqual(len(adjusted["review_ids"]), 1)

        d1 = self.service.get_claim_detail(CO, c1)
        self.assertEqual(d1["state"], "settled")
        self.assertEqual(d1["current_occupation"]["basis_json"]["contract_version"], 1)
        d2 = self.service.get_claim_detail(CO, c2)
        self.assertEqual(d2["state"], "recalculate_required")
        self.assertIsNone(d2["current_occupation_id"])
        self.assert_amounts(cid, [1_600_000.0])  # 只剩已结算那笔

        review = self.service.repository.get_review(adjusted["review_ids"][0])
        self.assertEqual(review["review_type"], "version_diff")
        self.assertEqual(review["details_json"]["old_version"], 1)
        self.assertEqual(review["details_json"]["new_version"], 2)
        self.assertEqual(review["details_json"]["difference"], 400_000.0)
        # 已结算差额不能重算，只能确认关闭
        with self.assertRaises(Conflict):
            self.service.resolve_review(FIN, review["id"], {"action": "recalculate"})
        closed = self.service.resolve_review(FIN, review["id"], {"action": "dismiss", "note": "维持原结算"})
        self.assertEqual(closed["resolution"]["action"], "dismiss")

        # 失效赔案按新版本重算，预占恢复（300万损失 × 50% = 100万，落在恢复层）
        recalc = self.calculate(c2, d2["claim_version"], 3_000_000)
        self.assertEqual(recalc["basis"]["contract_version"], 2)
        self.assertEqual(recalc["basis"]["recoverable_amount"], 1_000_000.0)
        self.assertEqual(recalc["basis"]["reinstatement_layer"], 1)


class ConcurrencyTest(LedgerCase):
    def test_two_windows_later_version_sees_conflict_and_input_is_retained(self):
        cid = self.create_contract()
        c1 = self.claim(cid, "CLM-1")
        self.calculate(c1, 1, 3_000_000)
        # 窗口B拿旧合约版本提交调整
        with self.assertRaises(OptimisticConflict) as caught:
            self.service.adjust_contract(UW, cid, 99, dict(TERMS, limit=6_000_000.0))
        retained_id = caught.exception.payload["retained_id"]
        self.assertEqual(caught.exception.payload["current_version"], 1)
        # 输入已保留
        retained = self.service.repository.get_retained(retained_id)
        self.assertEqual(retained["status"], "retained")
        self.assertEqual(retained["action"], "adjust_contract")
        # 窗口A先到版本生效
        adj = self.service.adjust_contract(UW, cid, 1, dict(TERMS, limit=5_500_000.0))
        self.assertEqual(adj["version"], 2)
        # 窗口B看清冲突后重放（基于v2）
        replay = self.service.replay_retained(UW, retained_id)
        self.assertEqual(replay["version"], 3)
        self.assertEqual(self.service.repository.get_contract(cid)["active_version"], 3)
        self.assertEqual(self.service.repository.get_retained(retained_id)["status"], "replayed")

    def test_stale_calculate_is_rejected(self):
        cid = self.create_contract()
        c1 = self.claim(cid, "CLM-1")
        r1 = self.calculate(c1, 1, 3_000_000)
        r2 = self.calculate(c1, r1["claim"]["claim_version"], 2_500_000)
        with self.assertRaises(OptimisticConflict):
            self.calculate(c1, r1["claim"]["claim_version"], 9_000_000)
        # 被拒的后到输入没有占额
        self.assert_amounts(cid, [600_000.0])
        retained = self.service.list_retained(CO)
        self.assertEqual(len(retained), 1)
        self.assertEqual(retained[0]["action"], "calculate")
        # 账本余额不受影响，后续正常结算
        self.service.settle(FIN, c1, r2["claim"]["claim_version"], {"payment_reference": "P"})


    def test_two_windows_submitting_together_first_wins(self):
        # 两个窗口同时基于 v1 调整同一合约：先到生效，后到者看到冲突且输入保留
        cid = self.create_contract()
        barrier = threading.Barrier(2)

        def adjust(limit):
            barrier.wait()
            try:
                return ("ok", self.service.adjust_contract(
                    UW, cid, 1, dict(TERMS, limit=limit), request_id="adj-%s" % limit))
            except OptimisticConflict as exc:
                return ("conflict", exc.payload)

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(adjust, [6_000_000.0, 7_000_000.0]))
        outcomes = [outcome for outcome, _ in results]
        self.assertEqual(sorted(outcomes), ["conflict", "ok"])
        contract = self.service.repository.get_contract(cid)
        self.assertEqual(contract["active_version"], 2)  # 只有先到的一次生效
        retained = self.service.list_retained(UW)
        self.assertEqual(len(retained), 1)  # 后到的输入保留
        # 容量账没有重复版本/审计
        versions = self.service.repository.list_versions(cid)
        self.assertEqual([v["version_no"] for v in versions], [1, 2])
        actions = [e["action"] for e in self.service.audit_timeline(UW, contract_id=cid)]
        self.assertEqual(actions.count("contract_adjusted"), 1)

    def test_two_claims_calculated_together_never_double_book(self):
        # 两个窗口同时为两张赔案预占：都成功但共用一本账，恢复层只开一次
        cid = self.create_contract()
        c1 = self.claim(cid, "CLM-X")
        c2 = self.claim(cid, "CLM-Y")
        barrier = threading.Barrier(2)

        def calculate(pair):
            claim_id, loss = pair
            barrier.wait()
            return self.service.calculate(CO, claim_id, 1, {"approved_loss": loss})

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(calculate, [(c1, 5_000_000), (c2, 5_000_000)]))
        self.assertEqual(sorted(r["basis"]["reinstatement_layer"] for r in results), [0, 1])
        self.assert_amounts(cid, [1_600_000.0, 1_600_000.0])
        detail = self.service.get_contract_detail(CO, cid)
        self.assertEqual(detail["ledger"]["reinstatements_used"], 1)
        self.assertEqual(detail["ledger"]["available"], 0.0)


class IdempotencyAndRecoveryTest(LedgerCase):
    def test_duplicate_request_id_does_not_double_reserve_or_audit(self):
        cid = self.create_contract()
        c1 = self.claim(cid, "CLM-1")
        request_id = "req-calc-001"
        r1 = self.calculate_with_request(c1, 1, 3_000_000, request_id)
        audit_before = len(self.service.audit_timeline(CO, contract_id=cid))
        r2 = self.calculate_with_request(c1, 1, 3_000_000, request_id)
        self.assertTrue(r2.get("replayed"))
        self.assertEqual(r2["occupation_id"], r1["occupation_id"])
        self.assert_amounts(cid, [800_000.0])
        audit_after = len(self.service.audit_timeline(CO, contract_id=cid))
        self.assertEqual(audit_before, audit_after)

    def calculate_with_request(self, claim_id, version, loss, request_id):
        return self.service.calculate(CO, claim_id, version, {"approved_loss": loss}, request_id=request_id)

    def test_crash_before_commit_recovers_without_double_occupation(self):
        cid = self.create_contract()
        c1 = self.claim(cid, "CLM-1")
        # 第一次：提交前崩溃（事务回滚，open 意图行残留）
        self.service.arm_crash("before_commit")
        from src.domain import StorageFailure
        with self.assertRaises(StorageFailure):
            self.service.calculate(CO, c1, 1, {"approved_loss": 3_000_000}, request_id="crash-1")
        # 容量账里没有任何占用
        self.assert_amounts(cid, [])
        repo = self.service.repository
        open_batches = repo._connect()
        count = open_batches.execute(
            "SELECT COUNT(*) AS n FROM %s WHERE status='open'" % BATCHES
        ).fetchone()["n"]
        open_batches.close()
        self.assertEqual(count, 1)
        # 新服务启动：从最近完整批次恢复，open批次标记abandoned
        restarted = LedgerService(LedgerRepository(self.db))
        self.assert_amounts(cid, [])
        # 用原 request_id 重试：复用abandoned意图，成功且不重复
        result = restarted.calculate(CO, c1, 1, {"approved_loss": 3_000_000}, request_id="crash-1")
        self.assertEqual(result["basis"]["recoverable_amount"], 800_000.0)
        self.assert_amounts(cid, [800_000.0])
        # 再重试仍是同一结果
        again = restarted.calculate(CO, c1, 1, {"approved_loss": 3_000_000}, request_id="crash-1")
        self.assertTrue(again.get("replayed"))
        self.assertEqual(again["occupation_id"], result["occupation_id"])
        self.assert_amounts(cid, [800_000.0])

    def test_crash_after_commit_replay_returns_same_result(self):
        cid = self.create_contract()
        c1 = self.claim(cid, "CLM-1")
        self.service.arm_crash("after_commit")
        from src.domain import StorageFailure
        with self.assertRaises(StorageFailure):
            self.service.calculate(CO, c1, 1, {"approved_loss": 3_000_000}, request_id="crash-2")
        # 已提交：恢复找不到open批次；重试拿到同一批结果，不重复占额
        recovered = self.service.recover()
        self.assertEqual(recovered, [])
        replay = self.service.calculate(CO, c1, 1, {"approved_loss": 3_000_000}, request_id="crash-2")
        self.assertTrue(replay.get("replayed"))
        self.assert_amounts(cid, [800_000.0])


class LegacyBackfillTest(LedgerCase):
    def test_legacy_claims_backfilled_by_current_amount_unclear_pending(self):
        cid = self.create_contract()
        # 三笔小金额旧赔案，回填后都落在初始层（40万×3=120万 ≤ 160万）
        imported = self.service.import_legacy_claims(CO, cid, [
            {"claim_number": "OLD-1", "approved_loss": 2_000_000, "legacy_batch_no": "B-2019-1"},
            {"claim_number": "OLD-2", "approved_loss": 2_000_000, "legacy_batch_no": "B-2019-2"},
            {"claim_number": "OLD-3"},
        ])
        self.assertEqual(imported["pending_count"], 1)
        self.assertEqual(imported["imported_count"], 3)
        old3 = self.service.repository.get_claim_by_number("OLD-3")
        self.assertEqual(old3["state"], "pending_verification")

        # 回填：两笔40万
        backfilled = self.service.backfill_legacy(CO, request_id="bf-1")
        self.assertEqual(backfilled["backfilled_count"], 2)
        self.assertEqual(backfilled["pending_count"], 0)
        self.assert_amounts(cid, [400_000.0, 400_000.0])
        # 回填幂等：再跑一次不追加占用/审计
        again = self.service.backfill_legacy(CO, request_id="bf-1")
        self.assertTrue(again.get("replayed"))
        self.assert_amounts(cid, [400_000.0, 400_000.0])
        fresh = self.service.backfill_legacy(CO, request_id="bf-2")
        self.assertEqual(fresh["backfilled_count"], 0)

        # 算不清的待核：补录金额后通过待复核重算进入容量账
        reviews = [r for r in self.service.list_reviews(CO) if r["claim_id"] == int(old3["id"])]
        self.assertEqual(len(reviews), 1)
        claim3 = int(old3["id"])
        self.service.repository.update_loss_direct(claim3, 2_000_000)
        resolved = self.service.resolve_review(CO, reviews[0]["id"], {"action": "recalculate"})
        self.assertEqual(resolved["resolution"]["occupation_id"] > 0, True)
        self.assert_amounts(cid, [400_000.0, 400_000.0, 400_000.0])

    def test_legacy_backfill_capacity_shortfall_goes_pending(self):
        # 0 次恢复的单层合约，第二笔旧赔案装不下时先待核
        cid = self.create_contract("T-2", dict(TERMS, max_reinstatements=0))
        self.service.import_legacy_claims(CO, cid, [
            {"claim_number": "OLD-1", "approved_loss": 5_000_000},   # 160万占满
            {"claim_number": "OLD-2", "approved_loss": 3_000_000},   # 80万装不下
        ])
        result = self.service.backfill_legacy(CO)
        self.assertEqual(result["backfilled_count"], 1)
        self.assertEqual(result["pending_count"], 1)
        self.assertEqual(result["pending"][0]["reason"], "容量不足")
        reviews = self.service.list_reviews(CO)
        self.assertTrue(any(r["review_type"] == "capacity_shortfall" for r in reviews))
        old2 = self.service.repository.get_claim_by_number("OLD-2")
        self.assertEqual(old2["state"], "pending_verification")


class PermissionTest(LedgerCase):
    def test_ledger_roles(self):
        outsider = Actor("x", "outsider")
        with self.assertRaises(PermissionDenied):
            self.service.create_contract(outsider, "X", TERMS)
        cid = self.create_contract()
        with self.assertRaises(PermissionDenied):
            self.service.submit_claim(UW, cid, {"claim_number": "X"})
        c1 = self.claim(cid, "CLM-1")
        r1 = self.calculate(c1, 1, 3_000_000)
        with self.assertRaises(PermissionDenied):
            self.service.settle(CO, c1, r1["claim"]["claim_version"], {"payment_reference": "P"})


class PureRulesTest(unittest.TestCase):
    def test_layer_allocation(self):
        terms = dict(rules.validate_contract_terms(TERMS), contract_version=1)
        self.assertEqual(rules.base_capacity(terms), 1_600_000.0)
        p = rules.project_occupations(terms, [800_000.0, 1_600_000.0], 800_000.0)
        self.assertEqual(p["layers"], [1_600_000.0, 1_600_000.0])
        self.assertEqual(p["new_layer"], 0)
        with self.assertRaises(CapacityConflict):
            rules.project_occupations(terms, [800_000.0, 1_600_000.0, 800_000.0], 100_000.0)
        with self.assertRaises(CapacityConflict):
            rules.project_occupations(terms, [], 1_700_000.0)
