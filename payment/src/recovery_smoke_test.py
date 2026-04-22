import tempfile
import sys
from pathlib import Path

# Allow running this file directly from the repository root without setting PYTHONPATH.
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app import PaymentService
from utils.pb.payment import payment_pb2


def assert_true(value, message):
    if not value:
        raise AssertionError(message)


def run_recovery_smoke_test():
    with tempfile.TemporaryDirectory() as tmp_dir:
        state_file = Path(tmp_dir) / "payment_state.json"

        # Phase 1: participant prepares, then crashes.
        service_before_crash = PaymentService(state_file=str(state_file))
        prepare_response = service_before_crash.Prepare(
            payment_pb2.PrepareRequest(order_id="order-prepare-commit"),
            context=None,
        )
        assert_true(prepare_response.ready, "prepare should be ready")

        # Phase 2: participant restarts and should recover prepared state.
        service_after_restart = PaymentService(state_file=str(state_file))
        commit_response = service_after_restart.Commit(
            payment_pb2.CommitRequest(order_id="order-prepare-commit"),
            context=None,
        )
        assert_true(commit_response.success, "commit should succeed after restart")

        # Commit must be idempotent after restart.
        service_after_second_restart = PaymentService(state_file=str(state_file))
        commit_again_response = service_after_second_restart.Commit(
            payment_pb2.CommitRequest(order_id="order-prepare-commit"),
            context=None,
        )
        assert_true(commit_again_response.success, "commit should be idempotent")

        # Abort path recovery test.
        prepare_for_abort = service_after_second_restart.Prepare(
            payment_pb2.PrepareRequest(order_id="order-prepare-abort"),
            context=None,
        )
        assert_true(prepare_for_abort.ready, "prepare for abort path should be ready")

        service_abort_restart = PaymentService(state_file=str(state_file))
        abort_response = service_abort_restart.Abort(
            payment_pb2.AbortRequest(order_id="order-prepare-abort"),
            context=None,
        )
        assert_true(abort_response.aborted, "abort should succeed after restart")

        # Aborted orders must not be committed.
        service_after_abort_restart = PaymentService(state_file=str(state_file))
        commit_aborted_response = service_after_abort_restart.Commit(
            payment_pb2.CommitRequest(order_id="order-prepare-abort"),
            context=None,
        )
        assert_true(not commit_aborted_response.success, "aborted order should not commit")

    print("payment recovery smoke test: PASS")


if __name__ == "__main__":
    run_recovery_smoke_test()
