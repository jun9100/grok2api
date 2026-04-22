import tempfile
import time
import unittest
from pathlib import Path

from app.control.account.backends.local import LocalAccountRepository
from app.control.account.commands import AccountUpsert
from app.control.account.enums import AccountStatus
from app.control.account.refresh import AccountRefreshService
from app.platform.errors import UpstreamError


class RecordFailureAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)

        db_path = Path(self._tmpdir.name) / "accounts.db"
        self.repo = LocalAccountRepository(db_path)
        await self.repo.initialize()
        await self.repo.upsert_accounts(
            [AccountUpsert(token="test-token", pool="basic")]
        )
        self.service = AccountRefreshService(self.repo)

    async def test_429_persists_cooling_state_and_quota_patch(self) -> None:
        before = (await self.repo.get_accounts(["test-token"]))[0]
        now = int(time.time() * 1000)

        await self.service.record_failure_async(
            "test-token", 0, UpstreamError("rate limited", status=429)
        )

        after = (await self.repo.get_accounts(["test-token"]))[0]
        auto_window = after.quota_set().auto

        self.assertEqual(after.status, AccountStatus.COOLING)
        self.assertEqual(after.state_reason, "rate_limited")
        self.assertEqual(after.last_fail_reason, "rate_limited")
        self.assertEqual(after.usage_fail_count, before.usage_fail_count + 1)
        self.assertEqual(auto_window.remaining, 0)
        self.assertIsNotNone(auto_window.reset_at)
        self.assertGreater(auto_window.reset_at, now)
        self.assertEqual(after.ext.get("cooldown_reason"), "rate_limited")
        self.assertGreater(after.ext.get("cooldown_until", 0), now)

    async def test_non_429_failure_only_updates_failure_counters(self) -> None:
        before = (await self.repo.get_accounts(["test-token"]))[0]

        await self.service.record_failure_async(
            "test-token", 0, UpstreamError("bad gateway", status=502)
        )

        after = (await self.repo.get_accounts(["test-token"]))[0]

        self.assertEqual(after.status, AccountStatus.ACTIVE)
        self.assertIsNone(after.state_reason)
        self.assertEqual(after.usage_fail_count, before.usage_fail_count + 1)


if __name__ == "__main__":
    unittest.main()
