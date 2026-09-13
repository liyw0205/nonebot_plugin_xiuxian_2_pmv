from __future__ import annotations

import asyncio
import unittest

from nonebot_plugin_xiuxian_2.core.result import ReplyPlan
from nonebot_plugin_xiuxian_2.infrastructure.messaging import MessageGatewayAdapter
from nonebot_plugin_xiuxian_2.infrastructure.observability import trace_context


class MessageGatewayTests(unittest.TestCase):
    def test_delivery_outcome_carries_trace_context(self) -> None:
        async def run():
            async def sender(_context, _reply):
                return type("Sent", (), {"message_id": "m-1"})()

            gateway = MessageGatewayAdapter(sender)
            with trace_context(request_id="req-1", operation_id="op-1", job_id="job-1", user_scope="user-1234"):
                outcome = await gateway.send(object(), ReplyPlan("ok"))
            self.assertTrue(outcome.ok)
            self.assertEqual(outcome.message_id, "m-1")
            self.assertEqual(outcome.operation_id, "op-1")
            self.assertEqual(outcome.job_id, "job-1")
            self.assertEqual(outcome.user_scope, "us***34")

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
