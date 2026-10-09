from __future__ import annotations

import typing
import unittest

from .. import (
    application,
    commands,
    jobs,
    migrations,
    repository,
    schemas,
    send_application,
    web,
)
from ..manifest import FEATURE
from tests.slice_contract import assert_contract_is_single_source, assert_no_autonomous_surface

SCHEMA_NAMES = (
    "ACTIVE_SEND_TRUE_VALUES",
    "ALLOWED_MEDIA_TYPES",
    "DEFAULT_SEND_MODE",
    "SCENES",
    "SEND_MODES",
    "STICKER_MEDIA_TYPE",
)


class MessageSendSliceContractTests(unittest.TestCase):
    def test_request_contract_has_one_home(self) -> None:
        assert_contract_is_single_source(self, schemas, ((send_application, SCHEMA_NAMES),))
        self.assertIs(send_application.MessageSendResult, schemas.MessageSendResult)

    def test_application_module_reexports_the_frozen_implementation(self) -> None:
        # The Phase 2 ledger anchors this slice onto send_application.py, so the
        # canonical application module must stay a facade, never a second class.
        self.assertIs(
            application.WebMessageSendApplication, send_application.WebMessageSendApplication
        )
        self.assertIs(application.MessageSendResult, send_application.MessageSendResult)

    def test_ports_are_declared_once(self) -> None:
        for name in (
            "MessageReplyLookupPort",
            "MessageTransportPort",
            "StickerPathResolverPort",
            "WebSendRecorderPort",
        ):
            self.assertTrue(hasattr(repository, name), name)
        hints = typing.get_type_hints(send_application.WebMessageSendApplication.__init__)
        self.assertIs(hints["transport"], repository.MessageTransportPort)
        self.assertIs(hints["sticker_application"], repository.StickerPathResolverPort)

    def test_slice_declares_no_autonomous_surface(self) -> None:
        assert_no_autonomous_surface(
            self,
            commands=commands.COMMANDS,
            routes=web.ROUTES,
            jobs=jobs.JOBS,
            migrations=migrations.MIGRATIONS,
            legacy_routes=web.LEGACY_ROUTES,
        )
        self.assertEqual(
            list(web.LEGACY_ROUTES),
            [("POST", "/api/messages/send", "WebMessageSendApplication.send")],
        )

    def test_manifest_matches_the_slice(self) -> None:
        self.assertEqual(FEATURE.key, "messages")
        self.assertEqual(FEATURE.test_tag, "messages")
        self.assertEqual(FEATURE.owner, "operations")
        self.assertEqual(FEATURE.commands, ())
        self.assertEqual(FEATURE.routes, ())
        self.assertEqual(FEATURE.jobs, ())
        self.assertIsNone(FEATURE.migration_version)


if __name__ == "__main__":
    unittest.main()
