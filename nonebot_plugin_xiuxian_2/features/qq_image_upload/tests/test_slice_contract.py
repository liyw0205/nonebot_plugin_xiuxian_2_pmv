from __future__ import annotations

import asyncio
import typing
import unittest
from typing import Any

from nonebot_plugin_xiuxian_2.features.qq_image_upload import (
    application,
    commands,
    jobs,
    migrations,
    repository,
    schemas,
    web,
)
from nonebot_plugin_xiuxian_2.features.qq_image_upload.manifest import FEATURE
from tests.slice_contract import assert_contract_is_single_source, assert_no_autonomous_surface

SCHEMA_NAMES = ("QQ_ADAPTER_NAME", "UPLOAD_FILE_MODE")


class _Bot:
    def __init__(self, name: str) -> None:
        self.adapter = self._Adapter(name)

    class _Adapter:
        def __init__(self, name: str) -> None:
            self._name = name

        def get_name(self) -> str:
            return self._name


class QqImageUploadSliceContractTests(unittest.TestCase):
    def test_request_contract_has_one_home(self) -> None:
        assert_contract_is_single_source(self, schemas, ((application, SCHEMA_NAMES),))

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
            [("POST", "/upload_image", "QqImageUploadApplication.upload_image")],
        )

    def test_manifest_matches_the_slice(self) -> None:
        self.assertEqual(FEATURE.key, "qq_image_upload")
        self.assertEqual(FEATURE.test_tag, "qq_image_upload")
        self.assertEqual(FEATURE.owner, "operations")
        self.assertEqual(FEATURE.commands, ())
        self.assertEqual(FEATURE.routes, ())
        self.assertEqual(FEATURE.jobs, ())
        self.assertIsNone(FEATURE.migration_version)

    def test_bot_selection_and_upload_use_the_declared_values(self) -> None:
        calls: dict[str, Any] = {}

        async def port(**fields: Any) -> str:
            calls.update(fields)
            return "https://media.test/image"

        app = application.QqImageUploadApplication(port)
        bots = {1: _Bot("OneBot V11"), 2: _Bot(schemas.QQ_ADAPTER_NAME)}
        self.assertEqual(app.select_qq_bot({1: _Bot("OneBot V11")}), None)
        self.assertIs(app.select_qq_bot(bots), bots[2])
        url = asyncio.run(app.upload_image(bot=bots[2], channel_id=7, image=b"bytes"))
        self.assertEqual(url, "https://media.test/image")
        self.assertEqual(calls["mode"], schemas.UPLOAD_FILE_MODE)
        self.assertEqual(calls["channel_id"], "7")

    def test_upload_port_protocol_is_the_declared_boundary(self) -> None:
        hints = typing.get_type_hints(application.QqImageUploadApplication.__init__)
        self.assertIs(hints["upload_image_and_get_url"], repository.UploadImageAndResolve)


if __name__ == "__main__":
    unittest.main()
