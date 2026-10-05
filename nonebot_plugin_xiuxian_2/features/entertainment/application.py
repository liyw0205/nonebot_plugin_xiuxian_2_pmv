from __future__ import annotations

from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .guess_application import EntertainmentGuessSessionApplication
from .guess_repository import EntertainmentGuessSessionSqlRepository
from .repository import EntertainmentRepository
from .room_repository import EntertainmentRoomSqlRepository
from .schemas import NewApiAccountListResult, NewApiCheckinTargetsResult
from .external_query import EntertainmentExternalQueryProvider
from .music_application import EntertainmentMusicApplication
from .webdav_repository import WebDavRepository


class EntertainmentApplication(MigratedFeatureApplication):
    def __init__(
        self,
        database: str | Path,
        *,
        repository: EntertainmentRepository | None = None,
        guess_session_repository: EntertainmentGuessSessionSqlRepository | None = None,
        room_repository: EntertainmentRoomSqlRepository | None = None,
        webdav_repository: WebDavRepository | None = None,
        external_query_provider: EntertainmentExternalQueryProvider | None = None,
        music_application: EntertainmentMusicApplication | None = None,
    ) -> None:
        super().__init__(database, feature="entertainment", repository=repository or EntertainmentRepository(database))
        self.guess_sessions = EntertainmentGuessSessionApplication(
            guess_session_repository or EntertainmentGuessSessionSqlRepository(database)
        )
        self.room_repository = room_repository or EntertainmentRoomSqlRepository(database)
        self.webdav_repository = webdav_repository or WebDavRepository()
        self.external_query_provider = external_query_provider or EntertainmentExternalQueryProvider()
        self.music = music_application or EntertainmentMusicApplication(
            self.external_query_provider
        )

    def external_json(
        self,
        api_url: str,
        params=None,
        timeout: float = 15,
        max_bytes=None,
        **request_options,
    ):
        return self.external_query_provider.get_json(
            api_url,
            params=params,
            timeout=timeout,
            max_bytes=max_bytes,
            **request_options,
        )

    def external_text(self, api_url: str, params=None, timeout: float = 15):
        return self.external_query_provider.get_text(
            api_url, params=params, timeout=timeout
        )

    def external_media_url(self, api_url: str, params=None, timeout: float = 20):
        return self.external_query_provider.get_media_url(
            api_url, params=params, timeout=timeout
        )

    def external_bytes(
        self, url: str, *, max_bytes: int, timeout: float = 20, **request_options
    ):
        return self.external_query_provider.get_bytes(
            url,
            max_bytes=max_bytes,
            timeout=timeout,
            **request_options,
        )

    def bangumi_seasons_now(self):
        return self.external_query_provider.fetch_bangumi_seasons_now()

    def room_states(self, game_type: str):
        return self.room_repository.list_states(game_type)

    def save_room_state(self, game_type: str, room_id: str, state: dict):
        self.room_repository.save_state(game_type, room_id, state)

    def delete_room_state(self, game_type: str, room_id: str):
        return self.room_repository.delete_state(game_type, room_id)

    def bind_newapi_account(
        self,
        *,
        operation_id: str,
        user_id: str,
        mode: str,
        api_user_id: str,
        secret: str,
        base_url: str,
        label: str = "",
    ):
        return self.repository.bind_account(
            operation_id,
            user_id,
            mode=mode,
            api_user_id=api_user_id,
            secret=secret,
            base_url=base_url,
            label=label,
        )

    def toggle_auto_checkin(
        self,
        *,
        operation_id: str,
        user_id: str,
        account_id: int,
        index: int,
        legacy_state_path: str,
    ):
        return self.repository.toggle_auto_checkin(
            operation_id,
            user_id,
            account_id,
            index,
            legacy_state_path=legacy_state_path,
        )

    def delete_accounts(
        self,
        *,
        operation_id: str,
        user_id: str,
        legacy_state_path: str,
        indices,
    ):
        return self.repository.delete_accounts(
            operation_id,
            user_id,
            indices,
            legacy_state_path=legacy_state_path,
        )

    def list_account_summaries(self, *, user_id: str) -> NewApiAccountListResult:
        return self.repository.list_account_summaries(user_id)

    def resolve_checkin_targets(self, *, user_id: str, selector: str) -> NewApiCheckinTargetsResult:
        return self.repository.resolve_checkin_targets(user_id, selector)

    def resolve_info_targets(self, *, user_id: str, selector: str) -> NewApiCheckinTargetsResult:
        return self.repository.resolve_info_targets(user_id, selector)

    def list_auto_checkin_targets(self, *, after_account_id: int = 0, limit: int = 32):
        return self.repository.list_auto_checkin_targets(after_account_id, limit)

    def list_checkin_history(self, *, user_id: str):
        return self.repository.list_checkin_history(user_id)

    def append_checkin_history(
        self,
        *,
        user_id: str,
        account_index: int,
        api_user_id: str,
        base_url: str,
        summary: str,
        source: str = "manual",
    ) -> None:
        self.repository.append_checkin_history(
            user_id,
            account_index=account_index,
            api_user_id=api_user_id,
            base_url_stored=base_url,
            summary=summary,
            source=source,
        )

    def webdav_bindings(self, *, bindings_path: str | Path):
        return self.webdav_repository.load_bindings(bindings_path)

    def webdav_bind(
        self,
        *,
        bindings_path: str | Path,
        label: str,
        dav_url: str,
        username: str,
        password: str,
    ):
        return self.webdav_repository.bind(
            bindings_path,
            label=label,
            dav_url=dav_url,
            username=username,
            password=password,
        )

    def webdav_delete(self, *, bindings_path: str | Path, text: str):
        return self.webdav_repository.delete(bindings_path, text)

    def webdav_download_link(self, *, bindings_path: str | Path, text: str):
        return self.webdav_repository.webdav_download_link(bindings_path, text)

    def webdav_info(self, *, bindings_path: str | Path, text: str):
        return self.webdav_repository.propfind(
            bindings_path,
            text,
            depth="0",
            need_path=True,
        )

    def webdav_list(self, *, bindings_path: str | Path, text: str):
        return self.webdav_repository.propfind(
            bindings_path,
            text,
            depth="1",
            need_path=False,
        )


__all__ = ["EntertainmentApplication"]
