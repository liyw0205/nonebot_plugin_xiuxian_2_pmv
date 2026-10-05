from __future__ import annotations

from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .repository import EntertainmentRepository
from .schemas import NewApiAccountListResult, NewApiCheckinTargetsResult
from .webdav_repository import WebDavRepository


class EntertainmentApplication(MigratedFeatureApplication):
    def __init__(
        self,
        database: str | Path,
        *,
        repository: EntertainmentRepository | None = None,
        webdav_repository: WebDavRepository | None = None,
    ) -> None:
        super().__init__(database, feature="entertainment", repository=repository or EntertainmentRepository(database))
        self.webdav_repository = webdav_repository or WebDavRepository()

    def toggle_auto_checkin(self, *, operation_id: str, user_id: str, state_path: str | Path, index: int):
        return self.execute(operation_id=operation_id, user_id=user_id, payload={"action": "toggle_auto_checkin", "state_path": str(state_path), "index": int(index)})

    def delete_accounts(self, *, operation_id: str, user_id: str, state_path: str | Path, indices):
        return self.execute(operation_id=operation_id, user_id=user_id, payload={"action": "delete_accounts", "state_path": str(state_path), "indices": indices})

    def list_account_summaries(self, *, state_path: str | Path) -> NewApiAccountListResult:
        return self.repository.list_account_summaries(state_path)

    def resolve_checkin_targets(self, *, state_path: str | Path, selector: str) -> NewApiCheckinTargetsResult:
        return self.repository.resolve_checkin_targets(state_path, selector)

    def append_checkin_history(
        self,
        *,
        history_path: str | Path,
        account_index: int,
        api_user_id: str,
        base_url: str,
        summary: str,
        source: str = "manual",
    ) -> None:
        self.repository.append_checkin_history(
            history_path,
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
