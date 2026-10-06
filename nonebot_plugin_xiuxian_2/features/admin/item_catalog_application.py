from __future__ import annotations

from .item_catalog_repository import AdminItemCatalogRepository, get_item_catalog_repository


class AdminItemCatalogApplication:
    def __init__(self, repository: AdminItemCatalogRepository | None = None) -> None:
        if repository is None:
            from ...paths import get_paths

            repository = get_item_catalog_repository(get_paths().data)
        self.repository = repository

    def reload(self) -> dict:
        return self.repository.reload()


__all__ = ["AdminItemCatalogApplication"]
