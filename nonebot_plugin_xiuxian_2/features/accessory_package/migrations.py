from ...infrastructure.database import DatabaseUnitOfWork
from .repository import AccessoryPackageGameRepository


def apply_accessory_package(uow: DatabaseUnitOfWork) -> None:
    AccessoryPackageGameRepository().ensure_schema(uow)


__all__ = ["apply_accessory_package"]
