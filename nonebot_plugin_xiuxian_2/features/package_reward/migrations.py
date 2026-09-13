from ...infrastructure.database import DatabaseUnitOfWork
from .repository import PackageRewardRepository


def apply_package_reward(uow: DatabaseUnitOfWork) -> None:
    PackageRewardRepository().ensure_schema(uow)


__all__ = ["apply_package_reward"]
