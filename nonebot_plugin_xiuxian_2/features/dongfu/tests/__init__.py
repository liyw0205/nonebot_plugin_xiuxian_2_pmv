"""Cave dwelling feature tests."""

from ....infrastructure.database import DatabaseUnitOfWork
from ..migrations import apply_dongfu_operations


def install_operation_schema(database):
    with DatabaseUnitOfWork(database) as uow:
        apply_dongfu_operations(uow)
