from dataclasses import dataclass


@dataclass(frozen=True)
class ItemOperation:
    operation_id: str
    user_id: str

    def validate(self) -> None:
        if not self.operation_id or not self.user_id:
            raise ValueError("operation_id and user_id are required")


__all__ = ["ItemOperation"]
