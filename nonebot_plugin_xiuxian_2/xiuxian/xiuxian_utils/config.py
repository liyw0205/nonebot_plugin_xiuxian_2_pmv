from typing import Set

from nonebot import get_driver
from pydantic.v1 import Field, BaseModel


class Config(BaseModel):
    disabled_plugins: Set[str] = Field(
        default_factory=set, alias="xiuxian_disabled_plugins"
    )
    priority: int = Field(2, alias="xiuxian_priority")


try:
    # Maintenance commands import legacy repositories without starting a
    # NoneBot driver.  Their configuration must still be importable; a real
    # driver remains the authoritative source whenever one is initialized.
    driver_config = get_driver().config
except ValueError:
    driver_config = {}

config = Config.parse_obj(driver_config)
priority = config.priority
