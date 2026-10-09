"""No NoneBot matcher belongs to this slice.

The three operator commands ``版本查询``, ``版本更新`` and ``检测更新`` are registered by the
legacy status module (``xiuxian/xiuxian_status/__init__.py:90-92``) and reach the
registry through ``compatibility.command_inventory.commands_for("status")``, so the
``status`` manifest is their only owner.  Declaring any of them here would raise a
duplicate-command error in ``FeatureRegistry.validate()``.
"""

COMMANDS = ()

__all__ = ["COMMANDS"]
