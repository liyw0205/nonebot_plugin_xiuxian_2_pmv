from __future__ import annotations

from ..infrastructure.clock import SystemClock


runtime_clock = SystemClock()
_player_data_manager_instance = None


def _resolve_player_data_manager():
    global _player_data_manager_instance
    if _player_data_manager_instance is None:
        from ..xiuxian.xiuxian_utils.xiuxian2_handle import PlayerDataManager

        _player_data_manager_instance = PlayerDataManager()
    return _player_data_manager_instance


def savef(user_id, data):
    """Keep the old bankinfo writer available only through its compatibility boundary."""
    user_id = str(user_id)
    manager = _resolve_player_data_manager()
    manager.update_or_write_data(user_id, "bankinfo", "savestone", int(data.get("savestone", 0)), data_type="INTEGER")
    manager.update_or_write_data(user_id, "bankinfo", "savetime", str(data.get("savetime", runtime_clock.now().strftime('%Y-%m-%d %H:%M:%S'))), data_type="TEXT")
    manager.update_or_write_data(user_id, "bankinfo", "banklevel", str(data.get("banklevel", "1")), data_type="TEXT")
    return True


__all__ = ["savef"]
