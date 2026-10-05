from ...features.entertainment.application import EntertainmentApplication
from ...paths import get_paths


entertainment_application = EntertainmentApplication(get_paths().game_db)


__all__ = ["entertainment_application"]
