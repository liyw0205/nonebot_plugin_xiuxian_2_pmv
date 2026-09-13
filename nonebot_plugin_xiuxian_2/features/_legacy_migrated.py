"""Manifest and migration collection for the remaining legacy slices."""
from .activity.application import ActivityApplication
from .admin.application import AdminApplication
from .compensation.application import CompensationApplication
from .dongfu.application import DongfuApplication
from .dufang.application import DufangApplication
from .entertainment.application import EntertainmentApplication
from .fusion.application import FusionApplication
from .impart.application import ImpartApplication
from .impart_pk.application import ImpartPkApplication
from .info.application import InfoApplication
from .lunhui.application import LunhuiApplication
from .past_life.application import PastLifeApplication
from .simulator.application import SimulatorApplication
from .status.application import StatusApplication
from .tasks.application import TasksApplication
from .tianti.application import TiantiApplication
from .training.application import TrainingApplication

APPLICATIONS = {
    "activity": ActivityApplication,
    "admin": AdminApplication,
    "compensation": CompensationApplication,
    "dongfu": DongfuApplication,
    "dufang": DufangApplication,
    "entertainment": EntertainmentApplication,
    "fusion": FusionApplication,
    "impart": ImpartApplication,
    "impart_pk": ImpartPkApplication,
    "info": InfoApplication,
    "lunhui": LunhuiApplication,
    "past_life": PastLifeApplication,
    "simulator": SimulatorApplication,
    "status": StatusApplication,
    "tasks": TasksApplication,
    "tianti": TiantiApplication,
    "training": TrainingApplication,
}

from .activity.manifest import FEATURE as ACTIVITY
from .admin.manifest import FEATURE as ADMIN
from .compensation.manifest import FEATURE as COMPENSATION
from .dongfu.manifest import FEATURE as DONGFU
from .dufang.manifest import FEATURE as DUFANG
from .entertainment.manifest import FEATURE as ENTERTAINMENT
from .fusion.manifest import FEATURE as FUSION
from .impart.manifest import FEATURE as IMPART
from .impart_pk.manifest import FEATURE as IMPART_PK
from .info.manifest import FEATURE as INFO
from .lunhui.manifest import FEATURE as LUNHUI
from .past_life.manifest import FEATURE as PAST_LIFE
from .simulator.manifest import FEATURE as SIMULATOR
from .status.manifest import FEATURE as STATUS
from .tasks.manifest import FEATURE as TASKS
from .tianti.manifest import FEATURE as TIANTI
from .training.manifest import FEATURE as TRAINING
from .activity.migrations import apply_activity
from .admin.migrations import apply_admin
from .compensation.migrations import apply_compensation
from .dongfu.migrations import apply_dongfu
from .dufang.migrations import apply_dufang
from .entertainment.migrations import apply_entertainment
from .fusion.migrations import apply_fusion
from .impart.migrations import apply_impart
from .impart_pk.migrations import apply_impart_pk
from .info.migrations import apply_info
from .lunhui.migrations import apply_lunhui
from .past_life.migrations import apply_past_life
from .simulator.migrations import apply_simulator
from .status.migrations import apply_status
from .tasks.migrations import apply_tasks
from .tianti.migrations import apply_tianti
from .training.migrations import apply_training
from .illusion.migrations import apply_illusion

FEATURES = tuple(sorted((
    ACTIVITY, ADMIN, COMPENSATION, DONGFU,
    DUFANG, ENTERTAINMENT, FUSION, IMPART, IMPART_PK, INFO, LUNHUI,
    PAST_LIFE, SIMULATOR, STATUS, TASKS, TIANTI, TRAINING,
), key=lambda feature: feature.migration_version or feature.key))

_MIGRATION_BY_KEY = {
    "activity": apply_activity,
    "admin": apply_admin,
    "compensation": apply_compensation,
    "dongfu": apply_dongfu,
    "dufang": apply_dufang,
    "entertainment": apply_entertainment,
    "fusion": apply_fusion,
    "impart": apply_impart,
    "impart_pk": apply_impart_pk,
    "info": apply_info,
    "lunhui": apply_lunhui,
    "past_life": apply_past_life,
    "simulator": apply_simulator,
    "status": apply_status,
    "tasks": apply_tasks,
    "tianti": apply_tianti,
    "training": apply_training,
}
MIGRATIONS = tuple(
    sorted(
        (*((feature.migration_version, _MIGRATION_BY_KEY[feature.key]) for feature in FEATURES),
         ("legacy.illusion.001", apply_illusion)),
        key=lambda item: item[0],
    )
)

__all__ = ["APPLICATIONS", "FEATURES", "MIGRATIONS"]
