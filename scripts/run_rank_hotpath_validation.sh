#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
PYTHON=${XIUXIAN_TEST_PYTHON:-/home/nonebot-shared-venv/bin/python}
LOCK=/tmp/nonebot-project-validation.lock

if [[ ! -x "$PYTHON" ]]; then
    printf 'development Python is missing: %s\n' "$PYTHON" >&2
    exit 2
fi

usage() {
    printf 'usage: %s correctness FIXTURE SMOKE_REPORT | performance FIXTURE REPORT | rank-key FIXTURE REPORT | rank-key-format FIXTURE REPORT | profile FIXTURE REPORT | config-profile FIXTURE REPORT | base-profile FIXTURE REPORT | buff-profile FIXTURE REPORT | title-profile FIXTURE REPORT | state-profile FIXTURE REPORT\n' "$0" >&2
}

case ${1:-} in
    correctness)
        [[ $# -eq 3 ]] || { usage; exit 2; }
        exec flock -w 600 "$LOCK" /bin/bash -c '
            set -euo pipefail
            root=$1
            python=$2
            fixture=$3
            report=$4
            cd "$root"
            tests=(
                tests/test_core_numeric.py
                tests/test_core_rank_indexes.py
                tests/test_rank_hotpath_probe_contract.py
                nonebot_plugin_xiuxian_2/features/info/tests/test_player_attribute_application.py
                tests/test_info_user_lazy_reader.py
                tests/test_info_profile_reuse.py
            )
            if [[ -f docs/refactor_phase2_legacy_path_items.json ]]; then
                tests+=(tests/test_info_frozen_command_owner.py::InfoFrozenCommandOwnerTests::test_text_and_image_handlers_share_the_same_projection)
            fi
            "$python" -m pytest -q "${tests[@]}"
            "$python" "$root/scripts/rank_hotpath_probe.py" \
                --fixture "$fixture" --report "$report" --smoke
        ' _ "$ROOT" "$PYTHON" "$2" "$3"
        ;;
    performance)
        [[ $# -eq 3 ]] || { usage; exit 2; }
        exec flock -w 600 "$LOCK" "$PYTHON" "$ROOT/scripts/rank_hotpath_probe.py" --fixture "$2" --report "$3"
        ;;
    rank-key)
        [[ $# -eq 3 ]] || { usage; exit 2; }
        exec flock -w 600 "$LOCK" "$PYTHON" "$ROOT/scripts/rank_hotpath_probe.py" --fixture "$2" --report "$3" --rank-key-compare
        ;;
    rank-key-format)
        [[ $# -eq 3 ]] || { usage; exit 2; }
        exec flock -w 600 "$LOCK" "$PYTHON" "$ROOT/scripts/rank_hotpath_probe.py" --fixture "$2" --report "$3" --compact-rank-compare
        ;;
    profile)
        [[ $# -eq 3 ]] || { usage; exit 2; }
        exec flock -w 600 "$LOCK" "$PYTHON" "$ROOT/scripts/rank_hotpath_probe.py" --fixture "$2" --report "$3" --profile-reuse
        ;;
    config-profile)
        [[ $# -eq 3 ]] || { usage; exit 2; }
        exec flock -w 600 "$LOCK" "$PYTHON" "$ROOT/scripts/rank_hotpath_probe.py" --fixture "$2" --report "$3" --config-reuse
        ;;
    base-profile)
        [[ $# -eq 3 ]] || { usage; exit 2; }
        exec flock -w 600 "$LOCK" "$PYTHON" "$ROOT/scripts/rank_hotpath_probe.py" --fixture "$2" --report "$3" --base-reuse
        ;;
    buff-profile)
        [[ $# -eq 3 ]] || { usage; exit 2; }
        exec flock -w 600 "$LOCK" "$PYTHON" "$ROOT/scripts/rank_hotpath_probe.py" --fixture "$2" --report "$3" --buff-reuse
        ;;
    title-profile)
        [[ $# -eq 3 ]] || { usage; exit 2; }
        exec flock -w 600 "$LOCK" "$PYTHON" "$ROOT/scripts/rank_hotpath_probe.py" --fixture "$2" --report "$3" --title-reuse
        ;;
    state-profile)
        [[ $# -eq 3 ]] || { usage; exit 2; }
        exec flock -w 600 "$LOCK" "$PYTHON" "$ROOT/scripts/rank_hotpath_probe.py" --fixture "$2" --report "$3" --state-reuse
        ;;
    *)
        usage
        exit 2
        ;;
esac
