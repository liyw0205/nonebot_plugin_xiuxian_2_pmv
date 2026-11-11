"""Merge a single command baseline with focused retries; preserve provenance."""
from __future__ import annotations

import argparse
from collections import Counter
from copy import deepcopy
import json
from pathlib import Path


def merge(paths):
    commands = {}
    for path in paths:
        report = json.loads(path.read_text(encoding="utf-8"))
        for row in report["results"]:
            key = row["module"], row["command"]
            current = commands.setdefault(key, {**row, "cases": {}})
            for case in row["cases"]:
                case = deepcopy(case)
                previous = current["cases"].get(case["scenario"])
                case["evidence"] = str(path)
                if previous:
                    case["previous_evidence"] = [*previous.get("previous_evidence", []), {"path": previous["evidence"], "status": previous["status"]}]
                # Older captures recorded offline failures only as warning text.
                denied = [log["message"] for log in case.get("logs", []) if "offline boundary:" in log["message"]]
                if denied and case["status"] == "pass":
                    case["status"] = "blocked"
                    case["blocked"] = list(dict.fromkeys([*case.get("blocked", []), *denied]))
                current["cases"][case["scenario"]] = case
            current.update({key: value for key, value in row.items() if key != "cases"})
    results = [{**row, "cases": list(row["cases"].values())} for row in commands.values()]
    return {"commands": len(results), "summary": dict(Counter(case["status"] for row in results for case in row["cases"])), "results": results}


def markdown(report):
    lines = [
        "# 已注册指令自动化检查清单", "",
        "只合并一次基线与受影响重试；pass 表示所列输入实际触发分支无异常并有回复，不能等同完整业务成功。",
        "正常成功/重放/资产与日志专项见 registered_command_cases 和 test_buff_command_replay；未覆盖的合法参数组合不推断通过。",
        "权限拒绝、未选中路由、无回复、外网/进程隔离分别记 blocked；QQ 真机呈现、生产、定时任务未执行。", "",
        f"{report['commands']} matcher；场景汇总：{json.dumps(report['summary'], ensure_ascii=False)}。", "",
        "| 指令 | 模块 | 无参/缺参 | 未注册玩家 | 通用参数1 | 管理员无参 | 受阻或异常原因 |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in report["results"]:
        cases = {case["scenario"]: case for case in row["cases"]}
        reasons = []
        for case in cases.values():
            if case["status"] != "pass":
                detail = "; ".join(filter(None, [case.get("reason"), *case.get("blocked", []), *[error.splitlines()[-1] for error in case.get("errors", []) if error.splitlines()]]))
                reasons.append(f"{case['scenario']}: {detail}")
        values = [row["command"], row["module"].removeprefix("nonebot_plugin_xiuxian_2."), *[cases.get(key, {}).get("status", "未测") for key in ("missing", "unregistered_player", "argument", "admin_missing")], "; ".join(reasons)]
        lines.append("| " + " | ".join(str(value).replace("|", "\\|").replace("\n", " ") for value in values) + " |")
    lines.extend(["", "JSON 保留每项实际输入、虚构actor、回复、日志、堆栈与基线/重试证据来源。", ""])
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports", nargs="+", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    report = merge(args.reports)
    args.output.with_suffix(".json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    args.output.write_text(markdown(report), encoding="utf-8")
    print(json.dumps({key: report[key] for key in ("commands", "summary")}))
    return int(bool(report["summary"].get("exception")))


if __name__ == "__main__":
    raise SystemExit(main())
