"""
前尘往事 - 故事引擎
"""
import copy
import hashlib
import json
import random
from .past_life_data import (
    STAGES, ENDINGS, POSITIVE_TALENTS, MIXED_TALENTS, NEGATIVE_TALENTS,
    BIRTH_SCENARIOS, REWARD_TABLE, get_choice_branch, check_early_death
)
from .past_life_limit import past_life_limit
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, UserBuffDate
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.utils import number_to
from ..xiuxian_config import XiuConfig, convert_rank
from ..xiuxian_utils.data_source import jsondata
from ...paths import get_paths
from .choice_service import PastLifeChoiceService
from .final_settlement_service import PastLifeFinalSettlementService
from .start_service import PastLifeStartService

sql_message = XiuxianDateManage()
items = Items()
_paths = get_paths()
final_settlement_service = PastLifeFinalSettlementService(
    _paths.game_db, _paths.player_db, max_goods_num=XiuConfig().max_goods_num
)
start_service = PastLifeStartService(_paths.game_db, _paths.player_db)
choice_service = PastLifeChoiceService(_paths.game_db, _paths.player_db)

ATTR_NAMES = ["悟性", "机缘", "根骨", "气运", "心性"]
INITIAL_APTITUDE_MIN = 3
INITIAL_APTITUDE_MAX = 15
INITIAL_APTITUDE_TOTAL_MIN = INITIAL_APTITUDE_MIN * len(ATTR_NAMES)
INITIAL_APTITUDE_TOTAL_MAX = 20
SCORE_CHOICE_MAX = 50
SCORE_APTITUDE_MAX = 30
SCORE_STAGE_MAX = 20
SCORE_APTITUDE_RAW_CAP = 80

class PastLifeEngine:
    """前世今生·剧本杀引擎"""

    def _scale_score(self, raw_score: int, raw_cap: int, score_cap: int):
        raw_cap = max(int(raw_cap or 0), 1)
        score_cap = max(int(score_cap or 0), 0)
        raw_score = max(0, min(int(raw_score or 0), raw_cap))
        return min(score_cap, int(raw_score * score_cap / raw_cap + 0.5))

    def _calculate_choice_raw_cap(self):
        """按当前剧情池计算一轮可取得的最高抉择原始分。"""
        cap = 0
        for stage in STAGES:
            stage_cap = 0
            for event in stage.get("events", []):
                for choice in event.get("choices", []):
                    branch_scores = []
                    branches = choice.get("branches")
                    if isinstance(branches, dict):
                        branch_scores = [
                            int(branch.get("score", 0) or 0)
                            for branch in branches.values()
                            if isinstance(branch, dict)
                        ]
                    if not branch_scores:
                        branch_scores = [int(choice.get("score", 0) or 0)]
                    stage_cap = max(stage_cap, max(branch_scores))
            cap += stage_cap
        return max(cap, 1)

    def _get_stage_event(self, state: dict, stage_idx: int):
        """优先使用本轮开始时保存的事件快照，兼容旧存档回退到索引。"""
        snapshots = state.get("event_snapshots", [])
        if (
            isinstance(snapshots, list)
            and stage_idx < len(snapshots)
            and isinstance(snapshots[stage_idx], dict)
        ):
            return snapshots[stage_idx]

        event_indices = state.get("event_indices", [])
        if isinstance(event_indices, list) and stage_idx < len(event_indices):
            event_idx = event_indices[stage_idx]
        else:
            event_idx = 0
        return STAGES[stage_idx]["events"][event_idx]

    def _resolve_choice_effect(self, choice: dict, accumulated: dict):
        """从数据层选择当前资质对应的分支。"""
        branch_name, branch, _ = get_choice_branch(choice, accumulated)
        effects = copy.deepcopy(branch.get("effects", {}))
        score = int(branch.get("score", 0))
        result_text = branch.get("result", choice.get("result", ""))
        judge_msg = branch.get("judge", "")
        return (
            branch_name,
            effects,
            score,
            result_text,
            f"\n{judge_msg}" if judge_msg else "",
        )

    @staticmethod
    def _choice_rng(operation_id):
        digest = hashlib.sha256(str(operation_id).encode("utf-8")).digest()
        return random.Random(int.from_bytes(digest, "big"))

    @staticmethod
    def _choice_response(result, status):
        response = copy.deepcopy(result)
        response["operation_status"] = str(status)
        return response

    @staticmethod
    def _serializable_ending(ending):
        return {
            key: copy.deepcopy(value)
            for key, value in ending.items()
            if not callable(value)
        }

    def _calculate_score_breakdown(self, state: dict):
        """终局评分：抉择分 + 最终资质分 + 完成幕数分。"""
        choice_raw = int(state.get("total_score", 0) or 0)
        choice_raw_cap = self._calculate_choice_raw_cap()
        choice_score = self._scale_score(choice_raw, choice_raw_cap, SCORE_CHOICE_MAX)
        accumulated = state.get("accumulated", {})
        if not isinstance(accumulated, dict):
            accumulated = {}

        aptitude_raw = sum(
            max(0, int(accumulated.get(attr, 0) or 0))
            for attr in ATTR_NAMES
        )
        aptitude_score = self._scale_score(
            aptitude_raw, SCORE_APTITUDE_RAW_CAP, SCORE_APTITUDE_MAX
        )
        completed_stages = max(0, min(int(state.get("stage", 0) or 0), len(STAGES)))
        stage_score = self._scale_score(completed_stages, len(STAGES), SCORE_STAGE_MAX)
        total = choice_score + aptitude_score + stage_score
        return {
            "choice": choice_score,
            "choice_max": SCORE_CHOICE_MAX,
            "choice_raw": choice_raw,
            "choice_raw_cap": choice_raw_cap,
            "aptitude": aptitude_score,
            "aptitude_max": SCORE_APTITUDE_MAX,
            "aptitude_raw": aptitude_raw,
            "aptitude_raw_cap": SCORE_APTITUDE_RAW_CAP,
            "stage": stage_score,
            "stage_max": SCORE_STAGE_MAX,
            "completed_stages": completed_stages,
            "total_stages": len(STAGES),
            "total": min(total, 100),
        }

    def _finalize_total_score(self, state: dict):
        breakdown = self._calculate_score_breakdown(state)
        state["score_breakdown"] = breakdown
        state["total_score"] = breakdown["total"]
        return breakdown

    def _format_score_breakdown(self, breakdown: dict):
        return (
            f"评分构成：抉择{breakdown['choice']}/{breakdown['choice_max']} + "
            f"资质{breakdown['aptitude']}/{breakdown['aptitude_max']}"
            f"（最终资质{breakdown['aptitude_raw']}） + "
            f"幕数{breakdown['stage']}/{breakdown['stage_max']}"
            f"（{breakdown['completed_stages']}/{breakdown['total_stages']}幕）"
        )

    def _roll_talent(self, alloc: dict, rng=None):
        """
        根据属性分配随机决定天赋
        返回: (talent_info, talent_type)
        """
        rng = rng or random
        roll = rng.random()

        if roll < 0.15:
            # 15% 负面天赋
            return rng.choice(NEGATIVE_TALENTS), "negative"
        elif roll < 0.40:
            # 25% 混合天赋
            return rng.choice(MIXED_TALENTS), "mixed"
        else:
            # 60% 正面天赋（根据最高属性从对应池中随机）
            max_attr = max(alloc, key=alloc.get)
            pool = POSITIVE_TALENTS.get(max_attr, POSITIVE_TALENTS["悟性"])
            return rng.choice(pool), "positive"

    def _generate_initial_aptitude(self, rng):
        shuffled_attrs = rng.sample(ATTR_NAMES, len(ATTR_NAMES))
        remaining = rng.randint(
            INITIAL_APTITUDE_TOTAL_MIN, INITIAL_APTITUDE_TOTAL_MAX
        )
        values = {}
        for index, attr in enumerate(shuffled_attrs):
            slots_left = len(shuffled_attrs) - index - 1
            low = max(
                INITIAL_APTITUDE_MIN,
                remaining - INITIAL_APTITUDE_MAX * slots_left,
            )
            high = min(
                INITIAL_APTITUDE_MAX,
                remaining - INITIAL_APTITUDE_MIN * slots_left,
            )
            values[attr] = rng.randint(low, high)
            remaining -= values[attr]
        return {attr: values[attr] for attr in ATTR_NAMES}

    @staticmethod
    def _start_rng(operation_id):
        digest = hashlib.sha256(str(operation_id).encode("utf-8")).digest()
        return random.Random(int.from_bytes(digest, "big"))

    def _format_start_message(
        self,
        alloc,
        accumulated,
        talent_info,
        talent_type,
        birth_scenario,
        event,
    ):
        base_attrs_str = "  ".join(f"{key}:{alloc.get(key, 0)}" for key in ATTR_NAMES)
        current_attrs_str = "  ".join(
            f"{key}:{accumulated[key]}" for key in ATTR_NAMES
        )
        effects_parts = []
        for key, value in talent_info.get("effects", {}).items():
            if key not in ATTR_NAMES:
                continue
            if value > 0:
                effects_parts.append(f"{key}+{value}")
            elif value < 0:
                effects_parts.append(f"{key}{value}")
        effects_str = " ".join(effects_parts) if effects_parts else "无"
        talent_prefix = ""
        if talent_type == "negative":
            talent_prefix = "⚠ "
        elif talent_type == "mixed":
            talent_prefix = "⚡ "
        stage = STAGES[0]
        message = (
            f"【前尘往事】\n"
            f"{talent_prefix}天赋觉醒：【{talent_info['name']}】\n"
            f"{talent_info['desc']}\n"
            f"天赋效果：{effects_str}\n"
            f"先天资质：{base_attrs_str}\n"
            f"当前属性：{current_attrs_str}\n"
            f"【第一幕·{stage['name']}】({stage['age']})\n"
            f"{birth_scenario}\n\n"
            f"{event['text']}\n"
        )
        for index, choice in enumerate(event["choices"], 1):
            message += f"\n[{index}] {choice['text']}"
        return message

    # ── 开始新人生 ──────────────────────────────────
    def start_new_life(self, user_id, operation_id, now=None):
        """Freeze and atomically persist a new run for one command operation."""
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id is required")
        rng = self._start_rng(operation_id)
        alloc = self._generate_initial_aptitude(rng)
        talent_info, talent_type = self._roll_talent(alloc, rng)
        accumulated = {k: alloc.get(k, 0) for k in ATTR_NAMES}
        effects = talent_info.get("effects", {})
        for k, v in effects.items():
            if k in accumulated:
                accumulated[k] = max(accumulated[k] + v, 0)

        event_indices = []
        event_snapshots = []
        for stage in STAGES:
            event_idx = rng.randint(0, len(stage["events"]) - 1)
            event_indices.append(event_idx)
            event_snapshots.append(copy.deepcopy(stage["events"][event_idx]))

        birth_list = BIRTH_SCENARIOS.get(
            talent_info["name"],
            BIRTH_SCENARIOS.get("_default", ["你降生于一个平凡的家庭。"])
        )
        birth_scenario = rng.choice(birth_list)
        first_event = event_snapshots[0]
        message = self._format_start_message(
            alloc,
            accumulated,
            talent_info,
            talent_type,
            birth_scenario,
            first_event,
        )
        result = start_service.start(
            operation_id,
            user_id,
            past_life_limit.get_user_state(user_id),
            alloc=alloc,
            accumulated=accumulated,
            talent=talent_info["name"],
            birth_scenario=birth_scenario,
            event_indices=event_indices,
            event_snapshots=event_snapshots,
            first_stage_message=message,
            choices_count=len(first_event["choices"]),
            refresh_slot_start=past_life_limit.get_refresh_slot_start(now),
        )
        if result.succeeded:
            return {
                "status": result.status,
                "message": result.message,
                "choices_count": result.choices_count,
                "alloc": result.alloc,
                "state": 2,
            }

        if result.status == "cooldown":
            return {
                "status": result.status,
                "message": (
                    f"前尘往事尚未刷新，{past_life_limit.get_cooldown_text(user_id)}"
                ),
                "choices_count": 0,
                "alloc": {},
                "state": 0,
            }
        if result.status in {"already_started", "state_changed"}:
            display = self.get_current_display(user_id)
            if display["state"] == 2:
                display["message"] = "本轮前尘已开始，资质与事件已锁定。\n" + display["message"]
            return {
                "status": result.status,
                "message": display["message"],
                "choices_count": 0,
                "alloc": {},
                "state": display["state"],
            }
        return {
            "status": result.status,
            "message": "前尘投胎未能完成，请重新查看当前进度。",
            "choices_count": 0,
            "alloc": {},
            "state": -1,
        }

    # ── 处理选择 ────────────────────────────────────
    def process_choice(self, user_id, choice_idx: int, operation_id=None):
        """
        处理玩家的选择 (choice_idx 从1开始)
        """
        operation_id = str(operation_id or "").strip()
        if operation_id:
            replay = choice_service.get_result(operation_id, user_id)
            if replay is not None:
                if replay.succeeded:
                    return self._choice_response(replay.response, replay.status)
                return {
                    "message": "该前尘选择 operation 已被其他请求占用。",
                    "is_end": False,
                    "ending": None,
                    "operation_status": replay.status,
                }

        state = past_life_limit.get_user_state(user_id)
        if state["state"] != 2:
            return {
                "message": "你当前没有进行中的前尘往事。",
                "is_end": False,
                "ending": None,
                "operation_status": "not_active",
            }
        expected_state = copy.deepcopy(state)
        if not operation_id:
            fingerprint = hashlib.sha256(json.dumps(
                [str(user_id), expected_state, int(choice_idx)],
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")).hexdigest()[:24]
            operation_id = f"past-life-choice:{user_id}:{fingerprint}"

        current_stage = state["stage"]
        event = self._get_stage_event(state, current_stage)

        # 校验选项
        if choice_idx < 1 or choice_idx > len(event["choices"]):
            return {
                "message": f"无效选项，请选择 1~{len(event['choices'])}。",
                "is_end": False,
                "ending": None,
                "operation_status": "invalid_choice",
            }

        choice = event["choices"][choice_idx - 1]

        # 应用效果
        accumulated = state["accumulated"]
        if not isinstance(accumulated, dict):
            accumulated = {k: 0 for k in ATTR_NAMES}
        (
            branch_name,
            resolved_effects,
            branch_score,
            resolved_result,
            attr_result_msg,
        ) = self._resolve_choice_effect(choice, accumulated)
        raw_accumulated = {k: int(accumulated.get(k, 0)) for k in ATTR_NAMES}
        for k, v in resolved_effects.items():
            raw_accumulated[k] = int(accumulated.get(k, 0)) + int(v)
            accumulated[k] = max(raw_accumulated[k], 0)

        # 更新分数
        state["total_score"] = state.get("total_score", 0) + branch_score
        state["accumulated"] = accumulated
        early_death_rolls = state.get("early_death_rolls", {})
        if not isinstance(early_death_rolls, dict):
            early_death_rolls = {}
        early_death = check_early_death(
            current_stage,
            raw_accumulated,
            accumulated,
            event,
            early_death_rolls,
            self._choice_rng(operation_id),
        )
        state["early_death_rolls"] = early_death_rolls

        # 记录历史
        history = state.get("history", [])
        history.append({
            "stage": current_stage,
            "stage_name": STAGES[current_stage]["name"],
            "event_text": event["text"][:20] + "...",
            "choice_text": choice["text"],
            "result": f"{resolved_result}{attr_result_msg}",
            "branch": branch_name,
            "effects": resolved_effects,
            "score": branch_score,
            "early_death": bool(early_death),
        })
        state["history"] = history
        state["revision"] = int(expected_state.get("revision", 0) or 0) + 1

        result_msg = f"你选择了【{choice['text']}】\n{resolved_result}{attr_result_msg}\n"

        # 显示当前属性
        effects_str = "  ".join(
            f"{k}:{accumulated[k]}" for k in ATTR_NAMES
        )

        if early_death:
            state["stage"] = current_stage + 1
            score_breakdown = self._finalize_total_score(state)
            ending = early_death["ending"]
            if ending.get("partial_reward"):
                reward_rate = self._calculate_partial_reward_rate(state)
                reward_plan = self._prepare_rewards(
                    user_id,
                    ending,
                    reward_rate,
                    include_item=False,
                    rng=self._choice_rng(operation_id),
                )
            else:
                reward_plan = self._empty_rewards(ending)
            rewards = self._format_rewards(reward_plan, reward_plan)
            reward_msg = rewards["msg"] if ending.get("partial_reward") else "本世过早夭折，未能留下可继承的前世馈赠。"
            ending_msg = (
                f"{result_msg}\n"
                f"当前属性：{effects_str}\n"
                f"{early_death['message']}\n\n"
                f"📜 前世评分：{state['total_score']}分\n"
                f"{self._format_score_breakdown(score_breakdown)}\n\n"
                f"🏆 结局：【{ending['name']}】\n"
                f"{ending['desc']}\n\n"
                f"【前世奖励】\n"
                f"{reward_msg}"
            )
            response = {
                "message": ending_msg,
                "is_end": True,
                "ending": self._serializable_ending(ending),
                "rewards": rewards,
            }
            settlement = self._settle_final(
                operation_id,
                user_id,
                choice_idx,
                expected_state,
                state,
                ending,
                reward_plan,
                response,
            )
            if not settlement.succeeded:
                return {
                    "message": "前尘状态已变化，请重新查看当前进度。",
                    "is_end": False,
                    "ending": None,
                    "operation_status": settlement.status,
                }
            if settlement.status == "duplicate":
                replay = choice_service.get_result(operation_id, user_id)
                if replay is not None and replay.succeeded:
                    return self._choice_response(replay.response, replay.status)
            return self._choice_response(response, settlement.status)

        # 推进到下一幕
        next_stage = current_stage + 1

        if next_stage >= len(STAGES):
            # 所有幕数完成 → 计算结局
            state["stage"] = next_stage
            score_breakdown = self._finalize_total_score(state)
            ending = self._calculate_ending(state)
            reward_plan = self._prepare_rewards(
                user_id, ending, rng=self._choice_rng(operation_id)
            )
            rewards = self._format_rewards(reward_plan, reward_plan)
            ending_msg = (
                f"{result_msg}\n"
                f"当前属性：{effects_str}\n"
                f"📜 前世评分：{state['total_score']}分\n"
                f"{self._format_score_breakdown(score_breakdown)}\n\n"
                f"🏆 结局：【{ending['name']}】\n"
                f"{ending['desc']}\n\n"
                f"【前世奖励】\n"
                f"{rewards['msg']}"
            )
            response = {
                "message": ending_msg,
                "is_end": True,
                "ending": self._serializable_ending(ending),
                "rewards": rewards,
            }
            settlement = self._settle_final(
                operation_id,
                user_id,
                choice_idx,
                expected_state,
                state,
                ending,
                reward_plan,
                response,
            )
            if not settlement.succeeded:
                return {
                    "message": "前尘状态已变化，请重新查看当前进度。",
                    "is_end": False,
                    "ending": None,
                    "operation_status": settlement.status,
                }
            if settlement.status == "duplicate":
                replay = choice_service.get_result(operation_id, user_id)
                if replay is not None and replay.succeeded:
                    return self._choice_response(replay.response, replay.status)
            return self._choice_response(response, settlement.status)
        else:
            state["stage"] = next_stage
            next_event = self._get_stage_event(state, next_stage)
            stage_info = STAGES[next_stage]

            next_msg = (
                f"{result_msg}\n"
                f"当前属性：{effects_str}\n"
                f"【第{next_stage + 1}幕·{stage_info['name']}】({stage_info['age']})\n\n"
                f"{next_event['text']}\n"
            )
            for i, c in enumerate(next_event["choices"], 1):
                next_msg += f"\n[{i}] {c['text']}"
            response = {"message": next_msg, "is_end": False, "ending": None}
            settlement = choice_service.advance(
                operation_id,
                user_id,
                choice_idx,
                expected_state,
                state,
                response,
            )
            if not settlement.succeeded:
                return {
                    "message": "前尘状态已变化，请重新查看当前进度。",
                    "is_end": False,
                    "ending": None,
                    "operation_status": settlement.status,
                }
            return self._choice_response(settlement.response, settlement.status)

    # ── 计算结局 ────────────────────────────────────
    def _calculate_ending(self, state):
        accumulated = state.get("accumulated", {})
        if not isinstance(accumulated, dict):
            accumulated = {}
        total = sum(accumulated.get(k, 0) for k in ATTR_NAMES)
        stats = {"total": total}
        stats.update(accumulated)

        for ending in ENDINGS:
            try:
                if ending["check"](stats):
                    return ending
            except Exception:
                continue

        return ENDINGS[-1]

    # ── 计算奖励 ────────────────────────────────────
    def _calculate_partial_reward_rate(self, state):
        completed_stages = int(state.get("stage", 0))
        rate = completed_stages / len(STAGES)
        return max(0.1, min(rate, 0.9))

    def _empty_rewards(self, ending):
        return {
            "exp": 0, "stone": 0, "points": 0, "tier": ending["tier"],
            "reward_rate": 0.0, "item": None,
        }

    def _prepare_rewards(
        self, user_id, ending, reward_rate=1.0, include_item=True, rng=None
    ):
        reward_rate = max(0.0, min(float(reward_rate), 1.0))
        rng = rng or random
        tier = ending["tier"]
        reward = REWARD_TABLE.get(tier, REWARD_TABLE[5])

        user_info = sql_message.get_user_info_with_id(user_id)
        if not user_info:
            raise ValueError("past life user missing")

        # 修为奖励
        user_rank = max(convert_rank(user_info["level"])[0] // 3, 1)
        exp_base = int(user_info["exp"] * reward["exp_rate"] * min(0.1 * user_rank, 1))
        exp_amount = int(exp_base * reward_rate)
        if reward_rate >= 1:
            exp_amount = max(exp_amount, 10000)
        elif reward_rate > 0:
            exp_amount = max(exp_amount, max(1000, int(10000 * reward_rate)))
        stone_amount = int(reward["stone"] * reward_rate)

        item_reward = None
        if include_item:
            user_rank_val = convert_rank(user_info["level"])[0]
            min_rank = max(user_rank_val - 16 - reward["item_rank_offset"], 5)
            item_rank = rng.randint(min_rank, min_rank + 20)
            item_types = ["功法", "神通", "药材"]
            item_type = rng.choice(item_types)
            item_id_list = items.get_random_id_list_by_rank_and_item_type(item_rank, item_type)

            if item_id_list:
                item_id = rng.choice(item_id_list)
                item_info = items.get_data_by_item_id(item_id)
                item_reward = {
                    "id": item_id, "name": item_info["name"], "type": item_info["type"],
                    "level": item_info["level"], "num": 1,
                }

        # 成就点
        points = int(reward["points"] * reward_rate)
        if reward_rate > 0:
            points = max(points, 1)

        return {
            "exp": exp_amount, "stone": stone_amount, "points": points, "tier": tier,
            "reward_rate": reward_rate, "item": item_reward,
        }

    def _format_rewards(self, plan, applied):
        item = applied.get("item") or plan.get("item")
        item_msg = f"\n物品：{plan['item']['level']}·{item['name']}" if item and plan.get("item") else ""
        reward_rate = float(plan.get("reward_rate", 0))
        partial_msg = ""
        if reward_rate < 1:
            partial_msg = f"提前终局，仅结算{int(reward_rate * 100)}%前世奖励\n"

        msg = (
            f"{partial_msg}"
            f"修为 +{number_to(applied.get('exp', 0))}\n"
            f"灵石 +{number_to(applied.get('stone', 0))}\n"
            f"前世成就点 +{applied.get('points', 0)}"
            f"{item_msg}"
        )

        return {
            "msg": msg,
            "details": {
                "exp": applied.get("exp", 0),
                "stone": applied.get("stone", 0),
                "points": applied.get("points", 0),
                "tier": plan["tier"],
                "reward_rate": reward_rate,
            }
        }

    def _settle_final(
        self,
        operation_id,
        user_id,
        choice_idx,
        expected_state,
        state,
        ending,
        plan,
        response,
    ):
        if not operation_id:
            fingerprint = hashlib.sha256(json.dumps(
                [str(user_id), expected_state, int(choice_idx)], ensure_ascii=False,
                sort_keys=True, separators=(",", ":")
            ).encode("utf-8")).hexdigest()[:24]
            operation_id = f"past-life-choice:{user_id}:{fingerprint}"
        return final_settlement_service.settle(
            operation_id, user_id, expected_state, state, ending["name"], state["total_score"],
            plan["exp"], plan["stone"], plan["points"], plan.get("item"),
            choice_response=response,
        )

    # ── 获取当前状态描述 ────────────────────────────
    def get_current_display(self, user_id):
        """获取用户当前前世状态的展示信息"""
        state = past_life_limit.get_user_state(user_id)
        s = state.get("state", 0)

        if s == 0:
            cd = past_life_limit.get_cooldown_remaining(user_id)
            if cd > 0:
                return {
                    "message": f"前尘往事尚未刷新，{past_life_limit.get_cooldown_text(user_id)}",
                    "state": 0,
                }
            else:
                runs = state.get("total_runs", 0)
                best = state.get("best_ending", "无")
                best_score = state.get("best_score", 0)
                return {
                    "message": (
                        f"【前尘往事】\n"
                        f"道友可开启一段前尘往事的回忆。\n"
                        f"累计前世：{runs}次\n"
                        f"最佳结局：{best}（{best_score}分）\n"
                        f"发送【投胎】开始\n"
                        f"投胎后先天资质即刻定下\n"
                        f"初始资质总和15~20随机，单项不低于3，也可能偏科极高"
                    ),
                    "state": 0,
                }

        elif s == 1:
            return {
                "message": (
                    f"发送【投胎】开始前尘往事。\n"
                    f"投胎后先天资质即刻定下。\n"
                    f"初始资质总和15~20随机，单项不低于3，也可能偏科极高。"
                ),
                "state": 1,
            }

        elif s == 2:
            current_stage = state.get("stage", 0)
            if current_stage >= len(STAGES):
                return {"message": "前世已结束，请查看回忆。", "state": 0}

            event = self._get_stage_event(state, current_stage)
            stage_info = STAGES[current_stage]

            accumulated = state.get("accumulated", {})
            if not isinstance(accumulated, dict):
                accumulated = {}
            attrs_str = "  ".join(f"{k}:{accumulated.get(k, 0)}" for k in ATTR_NAMES)

            talent = state.get("talent", "")

            msg = (
                f"【第{current_stage + 1}幕·{stage_info['name']}】({stage_info['age']})\n"
                f"天赋：{talent}\n"
                f"当前属性：{attrs_str}\n"
                f"当前评分：{state.get('total_score', 0)}分\n\n"
                f"{event['text']}\n"
            )
            for i, c in enumerate(event["choices"], 1):
                msg += f"\n[{i}] {c['text']}"

            return {"message": msg, "state": 2}

        return {"message": "状态异常，请联系管理员。", "state": -1}


past_life_engine = PastLifeEngine()
