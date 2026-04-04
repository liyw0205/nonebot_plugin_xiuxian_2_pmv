"""
前尘往事 - 故事引擎
"""
import random
from .past_life_data import (
    STAGES, ENDINGS, POSITIVE_TALENTS, MIXED_TALENTS, NEGATIVE_TALENTS,
    BIRTH_SCENARIOS, REWARD_TABLE
)
from .past_life_limit import past_life_limit
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, UserBuffDate
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.utils import number_to
from ..xiuxian_config import convert_rank
from ..xiuxian_utils.data_source import jsondata

sql_message = XiuxianDateManage()
items = Items()

ATTR_NAMES = ["悟性", "机缘", "根骨", "气运", "心性"]

# 天赋类型标记（用于显示）
TALENT_TYPE_LABELS = {
    "positive": "✦",
    "mixed": "◈",
    "negative": "✧",
}


class PastLifeEngine:
    """前世今生·剧本杀引擎"""

    def _roll_talent(self, alloc: dict):
        """
        根据属性分配随机决定天赋
        返回: (talent_info, talent_type)
        """
        roll = random.random()

        if roll < 0.15:
            # 15% 负面天赋
            return random.choice(NEGATIVE_TALENTS), "negative"
        elif roll < 0.40:
            # 25% 混合天赋
            return random.choice(MIXED_TALENTS), "mixed"
        else:
            # 60% 正面天赋（根据最高属性从对应池中随机）
            max_attr = max(alloc, key=alloc.get)
            pool = POSITIVE_TALENTS.get(max_attr, POSITIVE_TALENTS["悟性"])
            return random.choice(pool), "positive"

    # ── 开始新人生 ──────────────────────────────────
    def start_new_life(self, user_id, alloc: dict):
        """
        分配属性后启动人生
        alloc: {"悟性":5,"机缘":4,"根骨":3,"气运":4,"心性":4}
        """
        # 随机天赋
        talent_info, talent_type = self._roll_talent(alloc)

        # 应用天赋效果（正面+负面统一处理）
        accumulated = {k: alloc.get(k, 0) for k in ATTR_NAMES}
        effects = talent_info.get("effects", {})
        for k, v in effects.items():
            if k in accumulated:
                accumulated[k] = max(accumulated[k] + v, 0)  # 不低于0

        # 随机为每幕选定一个事件
        event_indices = []
        for stage in STAGES:
            event_indices.append(random.randint(0, len(stage["events"]) - 1))

        state = past_life_limit.get_user_state(user_id)
        state.update({
            "state": 2,  # 等待选择
            "stage": 0,
            "alloc": alloc,
            "accumulated": accumulated,
            "talent": talent_info["name"],
            "total_score": 0,
            "event_indices": event_indices,
            "history": [],
        })
        past_life_limit.save_user_state(user_id, state)

        # 获取出生场景
        birth_list = BIRTH_SCENARIOS.get(
            talent_info["name"],
            BIRTH_SCENARIOS.get("_default", ["你降生于一个平凡的家庭。"])
        )
        birth_desc = random.choice(birth_list)

        # 返回第一幕信息
        stage_0 = STAGES[0]
        event = stage_0["events"][event_indices[0]]

        attrs_str = "  ".join(f"{k}:{accumulated[k]}" for k in ATTR_NAMES)
        label = TALENT_TYPE_LABELS.get(talent_type, "")

        # 构建天赋效果描述
        effects_parts = []
        for k, v in effects.items():
            if k in ATTR_NAMES:
                if v > 0:
                    effects_parts.append(f"{k}+{v}")
                elif v < 0:
                    effects_parts.append(f"{k}{v}")
        effects_str = " ".join(effects_parts) if effects_parts else "无"

        talent_prefix = ""
        if talent_type == "negative":
            talent_prefix = "⚠ "
        elif talent_type == "mixed":
            talent_prefix = "⚡ "

        msg = (
            f"═══  前尘往事  ═════\n"
            f"{talent_prefix}天赋觉醒：【{talent_info['name']}】\n"
            f"{talent_info['desc']}\n"
            f"天赋效果：{effects_str}\n"
            f"先天资质：{attrs_str}\n"
            f"═════════════\n"
            f"【第一幕·{stage_0['name']}】({stage_0['age']})\n"
            f"{birth_desc}\n\n"
            f"{event['text']}\n"
        )
        for i, c in enumerate(event["choices"], 1):
            msg += f"\n[{i}] {c['text']}"

        return {"message": msg, "choices_count": len(event["choices"])}

    # ── 处理选择 ────────────────────────────────────
    def process_choice(self, user_id, choice_idx: int):
        """
        处理玩家的选择 (choice_idx 从1开始)
        """
        state = past_life_limit.get_user_state(user_id)
        if state["state"] != 2:
            return {"message": "你当前没有进行中的前尘往事。", "is_end": False, "ending": None}

        current_stage = state["stage"]
        event_idx = state["event_indices"][current_stage]
        event = STAGES[current_stage]["events"][event_idx]

        # 校验选项
        if choice_idx < 1 or choice_idx > len(event["choices"]):
            return {"message": f"无效选项，请选择 1~{len(event['choices'])}。", "is_end": False, "ending": None}

        choice = event["choices"][choice_idx - 1]

        # 应用效果
        accumulated = state["accumulated"]
        if not isinstance(accumulated, dict):
            accumulated = {k: 0 for k in ATTR_NAMES}
        for k, v in choice.get("effects", {}).items():
            accumulated[k] = max(accumulated.get(k, 0) + v, 0)

        # 更新分数
        state["total_score"] = state.get("total_score", 0) + choice.get("score", 0)
        state["accumulated"] = accumulated

        # 记录历史
        history = state.get("history", [])
        history.append({
            "stage": current_stage,
            "stage_name": STAGES[current_stage]["name"],
            "event_text": event["text"][:20] + "...",
            "choice_text": choice["text"],
            "result": choice["result"],
        })
        state["history"] = history

        result_msg = f"你选择了【{choice['text']}】\n{choice['result']}\n"

        # 显示当前属性
        effects_str = "  ".join(
            f"{k}:{accumulated[k]}" for k in ATTR_NAMES
        )

        # 推进到下一幕
        next_stage = current_stage + 1

        if next_stage >= len(STAGES):
            # 所有幕数完成 → 计算结局
            state["stage"] = next_stage
            past_life_limit.save_user_state(user_id, state)
            ending = self._calculate_ending(state)
            rewards = self._calculate_rewards(user_id, ending, state)

            ending_msg = (
                f"{result_msg}\n"
                f"当前属性：{effects_str}\n"
                f"═════════════\n"
                f"📜 前世评分：{state['total_score']}分\n\n"
                f"🏆 结局：【{ending['name']}】\n"
                f"{ending['desc']}\n\n"
                f"═══  前世奖励  ═════\n"
                f"{rewards['msg']}"
            )

            past_life_limit.save_run_result(user_id, ending["name"], state["total_score"])

            return {"message": ending_msg, "is_end": True, "ending": ending, "rewards": rewards}
        else:
            # 进入下一幕
            state["stage"] = next_stage
            past_life_limit.save_user_state(user_id, state)

            next_event_idx = state["event_indices"][next_stage]
            next_event = STAGES[next_stage]["events"][next_event_idx]
            stage_info = STAGES[next_stage]

            next_msg = (
                f"{result_msg}\n"
                f"当前属性：{effects_str}\n"
                f"═════════════\n"
                f"【第{next_stage + 1}幕·{stage_info['name']}】({stage_info['age']})\n\n"
                f"{next_event['text']}\n"
            )
            for i, c in enumerate(next_event["choices"], 1):
                next_msg += f"\n[{i}] {c['text']}"

            return {"message": next_msg, "is_end": False, "ending": None}

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
    def _calculate_rewards(self, user_id, ending, state):
        tier = ending["tier"]
        reward = REWARD_TABLE.get(tier, REWARD_TABLE[5])

        user_info = sql_message.get_user_info_with_id(user_id)
        if not user_info:
            return {"msg": "无法获取用户信息", "details": {}}

        # 修为奖励
        user_rank = max(convert_rank(user_info["level"])[0] // 3, 1)
        exp_amount = int(user_info["exp"] * reward["exp_rate"] * min(0.1 * user_rank, 1))
        exp_amount = max(exp_amount, 10000)
        sql_message.update_exp(user_id, exp_amount)

        # 灵石奖励
        stone_amount = reward["stone"]
        sql_message.update_ls(user_id, stone_amount, 1)

        # 物品奖励
        item_msg = ""
        user_rank_val = convert_rank(user_info["level"])[0]
        min_rank = max(user_rank_val - 16 - reward["item_rank_offset"], 5)
        item_rank = random.randint(min_rank, min_rank + 20)
        item_types = ["功法", "神通", "药材"]
        item_type = random.choice(item_types)
        item_id_list = items.get_random_id_list_by_rank_and_item_type(item_rank, item_type)

        if item_id_list:
            item_id = random.choice(item_id_list)
            item_info = items.get_data_by_item_id(item_id)
            sql_message.send_back(user_id, item_id, item_info["name"], item_info["type"], 1)
            item_msg = f"\n物品：{item_info['level']}·{item_info['name']}"

        # 成就点
        points = reward["points"]

        msg = (
            f"修为 +{number_to(exp_amount)}\n"
            f"灵石 +{number_to(stone_amount)}\n"
            f"前世成就点 +{points}"
            f"{item_msg}\n"
            f"═════════════"
        )

        return {
            "msg": msg,
            "details": {
                "exp": exp_amount,
                "stone": stone_amount,
                "points": points,
                "tier": tier,
            }
        }

    # ── 获取当前状态描述 ────────────────────────────
    def get_current_display(self, user_id):
        """获取用户当前前世状态的展示信息"""
        state = past_life_limit.get_user_state(user_id)
        s = state.get("state", 0)

        if s == 0:
            cd = past_life_limit.get_cooldown_remaining(user_id)
            if cd > 0:
                hours = cd // 60
                mins = cd % 60
                return {
                    "message": f"前尘往事冷却中，今日已游历前世，明日再来。剩余{hours}小时{mins}分钟。",
                    "state": 0,
                }
            else:
                runs = state.get("total_runs", 0)
                best = state.get("best_ending", "无")
                best_score = state.get("best_score", 0)
                return {
                    "message": (
                        f"═══  前尘往事  ═════\n"
                        f"道友可开启一段前尘往事的回忆。\n"
                        f"累计前世：{runs}次\n"
                        f"最佳结局：{best}（{best_score}分）\n"
                        f"═════════════\n"
                        f"发送【投胎 悟性X 机缘X 根骨X 气运X 心性X】开始\n"
                        f"或发送【投胎 随机】随机分配\n"
                        f"（五项属性之和须等于20，每项0~10）"
                    ),
                    "state": 0,
                }

        elif s == 1:
            return {
                "message": (
                    f"请分配先天资质（共20点）：\n"
                    f"发送【投胎 悟性X 机缘X 根骨X 气运X 心性X】\n"
                    f"或发送【投胎 随机】随机分配\n"
                    f"（五项之和=20，每项0~10）"
                ),
                "state": 1,
            }

        elif s == 2:
            current_stage = state.get("stage", 0)
            if current_stage >= len(STAGES):
                return {"message": "前世已结束，请查看回忆。", "state": 0}

            event_idx = state["event_indices"][current_stage]
            event = STAGES[current_stage]["events"][event_idx]
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