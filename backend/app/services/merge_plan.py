"""merge_plan 领域服务：流批合并策略。

业务逻辑：
1. 流批合并：流式来源 + 批式来源 按合并键做去重/冲突解决
2. 合并链路：stream_lookup结果 + batch_slice数据 → merge_plan去重
3. 失败场景：冲突版本、合并窗口超限、去重策略冲突
4. 与上下游流水线：消费 stream_lookup 和 batch_slice；输出合并后数据

跨模块数据流：
  batch_slice + stream_lookup → merge_plan（流批合并）
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import norm_num, range_gaps, readable_span, store


class MergePlanService(BaseModuleService):
    MODULE = "merge_plan"
    ENTITY = "合并计划"
    OPERATION_TERM = "去重"
    LIST_FIELDS = ["计划编号", "流式来源", "批式来源", "合并键列", "冲突解决", "输出路径", "合并窗口", "去重策略"]
    REQUIRED_FIELDS = ["计划编号", "流式来源", "批式来源"]
    STATUS_ORDER = ["草稿", "待验证", "运行中", "已完成"]
    ACTION_RULES = {"提交验证": "待验证", "启动合并": "运行中", "标记完成": "已完成"}
    NEGATIVE_ACTIONS = []

    PIPELINE = "stream_batch_merge"
    UPSTREAM_MODULES = ["batch_slice", "stream_lookup"]
    PRODUCES_ARTIFACT = "流批合并记录"

    # 已处理的键值对版本追踪（防冲突）
    _merge_versions: dict[str, str] = {}

    def execute_merge(self, entry_id: int) -> dict[str, Any]:
        """执行流批合并：消费上游 batch_slice 和 stream_lookup 的数据产物。

        失败场景：
        - 冲突版本：同一key有多个版本
        - 去重策略引冲突
        - 合并窗口二时效
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"计划 {entry_id} 不存在"}
        if entry.get("status") != "运行中":
            return {"ok": False, "reason": f"计划未运行，当前状态: {entry.get('status')}"}

        merge_key = str(entry.get("合并键列", "id"))
        conflict_policy = str(entry.get("冲突解决", "stream_win"))
        dedup_policy = str(entry.get("去重策略", "unique"))

        # 消费 batch_slice 产物
        batch_pl = store.pipeline("batch_slicing")
        batch_rows = batch_pl._artifacts[:30] if batch_pl else []
        # 消费 stream_lookup 产物
        lookup_pl = store.pipeline("stream_lookup_join")
        stream_rows = lookup_pl._artifacts[:30] if lookup_pl else []

        # 合并去重逻辑
        merged: dict[Any, dict[str, Any]] = {}
        conflicts = []

        for row in batch_rows:
            key = row.get(merge_key)
            if key is None:
                continue
            if key in merged:
                conflicts.append({"key": key, "existing": merged[key], "new": row, "type": "batch_batch_conflict"})
                if conflict_policy == "stream_win":
                    continue
            merged[key] = row

        for row in stream_rows:
            key = row.get(merge_key)
            if key is None:
                continue
            if key in merged:
                conflicts.append({"key": key, "existing": merged[key], "new": row, "type": "stream_batch_conflict"})
                if conflict_policy == "stream_win":
                    merged[key] = row  # 流式优先覆盖
            else:
                merged[key] = row

        result = list(merged.values())

        # 去重：根据去重策略
        if dedup_policy == "unique":
            seen = set()
            deduped = []
            for r in result:
                k = r.get(merge_key)
                if k not in seen:
                    seen.add(k)
                    deduped.append(r)
            result = deduped

        batch_id = f"MERGE_{entry_id}_{int(time.time())}"
        self.produce_artifact(result, batch_id)

        return {
            "ok": True,
            "entry_id": entry_id,
            "batch_rows_consumed": len(batch_rows),
            "stream_rows_consumed": len(stream_rows),
            "merged_result": len(result),
            "conflicts": len(conflicts),
            "conflict_samples": conflicts[:3],
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["active_plans"] = sum(1 for r in store.rows(self.MODULE) if r.get("status") == "运行中")
        return base

    # ---- 合并覆盖清单：消费 batch_slice 批量登记的区间覆盖 ----
    def coverage_manifest(
        self,
        source_table: str,
        key_column: str,
        sel_start: float | None = None,
        sel_end: float | None = None,
    ) -> dict[str, Any]:
        """合并覆盖清单视图：已登记区间、连续段、选定范围缺口、合并可读范围与采用日志。"""
        view = store.coverage_view(source_table, key_column)
        manifest: dict[str, Any] = {
            "ok": True,
            "源表名称": source_table,
            "分片键列": key_column,
            "已登记区间": [
                {
                    "编号": c["编号"],
                    "批次编号": c["批次编号"],
                    "起始值": c["起始值"],
                    "结束值": c["结束值"],
                    "来源": c.get("来源"),
                    "entry_id": c.get("entry_id"),
                }
                for c in view["intervals"]
            ],
            "连续覆盖": [
                {"起始值": norm_num(s), "结束值": norm_num(e)} for s, e in view["segments"]
            ],
            "采用日志": view["adoptions"],
        }
        if sel_start is not None and sel_end is not None:
            manifest.update(self._coverage_check(view["segments"], sel_start, sel_end))
        return manifest

    def adopt_coverage(
        self, plan_id: int, sel_start: float, sel_end: float
    ) -> dict[str, Any]:
        """合并计划采用覆盖清单：只在选定范围没有未解释缺口时才采用。

        采用结果（含失败原因）写入覆盖清单的采用日志，供事后核对。
        """
        plan = store.find(self.MODULE, plan_id)
        if plan is None:
            return {"ok": False, "reason": f"计划 {plan_id} 不存在"}
        source_table = str(plan.get("批式来源", "") or "").strip()
        key_column = str(plan.get("合并键列", "") or "").strip()
        if not source_table or not key_column:
            return {"ok": False, "reason": "计划缺少批式来源或合并键列，无法定位覆盖清单"}
        if float(sel_start) >= float(sel_end):
            return {"ok": False, "reason": f"选择范围无效: 起始值 {norm_num(sel_start)} >= 结束值 {norm_num(sel_end)}"}

        view = store.coverage_view(source_table, key_column)
        check = self._coverage_check(view["segments"], sel_start, sel_end)
        record = {
            "计划编号": plan.get("计划编号"),
            "ok": check["可整体采用"],
            "reason": "" if check["可整体采用"] else self._gap_reason(check["缺口"]),
            **check,
        }
        store.record_coverage_adoption(source_table, key_column, record)
        result = {
            "ok": check["可整体采用"],
            "计划编号": plan.get("计划编号"),
            "源表名称": source_table,
            "分片键列": key_column,
            **check,
        }
        if not check["可整体采用"]:
            result["reason"] = record["reason"]
        return result

    @staticmethod
    def _coverage_check(
        segments: list[tuple[float, float]], sel_start: float, sel_end: float
    ) -> dict[str, Any]:
        """核对选定范围：缺口、合并开始可读的范围、是否可整体采用。"""
        start = float(sel_start)
        end = float(sel_end)
        gaps = range_gaps(segments, start, end)
        readable = readable_span(segments, start, end)
        return {
            "选择范围": {"起始值": norm_num(start), "结束值": norm_num(end)},
            "缺口": [{"起始值": norm_num(s), "结束值": norm_num(e)} for s, e in gaps],
            "可读范围": (
                {"起始值": norm_num(readable[0]), "结束值": norm_num(readable[1])}
                if readable is not None
                else None
            ),
            "可整体采用": not gaps,
        }

    @staticmethod
    def _gap_reason(gaps: list[dict[str, Any]]) -> str:
        parts = "、".join(f"[{gap['起始值']},{gap['结束值']})" for gap in gaps)
        return f"选定范围存在未解释缺口: {parts}"