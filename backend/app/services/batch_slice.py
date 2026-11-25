"""batch_slice 领域服务：批次分片管理与数据切割。

业务逻辑：
1. 批次分片配置：源表→分片键→起始/结束值→并行线程规划
2. 数据切割链路：接收 stream_source 元信息 → 按分片键切割 → 输出数据行到下游
3. 失败场景：分片起始>结束值冲突、脏行清洗、重复批次拒绝、写入中断
4. 与上下游流水线：消费 stream_source 元信息；为 merge_plan 提供批式来源数据

跨模块数据流：
  stream_source → batch_slice（消费流式源元信息，进行批式切割）
  batch_slice → merge_plan（提供批式来源数据，参与流批合并）
  batch_slice → watermark（记录批次处理水位）
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import norm_num, store


class BatchSliceService(BaseModuleService):
    MODULE = "batch_slice"
    ENTITY = "分片批次"
    OPERATION_TERM = "读取"
    LIST_FIELDS = ["批次编号", "源表名称", "分片键列", "分片起始值", "分片结束值", "预计行数", "并行线程", "完成时间"]
    REQUIRED_FIELDS = ["批次编号", "源表名称", "分片键列"]
    STATUS_ORDER = ["待执行", "执行中", "已完成", "已跳过"]
    ACTION_RULES = {"发起执行": "执行中", "标记完成": "已完成", "跳过批次": "已跳过"}
    NEGATIVE_ACTIONS = []

    # 跨模块流水线
    PIPELINE = "batch_slicing"
    UPSTREAM_MODULES = ["stream_source"]
    DOWNSTREAM_MODULES = ["merge_plan", "watermark"]
    PRODUCES_ARTIFACT = "批次分片数据行"

    # 已处理的batch_id注册表（防重复）
    _processed_batches: set[str] = set()

    def execute_slice(self, entry_id: int) -> dict[str, Any]:
        """执行分片切割：从源表生成模拟数据行。

        失败场景：
        - 起始值 >= 结束值 → 分片参数无效
        - 批次状态非待执行 → 状态冲突
        - 重复批次 → 拒绝执行
        - 写入中断模拟 → 部分数据丢失
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"批次 {entry_id} 不存在"}
        if entry.get("status") != "待执行":
            return {"ok": False, "reason": f"批次 {entry_id} 状态为 {entry.get('status')}，非待执行"}

        batch_no = str(entry.get("批次编号", ""))
        if batch_no in self._processed_batches:
            return {"ok": False, "reason": f"重复批次 {batch_no}，已处理过"}

        start_str = str(entry.get("分片起始值", "0"))
        end_str = str(entry.get("分片结束值", "0"))
        try:
            start_val = float(start_str)
            end_val = float(end_str)
        except ValueError:
            start_val = 0.0
            end_val = 100.0

        if start_val >= end_val:
            error_msg = f"分片参数无效: 起始值 {start_val} >= 结束值 {end_val}"
            entry["abnormal"] = True
            entry["status"] = "待执行"
            return {"ok": False, "reason": error_msg}

        rows_count = int(end_val - start_val)
        if rows_count > 100000:
            return {"ok": False, "reason": f"分片范围过大({rows_count}行)，建议缩小"}

        # 生成模拟数据行
        data_rows = []
        dirty_rows = []
        for i in range(rows_count):
            val = start_val + i
            if i % 7 == 0 and i > 0:
                # 模拟脏行：空值或异常值
                dirty_rows.append({"row_index": i, "value": val, "issue": "空值分片键"})
                continue
            data_rows.append({
                "batch": batch_no,
                "source_table": entry.get("源表名称"),
                "slice_key": str(entry.get("分片键列")),
                "value": val,
                "ts": time.time() - i * 0.1,
                "batch_id": batch_no,
            })

        # 模拟写入中断（每第5个批次）
        if entry_id % 5 == 0 and data_rows:
            # 保留一半数据，模拟中断
            data_rows = data_rows[:len(data_rows) // 2]
            self._interrupted_write = True

        # 通过流水线写入数据产物
        batch_id = f"{batch_no}_{int(time.time())}"
        self.produce_artifact(data_rows, batch_id)

        self._processed_batches.add(batch_no)
        entry["status"] = "执行中"
        entry["pending"] = True

        return {
            "ok": True,
            "batch": batch_no,
            "rows_ingested": len(data_rows),
            "dirty_rows": len(dirty_rows),
            "total_range": rows_count,
            "pipeline": self.PIPELINE,
        }

    def consume_stream_sources(self) -> dict[str, Any]:
        """消费上游 stream_source 模块的已启用源清单，确定批次切割策略"""
        sources = store.rows("stream_source")
        enabled = [s for s in sources if s.get("status") == "已启用"]
        source_topics = [s.get("Topic名称") for s in enabled if s.get("Topic名称")]
        return {
            "enabled_stream_sources": len(enabled),
            "topics": source_topics,
            "mapped_batches": [r.get("批次编号") for r in store.rows(self.MODULE) if r.get("status") == "待执行"],
        }

    # ---- 批量登记：基于已启用流式源清单，一次提交多段区间 ----
    def batch_register(
        self,
        source_id: int,
        source_table: str,
        key_column: str,
        intervals: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """基于已启用流式源清单的分片批量登记。

        - 来源必须在已启用清单中；停用来源不能再登记新切割
        - 以（源表名称, 分片键列）确定范围，多段区间一次提交
        - 逐条给出登记编号或冲突范围；相邻区间允许拼接
        - 同编号同范围重投返回原结果；同编号不同范围按冲突拒绝
        - 并发提交由 store.coverage_lock 串行化，只确认无重叠的结果
        - 确认结果同步到合并覆盖清单，供 merge_plan 采用
        """
        source = store.find("stream_source", int(source_id))
        if source is None:
            return {"ok": False, "reason": f"流式源 {source_id} 不存在"}
        status = source.get("status")
        if status == "已停用":
            return {"ok": False, "reason": f"来源 {source.get('源标识')} 已停用，不能再登记新切割"}
        if status != "已启用":
            return {
                "ok": False,
                "reason": f"来源 {source.get('源标识')} 未启用（当前状态 {status}），不在已启用清单中",
            }
        source_table = str(source_table or "").strip()
        key_column = str(key_column or "").strip()
        if not source_table or not key_column:
            return {"ok": False, "reason": "源表名称和分片键列不能为空"}
        if not intervals:
            return {"ok": False, "reason": "至少提交一段区间"}

        # 预校验：逐段规范化，非法段直接给出原因，合法段进入原子提交
        slots: list[dict[str, Any] | None] = [None] * len(intervals)
        prepared: list[tuple[int, dict[str, Any]]] = []
        for idx, raw in enumerate(intervals):
            batch_no = str(raw.get("批次编号", "") or "").strip()
            label = batch_no or f"第{idx + 1}段"
            if not batch_no:
                slots[idx] = {"ok": False, "批次编号": label, "reason": "批次编号不能为空"}
                continue
            try:
                start = float(raw.get("分片起始值"))
                end = float(raw.get("分片结束值"))
            except (TypeError, ValueError):
                slots[idx] = {"ok": False, "批次编号": batch_no, "reason": "分片起始/结束值不是数值"}
                continue
            if start >= end:
                slots[idx] = {
                    "ok": False,
                    "批次编号": batch_no,
                    "起始值": norm_num(start),
                    "结束值": norm_num(end),
                    "reason": f"起始值 {norm_num(start)} 必须小于结束值 {norm_num(end)}",
                }
                continue
            prepared.append((idx, {"批次编号": batch_no, "起始值": start, "结束值": end}))

        # 原子提交：并发批量登记在此串行，只确认无重叠的区间
        with store.coverage_lock:
            committed = store.commit_coverage_intervals(
                source_table,
                key_column,
                str(source.get("源标识")),
                [item for _, item in prepared],
            )
            for (idx, _), result in zip(prepared, committed):
                if result["ok"] and not result.get("重投"):
                    # 同步登记一条分片批次条目，并把条目 id 回写进覆盖清单
                    entry = self._create_slice_entry(source_table, key_column, result)
                    result["entry_id"] = entry["id"]
                    record = store.find_coverage_record(source_table, key_column, result["编号"])
                    if record is not None:
                        record["entry_id"] = entry["id"]
                slots[idx] = result

        results = [slot for slot in slots if slot is not None]
        confirmed = [r for r in results if r["ok"]]
        view = store.coverage_view(source_table, key_column)
        return {
            "ok": True,
            "来源": source.get("源标识"),
            "源表名称": source_table,
            "分片键列": key_column,
            "确认数": len(confirmed),
            "冲突数": len(results) - len(confirmed),
            "结果": results,
            "连续覆盖": [
                {"起始值": norm_num(s), "结束值": norm_num(e)} for s, e in view["segments"]
            ],
        }

    def _create_slice_entry(
        self, source_table: str, key_column: str, result: dict[str, Any]
    ) -> dict[str, Any]:
        """为确认的分片区间登记一条批次分片条目。"""
        rows = store.rows(self.MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({
            "批次编号": result["批次编号"],
            "源表名称": source_table,
            "分片键列": key_column,
            "分片起始值": str(norm_num(result["起始值"])),
            "分片结束值": str(norm_num(result["结束值"])),
            "预计行数": str(int(float(result["结束值"]) - float(result["起始值"]))),
            "并行线程": "4",
            "完成时间": "",
        })
        entry["status"] = self.STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["stream_sources_consumed"] = self.consume_stream_sources()
        base["processed_batches"] = len(self._processed_batches)
        return base