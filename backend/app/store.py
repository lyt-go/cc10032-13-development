"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据，并支持跨模块数据流水线。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。
"""
from __future__ import annotations

import hashlib
import threading
import time
from typing import Any

from app.seed import SEED_ROWS


def norm_num(value: Any) -> int | float:
    """把分片边界规范成 int/float：整数值显示为 int，避免 10.0 这种噪音。"""
    num = float(value)
    return int(num) if num.is_integer() else num


def merge_segments(ranges: list[tuple[float, float]]) -> list[tuple[float, float]]:
    """把 half-open 区间列表合并成连续段：重叠或首尾相接（相邻）都拼成一段。"""
    segments: list[list[float]] = []
    for start, end in sorted((float(s), float(e)) for s, e in ranges):
        if segments and start <= segments[-1][1]:
            segments[-1][1] = max(segments[-1][1], end)
        else:
            segments.append([start, end])
    return [(s, e) for s, e in segments]


def range_gaps(
    segments: list[tuple[float, float]], start: float, end: float
) -> list[tuple[float, float]]:
    """计算连续段在选定范围 [start, end) 内留下的缺口。"""
    gaps: list[tuple[float, float]] = []
    cursor = float(start)
    for seg_start, seg_end in segments:
        if seg_end <= cursor or seg_start >= end:
            continue
        clipped_start = max(seg_start, cursor)
        if clipped_start > cursor:
            gaps.append((cursor, clipped_start))
        cursor = max(cursor, min(seg_end, float(end)))
    if cursor < end:
        gaps.append((cursor, float(end)))
    return gaps


def readable_span(
    segments: list[tuple[float, float]], start: float, end: float
) -> tuple[float, float] | None:
    """合并开始可读的范围：从 start 起连续被覆盖、且不超出 [start, end) 的最长区间。"""
    cursor = float(start)
    for seg_start, seg_end in segments:
        if seg_end <= cursor:
            continue
        if seg_start > cursor:
            break
        cursor = max(cursor, min(seg_end, float(end)))
        if cursor >= end:
            break
    if cursor <= float(start):
        return None
    return (float(start), cursor)


class DataPipeline:
    """跨模块数据流水线引擎：支持 输入→转换→版本化产物→下游消费 的完整链路。

    每条流水线定义了源模块、目标模块、转换函数和验证规则。
    产物存储为可版本化的数据快照，支持脏行检测、重复批次拒绝、冲突版本检测、中断恢复。
    """

    def __init__(self, name: str, sources: list[str], target_module: str) -> None:
        self.name = name
        self.sources = sources
        self.target_module = target_module
        self._artifacts: list[dict[str, Any]] = []
        self._versions: list[str] = []  # 版本号列表
        self._dirty_log: list[dict[str, Any]] = []
        self._batch_registry: set[str] = set()  # 防重复批次
        self._last_checkpoint: dict[str, Any] = {}  # 中断恢复点
        self._interrupted = False

    def ingest(self, rows: list[dict[str, Any]], batch_id: str) -> tuple[int, list[str]]:
        """摄入数据行，返回 (成功行数, 脏行详情列表)"""
        if batch_id in self._batch_registry:
            return 0, [f"重复批次 {batch_id} 已存在，拒绝摄入"]
        self._batch_registry.add(batch_id)
        clean_rows = []
        dirty_details = []
        for i, row in enumerate(rows):
            issues = self._validate_row(row, i)
            if issues:
                dirty_details.append(f"行 {i}: {'; '.join(issues)}")
                self._dirty_log.append({"batch": batch_id, "row_index": i, "issues": issues, "row": row, "ts": time.time()})
            else:
                clean_rows.append(row)
        self._artifacts.extend(clean_rows)
        return len(clean_rows), dirty_details

    def transform(self, transform_fn, *args, **kwargs) -> list[dict[str, Any]]:
        """对当前产物执行转换函数"""
        self._save_checkpoint("transform_enter", len(self._artifacts))
        try:
            result = transform_fn(self._artifacts, *args, **kwargs)
            version = self._make_version("transform")
            self._versions.append(version)
            self._save_checkpoint("transform_exit", len(result))
            return result
        except Exception:
            self._interrupted = True
            raise

    def versioned_snapshot(self) -> dict[str, Any]:
        """生成版本化产物快照"""
        version = self._versions[-1] if self._versions else self._make_version("snapshot")
        return {
            "pipeline": self.name,
            "version": version,
            "artifact_count": len(self._artifacts),
            "artifact_sample": self._artifacts[:5],
            "dirty_count": len(self._dirty_log),
            "batch_count": len(self._batch_registry),
            "interrupted": self._interrupted,
            "checkpoint": self._last_checkpoint,
        }

    def health_report(self) -> dict[str, Any]:
        """流水线健康报告：用于外部模块消费和容量材料生成"""
        return {
            "pipeline": self.name,
            "source_modules": self.sources,
            "target_module": self.target_module,
            "total_ingested": sum(1 for a in self._artifacts),
            "dirty_rows": len(self._dirty_log),
            "versions": len(self._versions),
            "batches_processed": len(self._batch_registry),
            "interrupted": self._interrupted,
            "last_artifact_ts": max((a.get("ts", 0) for a in self._artifacts), default=0) if self._artifacts else 0,
        }

    def _validate_row(self, row: dict[str, Any], index: int) -> list[str]:
        issues = []
        for key, val in row.items():
            if val is None or (isinstance(val, str) and not val.strip()):
                issues.append(f"空值字段 '{key}'")
        if any(row.get(k) is None for k in ["id"]):
            issues.append("缺少主键 id")
        return issues

    def _make_version(self, tag: str) -> str:
        ts = str(int(time.time() * 1000))
        raw = f"{self.name}:{tag}:{ts}"
        return hashlib.md5(raw.encode()).hexdigest()[:12]

    def _save_checkpoint(self, stage: str, count: int) -> None:
        self._last_checkpoint = {"stage": stage, "count": count, "ts": time.time()}

    def recover_checkpoint(self) -> dict[str, Any]:
        """从最近检查点恢复"""
        return {
            "pipeline": self.name,
            "interrupted": self._interrupted,
            "last_checkpoint": self._last_checkpoint,
            "artifacts_before_interrupt": len(self._artifacts),
        }


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        # 跨模块数据流水线
        self._pipelines: dict[str, DataPipeline] = {}
        # 全局变更日志（用于 changelog 跨模块追溯）
        self._global_changelog: list[dict[str, Any]] = []
        # Schema 注册表（cross-module schema registry）
        self._schema_registry: dict[str, list[dict[str, Any]]] = {}
        # 水位偏移记录（watermark → late_data 共享）
        self._watermark_offsets: dict[str, dict[str, Any]] = {}
        # 快照存储表（snapshot_mgr → backfill_job 共享）
        self._snapshot_store: dict[str, dict[str, Any]] = {}
        # 血缘图（lineage → table_format 共享）
        self._lineage_graph: dict[str, list[dict[str, Any]]] = {}
        # 归档清单（cold_archive → retention 共享）
        self._archive_manifest: list[dict[str, Any]] = []
        # 合并覆盖清单（batch_slice 批量登记 → merge_plan 采用 共享）
        # key: f"{源表名称}::{分片键列}"，value 含已确认区间与合并采用日志
        self._merge_coverage: dict[str, dict[str, Any]] = {}
        self._coverage_seq = 0
        # 批量登记的互斥锁：并发提交时保证“检查冲突 + 确认区间”原子完成
        self.coverage_lock = threading.RLock()

    # ---------- 基础 CRUD ----------
    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        # 跨模块流水线状态
        pipeline_stats = []
        for pname, pl in self._pipelines.items():
            pipeline_stats.append(pl.health_report())
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
            {"label": "流水线", "value": len(pipeline_stats)},
        ]
        return {"cards": cards, "modules": modules, "pipelines": pipeline_stats}

    # ---------- 跨模块流水线 ----------
    def create_pipeline(self, name: str, sources: list[str], target_module: str) -> DataPipeline:
        pl = DataPipeline(name, sources, target_module)
        self._pipelines[name] = pl
        return pl

    def pipeline(self, name: str) -> DataPipeline | None:
        return self._pipelines.get(name)

    def all_pipelines(self) -> dict[str, DataPipeline]:
        return dict(self._pipelines)

    # ---------- Schema 注册表 ----------
    def register_schema(self, topic: str, schema_def: dict[str, Any], version: str) -> dict[str, Any]:
        versions = self._schema_registry.setdefault(topic, [])
        entry = {"topic": topic, "definition": schema_def, "version": version, "ts": time.time()}
        versions.append(entry)
        return entry

    def schema_versions(self, topic: str) -> list[dict[str, Any]]:
        return self._schema_registry.get(topic, [])

    def check_schema_compatibility(self, topic: str, new_fields: list[str]) -> dict[str, Any]:
        versions = self.schema_versions(topic)
        if not versions:
            return {"compatible": True, "reason": "新主题，无兼容性问题"}
        latest = versions[-1]
        old_fields = set(latest.get("definition", {}).get("fields", []))
        missing_required = [f for f in old_fields if f not in new_fields]
        if missing_required:
            return {"compatible": False, "reason": f"缺少已有字段: {missing_required}"}
        return {"compatible": True, "reason": "向后兼容"}

    # ---------- 水位记录 ----------
    def update_watermark(self, partition: str, offset: dict[str, Any]) -> None:
        self._watermark_offsets[partition] = offset

    def get_watermark(self, partition: str) -> dict[str, Any] | None:
        return self._watermark_offsets.get(partition)

    # ---------- 快照存储 ----------
    def save_snapshot(self, snap_id: str, data: dict[str, Any]) -> None:
        self._snapshot_store[snap_id] = data

    def get_snapshot(self, snap_id: str) -> dict[str, Any] | None:
        return self._snapshot_store.get(snap_id)

    # ---------- 变更日志 ----------
    def append_changelog(self, entry: dict[str, Any]) -> None:
        entry["_ts"] = time.time()
        self._global_changelog.append(entry)

    def changelog_for_table(self, table_name: str) -> list[dict[str, Any]]:
        return [e for e in self._global_changelog if e.get("目标表") == table_name]

    # ---------- 血缘图 ----------
    def add_lineage_edge(self, upstream: str, downstream: str, edge_data: dict[str, Any]) -> None:
        edges = self._lineage_graph.setdefault(upstream, [])
        edges.append({"downstream": downstream, **edge_data, "ts": time.time()})

    def lineage_downstream(self, table_name: str) -> list[dict[str, Any]]:
        return self._lineage_graph.get(table_name, [])

    # ---------- 归档清单 ----------
    def add_archive_record(self, record: dict[str, Any]) -> None:
        self._archive_manifest.append(record)

    def archive_manifest_for_table(self, table_name: str) -> list[dict[str, Any]]:
        return [r for r in self._archive_manifest if r.get("源表名称") == table_name]

    # ---------- 合并覆盖清单 ----------
    @staticmethod
    def coverage_scope_key(source_table: str, key_column: str) -> str:
        """覆盖清单按（源表名称, 分片键列）确定范围。"""
        return f"{source_table}::{key_column}"

    def coverage_scope(
        self, source_table: str, key_column: str, *, create: bool = False
    ) -> dict[str, Any] | None:
        key = self.coverage_scope_key(source_table, key_column)
        scope = self._merge_coverage.get(key)
        if scope is None and create:
            scope = {
                "源表名称": source_table,
                "分片键列": key_column,
                "intervals": [],
                "adoptions": [],
            }
            self._merge_coverage[key] = scope
        return scope

    def commit_coverage_intervals(
        self,
        source_table: str,
        key_column: str,
        source_tag: str,
        intervals: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """把一批分片区间原子提交进合并覆盖清单，逐条返回编号或冲突范围。

        - 重叠判定按 half-open 区间：相邻（end == start）允许拼接，不算冲突
        - 同编号同范围重投：返回原登记结果（原编号、原条目），不重复登记
        - 同编号不同范围：按冲突拒绝
        调用方需持有 coverage_lock（方法内部可重入），保证并发批量提交时
        “检查冲突 + 确认区间”不会被另一批插队。
        """
        results: list[dict[str, Any]] = []
        with self.coverage_lock:
            scope = self.coverage_scope(source_table, key_column, create=True)
            assert scope is not None
            committed: list[dict[str, Any]] = scope["intervals"]
            for interval in intervals:
                batch_no = interval["批次编号"]
                start = float(interval["起始值"])
                end = float(interval["结束值"])

                # 幂等重投：同编号同范围返回原结果；同编号不同范围按冲突拒绝
                same_no = next((c for c in committed if c["批次编号"] == batch_no), None)
                if same_no is not None:
                    if float(same_no["起始值"]) == start and float(same_no["结束值"]) == end:
                        results.append({
                            "ok": True,
                            "重投": True,
                            "批次编号": batch_no,
                            "起始值": norm_num(start),
                            "结束值": norm_num(end),
                            "编号": same_no["编号"],
                            "entry_id": same_no.get("entry_id"),
                        })
                    else:
                        results.append({
                            "ok": False,
                            "批次编号": batch_no,
                            "起始值": norm_num(start),
                            "结束值": norm_num(end),
                            "reason": (
                                f"批次编号 {batch_no} 已登记范围 "
                                f"[{norm_num(same_no['起始值'])},{norm_num(same_no['结束值'])})，"
                                f"与本次 [{norm_num(start)},{norm_num(end)}) 不一致"
                            ),
                            "冲突范围": {
                                "起始值": norm_num(same_no["起始值"]),
                                "结束值": norm_num(same_no["结束值"]),
                                "冲突批次": same_no["批次编号"],
                                "冲突编号": same_no["编号"],
                            },
                        })
                    continue

                # 重叠检测：half-open 区间，相邻边界相等不算重叠
                overlap = next(
                    (c for c in committed
                     if start < float(c["结束值"]) and float(c["起始值"]) < end),
                    None,
                )
                if overlap is not None:
                    results.append({
                        "ok": False,
                        "批次编号": batch_no,
                        "起始值": norm_num(start),
                        "结束值": norm_num(end),
                        "reason": (
                            f"与已登记区间 [{norm_num(overlap['起始值'])},"
                            f"{norm_num(overlap['结束值'])}) 重叠"
                        ),
                        "冲突范围": {
                            "起始值": norm_num(max(start, float(overlap["起始值"]))),
                            "结束值": norm_num(min(end, float(overlap["结束值"]))),
                            "冲突批次": overlap["批次编号"],
                            "冲突编号": overlap["编号"],
                        },
                    })
                    continue

                self._coverage_seq += 1
                reg_no = f"COV-{self._coverage_seq:04d}"
                record = {
                    "编号": reg_no,
                    "批次编号": batch_no,
                    "起始值": norm_num(start),
                    "结束值": norm_num(end),
                    "来源": source_tag,
                    "entry_id": None,
                    "ts": time.time(),
                }
                committed.append(record)
                results.append({
                    "ok": True,
                    "重投": False,
                    "批次编号": batch_no,
                    "起始值": norm_num(start),
                    "结束值": norm_num(end),
                    "编号": reg_no,
                    "entry_id": None,
                })
        return results

    def find_coverage_record(
        self, source_table: str, key_column: str, reg_no: str
    ) -> dict[str, Any] | None:
        scope = self.coverage_scope(source_table, key_column)
        if scope is None:
            return None
        return next((c for c in scope["intervals"] if c["编号"] == reg_no), None)

    def coverage_view(self, source_table: str, key_column: str) -> dict[str, Any]:
        """覆盖清单视图：已登记区间（按起始值排序）与拼接后的连续段。"""
        scope = self.coverage_scope(source_table, key_column)
        intervals = [] if scope is None else sorted(
            scope["intervals"], key=lambda c: (float(c["起始值"]), float(c["结束值"]))
        )
        segments = merge_segments([(c["起始值"], c["结束值"]) for c in intervals])
        return {
            "intervals": intervals,
            "segments": segments,
            "adoptions": [] if scope is None else list(scope["adoptions"]),
        }

    def record_coverage_adoption(
        self, source_table: str, key_column: str, record: dict[str, Any]
    ) -> None:
        """记录一次合并采用结果（含失败原因），供事后核对。"""
        scope = self.coverage_scope(source_table, key_column, create=True)
        assert scope is not None
        scope["adoptions"].append({**record, "ts": time.time()})


store = Store()