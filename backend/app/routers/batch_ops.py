"""批次分片批量登记与合并覆盖清单接口：通用资源路由之外的专用操作。

注意：本路由必须先于通用资源路由注册，否则 /api/batch_slice/enabled-sources、
/api/merge_plan/coverage-manifest 会被 /api/{module}/{entry_id} 抢先匹配。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import AdoptCoveragePayload, BatchRegisterPayload
from app.services import service_for

router = APIRouter(prefix="/api", tags=["批次分片批量登记"])


@router.get("/batch_slice/enabled-sources", response_model=dict)
def enabled_sources() -> dict[str, Any]:
    """已启用流式源清单：批量登记只能从这里选择来源。"""
    service = service_for("stream_source")
    items = service.emit_source_manifest() if service else []
    return {"items": items, "total": len(items)}


@router.post("/batch_slice/batch-register", response_model=dict)
def batch_register(payload: BatchRegisterPayload) -> dict[str, Any]:
    """分片批量登记：多选区间一次提交，逐条给出编号或冲突范围。"""
    service = service_for("batch_slice")
    if service is None:
        raise HTTPException(status_code=404, detail="未知业务模块：batch_slice")
    return service.batch_register(
        source_id=payload.source_id,
        source_table=payload.源表名称,
        key_column=payload.分片键列,
        intervals=[interval.model_dump() for interval in payload.intervals],
    )


@router.get("/merge_plan/coverage-manifest", response_model=dict)
def coverage_manifest(
    source_table: str = Query(...),
    key_column: str = Query(...),
    start: float | None = Query(default=None),
    end: float | None = Query(default=None),
) -> dict[str, Any]:
    """合并覆盖清单：已登记区间、连续段、缺口、合并可读范围与采用日志。"""
    service = service_for("merge_plan")
    if service is None:
        raise HTTPException(status_code=404, detail="未知业务模块：merge_plan")
    if (start is None) != (end is None):
        raise HTTPException(status_code=400, detail="start 和 end 必须同时提供")
    return service.coverage_manifest(source_table, key_column, start, end)


@router.post("/merge_plan/{entry_id}/adopt-coverage", response_model=dict)
def adopt_coverage(entry_id: int, payload: AdoptCoveragePayload) -> dict[str, Any]:
    """合并采用覆盖清单：只在选定范围没有未解释缺口时才采用。"""
    service = service_for("merge_plan")
    if service is None:
        raise HTTPException(status_code=404, detail="未知业务模块：merge_plan")
    return service.adopt_coverage(entry_id, payload.选择起始值, payload.选择结束值)
