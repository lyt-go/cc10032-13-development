"""通用接口契约；模块字段由领域服务持有，不再为每个模块复制一份模型类。"""
from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class PageResult(BaseModel, Generic[T]):
    items: list[T]
    total: int
    page: int = 1
    size: int = 20


class ActionResult(BaseModel):
    ok: bool
    message: str
    entry: dict[str, Any] | None = None


class EntryPayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)
    remark: str | None = None


class ActionPayload(BaseModel):
    action: str
    remark: str | None = None


class SliceIntervalPayload(BaseModel):
    """批量登记中的一段分片区间（half-open：[起始, 结束)）。"""

    批次编号: str
    分片起始值: float
    分片结束值: float


class BatchRegisterPayload(BaseModel):
    """批次分片批量登记：基于已启用流式源，多选区间一次提交。"""

    source_id: int
    源表名称: str
    分片键列: str
    intervals: list[SliceIntervalPayload] = Field(default_factory=list)


class AdoptCoveragePayload(BaseModel):
    """合并计划采用覆盖清单的选定范围。"""

    选择起始值: float
    选择结束值: float
