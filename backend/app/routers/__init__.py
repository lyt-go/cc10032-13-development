"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import batch_ops, resource

# batch_ops 含 /api/batch_slice/enabled-sources 等固定路径，
# 必须先于通用资源路由 /api/{module}/{entry_id} 注册。
ROUTERS = [batch_ops, resource]
