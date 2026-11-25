"""批次分片批量登记与合并覆盖清单验收测试。

验收场景（对应需求）：
1. 基于已启用流式源清单批量登记 [0,10) [10,20) [19,25) [30,40) 四段：
   [0,10) 确认；[10,20) 与 [0,10) 相邻拼接确认；[19,25) 与 [10,20) 重叠返回冲突范围；
   [30,40) 确认 —— 覆盖清单在 [20,30) 留下缺口。
2. 合并采用选定范围 [0,40)：存在未解释缺口 [20,30)，拒绝并记录原因，可读范围 [0,20)。
3. 再登记恰好补齐缺口的 [20,30)，合并采用成功，可读范围 [0,40)，此前失败原因仍可查。
4. 并发两次批量登记只确认无重叠结果；停用来源不能再登记；同编号同范围重投返回原结果。
"""
from __future__ import annotations

import asyncio
import json
import threading

import pytest

from app.services import service_for
from app.store import store

SOURCE_ID = 3  # 种子数据里已启用的 kafka-trade-ticks
TABLE = "ods_accept_ticks"
KEY = "trade_id"
PLAN_NO = "MERG-9001"


def _interval(no: str, start: float, end: float) -> dict:
    return {"批次编号": no, "分片起始值": start, "分片结束值": end}


@pytest.fixture()
def env():
    """每个用例前重置覆盖清单与测试数据，保证相互独立。"""
    source = store.find("stream_source", SOURCE_ID)
    assert source is not None
    source["status"] = "已启用"

    store._merge_coverage.clear()
    store._coverage_seq = 0
    store.rows("batch_slice")[:] = [
        r for r in store.rows("batch_slice")
        if not str(r.get("批次编号", "")).startswith("BATC-9")
    ]
    store.rows("merge_plan")[:] = [
        r for r in store.rows("merge_plan")
        if str(r.get("计划编号", "")) != PLAN_NO
    ]
    plan_id = max((int(r.get("id", 0)) for r in store.rows("merge_plan")), default=0) + 1
    store.rows("merge_plan").append({
        "id": plan_id,
        "计划编号": PLAN_NO,
        "流式来源": "kafka-trade-ticks",
        "批式来源": TABLE,
        "合并键列": KEY,
        "冲突解决": "stream_win",
        "输出路径": "/dwd/accept/",
        "合并窗口": "60",
        "去重策略": "unique",
        "status": "运行中",
        "pending": True,
        "abnormal": False,
    })
    yield {
        "slice": service_for("batch_slice"),
        "merge": service_for("merge_plan"),
        "plan_id": plan_id,
    }
    store._merge_coverage.clear()
    store.rows("batch_slice")[:] = [
        r for r in store.rows("batch_slice")
        if not str(r.get("批次编号", "")).startswith("BATC-9")
    ]
    store.rows("merge_plan")[:] = [
        r for r in store.rows("merge_plan")
        if str(r.get("计划编号", "")) != PLAN_NO
    ]


def _register_four_segments(svc) -> dict:
    """登记验收四段：[0,10) [10,20) [19,25) [30,40)。"""
    return svc.batch_register(SOURCE_ID, TABLE, KEY, [
        _interval("BATC-9001", 0, 10),
        _interval("BATC-9002", 10, 20),
        _interval("BATC-9003", 19, 25),
        _interval("BATC-9004", 30, 40),
    ])


class TestBatchRegisterAcceptance:
    def test_four_segments_conflict_and_adjacency(self, env):
        """四段登记：相邻拼接确认、重叠段返回冲突范围、留下 [20,30) 缺口。"""
        svc = env["slice"]
        result = _register_four_segments(svc)

        assert result["ok"] is True
        assert result["来源"] == "kafka-trade-ticks"
        assert result["确认数"] == 3
        assert result["冲突数"] == 1

        r0, r1, r2, r3 = result["结果"]
        # [0,10) 确认，给出登记编号
        assert r0["ok"] is True and r0["编号"].startswith("COV-")
        # [10,20) 与 [0,10) 相邻，允许拼接
        assert r1["ok"] is True and r1["编号"].startswith("COV-")
        # [19,25) 与已登记的 [10,20) 重叠，返回冲突范围 [19,20)
        assert r2["ok"] is False
        assert r2["冲突范围"] == {
            "起始值": 19, "结束值": 20,
            "冲突批次": "BATC-9002", "冲突编号": r1["编号"],
        }
        # [30,40) 确认
        assert r3["ok"] is True

        # 相邻区间拼接成连续段，[20,30) 缺口清晰可见
        assert result["连续覆盖"] == [
            {"起始值": 0, "结束值": 20},
            {"起始值": 30, "结束值": 40},
        ]

        # 确认的三段同步登记为分片批次条目
        rows = [r for r in store.rows("batch_slice")
                if str(r.get("批次编号", "")).startswith("BATC-9")]
        assert [r["批次编号"] for r in rows] == ["BATC-9001", "BATC-9002", "BATC-9004"]
        assert all(r["status"] == "待执行" for r in rows)

    def test_merge_adoption_blocked_by_gap_then_filled(self, env):
        """合并只在无未解释缺口时采用；补齐缺口后可读范围覆盖整个选定范围。"""
        svc = env["slice"]
        merge = env["merge"]
        _register_four_segments(svc)

        # 有缺口 [20,30)：采用被拒，原因记录缺口，可读范围只到 20
        blocked = merge.adopt_coverage(env["plan_id"], 0, 40)
        assert blocked["ok"] is False
        assert "未解释缺口" in blocked["reason"]
        assert "[20,30)" in blocked["reason"]
        assert blocked["缺口"] == [{"起始值": 20, "结束值": 30}]
        assert blocked["可读范围"] == {"起始值": 0, "结束值": 20}

        # 再登记恰好补齐缺口的分片
        fill = svc.batch_register(SOURCE_ID, TABLE, KEY, [_interval("BATC-9005", 20, 30)])
        assert fill["ok"] is True and fill["确认数"] == 1
        assert fill["连续覆盖"] == [{"起始值": 0, "结束值": 40}]

        # 合并开始可读的范围覆盖整个选定范围，采用成功
        adopted = merge.adopt_coverage(env["plan_id"], 0, 40)
        assert adopted["ok"] is True
        assert adopted["可读范围"] == {"起始值": 0, "结束值": 40}
        assert adopted["缺口"] == []

        # 此前的失败原因仍可在采用日志里核对
        manifest = merge.coverage_manifest(TABLE, KEY, 0, 40)
        log = manifest["采用日志"]
        assert len(log) == 2
        assert log[0]["ok"] is False and "未解释缺口" in log[0]["reason"]
        assert log[0]["缺口"] == [{"起始值": 20, "结束值": 30}]
        assert log[1]["ok"] is True
        assert manifest["可整体采用"] is True

    def test_disabled_source_cannot_register(self, env):
        """停用来源不能再登记新切割；未启用来源同样不在已启用清单中。"""
        svc = env["slice"]
        source = store.find("stream_source", SOURCE_ID)
        source["status"] = "已停用"
        result = svc.batch_register(SOURCE_ID, TABLE, KEY, [_interval("BATC-9101", 0, 10)])
        assert result["ok"] is False
        assert "已停用" in result["reason"]

        source["status"] = "草稿"
        result = svc.batch_register(SOURCE_ID, TABLE, KEY, [_interval("BATC-9101", 0, 10)])
        assert result["ok"] is False
        assert "未启用" in result["reason"]

    def test_idempotent_replay_returns_original(self, env):
        """同编号同范围重投返回原结果；同编号不同范围按冲突拒绝。"""
        svc = env["slice"]
        first = svc.batch_register(SOURCE_ID, TABLE, KEY, [_interval("BATC-9201", 0, 10)])
        assert first["ok"] is True
        original = first["结果"][0]
        assert original["ok"] is True

        replay = svc.batch_register(SOURCE_ID, TABLE, KEY, [_interval("BATC-9201", 0, 10)])
        replayed = replay["结果"][0]
        assert replayed["ok"] is True
        assert replayed["重投"] is True
        assert replayed["编号"] == original["编号"]
        assert replayed["entry_id"] == original["entry_id"]

        # 重投不产生重复条目
        rows = [r for r in store.rows("batch_slice") if r.get("批次编号") == "BATC-9201"]
        assert len(rows) == 1

        # 同编号不同范围：冲突拒绝
        changed = svc.batch_register(SOURCE_ID, TABLE, KEY, [_interval("BATC-9201", 0, 15)])
        changed_result = changed["结果"][0]
        assert changed_result["ok"] is False
        assert "不一致" in changed_result["reason"]

    def test_concurrent_registration_only_non_overlapping_confirmed(self, env):
        """两次批量登记并发提交：重叠段只有一方确认，无重叠段双方都确认。"""
        svc = env["slice"]
        barrier = threading.Barrier(2)
        outcomes: dict[str, dict] = {}

        def submit(tag: str, intervals: list[dict]) -> None:
            barrier.wait()
            outcomes[tag] = svc.batch_register(SOURCE_ID, TABLE, KEY, intervals)

        # A: [0,10) [20,30)；B: [5,15) [40,50)。[0,10) 与 [5,15) 重叠，其余互不重叠。
        threads = [
            threading.Thread(target=submit, args=("A", [
                _interval("BATC-9301", 0, 10), _interval("BATC-9302", 20, 30)])),
            threading.Thread(target=submit, args=("B", [
                _interval("BATC-9303", 5, 15), _interval("BATC-9304", 40, 50)])),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        res_a = outcomes["A"]["结果"]
        res_b = outcomes["B"]["结果"]
        overlap_pair = [res_a[0]["ok"], res_b[0]["ok"]]
        # 重叠段只确认一方
        assert overlap_pair.count(True) == 1
        # 无重叠段双方都确认
        assert res_a[1]["ok"] is True
        assert res_b[1]["ok"] is True
        # 总共只确认三段
        confirmed = sum(1 for r in res_a + res_b if r["ok"])
        assert confirmed == 3
        # 覆盖清单里不存在相互重叠的区间
        view = store.coverage_view(TABLE, KEY)
        ranges = [(float(c["起始值"]), float(c["结束值"])) for c in view["intervals"]]
        for i, (s1, e1) in enumerate(ranges):
            for s2, e2 in ranges[i + 1:]:
                assert not (s1 < e2 and s2 < e1)


class TestBatchRegisterRoutes:
    """路由层：固定路径不被通用资源路由抢占，经 ASGI 全链路验证。"""

    @staticmethod
    def _effective_paths(app) -> list[str]:
        """展开路由表（本版本 FastAPI 对 include_router 使用惰性包装）。"""
        paths: list[str] = []
        for route in app.routes:
            candidates = getattr(route, "effective_candidates", None)
            if callable(candidates):
                paths.extend(getattr(c, "path", "") for c in candidates())
            else:
                paths.append(getattr(route, "path", ""))
        return paths

    @staticmethod
    def _asgi_request(method: str, path: str, body: dict | None = None, query: str = ""):
        """不依赖 httpx 的最小 ASGI 请求，验证真实路由匹配与响应。"""
        from app.main import app

        request_body = json.dumps(body).encode() if body is not None else b""
        scope = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": "1.1",
            "method": method,
            "scheme": "http",
            "path": path,
            "raw_path": path.encode(),
            "query_string": query.encode(),
            "root_path": "",
            "headers": [(b"content-type", b"application/json")] if body is not None else [],
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
        }
        messages: list[dict] = []

        async def receive() -> dict:
            return {"type": "http.request", "body": request_body, "more_body": False}

        async def send(message: dict) -> None:
            messages.append(message)

        asyncio.run(app(scope, receive, send))
        status = next(m["status"] for m in messages if m["type"] == "http.response.start")
        payload = b"".join(m.get("body", b"") for m in messages if m["type"] == "http.response.body")
        return status, json.loads(payload) if payload else None

    def test_routes_registered_before_resource_router(self):
        from app.main import app

        paths = self._effective_paths(app)
        assert "/api/batch_slice/enabled-sources" in paths
        assert "/api/batch_slice/batch-register" in paths
        assert "/api/merge_plan/coverage-manifest" in paths
        assert "/api/merge_plan/{entry_id}/adopt-coverage" in paths
        # 固定路径排在通用 /api/{module}/{entry_id} 之前
        assert paths.index("/api/batch_slice/enabled-sources") < paths.index("/api/{module}/{entry_id}")
        assert paths.index("/api/merge_plan/coverage-manifest") < paths.index("/api/{module}/{entry_id}")

    def test_enabled_sources_endpoint(self, env):
        status, payload = self._asgi_request("GET", "/api/batch_slice/enabled-sources")
        assert status == 200
        assert payload["total"] >= 1
        assert all(s["status"] == "已启用" for s in payload["items"])

    def test_full_acceptance_over_http(self, env):
        """HTTP 全链路：四段登记 → 缺口拒并 → 补齐 → 采用成功且失败原因可查。"""
        body = {
            "source_id": SOURCE_ID,
            "源表名称": TABLE,
            "分片键列": KEY,
            "intervals": [
                {"批次编号": "BATC-9401", "分片起始值": 0, "分片结束值": 10},
                {"批次编号": "BATC-9402", "分片起始值": 10, "分片结束值": 20},
                {"批次编号": "BATC-9403", "分片起始值": 19, "分片结束值": 25},
                {"批次编号": "BATC-9404", "分片起始值": 30, "分片结束值": 40},
            ],
        }
        status, registered = self._asgi_request("POST", "/api/batch_slice/batch-register", body)
        assert status == 200
        assert registered["确认数"] == 3
        assert registered["冲突数"] == 1
        assert registered["结果"][2]["冲突范围"]["起始值"] == 19

        query = f"source_table={TABLE}&key_column={KEY}&start=0&end=40"
        status, manifest = self._asgi_request("GET", "/api/merge_plan/coverage-manifest", query=query)
        assert status == 200
        assert manifest["缺口"] == [{"起始值": 20, "结束值": 30}]
        assert manifest["可整体采用"] is False
        assert manifest["可读范围"] == {"起始值": 0, "结束值": 20}

        status, blocked = self._asgi_request(
            "POST", f"/api/merge_plan/{env['plan_id']}/adopt-coverage",
            {"选择起始值": 0, "选择结束值": 40},
        )
        assert status == 200
        assert blocked["ok"] is False
        assert "未解释缺口" in blocked["reason"]

        fill_body = {
            "source_id": SOURCE_ID,
            "源表名称": TABLE,
            "分片键列": KEY,
            "intervals": [{"批次编号": "BATC-9405", "分片起始值": 20, "分片结束值": 30}],
        }
        status, filled = self._asgi_request("POST", "/api/batch_slice/batch-register", fill_body)
        assert status == 200
        assert filled["确认数"] == 1

        status, adopted = self._asgi_request(
            "POST", f"/api/merge_plan/{env['plan_id']}/adopt-coverage",
            {"选择起始值": 0, "选择结束值": 40},
        )
        assert status == 200
        assert adopted["ok"] is True
        assert adopted["可读范围"] == {"起始值": 0, "结束值": 40}

        # 通用资源路由不受影响：列表与详情照常工作
        status, listing = self._asgi_request("GET", "/api/batch_slice")
        assert status == 200
        assert listing["total"] >= 1
