"""模拟数据集与模拟站接口的基础校验。"""

from __future__ import annotations

import httpx

from mock_site import data


def test_dataset_shape():
    assert len(data.PERMITS) == 35
    assert len({p["permit_no"] for p in data.PERMITS}) == 35
    # 目录 38 条 = 35 唯一 + 3 跨页重复
    assert len(data.CATALOG_ENTRIES) == 38
    assert data.TOTAL_PAGES == 4
    for permit in data.PERMITS:
        assert {"permit_no", "project_name", "address", "issue_date"} <= permit.keys()


def test_catalog_pagination():
    seen = []
    for page in range(1, data.TOTAL_PAGES + 1):
        payload = data.catalog_page(page)
        assert payload["page"] == page
        assert len(payload["items"]) <= data.PAGE_SIZE
        seen.extend((i["permit_no"], i["detail_id"]) for i in payload["items"])
    assert seen == data.CATALOG_ENTRIES
    assert data.catalog_page(99) is None


def test_duplicates_span_different_pages():
    # 三条重复条目必须出现在与原条目不同的目录页上，才构成“跨页重复”
    pages_by_permit: dict[str, list[int]] = {}
    for page in range(1, data.TOTAL_PAGES + 1):
        for item in data.catalog_page(page)["items"]:
            pages_by_permit.setdefault(item["permit_no"], []).append(page)
    for permit_no in ("PERMIT-004", "PERMIT-017", "PERMIT-031"):
        assert len(pages_by_permit[permit_no]) >= 2


async def test_mock_site_http_contract(mock_base_url):
    async with httpx.AsyncClient(base_url=mock_base_url, timeout=10) as http:
        page1 = (await http.get("/api/permits", params={"page": 1})).json()
        assert page1["total_pages"] == 4
        detail_id = page1["items"][0]["detail_id"]
        detail = (await http.get(f"/api/permits/details/{detail_id}")).json()
        assert {"permit_no", "project_name", "address", "issue_date"} <= detail.keys()

        resp = await http.get("/api/permits", params={"page": 99})
        assert resp.status_code == 404
        resp = await http.get("/api/permits/details/NOPE")
        assert resp.status_code == 404


async def test_mock_first_fail_then_succeeds(mock_base_url):
    async with httpx.AsyncClient(base_url=mock_base_url, timeout=10) as http:
        await http.post("/control/reset")
        await http.post("/control/first-fail", params={"detail_id": "DET-001"})
        first = await http.get("/api/permits/details/DET-001")
        second = await http.get("/api/permits/details/DET-001")
        assert first.status_code == 500
        assert second.status_code == 200
        state = (await http.get("/control/state")).json()
        assert len(state["failed_attempts"]) == 1
        await http.post("/control/reset")
