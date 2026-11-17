"""SQLite 持久化层（aiosqlite）。

两张表：
- tasks         采集任务状态，进程重启后据此恢复
- permit_records 许可记录，按许可号唯一（跨页/跨目录条目重复只留一条）

所有写操作由 engine 里的全局写锁串行化，SQLite 自身也开启 WAL。
"""

from __future__ import annotations

import json
import os
from typing import Any, Optional

import aiosqlite

from . import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id              TEXT PRIMARY KEY,
    target_url           TEXT NOT NULL,
    status               TEXT NOT NULL,              -- running/paused/failed/completed/interrupted
    run_id               TEXT,
    catalog_pages_done   INTEGER NOT NULL DEFAULT 0,
    catalog_total_pages  INTEGER,
    last_error           TEXT,
    started_at           TEXT NOT NULL,
    updated_at           TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS permit_records (
    task_id          TEXT NOT NULL,
    permit_no        TEXT NOT NULL,
    project_name     TEXT,
    address          TEXT,
    issue_date       TEXT,
    source_detail_id TEXT,
    source_pages     TEXT NOT NULL DEFAULT '[]',     -- 在哪些目录页出现过
    status           TEXT NOT NULL DEFAULT 'pending', -- pending/success/failed
    attempts         INTEGER NOT NULL DEFAULT 0,
    last_error       TEXT,
    updated_at       TEXT NOT NULL,
    PRIMARY KEY (task_id, permit_no),                -- 每任务内同一许可只留一条
    FOREIGN KEY (task_id) REFERENCES tasks(task_id)
);

CREATE INDEX IF NOT EXISTS idx_permits_task_status
    ON permit_records(task_id, status);
"""


class Database:
    def __init__(self, path: str) -> None:
        self.path = path
        self._db: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        parent = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(parent, exist_ok=True)
        self._db = await aiosqlite.connect(self.path)
        self._db.row_factory = aiosqlite.Row
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA foreign_keys=ON")
        await self._db.executescript(SCHEMA)
        await self._db.commit()

    async def close(self) -> None:
        if self._db is not None:
            await self._db.close()
            self._db = None

    @property
    def conn(self) -> aiosqlite.Connection:
        assert self._db is not None, "Database not connected"
        return self._db

    # ---------------- tasks ----------------

    async def insert_task(self, task: dict[str, Any]) -> None:
        await self.conn.execute(
            """INSERT INTO tasks (task_id, target_url, status, run_id,
                   catalog_pages_done, catalog_total_pages, last_error,
                   started_at, updated_at)
               VALUES (:task_id, :target_url, :status, :run_id,
                   :catalog_pages_done, :catalog_total_pages, :last_error,
                   :started_at, :updated_at)""",
            task,
        )
        await self.conn.commit()

    async def get_task(self, task_id: str) -> Optional[dict[str, Any]]:
        async with self.conn.execute(
            "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
        ) as cur:
            row = await cur.fetchone()
        return dict(row) if row else None

    async def list_tasks(self) -> list[dict[str, Any]]:
        async with self.conn.execute(
            "SELECT * FROM tasks ORDER BY started_at DESC"
        ) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def update_task(
        self, task_id: str, fields: dict[str, Any], expected_run_id: Optional[str] = None
    ) -> bool:
        """更新任务。expected_run_id 不为 None 时做 CAS（代际校验）。

        返回是否更新成功。暂停后旧轮次持有的 run_id 与当前 run_id 不同，
        其任何收尾写入都会被 CAS 拒绝。
        """
        sets = ", ".join(f"{k} = :{k}" for k in fields)
        params: dict[str, Any] = dict(fields)
        params["task_id"] = task_id
        where = "task_id = :task_id"
        if expected_run_id is not None:
            where += " AND run_id = :expected_run_id"
            params["expected_run_id"] = expected_run_id
        cur = await self.conn.execute(
            f"UPDATE tasks SET {sets} WHERE {where}", params
        )
        await self.conn.commit()
        return cur.rowcount > 0

    async def mark_interrupted_on_startup(self) -> int:
        """进程重启恢复：上次没正常结束的 running 任务标为 interrupted。"""
        cur = await self.conn.execute(
            "UPDATE tasks SET status='interrupted', "
            "run_id=NULL, updated_at=datetime('now') WHERE status='running'"
        )
        await self.conn.commit()
        return cur.rowcount

    # ---------------- permits ----------------

    async def upsert_catalog_permit(
        self,
        task_id: str,
        permit_no: str,
        detail_id: str,
        page_no: int,
    ) -> None:
        """目录发现阶段写入/合并一个许可条目。

        已存在（同许可号，哪怕来自别的目录页或别的 detail_id）只合并来源页，
        不覆盖已采集成功的字段，保证跨页重复只留一条。
        """
        now = "datetime('now')"
        existing = await self.get_permit(task_id, permit_no)
        if existing is None:
            await self.conn.execute(
                """INSERT INTO permit_records (permit_no, task_id, source_detail_id,
                       source_pages, status, attempts, updated_at)
                   VALUES (?, ?, ?, ?, 'pending', 0, datetime('now'))""",
                (permit_no, task_id, detail_id, json.dumps([page_no])),
            )
        else:
            pages = json.loads(existing["source_pages"] or "[]")
            if page_no not in pages:
                pages.append(page_no)
            await self.conn.execute(
                "UPDATE permit_records SET source_pages = ?, updated_at=datetime('now') "
                "WHERE task_id = ? AND permit_no = ?",
                (json.dumps(sorted(pages)), task_id, permit_no),
            )
        await self.conn.commit()

    async def get_permit(self, task_id: str, permit_no: str) -> Optional[dict[str, Any]]:
        async with self.conn.execute(
            "SELECT * FROM permit_records WHERE task_id=? AND permit_no=?",
            (task_id, permit_no),
        ) as cur:
            row = await cur.fetchone()
        return dict(row) if row else None

    async def list_permits(
        self, task_id: str, status: Optional[str] = None, limit: int = 100, offset: int = 0
    ) -> list[dict[str, Any]]:
        if status:
            sql = (
                "SELECT * FROM permit_records WHERE task_id=? AND status=? "
                "ORDER BY permit_no LIMIT ? OFFSET ?"
            )
            params: tuple[Any, ...] = (task_id, status, limit, offset)
        else:
            sql = (
                "SELECT * FROM permit_records WHERE task_id=? "
                "ORDER BY permit_no LIMIT ? OFFSET ?"
            )
            params = (task_id, limit, offset)
        async with self.conn.execute(sql, params) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def count_permits(self, task_id: str, status: Optional[str] = None) -> int:
        if status:
            sql = "SELECT COUNT(*) FROM permit_records WHERE task_id=? AND status=?"
            params: tuple[Any, ...] = (task_id, status)
        else:
            sql = "SELECT COUNT(*) FROM permit_records WHERE task_id=?"
            params = (task_id,)
        async with self.conn.execute(sql, params) as cur:
            (n,) = await cur.fetchone()
        return n

    async def list_unfinished_permits(self, task_id: str) -> list[dict[str, Any]]:
        """所有未成功项：pending + failed，恢复后只补采这些。"""
        async with self.conn.execute(
            "SELECT * FROM permit_records WHERE task_id=? AND status != 'success' "
            "ORDER BY permit_no",
            (task_id,),
        ) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def mark_permit_attempt(
        self, task_id: str, permit_no: str, error: str
    ) -> None:
        await self.conn.execute(
            "UPDATE permit_records SET status='failed', attempts=attempts+1, "
            "last_error=?, updated_at=datetime('now') WHERE task_id=? AND permit_no=?",
            (error, task_id, permit_no),
        )
        await self.conn.commit()

    async def mark_permit_success(
        self, task_id: str, permit_no: str, detail: dict[str, Any]
    ) -> None:
        await self.conn.execute(
            """UPDATE permit_records SET status='success', attempts=attempts+1,
                   project_name=:project_name, address=:address,
                   issue_date=:issue_date, last_error=NULL,
                   updated_at=datetime('now')
               WHERE task_id=:task_id AND permit_no=:permit_no""",
            {
                "task_id": task_id,
                "permit_no": permit_no,
                "project_name": detail["project_name"],
                "address": detail["address"],
                "issue_date": detail["issue_date"],
            },
        )
        await self.conn.commit()


db = Database(config.DB_PATH)
