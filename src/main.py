import asyncio
import json
import math
import os
import queue
import sys
from typing import Any, Final, List, Set

import requests
import structlog
import uvicorn
from fastapi import FastAPI, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware

APP: Final[FastAPI] = FastAPI()
LOGGER: Final[structlog.stdlib.BoundLogger] = structlog.getLogger()
EVLOOP: Final[asyncio.AbstractEventLoop] = asyncio.new_event_loop()
PENDING_SUBMISSIONS: queue.SimpleQueue[str] = queue.SimpleQueue()
ERRNO: int = 0
GITHUB_TOKEN: Final[str] = os.environ.get("GITHUB_TOKEN")
BACKGROUND_JOBS: Set[asyncio.Task[None]] = set()


def upload_manifest(manifest: List[Any]) -> str:
    resp = requests.post(
        "https://paste.mozilla.org/api/v2/",
        data={
            "content": str.encode(json.dumps(manifest)),
            "syntax": "json",
            "expiry_days": 3
        },
        headers={"User-Agent": "AWACY Python Manifest Exporter"},
    )
    if resp.ok:
        LOGGER.info("upload ok!", url=resp.text)
        PENDING_SUBMISSIONS.put(resp.text)
        return resp.text
    else:
        raise Exception("Manifest upload did not finish properly")


def batch_to_github():
    work_item: str
    try:
        work_item = PENDING_SUBMISSIONS.get(block=False)
    except queue.Empty as e:
        LOGGER.warn("no items in the queue", err=e)
        return
    resp = requests.post(
        "https://api.github.com/repos/AreWeAntiCheatYet/master-manifest/dispatches",
        data=json.dumps({
            "event_type": "manually_dispatched",
            "client_payload": {
                "edit_url": work_item
            },
        }),
        headers={
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json",
            "Content-Type": "application/json",
        },
    )
    if resp.ok:
        LOGGER.info("manifest edit batched to github")
    else:
        LOGGER.error("problem occurred when batching edit",
                     response=resp.text,
                     item=work_item)


async def batch_to_github_job():
    # HACK: the poormans jobqueue
    # NOTE: more items in the queue, means the wait
    # gets exponentially longer, capped at 1 hour
    items = PENDING_SUBMISSIONS.qsize()
    wait = (300 + min(math.exp(min(items, 10)), 3600)) - 1
    LOGGER.debug(
        "preparing to sleep until next job run.",
        wait_time=wait,
        estimated_queue_size=items,
    )

    await asyncio.sleep(wait)

    await asyncio.create_task(asyncio.to_thread(batch_to_github))
    rerun = asyncio.create_task(batch_to_github_job())
    BACKGROUND_JOBS.add(rerun)
    rerun.add_done_callback(BACKGROUND_JOBS.discard)


@APP.post("/submit")
async def forward_to_github(req: Request, resp: Response):
    games = await req.json()
    await asyncio.create_task(asyncio.to_thread(upload_manifest, games))
    resp.status_code = status.HTTP_200_OK
    return {"err": "ok"}


@APP.get("/")
async def hello_world():
    return {"hello": "world"}


async def main() -> int:
    srv_cfg = uvicorn.Config(APP, host="0.0.0.0", port=8080, log_level="debug")
    srv = uvicorn.Server(srv_cfg)

    main_background_task = asyncio.create_task(batch_to_github_job())
    BACKGROUND_JOBS.add(main_background_task)
    main_background_task.add_done_callback(BACKGROUND_JOBS.discard)

    cors_origins = [
        "https://areweanticheatyet.com",
        "https://export.areweanticheatyet.com",
    ]
    APP.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    await srv.serve()
    await asyncio.wait(*BACKGROUND_JOBS, timeout=60)
    return ERRNO


if __name__ == "__main__":
    sys.exit(EVLOOP.run_until_complete(main()))
