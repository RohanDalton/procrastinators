"""
Coroutines sharing a quota without blocking the event loop.

Twenty fetches run concurrently against a limit of five per 200 ms. Waiting for
quota happens on each task, on the loop; a heartbeat task keeps running
throughout, and storage calls run on the SQLite handle's own worker thread.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import asyncio
import tempfile
import time
from pathlib import Path

from procrastinators import Limit, RateLimiter, idempotent_key

KEY = idempotent_key({"vendor": "clacks", "endpoint": "semaphore"})


async def main() -> None:
    with tempfile.TemporaryDirectory() as directory:
        limiter = RateLimiter(
            key=KEY,
            limits=[Limit(5, per="200ms")],
            backend=f"sqlite:///{Path(directory) / 'quota.sqlite3'}",
        )
        started = time.monotonic()
        beats = 0

        @limiter
        async def send(message: int) -> float:
            await asyncio.sleep(0)
            return time.monotonic() - started

        async def heartbeat() -> None:
            nonlocal beats
            while True:
                beats += 1
                await asyncio.sleep(0.01)

        beating = asyncio.ensure_future(heartbeat())
        sent = await asyncio.gather(*(send(message) for message in range(20)))
        beating.cancel()
        async with limiter:
            pass
        await limiter.aclose()
        print(f"20 messages in {max(sent):.2f} s; the heartbeat beat {beats} times meanwhile")


if __name__ == "__main__":
    asyncio.run(main())
