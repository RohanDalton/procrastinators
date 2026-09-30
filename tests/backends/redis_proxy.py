"""A TCP proxy that loses, delays, or forges one reply from a Redis server.

Stands between a handle and a real server so a failure can be placed exactly:
the request reaches the server and commits, and only then is the reply lost,
held back past the caller's budget, or replaced by a refusal the server never
made. Faults are one-shot and fire on the first request carrying a trigger —
normally the admission script's digest — so connection setup and other
commands pass untouched.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import contextlib
import socket
import threading
import time
import urllib.parse
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator
else:
    pass

_CHUNK = 65536


class Fault(StrEnum):
    """What happens to the reply of the first triggering request."""

    NONE = "none"
    LOSE_REPLY = "lose_reply"
    """Forward the request, discard the reply, and drop the connection."""

    DELAY_REPLY = "delay_reply"
    """Forward the request and hold the reply back for ``delay_s``."""

    REFUSE = "refuse"
    """Never forward the request; answer with ``refusal`` instead."""


@dataclass
class _Armed:
    fault: Fault = Fault.NONE
    trigger: bytes = b""
    delay_s: float = 0.0
    refusal: bytes = b""
    fired: threading.Event = field(default_factory=threading.Event)


class FaultyProxy:
    """A proxy listening on an ephemeral local port in front of ``upstream``.

    :param upstream: The real server's address, as ``redis://host:port/db``.
    """

    def __init__(self, upstream: str) -> None:
        parts = urllib.parse.urlsplit(upstream)
        self._upstream = (parts.hostname or "localhost", parts.port or 6379)
        self._database = parts.path
        self._listener = socket.create_server(("127.0.0.1", 0))
        self._armed = _Armed()
        self._lock = threading.Lock()
        self._closed = threading.Event()
        self._sockets: list[socket.socket] = list()
        self._thread = threading.Thread(target=self._accept, daemon=True)
        self._thread.start()

    @property
    def url(self) -> str:
        """The proxy's own address, naming the upstream database."""
        port = self._listener.getsockname()[1]
        url = f"redis://127.0.0.1:{port}{self._database}"
        return url

    def arm(
        self, fault: Fault, *, trigger: bytes, delay_s: float = 0.0, refusal: bytes = b""
    ) -> threading.Event:
        """Fire ``fault`` on the next request containing ``trigger``.

        :returns: An event set once the fault has fired.
        """
        armed = _Armed(fault, trigger, delay_s, refusal)
        with self._lock:
            self._armed = armed
        return armed.fired

    def _take(self, data: bytes) -> _Armed | None:
        with self._lock:
            armed = self._armed
            if armed.fault is not Fault.NONE and armed.trigger in data:
                self._armed = _Armed()
                taken: _Armed | None = armed
            else:
                taken = None
        return taken

    def _accept(self) -> None:
        while not self._closed.is_set():
            try:
                client, _ = self._listener.accept()
            except OSError:
                break
            upstream = socket.create_connection(self._upstream)
            with self._lock:
                self._sockets.extend((client, upstream))
            threading.Thread(target=self._pump, args=(client, upstream), daemon=True).start()

    def _pump(self, client: socket.socket, upstream: socket.socket) -> None:
        with contextlib.suppress(OSError):
            while data := client.recv(_CHUNK):
                if (armed := self._take(data)) is None:
                    upstream.sendall(data)
                    client.sendall(self._reply(upstream))
                elif armed.fault is Fault.REFUSE:
                    armed.fired.set()
                    client.sendall(armed.refusal)
                elif armed.fault is Fault.LOSE_REPLY:
                    upstream.sendall(data)
                    self._reply(upstream)
                    armed.fired.set()
                    break
                else:
                    upstream.sendall(data)
                    reply = self._reply(upstream)
                    armed.fired.set()
                    time.sleep(armed.delay_s)
                    client.sendall(reply)
        for end in (client, upstream):
            with contextlib.suppress(OSError):
                end.close()

    @staticmethod
    def _reply(upstream: socket.socket) -> bytes:
        # One request, one reply: the driver sends a command and waits for its
        # answer before sending another on the same connection, and every reply
        # a test provokes fits in what arrives before the server goes quiet.
        upstream.settimeout(5)
        reply = upstream.recv(_CHUNK)
        upstream.settimeout(0.05)
        with contextlib.suppress(TimeoutError):
            while more := upstream.recv(_CHUNK):
                reply += more
        upstream.settimeout(None)
        return reply

    def close(self) -> None:
        """Stop accepting and drop every proxied connection."""
        self._closed.set()
        self._listener.close()
        with self._lock:
            sockets = list(self._sockets)
        for end in sockets:
            with contextlib.suppress(OSError):
                end.close()


@contextlib.contextmanager
def faulty_proxy(upstream: str) -> Iterator[FaultyProxy]:
    """A :class:`FaultyProxy` for the duration of a block."""
    proxy = FaultyProxy(upstream)
    try:
        yield proxy
    finally:
        proxy.close()


if __name__ == "__main__":
    pass
else:
    pass
