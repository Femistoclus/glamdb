"""Retain abandoned transport owners until explicit client execution resumes."""
from collections import deque
from threading import RLock
import weakref

from .errors import TransportError

# Retain resources even if both their wrapper and client become unreachable.
# Only normal execution may release them; finalizers only append a ticket.
_OWNERS = set()


class _Cleanup:
    def __init__(self, queue, cleanup):
        self.queue = queue
        self.cleanup = cleanup
        self.finalizer = None

    def detach(self):
        self.queue.discard(self)


class CleanupQueue:
    def __init__(self):
        self._pending = deque()
        self._registered = {}
        self._lock = RLock()
        self._draining = False
        self._closed = False

    def register(self, owner, cleanup):
        with self._lock:
            if self._closed:
                cleanup()
                raise TransportError("client is closed")
            ticket = _Cleanup(self, cleanup)
            ticket.finalizer = weakref.finalize(owner, self._enqueue, ticket)
            ticket.finalizer.atexit = False
            self._registered[ticket] = None
            _OWNERS.add(ticket)
            return ticket

    def _enqueue(self, ticket):
        # No locks, owner destruction, response close or network calls under GC.
        self._pending.append(ticket)

    def _claim(self, ticket):
        self._registered.pop(ticket, None)
        ticket.finalizer.detach()
        cleanup, ticket.cleanup = ticket.cleanup, None
        _OWNERS.discard(ticket)
        return cleanup

    def discard(self, ticket):
        with self._lock:
            self._claim(ticket)

    def _run(self, ticket):
        cleanup = self._claim(ticket)
        if cleanup is not None:
            try:
                cleanup()
            except BaseException:
                pass  # Abandoned cleanup must not replace the caller's result.

    def drain(self):
        with self._lock:
            if self._draining:
                return  # Cancellation can recursively enter the same client's _send.
            self._draining = True
            try:
                while self._pending:
                    self._run(self._pending.popleft())
            finally:
                self._draining = False

    def close(self):
        with self._lock:
            self._closed = True
            self._draining = True
            try:
                # Claim all registered owners, including wrappers collected after
                # Client.close took its WeakSet snapshot. Outer paged wrappers
                # close their inner owners first and unregister those tickets.
                for ticket in reversed(tuple(self._registered)):
                    self._run(ticket)
                self._pending.clear()
            finally:
                self._draining = False
