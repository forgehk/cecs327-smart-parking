"""
Lamport logical clocks (Milestone 4).

Every process keeps one counter C. Wall clocks on different machines drift and
can't order events between processes; C can, for any two events that are
causally related.

    local event / send   C = C + 1            the message carries "lamport": C
    receive              C = max(C, Cmsg) + 1

So if event a happened before event b (same process earlier, or a is the send
of the message b receives), then C(a) < C(b). Ties between unrelated events are
broken by process name, which gives one total order over every log:
sort by (lamport, process).

Where the rules run, so no call site has to remember them:

    send      envelope.make() stamps every new message from the sender's clock,
              and stamp() re-stamps a message that is sent again (a retry).
    receive   MessageLog.record("recv", ...) applies the receive rule. Every
              receive in comm/ and p2p/ is logged right after recv_msg(), before
              the message is handled, so a reply always has a later time than the
              request that caused it.

Clocks are kept per process name, so several nodes in one Python process (the
unit tests) still each have their own clock.
"""

import threading


class LamportClock:
    def __init__(self):
        self._value = 0
        self._lock = threading.Lock()

    def tick(self):
        """A local event (including a send). Returns the new time."""
        with self._lock:
            self._value += 1
            return self._value

    def send(self):
        """Time to put on an outgoing message."""
        return self.tick()

    def receive(self, remote):
        """Merge the time carried by an incoming message. Returns the new time."""
        try:
            remote = int(remote)
        except (TypeError, ValueError):
            remote = 0                      # no usable stamp: still counts as an event
        with self._lock:
            self._value = max(self._value, remote) + 1
            return self._value

    def now(self):
        """Current value, without counting an event."""
        with self._lock:
            return self._value

    def restore(self, value):
        """After a crash: continue from the highest time found in our own log, so
        events after the restart never get smaller times than events before it."""
        with self._lock:
            self._value = max(self._value, int(value))
            return self._value


_clocks = {}
_clocks_lock = threading.Lock()


def clock_for(process):
    """The clock of one process (lot_server_A, client_alice, broker, ...)."""
    with _clocks_lock:
        c = _clocks.get(process)
        if c is None:
            c = _clocks[process] = LamportClock()
        return c
