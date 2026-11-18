"""
The shared resource (Milestone 4): the campus overflow garage.

Spaces O-01 .. O-05 belong to no lot. Any lot server may give one to a driver
whose own lot is full, so every lot server reads and writes the same table:
shared/overflow.json, standing in for a shared campus database.

    {"capacity": 5, "version": 7,
     "spaces": {"O-01": {"client": "alice", "by": "lot_server_A", "at": ...}, "O-02": null, ...},
     "history": [{"version": 7, "op": "assign", "space": "O-01", "client": "alice", "by": "lot_server_A"}, ...]}

Assigning a space is read -> pick the first free space -> write. The read and
the write are WORK_TIME apart (the round trip to a slow shared database). If two
lot servers do that at the same time, both read the same free space, both write
it with their own driver, and the second write silently replaces the first:
two drivers are told they have O-01 and only one of them is in the table, and
both writes carry the same new version number. That is why an assignment must
run inside the distributed mutual exclusion (coord/mutex.py), and why
`no_lock=True` exists only to show what goes wrong without it.

Writes go to a temporary file first and are then renamed over the table, so a
reader never sees half a file. (Windows briefly refuses the rename while
someone else has the file open, so both sides retry for a moment.)
"""

import json
import os
import threading
import time

from common import OVERFLOW_FILE, OVERFLOW_SPACES, OVERFLOW_WORK_TIME, now

HISTORY_KEEP = 200


def space_ids(capacity):
    return ["O-%02d" % i for i in range(1, capacity + 1)]


class OverflowTable:
    def __init__(self, path=OVERFLOW_FILE, capacity=OVERFLOW_SPACES, work_time=OVERFLOW_WORK_TIME):
        self.path = path
        self.capacity = capacity
        self.work_time = work_time
        os.makedirs(os.path.dirname(path), exist_ok=True)

    # ------------------------------------------------------------------ file access

    def _empty(self, capacity=None):
        capacity = capacity or self.capacity
        return {"capacity": capacity, "version": 0, "spaces": {s: None for s in space_ids(capacity)}, "history": []}

    def read(self):
        for _ in range(100):
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except FileNotFoundError:
                return self._empty()
            except (PermissionError, json.JSONDecodeError):
                time.sleep(0.01)            # someone is replacing the file right now
        raise OSError("could not read %s" % self.path)

    def _write(self, table):
        tmp = "%s.%d.%d.tmp" % (self.path, os.getpid(), threading.get_ident())
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(table, f, indent=1)
        for _ in range(100):
            try:
                os.replace(tmp, self.path)
                return
            except PermissionError:
                time.sleep(0.01)
        os.remove(tmp)
        raise OSError("could not write %s" % self.path)

    # ------------------------------------------------------------------ operations (call inside the lock)

    def reset(self, by, capacity=None):
        t = self._empty(capacity)
        t["history"].append({"version": 0, "op": "reset", "by": by, "at": round(now(), 3)})
        self._write(t)
        return t

    def assign(self, client, by):
        """Give `client` an overflow space. Returns (space or None, reason, version written)."""
        t = self.read()
        for sid, h in sorted(t["spaces"].items()):
            if h and h["client"] == client:
                return sid, "already yours", t["version"]
        free = [sid for sid, h in sorted(t["spaces"].items()) if h is None]
        if not free:
            return None, "overflow garage full", t["version"]
        sid = free[0]
        time.sleep(self.work_time)          # the slow part: between our read and our write
        t["spaces"][sid] = {"client": client, "by": by, "at": round(now(), 3)}
        return sid, "assigned", self._commit(t, {"op": "assign", "space": sid, "client": client, "by": by})

    def release(self, client, by):
        """Give back `client`'s overflow space. Returns (space or None, reason, version written)."""
        t = self.read()
        mine = [sid for sid, h in sorted(t["spaces"].items()) if h and h["client"] == client]
        if not mine:
            return None, "no overflow space held by %s" % client, t["version"]
        sid = mine[0]
        time.sleep(self.work_time)
        t["spaces"][sid] = None
        return sid, "released", self._commit(t, {"op": "release", "space": sid, "client": client, "by": by})

    def _commit(self, t, entry):
        t["version"] += 1
        entry.update(version=t["version"], at=round(now(), 3))
        t["history"] = (t["history"] + [entry])[-HISTORY_KEEP:]
        self._write(t)
        return t["version"]

    def summary(self):
        t = self.read()
        taken = {sid: h["client"] for sid, h in sorted(t["spaces"].items()) if h}
        return {"capacity": t["capacity"], "version": t["version"], "taken": taken,
                "free": [sid for sid, h in sorted(t["spaces"].items()) if h is None]}
