#!/usr/bin/env python3
"""
Group membership registry process.

    python registry.py          # listens on REGISTRY_PORT (5010)

Groups used by the parking system:
    lot_servers    every lot server joins; summaries are sent to this group
"""

import argparse

from comm.group import Registry
from common import REGISTRY_PORT, MessageLog


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=REGISTRY_PORT)
    args = ap.parse_args()
    Registry("registry", args.port, MessageLog("registry")).serve_forever()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
