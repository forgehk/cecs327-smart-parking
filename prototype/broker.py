#!/usr/bin/env python3
"""
Pub-sub broker process.

    python broker.py            # listens on BROKER_PORT (5000)

Topics used by the parking system:
    lot/<lot>/sensor   sensor node -> lot server   (events and heartbeats)
    lot/<lot>/status   lot server -> anyone        (space state changes)
"""

import argparse

from comm.pubsub import Broker
from common import BROKER_PORT, MessageLog


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=BROKER_PORT)
    args = ap.parse_args()
    Broker("broker", args.port, MessageLog("broker")).serve_forever()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
