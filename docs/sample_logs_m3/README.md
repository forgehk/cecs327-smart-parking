# Milestone 3 sample run

Produced by `run_demo.sh` (lots A to D), then lot E joining through C, then C leaving
gracefully (`kill`), then D crashing (`kill -9`).

| file | what |
|---|---|
| `client_session.txt` | every client command run during the session with its output |
| `overlay_trace.txt` | the P2P lines from all five lot server consoles, in time order |
| `console_lot_server_*.txt` | full console of each lot server |
| `p2p_metrics_four_lots.txt` | `p2p_metrics.py` with A to D running: degree, diameter, 12 owner-only lookups |
| `p2p_metrics_after_leave_and_crash.txt` | the same after C left and D crashed: A, B, E still one mesh |
| `lot_server_*.p2p.log` | first 60 P2P message log lines of A, B and E (JSON lines, one per sent or received message) |
