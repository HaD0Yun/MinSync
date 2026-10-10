# Bulk-commit follow-up (batch_files = 25) vs earlier builds

Fake TEI embedder (dim 16, 0 ms), release builds, macOS arm64, 600 files (~8 chunks each) unless noted.
`main` = 9ff586f (before #71), `v1` = PR #72 (per-file commits), `v2` = this change.

| Scenario | main | v1 (#72) | v2 | v2 vs v1 | v2 vs main |
|---|---:|---:|---:|---:|---:|
| initial, N=600 (median of 3) | 32.50 s | 13.46 s | 1.57 s | 8.6x | 20.7x |
| incremental 30% edited, N=600 (median of 3) | 65.88 s | 7.80 s | 1.16 s | 6.7x | 56.8x |
| fragmented store (~2440 frag / ~3070 ver), N=600 | 178.62 s | n/a | 15.40 s | n/a | 11.6x |
| initial, N=2000 | 327.73 s | n/a | 4.75 s | n/a | 69.0x |
| incremental 30% edited, N=2000 | 630.50 s | n/a | 8.10 s | n/a | 77.8x |
| resume after SIGKILL | n/a | 1.000x work | 1.042x work (one batch redone) | | |

Final (path, chunk id) sets identical between binaries in every scenario (4798 rows at N=600, 15994 at N=2000).
Table versions after the N=600 initial sync: main 1806, v1 630, v2 42. After N=2000: main 6006, v2 128.
Profile before the change (v1, N=600 initial, 17.4 s): upsert 5.2 s, fetch 2.4 s, progress fsync 5.7 s, delete sweep 1.4 s, checkpoints 1.3 s, embed 0.3 s.
