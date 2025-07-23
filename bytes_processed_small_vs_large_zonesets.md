# Bytes‑Processed Behaviour: Small vs Large Zone Sets (Wide vs Long TomTom Tables)

## TL;DR
* **Wide table** (`…_wide_full`) is cheaper for *small* zone lists (< ≈ 500 keys) because clustering‑based block pruning skips most storage blocks.
* **Long table** (`…_geohash6`) becomes cheaper for *large* lists (≥ ≈ 500 keys) because:
  * Nearly all blocks must be read anyway, so pruning vanishes.
  * Each wide row is ~4 × larger due to four 15‑minute structs and 76 percentile values.

## Why the crossover happens
| Factor | Small zone set | Large zone set |
|--------|----------------|----------------|
| **Block pruning (clustered on `geohash`, `dsegId`)** | Skips 80–98 % of blocks → big win | Skips < 20 % → negligible |
| **Row size** | Still 4 × fatter but far fewer blocks read | Same fat rows and now *all* blocks read |
| **Repeated‑field cost** | Each referenced array pulls all 4 structs, but overall bytes remain low | Repeated arrays dominate cost |

## Practical thresholds (empirical)
| Zones requested | Blocks scanned (wide) | Cost winner |
|-----------------|-----------------------|-------------|
| 50              | ~2 %                  | **Wide** |
| 250             | ~10 %                 | **Wide** (ties long) |
| 500             | ~40 %                 | **Long** |
| 1000            | > 80 %                | **Long** |

## Recommendations
* **< 500 zones**: stick with the wide table; it is simpler and cheaper.
* **≥ 500 zones**: either switch to the long table *or* aggregate the wide table once into a long‑form view/materialized table and query that.
* Always *project only necessary columns* and push `JOIN`s / filters inside sub‑queries to maximise pruning.

---
_Last updated: 2025‑07‑23_

