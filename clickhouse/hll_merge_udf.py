#!/usr/bin/env python
"""ClickHouse executable UDF: union Apache DataSketches HLL sketches at query time.

ClickHouse has no user-defined *aggregate* functions, so the usable shape is
groupArray() -> scalar UDF:

    SELECT hllMergeEstimate(arrayStringConcat(groupArray(base64Encode(users_sketch)), ','))
    FROM wikipedia_rollup_hll WHERE grain = 'hour_dims'

Protocol: TabSeparated. One input line per row, holding a comma-separated list of
base64-encoded sketches (base64's alphabet contains no comma or tab, so this is safe).
One output line per input line with the merged estimate. Must stay line-for-line aligned
with the input or ClickHouse will error.

lgK and target type come from each sketch's preamble; nothing is assumed.
"""

import base64
import sys

from datasketches import hll_sketch, hll_union, tgt_hll_type

TGT_TYPES = {0: tgt_hll_type.HLL_4, 1: tgt_hll_type.HLL_6, 2: tgt_hll_type.HLL_8}


def merge(blobs: list[bytes]) -> float:
    if not blobs:
        return 0.0
    lg_k = blobs[0][3]            # preamble byte 3
    tgt = TGT_TYPES[(blobs[0][7] >> 2) & 0x03]
    u = hll_union(lg_k)
    for b in blobs:
        u.update(hll_sketch.deserialize(b))
    return u.get_result(tgt).get_estimate()


def main() -> None:
    for line in sys.stdin:
        line = line.rstrip("\n")
        try:
            blobs = [base64.b64decode(p) for p in line.split(",") if p]
            print(f"{merge(blobs):.6f}", flush=True)
        except Exception as exc:  # never die: a crash aborts the whole query
            print(f"-1", flush=True)
            print(f"hll_merge_udf: {exc}", file=sys.stderr)


if __name__ == "__main__":
    main()
