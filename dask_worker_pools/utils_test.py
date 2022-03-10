from __future__ import annotations

from dask.highlevelgraph import HighLevelGraph

from .helpers import POOL_PREFIX
from .propagate import get_layer_pool


def assert_pools(dsk: HighLevelGraph, expected: dict[str, str | None]):
    actual = {name: get_layer_pool(lyr) for name, lyr in dsk.layers.items()}
    exp = {x: POOL_PREFIX + p if p else None for x, p in expected.items()}
    assert actual == exp


def lr(dask_collection) -> str:
    "Layer name of a dask collection"
    return dask_collection.__dask_layers__()[0]


def hlg_layer_name(hlg: HighLevelGraph, prefix: str) -> str:
    "Get the first layer name from a HighLevelGraph whose name starts with a prefix"
    for key in hlg.layers:
        if key.startswith(prefix):
            return key
    raise KeyError(f"No layer starts with {prefix!r}: {list(hlg.layers)}")
