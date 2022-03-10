from __future__ import annotations

import functools
import math
import operator
from hashlib import md5
from typing import Any, Mapping, NewType, Sequence

from dask.highlevelgraph import HighLevelGraph, Layer

Pool = NewType("Pool", str)
POOL_PREFIX = "pool-"


def get_pool(obj: Any) -> Pool | None:
    "Get the worker pool assigned to a Dask collection"
    try:
        dsk = obj.__dask_graph__()
    except AttributeError:
        raise TypeError(f"{obj} is not a dask collection") from None

    if not isinstance(dsk, HighLevelGraph):
        raise ValueError(
            f"The graph of {obj} is not a HighLevelGraph, so it cannot have worker pool annotations"
        )
    layer = obj.__dask_layers__()[0]
    return get_layer_pool(dsk.layers[layer])


def get_layer_pool(lyr: Layer) -> Pool | None:
    "Current worker pool of a Layer"
    if not lyr.annotations:
        return None
    try:
        resources: dict[str, Any] = lyr.annotations["resources"]
    except KeyError:
        return None

    pools = [Pool(r) for r in resources if r.lower().startswith(POOL_PREFIX)]
    if not pools:
        return None
    if len(pools) > 1:
        raise ValueError(f"Layer {lyr} has multiple worker pools: {pools}")
    return pools[0]


def layer_bytes_array(collection_annotations: Mapping[str, Any]) -> int | None:
    try:
        shape: Sequence[int | float] = collection_annotations["shape"]
        dtype = collection_annotations["dtype"]
    except KeyError:
        return None

    size = functools.reduce(operator.mul, shape, 1)
    if math.isnan(size):
        return None
    return size * dtype.itemsize


def layer_bytes_dataframe(collection_annotations: Mapping[str, Any]) -> int | None:
    try:
        npartitions: int = collection_annotations["npartitions"]
        series_dtypes: dict = collection_annotations["series_dtypes"]
    except KeyError:
        return None

    # TODO number of rows is unknown!!
    psize = sum(
        dt.itemsize if dt.kind != "o" else 1024 for dt in series_dtypes.values()
    )
    return psize * npartitions


def layer_bytes(lyr: Layer) -> int | None:
    ca = lyr.collection_annotations
    if not ca:
        return None
    return layer_bytes_array(ca) or layer_bytes_dataframe(ca)


# Copied from https://github.com/dask/distributed/blob/936fba5/distributed/utils.py#L1100-L1126
palette = [
    "#440154",
    "#471669",
    "#472A79",
    "#433C84",
    "#3C4D8A",
    "#355D8C",
    "#2E6C8E",
    "#287A8E",
    "#23898D",
    "#1E978A",
    "#20A585",
    "#2EB27C",
    "#45BF6F",
    "#64CB5D",
    "#88D547",
    "#AFDC2E",
    "#D7E219",
    "#FDE724",
]


@functools.cache
def color_of(x):
    h = md5(str(x).encode())
    n = int(h.hexdigest()[:8], 16)
    return palette[n % len(palette)]
