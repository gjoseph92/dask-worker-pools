from __future__ import annotations

import copy
from collections import Counter
from typing import Iterable

from dask.highlevelgraph import HighLevelGraph

from .helpers import Pool, get_layer_pool, layer_bytes


def pick_pool(
    deps_pools: Iterable[tuple[str, Pool | None]], dsk: HighLevelGraph
) -> Pool | None:
    """
    Pick which worker pool a layer should use to minimize data transfer.

    Tries to select the pool that will already be holding the most total bytes, summed
    across all dependencies.

    If bytes per layer cannot be calculated for all layers (only works on Array or
    DataFrame layers), selects the pool that will already be holding the greatest number
    of keys, summed across all dependencies.

    Input layers that do not define a pool are ignored in selection.
    """
    if not deps_pools:
        return None

    pool_bytes: Counter[Pool] = Counter()
    pool_key_counts: Counter[Pool] = Counter()
    unknown_sizes: bool = False

    # TODO fastpath when only one input
    for dep, pool in deps_pools:
        if not pool:
            continue

        lyr = dsk.layers[dep]
        pool_key_counts[pool] += len(lyr)
        # ^ NOTE: len not technically part of Layer interface

        if not unknown_sizes:
            size = layer_bytes(lyr)
            if size is None:
                unknown_sizes = True
            else:
                pool_bytes[pool] += size

    if not pool_key_counts:
        return None

    if unknown_sizes:
        costs = pool_key_counts.most_common()
    costs = pool_bytes.most_common()

    (biggest_pool, biggest_cost), *other_costs = costs
    transfer_to_biggest_pool = sum((c[1] for c in other_costs), 0)

    total_transfer = transfer_to_biggest_pool + biggest_cost
    n_pools = len(pool_key_counts)
    total_transfer_if_mixed = total_transfer * ((n_pools - 1) / n_pools)
    # ^ Does this metric make sense? Assuming each task needs an equal number of dependencies
    # from each pool, and dependencies are equally sized, so tasks will balance equally
    # between pools. We could do better than assume this. But might require looking at
    # individual task structure?
    # TODO this metric is wrong for linear chains. Also for 2 deps like `a + b`, if `b`
    # is even 1 less than `a`, everything will go to `a`. Need some sort of parallelization
    # bonus in these cases?

    # Is the cost of moving everything else to one pool less than the cost of letting
    # them all mix?
    if transfer_to_biggest_pool < total_transfer_if_mixed:
        return biggest_pool
    return None


def set_pool(lyr_name: str, dsk: HighLevelGraph, pool: Pool) -> None:
    "Mutate `dsk`, setting a worker pool annotation on a copy of the layer at ``lyr_name``"
    lyr = copy.copy(dsk.layers[lyr_name])
    dsk.layers[lyr_name] = lyr  # type: ignore
    # ^ `dsk.layers` defined as a `Mapping`, not `MutableMapping`

    if lyr.annotations is None:
        anno = lyr.annotations = {}
    else:
        anno = dict(lyr.annotations)

    try:
        resources = dict(anno["resources"])
    except KeyError:
        resources = {}

    resources[pool] = 1
    anno["resources"] = resources


def _propagate_pool_recursive(lyr_name: str, dsk: HighLevelGraph) -> Pool | None:
    """
    Recursively traverse the HLG, mutating layers to use a pool based on their dependencies

    Parameters
    ----------
    lyr_name:
        The layer to start at
    dsk:
        HighLevelGraph to add pool annotations to.
        Will be mutated: copies of layers will be inserted, with their
        annotations changed.

    Returns
    -------
    The name of the pool (or None) used for ``lyr_name``
    """
    lyr = dsk.layers[lyr_name]
    if pool := get_layer_pool(lyr):
        # Base case: layer already has pool set, so respect it. Assume all its inputs are already fine.
        return pool

    deps = dsk.dependencies[lyr_name]
    if not deps:
        # Base case: root layer. If we've gotten here, the root layer has no pool.
        return None

    input_pools = [_propagate_pool_recursive(dep, dsk) for dep in deps]
    new_pool = pick_pool(zip(deps, input_pools), dsk)
    if new_pool:
        set_pool(lyr_name, dsk, new_pool)
    return new_pool


def propagate_pool_optimization(dsk: HighLevelGraph, keys=()) -> HighLevelGraph:
    """
    Optimization function: propagate worker pool annotations to downstream layers.
    """
    dsk = dsk.copy()
    dependents = dsk.dependents
    leaves = [k for k in dsk.layers if not dependents.get(k)]
    for lyr in leaves:
        _propagate_pool_recursive(lyr, dsk)
    return dsk
