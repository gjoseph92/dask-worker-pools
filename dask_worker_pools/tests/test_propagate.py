from __future__ import annotations

import dask
import dask.array as da

from ..interface import pool, propagate_pools
from ..propagate import propagate_pool_optimization
from ..utils_test import assert_pools, hlg_layer_name, lr


def test_propagate_basic():
    with pool("a"):
        a = da.zeros(10)
    a2 = a + 1

    b = da.ones(10)
    c = a2 + b
    d = c + 1

    opt = propagate_pool_optimization(d.dask)
    assert_pools(
        opt,
        {lr(a): "a", lr(a2): "a", lr(b): None, lr(c): "a", lr(d): "a"},
    )


def test_optimize():
    with pool("a"):
        a = da.zeros((10, 10))

    with pool("b"):
        b = da.ones(10)

    c = a + b
    d = b - 1

    with propagate_pools():
        opt_c, opt_d = dask.optimize(c, d)
        assert opt_c.dask is opt_d.dask
        dsk = opt_c.dask

        assert_pools(
            dsk,
            {
                hlg_layer_name(dsk, "zeros"): "a",
                hlg_layer_name(dsk, "ones"): "b",
                hlg_layer_name(dsk, "add"): "a",
                hlg_layer_name(dsk, "sub"): "b",
            },
        )
