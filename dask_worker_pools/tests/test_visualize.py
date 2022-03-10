from pathlib import Path

import dask.array as da

from ..interface import pool, visualize_pools


def test_visualize(tmp_path: Path):
    # Just test that it doesn't fail
    with pool("A"):
        a = da.random.random((10, 10))

    with pool("B"):
        b = da.random.random(10)

    run_in_a = (a - 1).sum()
    run_in_b = b - a.mean()

    visualize_pools(run_in_a, filename=tmp_path / "a.svg")
    assert (tmp_path / "a.svg").exists()

    visualize_pools(run_in_a, run_in_b, filename=tmp_path / "both.svg")
    assert (tmp_path / "both.svg").exists()

    visualize_pools(run_in_a, run_in_b.dask, filename=tmp_path / "both-hlg.svg")
    assert (tmp_path / "both-hlg.svg").exists()
