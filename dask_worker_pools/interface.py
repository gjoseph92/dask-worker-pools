import os
from typing import Any, TypeVar, Union

import dask
from dask.highlevelgraph import HighLevelGraph

from .helpers import POOL_PREFIX, color_of, get_layer_pool
from .propagate import propagate_pool_optimization

T = TypeVar("T", bound=Any)


def pool(name: str):
    "Contextmanager: annotate objects to run in the worker pool ``name``"
    return dask.annotate(resources={POOL_PREFIX + name: 1})


def propagate_pools_in_collection(obj: T) -> T:
    "Propagate worker pool assignments in a dask collection to downstream tasks"
    try:
        dsk = obj.__dask_graph__()
    except AttributeError:
        raise TypeError(f"{obj} is not a dask collection") from None

    hlg = propagate_pool_optimization(dsk)
    rebuild, *args = obj.__dask_postpersist__()
    return rebuild(hlg, *args)


def propagate_pools():
    """
    Contextmanager: propagate worker pools during a `compute` or `persist` call.

    Call as a normal function (not context manager) to activate the propagation
    optimization for all compute/persist calls during this session.

    Also sets ``"optimization.fuse.active": False``, so that annotations aren't lost by
    low-level optimization. See https://github.com/dask/dask/issues/7036 for details.
    """
    return dask.config.set(
        {
            "optimizations": (propagate_pool_optimization,),
            "optimization.fuse.active": False
            # ^ Work around https://github.com/dask/dask/issues/7036
        }
    )


PathLike = Union[str, bytes, os.PathLike]


def visualize_pools(
    *objs: Any, filename: PathLike = "dask-pools.svg", format=None, **kwargs
):
    """
    Visualize the Dask collections' high-level graphs, colored by worker pool assignment.

    Worker pool propagation is automatically applied before visualization.

    Requires both the ``graphviz`` Python package and system library to be installed.

    Parameters
    ----------
    *objs : Dask collection or HighLevelGraph
        Dask objects or HighLevelGraphs to visualize. All graphs are
        merged before pool-propagation.
    filename : str or None, optional
        The name of the file to write to disk. If the provided `filename`
        doesn't include an extension, '.png' will be used by default.
        If `filename` is None, no file will be written, and the graph is
        rendered in the Jupyter notebook only.
    format : {'png', 'pdf', 'dot', 'svg', 'jpeg', 'jpg'}, optional
        Format in which to write output file. Default is 'svg'.
    color : {None, 'layer_type'}, optional (default: None)
        Options to color nodes.
        - None, no colors.
        - layer_type, color nodes based on the layer type.
    **kwargs
    Additional keyword arguments to forward to ``to_graphviz``.

    Examples
    --------
    >>> visualize_pools(x)  # doctest: +SKIP
    >>> visualize_pools(x, filename='pools.png')  # doctest: +SKIP

    Returns
    -------
    result : IPython.diplay.Image, IPython.display.SVG, or None
        See dask.dot.dot_graph for more information.
    """
    from dask.dot import label

    hlgs = []
    for obj in objs:
        if isinstance(obj, HighLevelGraph):
            hlgs.append(obj)
        else:
            try:
                hlgs.append(obj.__dask_graph__())
            except AttributeError:
                raise TypeError(f"{obj} is not a dask collection") from None

    hlg = HighLevelGraph.merge(*hlgs)
    opt = propagate_pool_optimization(hlg)
    label_cache = {}
    attrs = {}
    for name, lyr in opt.layers.items():
        p = get_layer_pool(lyr)
        attrs[name] = a = {
            "label": f"{label(name, label_cache)}\\n{p or 'Any pool'}",
        }
        if p:
            a["color"] = color_of(p)

    return opt.visualize(
        filename=str(filename), format=format, data_attributes=attrs, **kwargs
    )
