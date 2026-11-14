"""Compatibility facade.

The fused interpreter was split into independently drivable generator
kernels (``exprkernel`` and ``relationkernel``) coordinated by
``coordinator``. This module only forwards the legacy entry point and keeps
no interpreter or fallback execution path.
"""
from coordinator import run as _coordinated_run


def run(query, tables, params=None):
    return _coordinated_run(query, tables, params)
