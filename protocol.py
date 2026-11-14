"""Shared exception identity and control markers; no evaluation behavior."""


class Fault(Exception):
    pass


class _Shutdown:
    """Sentinel for deterministic, non-GC shutdown of relation pipelines.

    The coordinator sends this marker to release an abandoned relation.
    It is data only: the kernels own the rules that react to it.
    """


SHUTDOWN = _Shutdown()
