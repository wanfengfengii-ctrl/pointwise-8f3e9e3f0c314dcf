"""Request-routing coordinator.

Owns exclusively: live generators, opaque relation handles, request
routing, fault injection and trace collection. It contains no query rules:
expression requests are answered by the expression kernel module, relation
requests by the relation kernel module. Both kernels are referenced through
module attributes (``exprkernel.evaluate`` / ``relationkernel.execute``) so
either can be replaced to verify routing.

Fault discipline. A business fault is never raised across an abandoned
generator. When servicing an expression request, a fault coming back from
a relation is injected into the live expression generator with ``throw``;
the coordinator then keeps driving that same generator, servicing the
cleanup requests its ``finally`` blocks emit (the mandated subquery
``close`` requests and ``trace`` events) until the generator re-raises the
fault and terminates. Standard ``yield from`` delegation in the kernel
carries the throw through every nested expression frame, so each one
unwinds in order. Any relation handles still alive afterwards are released
with the explicit shutdown sentinel. No generator is therefore ever left
suspended at a yield for the garbage collector.
"""
import exprkernel
import relationkernel
from protocol import Fault, SHUTDOWN as _SHUTDOWN


class _Handle:
    """Opaque carrier; the expression kernel only passes it back verbatim."""
    __slots__ = ('generator', 'parent', 'children', 'active')

    def __init__(self, generator, parent):
        self.generator = generator
        self.parent = parent
        self.children = []
        self.active = True


class _Driver:
    def __init__(self, tables, params):
        self._tables = tables
        self._params = params
        self._trace = []
        self._handles = set()
        self._active_relations = []

    def _drive_expression(self, generator):
        """Service one expression generator until it returns a scalar.

        A fault returned from a relation is thrown back into this
        generator and the resulting cleanup requests are serviced here, so
        the generator always terminates (re-raising the fault) rather than
        being abandoned mid-yield.
        """
        reply = None
        fault = None
        while True:
            try:
                if fault is not None:
                    request = generator.throw(fault)
                else:
                    request = generator.send(reply)
            except StopIteration as stop:
                return stop.value
            reply = None
            fault = None
            kind = request[0]
            if kind == 'trace':
                self._trace.append(request[1])
            elif kind == 'open':
                _, query, outer, existence = request
                parent = (self._active_relations[-1]
                          if self._active_relations else None)
                handle = _Handle(
                    relationkernel.execute(
                        query, outer, self._tables, existence),
                    parent)
                if parent is not None:
                    parent.children.append(handle)
                self._handles.add(handle)
                reply = handle
            elif kind == 'next':
                handle = request[1]
                reply, fault = self._advance_relation(handle)
            else:  # 'close'
                self._close_handle(request[1])

    def _advance_relation(self, handle):
        """Pull until the relation delivers a row or ends.

        Returns (reply, fault); a non-none fault is to be injected into the
        requesting expression generator. Faults from deeper expression
        frames have already completed those frames' own cleanup.
        """
        self._active_relations.append(handle)
        reply = None
        fault = None
        try:
            while True:
                try:
                    if fault is not None:
                        request = handle.generator.throw(fault)
                    else:
                        request = handle.generator.send(reply)
                except StopIteration:
                    handle.active = False
                    self._handles.discard(handle)
                    return ('end',), None
                except Fault as exc:
                    # The relation itself surfaced a fault (e.g. a table
                    # error raised between requests). Hand it upward for
                    # injection into the requesting expression.
                    return None, exc
                reply = None
                fault = None
                kind = request[0]
                if kind == 'trace':
                    self._trace.append(request[1])
                elif kind == 'eval':
                    _, expression, env, group, aggregating = request
                    try:
                        reply = self._drive_expression(
                            exprkernel.evaluate(
                                expression, env, self._params,
                                group, aggregating))
                    except Fault as exc:
                        fault = exc
                elif kind == 'row':
                    return ('row', request[1]), None
                else:
                    raise AssertionError(
                        'unexpected relation request: ' + repr(request))
        finally:
            self._active_relations.remove(handle)

    def _close_handle(self, handle):
        """Shut down a relation and descendants it has not finished.

        Closure is an explicit business-level shutdown (a sentinel routed
        through the relation pipeline), not generator garbage collection:
        it performs no reads, evaluates no expressions and adds no trace
        events. Depth-first order mirrors how expression opens nest.
        """
        if not handle.active:
            return
        handle.active = False
        for child in list(handle.children):
            self._close_handle(child)
        handle.children.clear()
        self._handles.discard(handle)
        try:
            handle.generator.send(_SHUTDOWN)
        except StopIteration:
            pass
        except Exception:
            try:
                handle.generator.close()
            except Exception:
                pass

    def _close_all(self):
        for handle in list(self._handles):
            self._close_handle(handle)

    def run(self, query):
        root = _Handle(
            relationkernel.execute(query, [], self._tables), None)
        self._handles.add(root)
        rows = []
        try:
            while True:
                reply, fault = self._advance_relation(root)
                if fault is not None:
                    raise fault
                if reply[0] == 'end':
                    break
                rows.append(reply[1])
            return {
                'columns': [pair[0] for pair in query['select']],
                'rows': rows,
                'trace': self._trace,
            }
        except Fault as exc:
            return {'error': str(exc), 'trace': self._trace}
        finally:
            self._close_all()


def run(query, tables, params=None):
    return _Driver(tables, {} if params is None else params).run(query)
