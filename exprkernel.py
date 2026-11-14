"""Expression generator kernel.

Owns exclusively: name resolution, scalar operators, short-circuit logic,
aggregate parameter evaluation and the consumption rules of subqueries.

It never receives tables, reads a table, creates a relation executor or
imports the relation kernel or the coordinator. Every table/relation access
leaves this kernel as an ``open``/``next``/``close`` request and every trace
entry leaves it as a ``trace`` request; the driving coordinator answers
them.

``evaluate`` is a generator. Start it with ``next`` and answer each yielded
request with ``send`` (or inject a business failure with
``throw(protocol.Fault(...))``). The scalar result is delivered through
``StopIteration.value``.

Nested expression generators use standard ``yield from`` delegation. This
matters for fault semantics: when the coordinator throws a business fault
into the live generator, the throw travels down every delegated frame to
the active yield; each frame's ``finally`` (the mandated subquery
``close``) then bubbles its request back up the same delegation chain. As
long as the coordinator keeps servicing those requests instead of
abandoning the generator, every nested frame unwinds to completion and no
generator is left suspended at a yield.
"""
from protocol import Fault


def _truth(value):
    if value is None:
        return None
    if type(value) is not bool:
        raise Fault('BOOL_REQUIRED')
    return value


def _column(env, ref):
    for frame in env:
        if len(ref) == 2:
            hits = [row[i] for alias, names, row in frame
                    for i, name in enumerate(names) if name == ref[1]]
        else:
            bindings = [b for b in frame if b[0] == ref[1]]
            if not bindings:
                continue
            if len(bindings) != 1:
                raise Fault('AMBIGUOUS:' + '.'.join(ref[1:]))
            _, names, row = bindings[0]
            hits = [row[i] for i, name in enumerate(names) if name == ref[2]]
            if not hits:
                raise Fault('UNKNOWN:' + '.'.join(ref[1:]))
        if hits:
            if len(hits) != 1:
                raise Fault('AMBIGUOUS:' + '.'.join(ref[1:]))
            return hits[0]
    raise Fault('UNKNOWN:' + '.'.join(ref[1:]))


def evaluate(e, env, params, group=None, aggregating=False):
    """Yield kernel requests; return the computed scalar on exhaustion."""
    tag = e[0]
    if tag == 'lit':
        return e[1]
    if tag == 'col':
        return _column(env, e)
    if tag == 'param':
        if e[1] not in params:
            raise Fault('PARAM:' + e[1])
        return params[e[1]]
    if tag == 'if':
        condition = yield from evaluate(
            e[1], env, params, group, aggregating)
        branch = e[2] if _truth(condition) is True else e[3]
        return (yield from evaluate(
            branch, env, params, group, aggregating))
    if tag == 'coalesce':
        for part in e[1:]:
            value = yield from evaluate(part, env, params,
                                        group, aggregating)
            if value is not None:
                return value
        return None
    if tag == 'not':
        value = _truth((yield from evaluate(
            e[1], env, params, group, aggregating)))
        return None if value is None else not value
    if tag in ('scalar', 'exists', 'in'):
        # IN must evaluate the needle even for an empty right side.
        if tag == 'in':
            needle = yield from evaluate(
                e[1], env, params, group, aggregating)
        else:
            needle = None
        sub = e[2] if tag == 'in' else e[1]
        yield ('trace', ['subquery', tag])
        if tag != 'exists' and len(sub['select']) != 1:
            raise Fault('SUBQUERY_WIDTH')
        # The current environment becomes the subquery's outer frame list;
        # the caller's group and aggregating flag are never passed on.
        # Open is outside the try block so an open failure is not followed
        # by a close; every successful open is closed exactly once,
        # including early matches and injected faults.
        handle = yield ('open', sub, env, tag == 'exists')
        try:
            if tag == 'exists':
                response = yield ('next', handle)
                return response[0] == 'row'
            if tag == 'scalar':
                response = yield ('next', handle)
                if response[0] == 'end':
                    return None
                first = response[1]
                # A fault while projecting the second row precedes the
                # cardinality rejection and no third row is read.
                response = yield ('next', handle)
                if response[0] == 'row':
                    raise Fault('SCALAR_MANY')
                return first[0]
            unknown = False
            while True:
                response = yield ('next', handle)
                if response[0] == 'end':
                    return None if unknown else False
                value = response[1][0]
                if needle is None or value is None:
                    unknown = True
                elif needle == value:
                    return True
        finally:
            yield ('close', handle)
    if tag == 'agg':
        if aggregating:
            raise Fault('NESTED_AGG')
        if group is None:
            raise Fault('NO_GROUP')
        op = e[1]
        values = []
        for member in group:
            # Subqueries inside aggregate arguments open without carrying
            # the caller's group/aggregating context.
            value = yield from evaluate(e[2], member, params, None, True)
            values.append(value)
        nonnull = [v for v in values if v is not None]
        if op == 'count':
            return len(nonnull)
        if op == 'sum':
            if any(type(v) is not int for v in nonnull):
                raise Fault('INT_REQUIRED')
            return sum(nonnull) if nonnull else None
        if op == 'min':
            try:
                return min(nonnull) if nonnull else None
            except TypeError:
                raise Fault('COMPARISON')
        raise Fault('AGG:' + op)
    if tag == 'op':
        op = e[1]
        left = yield from evaluate(e[2], env, params, group, aggregating)
        if op in ('and', 'or'):
            left = _truth(left)
            if op == 'and' and left is False:
                return False
            if op == 'or' and left is True:
                return True
            # An unknown left operand still evaluates the right side.
            right = _truth((yield from evaluate(
                e[3], env, params, group, aggregating)))
            if op == 'and':
                return False if right is False else (
                    None if left is None or right is None else True)
            return True if right is True else (
                None if left is None or right is None else False)
        # Non-short-circuit operators always evaluate the right operand too.
        right = yield from evaluate(e[3], env, params, group, aggregating)
        if left is None or right is None:
            return None
        if op == 'eq':
            return left == right
        if op in ('add', 'div'):
            if type(left) is not int or type(right) is not int:
                raise Fault('INT_REQUIRED')
            if op == 'div' and right == 0:
                raise Fault('DIV_ZERO')
            return left + right if op == 'add' else left // right
        if op == 'lt':
            try:
                return left < right
            except TypeError:
                raise Fault('COMPARISON')
        raise Fault('OP:' + op)
    raise Fault('EXPR:' + tag)
