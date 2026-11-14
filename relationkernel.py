"""Relation generator kernel.

Owns exclusively: table source reads, lateral source enumeration, outer
join null padding, grouping, projection ordering, distinct, sorting and
limit. It also performs the strict boolean type check and filtering on the
where/having values returned by expression evaluation.

It never evaluates an expression -- not even literals, columns or aggregate
arguments. Every value it needs leaves the generator as an
``('eval', e, env, group, aggregating)`` request and the driver's scalar
reply is used verbatim. Table scans leave it as ``('trace', event)`` and
final result rows as ``('row', row)``.

``execute`` is a generator returning ``None`` at exhaustion. It reuses
itself recursively for lateral derived sources; the rows of such internal
relations are consumed here and never delivered as outer results.

Two event kinds flow between internal pipeline stages: protocol request
tuples (``eval``/``trace``), whose driver reply arrives through ``send``,
and marker carriers (``_Env``/``_Candidate``/``_Row``), which always get a
``None`` reply.

Early termination and error unwind never rely on garbage collection. The
coordinator releases an abandoned relation by sending the shared
``SHUTDOWN`` sentinel; every driving loop treats it as an immediate stop and
its cleanup forwards the same sentinel to the nested stages it owns.
Shutdown therefore pulls no rows, evaluates no expressions and adds no
trace events, and Python's prohibition on yielding during ``GeneratorExit``
never comes into play.
"""
import json

from protocol import Fault, SHUTDOWN as _SHUTDOWN


def key(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False)


def _truth(value):
    if value is None:
        return None
    if type(value) is not bool:
        raise Fault('BOOL_REQUIRED')
    return value


class _Env:
    """Internal carrier for one enumerated environment."""
    __slots__ = ('env',)

    def __init__(self, env):
        self.env = env


class _Candidate:
    """Internal carrier for one (representative env, member group) pair."""
    __slots__ = ('env', 'group')

    def __init__(self, env, group):
        self.env = env
        self.group = group


class _Row:
    """Internal carrier for a projected row before final delivery."""
    __slots__ = ('row',)

    def __init__(self, row):
        self.row = row


_MARKERS = (_Env, _Candidate, _Row)


def _eval(expression, env, group=None, aggregating=False):
    """Build one expression request tuple; callers drive it explicitly."""
    return ('eval', expression, env, group, aggregating)


def _shutdown(generator):
    """Forward the shared shutdown sentinel to an owned stage."""
    try:
        generator.send(_SHUTDOWN)
    except StopIteration:
        pass


def _pull(generator, reply):
    """Send one reply; return the event, or None when the stage ended."""
    try:
        return generator.send(reply)
    except StopIteration:
        return None


def execute(q, outer, tables, existence=False):
    """Yield trace/eval/row requests; return None at exhaustion."""
    if q.get('limit') == 0:
        return

    def source(index, frame):
        if index == len(q.get('from', [])):
            env = [frame] + outer
            where = q.get('where', ['lit', True])
            value = yield _eval(where, env)
            if value is _SHUTDOWN:
                return
            if _truth(value) is True:
                yield _Env(env)
            return
        item = q['from'][index]
        if 'table' in item:
            name = item['table']
            yield ('trace', ['scan', name])
            if name not in tables:
                raise Fault('TABLE:' + name)
            names = tables[name]['columns']

            def table_rows():
                reply = None
                for table_row in tables[name]['rows']:
                    if reply is _SHUTDOWN:
                        return
                    reply = yield _Row(table_row)

            row_stream = table_rows()
        else:
            names = [pair[0] for pair in item['query']['select']]
            # A lateral relation sees the current prefix, including an
            # empty frame; its own alias is not in scope yet.
            inner = execute(item['query'], [frame] + outer, tables)

            def lateral_rows():
                reply = None
                while True:
                    event = _pull(inner, reply)
                    if event is None:
                        return
                    if event is _SHUTDOWN:
                        return
                    if event[0] == 'row':
                        yield _Row(event[1])
                        reply = None
                    else:
                        reply = yield event

            row_stream = lateral_rows()
        try:
            seen = False
            reply = None
            while True:
                event = _pull(row_stream, reply)
                if event is None:
                    break
                if event is _SHUTDOWN:
                    return
                if isinstance(event, _Row):
                    seen = True
                    binding = (item['as'], names, event.row)
                    nested = source(index + 1, frame + [binding])
                    try:
                        nested_reply = None
                        while True:
                            nested_event = _pull(nested, nested_reply)
                            if nested_event is None:
                                break
                            if nested_event is _SHUTDOWN:
                                return
                            if isinstance(nested_event, _MARKERS):
                                yield nested_event
                                nested_reply = None
                            else:
                                nested_reply = yield nested_event
                                if nested_reply is _SHUTDOWN:
                                    return
                    finally:
                        _shutdown(nested)
                    reply = None
                else:
                    reply = yield event
                    if reply is _SHUTDOWN:
                        return
            if not seen and item.get('left', False):
                binding = (item['as'], names, [None] * len(names))
                padding = source(index + 1, frame + [binding])
                try:
                    padding_reply = None
                    while True:
                        padding_event = _pull(padding, padding_reply)
                        if padding_event is None:
                            break
                        if padding_event is _SHUTDOWN:
                            break
                        if isinstance(padding_event, _MARKERS):
                            yield padding_event
                            padding_reply = None
                        else:
                            padding_reply = yield padding_event
                            if padding_reply is _SHUTDOWN:
                                break
                finally:
                    _shutdown(padding)
        finally:
            _shutdown(row_stream)

    def candidates():
        if 'group' not in q:
            stream = source(0, [])
            try:
                reply = None
                while True:
                    event = _pull(stream, reply)
                    if event is None:
                        return
                    if event is _SHUTDOWN:
                        return
                    if isinstance(event, _Env):
                        yield _Candidate(event.env, None)
                        reply = None
                    else:
                        reply = yield event
                        if reply is _SHUTDOWN:
                            return
            finally:
                _shutdown(stream)
        groups = {}
        stream = source(0, [])
        try:
            reply = None
            while True:
                event = _pull(stream, reply)
                if event is None:
                    break
                if event is _SHUTDOWN:
                    break
                if isinstance(event, _Env):
                    env = event.env
                    values = []
                    for key_expr in q['group']:
                        value = yield _eval(key_expr, env)
                        if value is _SHUTDOWN:
                            return
                        values.append(value)
                    groups.setdefault(key(values), []).append(env)
                    reply = None
                else:
                    reply = yield event
                    if reply is _SHUTDOWN:
                        return
        finally:
            _shutdown(stream)
        if not q['group'] and not groups:
            groups['[]'] = []
        for members in groups.values():
            if members:
                env = members[0]
            else:
                # No physical source is scanned here; only declared column
                # layouts matter.
                frame = []
                for src in q.get('from', []):
                    if 'table' in src:
                        name = src['table']
                        if name not in tables:
                            raise Fault('TABLE:' + name)
                        src_names = tables[name]['columns']
                    else:
                        src_names = [pair[0] for pair in
                                     src['query']['select']]
                    frame.append((src['as'], src_names,
                                  [None] * len(src_names)))
                env = [frame] + outer
            having = q.get('having', ['lit', True])
            value = yield _eval(having, env, members)
            if value is _SHUTDOWN:
                return
            if _truth(value) is True:
                yield _Candidate(env, members)

    def projected(upstream):
        try:
            reply = None
            while True:
                event = _pull(upstream, reply)
                if event is None:
                    return
                if event is _SHUTDOWN:
                    return
                if isinstance(event, _Candidate):
                    if existence:
                        # EXISTS ignores projection, distinct and order.
                        yield _Row([])
                    else:
                        values = []
                        for _, expression in q['select']:
                            value = yield _eval(expression, event.env,
                                                event.group)
                            if value is _SHUTDOWN:
                                return
                            values.append(value)
                        yield _Row(values)
                    reply = None
                else:
                    reply = yield event
                    if reply is _SHUTDOWN:
                        return
        finally:
            _shutdown(upstream)

    def unique(upstream):
        try:
            seen_keys = set()
            reply = None
            while True:
                event = _pull(upstream, reply)
                if event is None:
                    return
                if event is _SHUTDOWN:
                    return
                if isinstance(event, _Row):
                    encoded = key(event.row)
                    if encoded not in seen_keys:
                        seen_keys.add(encoded)
                        yield event
                    reply = None
                else:
                    reply = yield event
                    if reply is _SHUTDOWN:
                        return
        finally:
            _shutdown(upstream)

    stream = projected(candidates())
    try:
        if not existence and q.get('distinct', False):
            stream = unique(stream)
        if not existence and q.get('order'):
            materialized = []
            reply = None
            while True:
                event = _pull(stream, reply)
                if event is None:
                    break
                if event is _SHUTDOWN:
                    return
                if isinstance(event, _Row):
                    materialized.append(event.row)
                    reply = None
                else:
                    reply = yield event
                    if reply is _SHUTDOWN:
                        return
            for index, descending in reversed(q['order']):
                def sort_key(row, index=index):
                    value = row[index]
                    return (value is not None, type(value).__name__, value)
                materialized.sort(key=sort_key, reverse=descending)

            def ordered(rows):
                for row in rows:
                    yield _Row(row)

            stream = ordered(materialized)

        limit = q.get('limit')
        count = 0
        reply = None
        while limit is None or count < limit:
            event = _pull(stream, reply)
            if event is None:
                return
            if event is _SHUTDOWN:
                return
            if isinstance(event, _Row):
                # The coordinator only resumes on the following pull.
                delivered = yield ('row', event.row)
                if delivered is _SHUTDOWN:
                    return
                reply = None
                count += 1
            else:
                reply = yield event
                if reply is _SHUTDOWN:
                    return
    finally:
        _shutdown(stream)
