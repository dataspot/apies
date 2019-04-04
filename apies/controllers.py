import elasticsearch

from .query import Query


# ### HIGHLIGHT HANDLING
# DONT_HIGHLIGHT = {
#     'kind',
#     'kind_he',
#     'budget_code',
#     'entity_kind',
#     'entity_id',
#     'code',
# }


def _prepare_replacements(highlighted):
    return [
        (h.replace('<em>', '').replace('</em>', ''), h)
        for h in highlighted
    ]


def _do_replacements(value, replacements):
    if value is None:
        return None

    if isinstance(value, str):
        for src, dst in replacements:
            value = value.replace(src, dst)
        value = value.replace('</em> <em>', ' ')
        return value

    if isinstance(value, (int, bool, float)):
        return value

    if isinstance(value, list):
        return [_do_replacements(v, replacements) for v in value]

    if isinstance(value, dict):
        return dict((k, _do_replacements(v, replacements)) for k, v in value.items())

    assert False, 'Unknown type %r' % value


def _merge_highlight_into_source(source, highlights, dont_highlight):
    for field, highlighted in highlights.items():
        if field in dont_highlight:
            continue
        highlighted = _prepare_replacements(highlighted)
        field_parts = field.split('.')
        src = source
        field = field_parts[0]
        while len(field_parts) > 1:
            if isinstance(src[field], dict):
                field_parts.pop(0)
                src = src[field]
                field = field_parts[0]
            else:
                break

        src[field] = _do_replacements(src[field], highlighted)
    return source


# ### UTILS
def _validate_types(text_fields, types):
    if 'all' in types:
        types = text_fields.keys()

    for type_name in types:
        if type_name not in text_fields:
            raise ValueError('not a real type %s' % type_name)
    return types


# ### Main API
def search(es_client,
           index_name,
           text_fields,
           types,
           term,
           from_date,
           to_date,
           size,
           offset,
           filters,
           dont_highlight,
           score_threshold=0.5,
           sort_fields=None):
    types = _validate_types(text_fields, types)

    query_results = Query(types)
    if term:
        query_results = query_results.apply_term(term, text_fields)

    # Apply the filters
    query_results = query_results.apply_filters(filters)

    # Apply sorting - if there are fields to sort by, apply the scoring as the sorting
    if sort_fields is None:
        if term:
            query_results.apply_scoring()
        else:
            query_results.apply_sorting({'score': {'order': 'desc'}}, 0)
    else:
        query_results.apply_sorting(sort_fields, score_threshold)

    # Apply pagination
    query_results = query_results.apply_pagination(size, offset)

    # Apply highlighting
    if term and dont_highlight != '*' and '*' not in dont_highlight:
        highlighted = True
        query_results = query_results.apply_highlighting()
    else:
        highlighted = False

    # Apply the time range
    query_results = query_results.apply_time_range(from_date, to_date)\
        .run(es_client, index_name)

    default_sort_score = (0,)
    if highlighted:
        search_results = [
            dict(
                source=_merge_highlight_into_source(hit['_source'],
                                                hit['highlight'],
                                                dont_highlight),
                type=hit['_type'],
                score=hit['_score'] or hit.get('sort', default_sort_score)[0]
            )
            for hit in query_results['hits']['hits']
        ]
    else:
        search_results = [
            dict(
                source=hit['_source'],
                type=hit['_type'],
                score=hit['_score'] or hit.get('sort', default_sort_score)[0]
            )
            for hit in query_results['hits']['hits']
        ]

    return dict(
        search_counts=dict(
            _current=dict(
                total_overall=query_results['hits']['total']
            )
        ),
        search_results=search_results
    )


def count(es_client, index_name, text_fields,
          term, from_date, to_date, config):
    counts = {}
    for item in config:
        doc_types = item['doc_types']
        doc_types = _validate_types(text_fields, doc_types)
        filters = item['filters']
        id = item['id']
        query_results = Query(doc_types)
        if term:
            query_results = query_results.apply_term(term, text_fields)
        query_results = query_results\
            .apply_filters(filters)\
            .apply_pagination(0, 0)\
            .apply_time_range(from_date, to_date)\
            .run(es_client, index_name)
        counts[id] = dict(
            total_overall=query_results['hits']['total']
        )
    return dict(
        search_counts=counts
    )


def timeline(es_client, index_name, text_fields,
             types, term, from_date, to_date, filters):
    types = _validate_types(text_fields, types)

    query_results = Query(types)\
        .apply_term(term, text_fields)\
        .apply_filters(filters)\
        .apply_pagination(0, 0)\
        .apply_time_range(from_date, to_date)\
        .apply_month_aggregates()\
        .run(es_client, index_name)

    timeline = query_results.get('aggregations', {}).get('timeline', {}).get('buckets', [])
    timeline = ((b['key'], b['doc_count'])
                for b in timeline
                if len(b['key']) == 7)
    if None not in (from_date, to_date):
        timeline = filter(lambda k: k[0] >= from_date[:7] and k[0] <= to_date[:7],
                          timeline)
    timeline = sorted(timeline)

    return dict(
        timeline=timeline
    )


def get_document(es_client, index_name, type_name, doc_id):
    try:
        result = es_client.get(index_name, doc_id, doc_type=type_name)
        return result.get('_source')
    except elasticsearch.exceptions.NotFoundError:
        return None
