from .logger import logger

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

class Controllers():

    def __init__(self,
                 search_indexes,
                 text_fields,
                 document_index,
                 multi_match_type='most_fields',
                 multi_match_operator='and',
                 dont_highlight=tuple(),
                 debug_queries=False):

        self.text_fields = text_fields
        self.search_indexes = search_indexes
        self.document_index = document_index
        self.multi_match_type = multi_match_type
        self.multi_match_operator = multi_match_operator
        self.dont_highlight = dont_highlight
        self.debug_queries = debug_queries

    # REPLACEMENTS
    def _prepare_replacements(self, highlighted):
        return [
            (h.replace('<em>', '').replace('</em>', ''), h)
            for h in highlighted
        ]

    def _do_replacements(self, value, replacements):
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
            return [self._do_replacements(v, replacements) for v in value]

        if isinstance(value, dict):
            return dict((k, self._do_replacements(v, replacements)) for k, v in value.items())

        assert False, 'Unknown type %r' % value

    def _merge_highlight_into_source(self, source, highlights, dont_highlight):
        for field, highlighted in highlights.items():
            if field in dont_highlight:
                continue
            highlighted = self._prepare_replacements(highlighted)
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

            src[field] = self._do_replacements(src[field], highlighted)
        return source

    # UTILS
    def _validate_types(self, types):
        if 'all' in types:
            types = self.search_indexes

        for type_name in types:
            if type_name not in self.search_indexes:
                raise ValueError('not a real type %s' % type_name)
        return dict((k, v) for k, v in self.search_indexes.items() if k in types)

    # Main API
    def search(self,
               es_client,
               types,
               term,
               from_date,
               to_date,
               size,
               offset,
               filters,
               lookup,
               term_context,
               *,
               score_threshold=0,
               sort_fields=None):
        search_indexes = self._validate_types(types)

        query = Query(search_indexes)
        if term:
            query = query.apply_term(
                term, self.text_fields,
                multi_match_type=self.multi_match_type,
                multi_match_operator=self.multi_match_operator)

        if term_context:
            query = query.apply_term_context(term_context, self.text_fields)

        # Apply the filters
        query = query.apply_filters(filters)

        # Apply the lookup
        query = query.apply_lookup(lookup)

        # Apply sorting - if there are fields to sort by, apply the scoring as the sorting
        if sort_fields is None:
            if term:
                query.apply_scoring()
            else:
                query.apply_sorting({'score': {'order': 'desc'}}, 0)
        else:
            query.apply_sorting(sort_fields, score_threshold)

        # Apply pagination
        query = query.apply_pagination(size, offset)

        # Apply highlighting
        if term and self.dont_highlight != '*' and '*' not in self.dont_highlight:
            query = query.apply_highlighting(term, self.text_fields)

        # Ensure correct counts
        query = query.apply_exact_total()

        # Apply the time range
        query = query.apply_time_range(from_date, to_date)

        # Execute the query
        query_results = query.run(es_client, self.debug_queries)
        query_results = query_results['responses']
        hits = []
        total_overall = 0
        for _type, result in zip(query.types, query_results):
            result_hits = result.get('hits', {})
            for i, hit in enumerate(result_hits.get('hits', [])):
                hit['_type'] = _type
                hits.append((i, hit))
            total_overall += result_hits.get('total', {}).get('value', 0)
            if 'hits' not in result or 'hits' not in result['hits']:
                logger.warning('no hits element for query for type %s: %r', _type, result)
        hits = [j[1] for j in sorted(hits, key=lambda i: i[0])]

        default_sort_score = (0,)
        search_results = [
            dict(
                source=self._merge_highlight_into_source(
                    hit['_source'],
                    hit['highlight'],
                    self.dont_highlight
                ),
                type=hit['_type'],
                score=hit['_score'] or hit.get('sort', default_sort_score)[0]
            ) if 'highlight' in hit else dict(
                source=hit['_source'],
                type=hit['_type'],
                score=hit['_score'] or hit.get('sort', default_sort_score)[0]
            )
            for hit in hits
        ]

        return dict(
            search_counts=dict(
                _current=dict(
                    total_overall=total_overall
                )
            ),
            search_results=search_results
        )

    def count(self, es_client, term, from_date, to_date, config, term_context):
        counts = {}
        for item in config:
            doc_types = item['doc_types']
            search_indexes = self._validate_types(doc_types)
            filters = item['filters']
            id = item['id']
            query_results = Query(search_indexes)
            if term:
                query_results = query_results.apply_term(
                    term, self.text_fields,
                    multi_match_type=self.multi_match_type,
                    multi_match_operator=self.multi_match_operator
                )
            if term_context:
                query_results = query_results.apply_term_context(term_context, self.text_fields)
            query_results = query_results\
                .apply_filters(filters)\
                .apply_pagination(0, 0)\
                .apply_time_range(from_date, to_date)\
                .apply_exact_total()\
                .run(es_client, self.debug_queries)
            counts[id] = dict(
                total_overall=sum(
                    results['hits']['total']['value']
                    for results in query_results['responses']
                )
            )
        return dict(
            search_counts=counts
        )

    def timeline(self, es_client, index_name, text_fields,
                 types, term, from_date, to_date, filters):
        search_indexes = self._validate_types(types)

        query_results = Query(search_indexes)\
            .apply_term(
                term, text_fields,
                multi_match_type=self.multi_match_type,
                multi_match_operator=self.multi_match_operator
            )\
            .apply_filters(filters)\
            .apply_pagination(0, 0)\
            .apply_time_range(from_date, to_date)\
            .apply_month_aggregates()\
            .run(es_client, self.debug_queries)

        # TODO: combine all responses
        timeline = query_results['responses'][0].get('aggregations', {}).get('timeline', {}).get('buckets', [])
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

    def get_document(self, es_client, doc_id):
        try:
            result = es_client.get(self.document_index, doc_id)
            return result.get('_source')
        except elasticsearch.exceptions.NotFoundError:
            return None
