from .logger import logger
from .query import Query

import elasticsearch

class Controllers():

    def __init__(self,
                 search_indexes,
                 text_fields,
                 document_index,
                 multi_match_type='most_fields',
                 multi_match_operator='and',
                 debug_queries=False,
                 query_cls=Query):

        self.text_fields = text_fields
        self.search_indexes = search_indexes
        self.document_index = document_index
        self.multi_match_type = multi_match_type
        self.multi_match_operator = multi_match_operator
        self.debug_queries = debug_queries
        self.query_cls = query_cls

    # REPLACEMENTS
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

    def _merge_highlight_into_source(self, source, highlights, highlight, snippets):
        _snippets = source.setdefault('_snippets', dict())
        _highlights = source.setdefault('_highlights', dict())
        for field, highlighted in highlights.items():
            if field in snippets:
                _snippets[field] = highlighted
            elif field in highlight:
                field_parts = field.split('.')
                out_field = []
                src = source
                while len(field_parts) > 0:
                    field = field_parts.pop(0)
                    out_field.append(field)
                    if isinstance(src.get(field), dict):
                        src = src[field]
                    else:
                        break
                out_field = '.'.join(out_field)
                _highlights[out_field] = highlighted[0]
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
               *,
               from_date=None,
               to_date=None,
               size=10,
               offset=0,
               filters=None,
               lookup=None,
               term_context=None,
               extra=None,
               score_threshold=0,
               sort_fields=None,
               highlight=None,
               snippets=None,
               match_type=None,
               match_operator=None):
        search_indexes = self._validate_types(types)

        query = self.query_cls(search_indexes)
        if term:
            query = query.apply_term(
                term, self.text_fields,
                multi_match_type=match_type or self.multi_match_type,
                multi_match_operator=match_operator or self.multi_match_operator)

        if term_context:
            query = query.apply_term_context(term_context, self.text_fields)

        # Apply the filters
        query = query.apply_filters(filters)

        # Apply the lookup
        query = query.apply_lookup(lookup)

        # Apply extra processing
        query = query.apply_extra(extra)

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
        if term and highlight or snippets:
            query = query.apply_highlighting(term, highlight, snippets)

        # Ensure correct counts
        query = query.apply_exact_total()

        # Apply the time range
        query = query.apply_time_range(from_date, to_date)

        # Execute the query
        results = query.run(es_client, self.debug_queries)
        query_results = results['responses']
        hits = []
        total_overall = 0
        search_counts = dict()
        for _type, result in zip(query.types, query_results):
            result_hits = result.get('hits', {})
            for i, hit in enumerate(result_hits.get('hits', [])):
                hit['_type'] = _type
                hits.append((i, hit))
            count = result_hits.get('total', {}).get('value', 0)
            total_overall += count
            search_counts[_type] = dict(total_overall=count)
            if 'hits' not in result or 'hits' not in result['hits']:
                logger.warning('no hits element for query for type %s: %r', _type, result)
        hits = [j[1] for j in sorted(hits, key=lambda i: i[0])]

        default_sort_score = (0,)
        search_results = [
            dict(
                source=self._merge_highlight_into_source(
                    hit['_source'],
                    hit['highlight'],
                    highlight,
                    snippets
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

        search_counts['_current'] = dict(
            total_overall=total_overall
        )
        ret = dict(
            search_counts=search_counts,
            search_results=search_results
        )
        query.process_extra(ret, results)
        return ret

    def count(self, es_client, term, from_date, to_date, config, term_context, extra):
        counts = {}
        for item in config:
            doc_types = item['doc_types']
            search_indexes = self._validate_types(doc_types)
            filters = item['filters']
            id = item['id']
            query_results = self.query_cls(search_indexes)
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
                .apply_exact_total()

            # Apply extra processing
            if extra:
                query_results = query_results.apply_extra(extra)

            query_results = query_results.run(es_client, self.debug_queries)
            counts[id] = dict(
                total_overall=sum(
                    results['hits']['total']['value']
                    for results in query_results['responses']
                )
            )
        return dict(
            search_counts=counts
        )


    def get_document(self, es_client, doc_id, doc_type=None):
        try:
            index = self.document_index
            if doc_type is not None:
                types = [doc_type]
                types = self._validate_types(types)
                index = types[doc_type]
            logger.debug('FETCH %r in %s (%r)', doc_id, index, doc_type)
            result = es_client.get(index, doc_id)
            return result.get('_source')
        except elasticsearch.exceptions.NotFoundError:
            return None
