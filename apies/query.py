import demjson
import json
from elasticsearch import Elasticsearch

from .logger import logger


# ### QUERY DSL HANDLING
class Query():

    def __init__(self, search_indexes):
        self.q = {}
        self.types = list(search_indexes.keys())
        self.indexes = list(search_indexes.values())

    def __str__(self):
        return demjson.encode(self.q)

    def run(self, es_client: Elasticsearch, debug):
        if debug:
            logger.debug('QUERY:\n%s', json.dumps(self.q, indent=2, ensure_ascii=False))
        body = json.dumps(self.q)
        body = ''.join(
            '{}\n{}\n'.format(
                json.dumps(dict(index=index)),
                body
            )
            for index in self.indexes
        )
        return es_client.msearch(body)

    def must(self):
        return self.q.setdefault('query', {})\
                     .setdefault('function_score', {})\
                     .setdefault('query', {})\
                     .setdefault('bool', {})\
                     .setdefault('must', [])

    def must_not(self):
        return self.q.setdefault('query', {})\
                     .setdefault('function_score', {})\
                     .setdefault('query', {})\
                     .setdefault('bool', {})\
                     .setdefault('must_not', [])

    def apply_term(self, term, text_fields,
                   multi_match_type='most_fields', multi_match_operator='and'):
        search_fields = [text_fields[type_name] for type_name in self.types]
        search_fields = list(set().union(*search_fields))
        search_fields = dict(
            (k, [x[1] for x in search_fields if x[0] == k])
            for k in ('exact', 'inexact', 'natural')
        )
        matchers = []

        # Multimatch
        matchers.append(dict(
            multi_match=dict(
                query=term,
                fields=[f for f in search_fields['inexact']],
                type=multi_match_type,
                operator=multi_match_operator
            )
        ))

        # Common Terms
        for field in search_fields['natural']:
            if '^' in field:
                name, boost = field.split('^')
            else:
                name, boost = field, 1.0
            matchers.append(dict(
                common={
                    name: dict(
                        query=term,
                        boost=float(boost),
                        cutoff_frequency=0.001,
                    )
                }
            ))
        # Tuples
        parts = term.split()
        parts = [term] + parts + [' '.join(z) for z in zip(parts[:-1], parts[1:])]
        for field in search_fields['exact']:
            matchers.append(dict(
                terms={
                    field: tuple(set(parts))
                }
            ))

        # Apply boosters
        if len(matchers) > 0:
            self.must().append(dict(
                bool=dict(
                    should=matchers,
                    minimum_should_match=1
                )
            ))

        return self

    def apply_scoring(self):
        fs = self.q.setdefault('query', {}).setdefault('function_score', {})
        fs.update(dict(
            boost_mode='multiply',
            field_value_factor=dict(
                field='score',
                modifier='sqrt',
                missing=1
            )
        ))
        return self

    def apply_sorting(self, sort_fields, score_threshold):
        # Apply the scoring threshold - since we are no longer sorting by score, it is important to use a score
        # threshold so as not to get irrelevant results
        self.q.setdefault('min_score', score_threshold)
        if isinstance(sort_fields, str):
            if sort_fields[0] == '-':
                sort_fields = sort_fields[1:]
                sort = {sort_fields: {'order': 'desc'}}
            else:
                sort = {sort_fields: {'order': 'asc'}}
        else:
            sort = sort_fields

        # Then sort by the sort fields, for example - {'__last_modified_at': {'order': 'desc'}}
        self.q.setdefault('sort', sort)

        return self

    def apply_pagination(self, page_size, offset):
        self.q.update({
            'size': int(page_size),
            'from': int(offset)
        })
        return self

    def apply_highlighting(self):
        self.q['highlight'] = dict(
            fields={'*': {}}
        )
        return self

    def parse_filter_op(self, k, v):
        must = True
        parts = k.split('__')
        op = None

        if len(parts) > 1 and parts[-1] in ('gt', 'gte', 'lt', 'lte', 'eq', 'not', 'like'):
            op = parts[-1]
            k = '__'.join(parts[:-1])

        if op == 'not':
            must = False
            op = None

        if op is not None:
            if op == 'like':
                ret = dict(
                    match={
                        k: v
                    }
                )
            else:
                ret = dict(
                    range={
                        k: {
                            op: v
                        }
                    }
                )
        else:
            if isinstance(v, list):
                ret = dict(
                    terms={
                        k: v
                    }
                )
            else:
                ret = dict(
                    term={
                        k: v
                    }
                )
        return ret, must

    def apply_filters(self, filters):
        if not filters:
            return self

        if isinstance(filters, str):
            if filters.startswith('[') and filters.endswith(']'):
                pass
            elif not filters.startswith('{'):
                filters = '{' + filters + '}'
            filters = demjson.decode(filters)

        if isinstance(filters, dict):
            filters = [filters]

        should_clauses = []
        if isinstance(filters, list):
            for i in filters:
                bool_clause = {}
                for k, v in i.items():
                    clause, positive = self.parse_filter_op(k, v)
                    if positive:
                        bool_clause.setdefault('must', []).append(clause)
                    else:
                        bool_clause.setdefault('must_not', []).append(clause)
                should_clauses.append(dict(bool=bool_clause))
        self.must().append(dict(bool=dict(should=should_clauses)))
        return self

    def apply_time_range(self, from_date, to_date):
        if None not in (from_date, to_date):
            self.must().extend([
                dict(
                    range=dict(
                        __date_range_from=dict(
                            lte=to_date
                        )
                    )
                ),
                dict(
                    range=dict(
                        __date_range_to=dict(
                            gte=from_date
                        )
                    )
                ),
            ])
        return self

    def apply_month_aggregates(self):
        self.q.setdefault('aggs', {}).update(dict(
            timeline=dict(
                terms=dict(
                    field='__date_range_months',
                    size=2500,
                    order=dict(_term='asc')
                )
            )
        ))
        return self
