import demjson
import json
from elasticsearch import Elasticsearch

from .logger import logger


# ### QUERY DSL HANDLING
class Query():

    def __init__(self, search_indexes):
        self.types = list(search_indexes.keys())
        self.filtered_type_names = set(self.types)
        self.indexes = list(search_indexes.values())
        self.q = dict((t, {}) for t in self.types)

    def __str__(self):
        return demjson.encode(self.q)

    def run(self, es_client: Elasticsearch, debug):
        if debug:
            logger.debug('QUERY (for %s):\n%s', self.types[0],
                         json.dumps(self.q[self.types[0]], indent=2, ensure_ascii=False))
        body = ''.join(
            '{}\n{}\n'.format(
                json.dumps(dict(index=index)),
                json.dumps(self.q[t])
            )
            for t, index in zip(self.types, self.indexes)
            if t in self.filtered_type_names
        )
        return es_client.msearch(body)

    def query_bool(self, t):
        return self.q[t].setdefault('query', {})\
                        .setdefault('function_score', {})\
                        .setdefault('query', {})\
                        .setdefault('bool', {})

    def must(self, t):
        return self.query_bool(t)\
                        .setdefault('must', [])

    def should(self, t):
        return self.query_bool(t)\
                        .setdefault('should', [])

    def filter(self, t):
        return self.query_bool(t)\
                        .setdefault('filter', {})\
                        .setdefault('bool', {})

    def must_not(self, t):
        return self.query_bool(t)\
                        .setdefault('must_not', [])

    def apply_term(self, term, text_fields,
                   multi_match_type='most_fields', multi_match_operator='and'):
        for type_name in self.types:
            search_fields = text_fields[type_name]
            search_fields = dict(
                (k, [x[1] for x in search_fields if x[0] == k])
                for k in ('exact', 'inexact', 'natural')
            )
            matchers = []

            # Multimatch for inexact fields
            matchers.append(dict(
                multi_match=dict(
                    query=term,
                    fields=[f for f in search_fields['inexact'] + search_fields['natural']],
                    type=multi_match_type,
                    operator=multi_match_operator,
                    tie_breaker=0.3
                )
            ))

            # Tuples
            parts = term.split()
            parts = [term] + parts + [' '.join(z) for z in zip(parts[:-1], parts[1:])]
            for field in search_fields['exact']:
                fparts = field.split('^')
                if len(fparts) == 1:
                    matchers.append(dict(
                        terms={
                            field: tuple(set(parts))
                        }
                    ))
                else:
                    matchers.append(dict(
                        terms={
                            fparts[0]: tuple(set(parts)),
                            'boost': float(fparts[1])
                        }
                    ))

            # Apply boosters
            if len(matchers) > 0:
                self.must(type_name).append(dict(
                    bool=dict(
                        should=matchers,
                        minimum_should_match=1
                    )
                ))

        return self

    def apply_term_context(self, terms, text_fields):
        multi_match_type = 'most_fields'
        multi_match_operator = 'or'
        for type_name in self.types:
            search_fields = text_fields[type_name]
            search_fields = [
                x[1] for x in search_fields if x[0] == 'inexact'
            ]
            matcher = dict(
                multi_match=dict(
                    query=terms,
                    fields=search_fields,
                    type=multi_match_type,
                    operator=multi_match_operator,
                    tie_breaker=0.3
                )
            )
            self.filter(type_name).setdefault('must', []).append(matcher)

        return self

    def apply_scoring(self):
        for type_name in self.types:
            fs = self.q[type_name].setdefault('query', {}).setdefault('function_score', {})
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
        for type_name in self.types:
            q = self.q[type_name]
            q.setdefault('min_score', score_threshold)
            if isinstance(sort_fields, str):
                if sort_fields[0] == '-':
                    sort_fields = sort_fields[1:]
                    sort = {sort_fields: {'order': 'desc'}}
                else:
                    sort = {sort_fields: {'order': 'asc'}}
            else:
                sort = sort_fields

            # Then sort by the sort fields, for example - {'__last_modified_at': {'order': 'desc'}}
            q.setdefault('sort', sort)

        return self

    def apply_pagination(self, page_size, offset):
        for type_name in self.types:
            self.q[type_name].update({
                'size': int(page_size),
                'from': int(offset)
            })
        return self

    def apply_highlighting(self, term, search_fields):
        for type_name in self.types:
            self.q[type_name]['highlight'] = dict(
                fields={'*': {}},
                highlight_query=dict(
                    multi_match=dict(
                        query=term,
                        fields=[f for k, f in search_fields[type_name] if k in ('inexact', 'natural')],
                    )
                )
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

    def _process_complex(self, param):
        if not param:
            return None

        if isinstance(param, str):
            if param.startswith('[') and param.endswith(']'):
                pass
            elif not param.startswith('{'):
                param = '{' + param + '}'
            param = demjson.decode(param)

        if isinstance(param, dict):
            param = [param]

        if isinstance(param, list):
            should_clauses = dict()
            for i in param:
                bool_clause = {}
                type_names = self.types
                for k, v in i.items():
                    if k == '_type':
                        if not isinstance(v, list):
                            type_names = [v]
                        else:
                            type_names = v
                    else:
                        clause, positive = self.parse_filter_op(k, v)
                        if positive:
                            bool_clause.setdefault('must', []).append(clause)
                        else:
                            bool_clause.setdefault('must_not', []).append(clause)
                for type_name in type_names:
                    should_clauses.setdefault(type_name, []).append(dict(bool=bool_clause))
            return should_clauses

        return None

    def apply_filters(self, filters):
        should_clauses = self._process_complex(filters)
        if not should_clauses:
            return self

        for type_name, clauses in should_clauses.items():
            if len(clauses) > 0:
                flt = self.filter(type_name)
                flt.setdefault('should', []).extend(clauses)
                flt['minimum_should_match'] = 1
        self.filtered_type_names = set(should_clauses.keys())
        return self

    def apply_lookup(self, lookup):
        should_clauses = self._process_complex(lookup)
        if not should_clauses:
            return self

        for type_name, clause in should_clauses.items():
            self.should(type_name).append(
                dict(
                    bool=dict(
                        should=clause
                    )
                )
            )
            self.query_bool(type_name)['minimum_should_match'] = 1
        self.filtered_type_names = set(should_clauses.keys())
        return self

    def apply_time_range(self, from_date, to_date):
        if None not in (from_date, to_date):
            for type_name in self.types:
                self.must(type_name).extend([
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

    def apply_exact_total(self):
        for type_name in self.types:
            self.q[type_name]['track_total_hits'] = True
        return self

    def apply_month_aggregates(self):
        for type_name in self.types:
            self.q[type_name].setdefault('aggs', {}).update(dict(
                timeline=dict(
                    terms=dict(
                        field='__date_range_months',
                        size=2500,
                        order=dict(_term='asc')
                    )
                )
            ))
        return self
