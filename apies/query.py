import demjson


# ### QUERY DSL HANDLING
class Query():

    def __init__(self, types):
        self.q = {}
        self.types = types

    def run(self, es_client, index_name):
        return es_client.search(
                    index=index_name,
                    doc_type=','.join(self.types),
                    body=self.q
               )

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

    def apply_term(self, term, text_fields):
        search_fields = [text_fields[type_name] for type_name in self.types]
        search_fields = list(set().union(*search_fields))
        self.must().append(dict(
            multi_match=dict(
                query=term,
                fields=search_fields,
                type='most_fields',
                operator='and'
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

        # Then sort by the sort fields, for example - {'__last_modified_at': {'order': 'desc'}}
        self.q.setdefault('sort', sort_fields)

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

    def apply_filters(self, filters):
        if not filters:
            return self

        if isinstance(filters, str):
            if not filters.startswith('{'):
                filters = '{' + filters + '}'
            filters = demjson.decode(filters)

        for k, v in filters.items():
            must = self.must()
            parts = k.split('__')
            op = None

            if len(parts) > 1 and parts[-1] in ('gt', 'gte', 'lt', 'lte', 'eq', 'not'):
                op = parts[-1]
                k = '__'.join(parts[:-1])

            if op == 'not':
                must = self.must_not()
                op = None

            if op is not None:
                must.append(dict(
                    range={
                        k: {
                            op: v
                        }
                    }
                ))
            else:
                if isinstance(v, list):
                    must.append(dict(
                        terms={
                            k: v
                        }
                    ))
                else:
                    must.append(dict(
                        term={
                            k: v
                        }
                    ))
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
