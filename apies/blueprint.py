import json

from flask import Blueprint, make_response, request, current_app, send_file
from flask_jsonpify import jsonpify
from flask.helpers import NotFound

import demjson3 as demjson

from .controllers import Controllers
from .sources import extract_text_fields
from .logger import logger, logging
from .utils.file_maker import get_csv, get_xls, get_xlsx
from .query import Query


def default_rules(field):
    if field.get('es:title') or field.get('es:hebrew'):
        if field.get('es:keyword'):
            return [('exact', '^10')]
        else:
            return [('inexact', '^3'), ('natural', '.hebrew^10')]
    elif field.get('es:boost'):
        if field.get('es:keyword'):
            return [('exact', '^10')]
        else:
            return [('inexact', '^10')]
    elif field.get('es:keyword'):
        return [('exact', '')]
    else:
        return [('inexact', '')]


class APIESBlueprint(Blueprint):

    def __init__(self, app,
                 sources,
                 es_client,
                 search_indexes,
                 document_index,
                 dont_highlight=None,
                 text_field_rules=default_rules,
                 text_field_select=None,
                 multi_match_type='most_fields',
                 multi_match_operator='and',
                 debug_queries=False,
                 query_cls=Query):
        super().__init__('apies', 'apies')

        if debug_queries:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)

        if dont_highlight is not None:
            # deprecation message:
            logger.warning('dont_highlight is deprecated, use request parameters instead')

        self.controllers = Controllers(
            search_indexes=search_indexes,
            text_fields=extract_text_fields(sources, text_field_rules, text_field_select, debug_queries),
            document_index=document_index,
            multi_match_type=multi_match_type,
            multi_match_operator=multi_match_operator,
            debug_queries=debug_queries,
            query_cls=query_cls
        )

        self.add_url_rule(
            '/get/<path:doc_id>',
            'get_document_handler',
            self.get_document_handler,
            methods=['GET']
        )
        self.add_url_rule(
            '/search/count',
            'simple_count_handler',
            self.count_handler,
            methods=['GET']
        )
        self.add_url_rule(
            '/search/<string:types>',
            'dynamic_search_handler',
            self.search_handler,
            methods=['GET']
        )
        self.add_url_rule(
            '/download/<string:types>',
            'download',
            self.download,
            methods=['GET']
        )

        app.config['ES_CLIENT'] = es_client

    def search_handler(self, types):
        es_client = current_app.config['ES_CLIENT']

        try:
            types_formatted = str(types).split(',')
            filters = request.values.get('filter')
            lookup = request.values.get('lookup')
            search_term = request.values.get('q')
            term_context = request.values.get('context')
            extra = request.values.get('extra')
            from_date = request.values.get('from_date')
            to_date = request.values.get('to_date')
            size = request.values.get('size', 10)
            offset = request.values.get('offset', 0)
            order = request.values.get('order')
            highlight = [x.strip() for x in request.values.get('highlight', '').split(',') if x]
            snippets = [x.strip() for x in request.values.get('snippets', '').split(',') if x]
            match_type = request.values.get('match_type')
            match_operator = request.values.get('match_operator')
            result = self.controllers.search(
                es_client, types_formatted, search_term,
                from_date=from_date,
                to_date=to_date,
                size=size,
                offset=offset,
                filters=filters,
                lookup=lookup,
                term_context=term_context,
                extra=extra,
                score_threshold=0, 
                sort_fields=order,
                highlight=highlight,
                snippets=snippets,
                match_type=match_type,
                match_operator=match_operator,
            )
        except Exception as e:
            logger.exception('Error searching %s for types: %s ' % (search_term, str(types)))
            result = {'error': str(e)}
        return jsonpify(result)

    def download(self, types):
        """
        Performs a search and returns the results in a file (CSV or Excel) response

        :param types: The types of the document
        :return Response: The file response with the results of the query
        """

        # Get values from the config
        es_client = current_app.config['ES_CLIENT']

        # Get parameters from the query string
        try:
            types_formatted = str(types).split(',')
            search_term = request.values.get('q')
            term_context = request.values.get('context')
            extra = request.values.get('extra')
            from_date = request.values.get('from_date')
            to_date = request.values.get('to_date')
            size = request.values.get('size', 10)
            offset = request.values.get('offset', 0)
            filters = request.values.get('filter')
            lookup = request.values.get('lookup')
            order = request.values.get('order')

            # Get the query results
            result = self.controllers.search(es_client,
                                             types_formatted,
                                             search_term,
                                             from_date,
                                             to_date,
                                             size,
                                             offset,
                                             filters,
                                             lookup,
                                             term_context,
                                             extra,
                                             score_threshold=0,
                                             sort_fields=order)

        except Exception as e:
            logging.exception('Error searching %s for types: %s ' % (search_term, str(types)))
            result = {'error': str(e)}

        # Get the file name from the querystring
        file_name = request.values.get('file_name') or 'budgetkey'

        # Do the column mapping
        column_mapping = request.values.get('column_mapping')
        if column_mapping:
            column_mapping = json.loads(column_mapping)

        # Get the file name and format from the query string, or give them default values
        file_format = request.values.get('file_format')
        if file_format == 'csv':
            file = get_csv(result, column_mapping)

            # Make the response object
            response = make_response(file)
            response.headers["Content-Disposition"] = "attachment; filename={}".format(file_name + '.csv')
            response.headers["Content-type"] = 'text/csv'

        elif file_format == 'xls':
            file_stream = get_xls(result, column_mapping)

            # Make the response object
            response = send_file(file_stream,
                                 as_attachment=True,
                                 mimetype='application/vnd.ms-excel',
                                 attachment_filename=file_name + '.xlsx')

        else:
            file_stream = get_xlsx(result, column_mapping)

            # Make the response object
            response = send_file(file_stream,
                                 as_attachment=True,
                                 mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                                 attachment_filename=file_name + '.xlsx')

        return response

    def count_handler(self):
        es_client = current_app.config['ES_CLIENT']

        config = request.values.get('config')
        try:
            config = demjson.decode(config)
            search_term = request.values.get('q')
            from_date = request.values.get('from_date')
            to_date = request.values.get('to_date')
            term_context = request.values.get('context')
            extra = request.values.get('extra')
            result = self.controllers.count(
                es_client, search_term, from_date, to_date, config, term_context, extra
            )
        except Exception as e:
            logger.exception('Error counting with config %r', config)
            result = {'error': str(e)}
        return jsonpify(result)


    def get_document_handler(self, doc_id):
        es_client = current_app.config['ES_CLIENT']
        type_ = request.values.get('type')

        result = self.controllers.get_document(
            es_client, doc_id, type_
        )
        if result is None:
            logger.warning('Failed to fetch document for %r', doc_id)
            raise NotFound()
        return jsonpify(result)
