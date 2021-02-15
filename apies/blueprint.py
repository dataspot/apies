import json

from flask import Blueprint, make_response, request, current_app, send_file
from flask_jsonpify import jsonpify
from flask.helpers import NotFound

import demjson

from .controllers import Controllers
from .sources import extract_text_fields
from .logger import logger, logging
from .utils.file_maker import get_csv, get_xls, get_xlsx


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
                 dont_highlight=[],
                 text_field_rules=default_rules,
                 multi_match_type='most_fields',
                 multi_match_operator='and',
                 debug_queries=False):
        super().__init__('apies', 'apies')
        self.controllers = Controllers(
            search_indexes=search_indexes,
            text_fields=extract_text_fields(sources, text_field_rules),
            document_index=document_index,
            multi_match_type=multi_match_type,
            multi_match_operator=multi_match_operator,
            dont_highlight=dont_highlight,
            debug_queries=debug_queries
        )
        if debug_queries:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)
        logger.error('DEBUG')
        logger.debug('DEBUG')

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
            '/search/timeline/<string:types>/<string:search_term>/'
            '<string:from_date>/<string:to_date>',
            'timeline_handler',
            self.timeline_handler,
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
            from_date = request.values.get('from_date')
            to_date = request.values.get('to_date')
            size = request.values.get('size', 10)
            offset = request.values.get('offset', 0)
            order = request.values.get('order')
            result = self.controllers.search(
                es_client, types_formatted, search_term,
                from_date, to_date, size, offset, filters,
                lookup, term_context,
                score_threshold=0, sort_fields=order
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
            result = self.controllers.count(
                es_client, search_term, from_date, to_date, config, term_context
            )
        except Exception as e:
            logger.exception('Error counting with config %r', config)
            result = {'error': str(e)}
        return jsonpify(result)

    def timeline_handler(self, types, search_term, from_date, to_date):
        es_client = current_app.config['ES_CLIENT']
        index_name = current_app.config['INDEX_NAME']
        text_fields = current_app.config['TEXT_FIELDS']

        try:
            types_formatted = str(types).split(',')
            filters = request.values.get('filter')
            result = self.controllers.timeline(
                es_client, index_name, text_fields,
                types_formatted, search_term,
                from_date, to_date, filters
            )
        except Exception as e:
            logger.exception('Error getting timeline %s for types: %s ' % (search_term, str(types)))
            result = {'error': str(e)}
        return jsonpify(result)

    def get_document_handler(self, doc_id):
        es_client = current_app.config['ES_CLIENT']

        result = self.controllers.get_document(
            es_client, doc_id
        )
        if result is None:
            logger.warning('Failed to fetch document for %r', doc_id)
            raise NotFound()
        return jsonpify(result)
