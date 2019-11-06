import csv
import xlwt
import xlsxwriter

from io import BytesIO, StringIO


def get_csv(es_result, column_mapping):
    """
    Creates a string for a csv file through the Python csv writer and StringIo

    :param (dict): The result of the ElasticSearch search
    :param column_mapping (dict): An dict mapping the desired column name to the actual field name in Elasticsearch.
    This can also be a nested field.
    For example:
    {
        "address.city":"עיר",
        "details.budget": "תקציב"
    }

    :return column_mapping (dict): A dict mapping the column names in the original data, to the desired column headers
    for the output file
    """

    # Create a string stream and writer object, passing the stream
    file_stream = StringIO()
    writer = csv.writer(file_stream)

    # Get ordered lists of column headers (the 'titles' of the columns) and the column names (how the fields are called
    # in ElasticSearch
    document_source = es_result['search_results'][0]['source']
    column_headers, column_names = _get_column_headers_and_names(document_source, column_mapping)

    # Write the header row
    writer.writerow(column_headers)

    # Loop over the documents, for each document loop over the fields and retrieve the field from the document
    documents = es_result['search_results']
    for document in documents:
        document_row = []
        for field_name in column_names:
            field_value = _get_field_value(field_name, document['source'])
            document_row.append(field_value)
        # Write the document field
        writer.writerow(document_row)

    # Return the stream string
    return file_stream.getvalue()


def get_xls(es_result, column_mapping):
    """
    Creates a stream with the Excel file, the column headers, and the result rows

    :param es_result: The result of the ElasticSearch search
    :param column_mapping (dict): A dict mapping the column names in the original data, to the desired column headers
    for the output file

    :return (BytesIO): A stream with the Excel file
    """

    # Create a bytes stream
    file_stream = BytesIO()

    # Create an excel workbook and sheet
    workbook = xlwt.Workbook()
    worksheet = workbook.add_sheet('result')

    # Get ordered lists of the column headers ('column titles') and the column names (how the fields are actually called
    # in the ElasticSearch results)
    document_source = es_result['search_results'][0]['source']
    column_headers, column_names = _get_column_headers_and_names(document_source, column_mapping)

    # Write the first row of column names
    for index, column_name in enumerate(column_headers):
        worksheet.write(0, index, column_name)

    # Write the rows
    documents = es_result['search_results']
    # Loop over the documents
    for document_index, document in enumerate(documents):
        # Loop over the columns
        for column_index, column_name in enumerate(column_names):
            column_value = _get_field_value(column_name, document['source'])
            worksheet.write(document_index+1, column_index, column_value)

    # Save the workbook to the stream
    workbook.save(file_stream)
    file_stream.seek(0)

    return file_stream


def get_xlsx(es_result, column_mapping):
    """
    Creates a stream with the Excel file, the column headers, and the result rows

    :param es_result (dict): The result of the Elasticsearch search
    :param column_mapping (dict): A dict mapping the column names in the original data, to the desired column headers
    for the output file

    :return (IO.BytesIO): A stream with the Excel file
    """

    # Create a file stream
    output = BytesIO()

    # Create a workbook with the file stream, and worksheet object
    workbook = xlsxwriter.Workbook(output)
    worksheet = workbook.add_worksheet()

    # Get ordered lists of the column headers ('column titles') and the column names (how the fields are actually called
    # in the ElasticSearch results)
    document_source = es_result['search_results'][0]['source']
    column_headers, column_names = _get_column_headers_and_names(document_source, column_mapping)

    # Write the first row of column names
    for index, column_header in enumerate(column_headers):
        worksheet.write(0, index, column_header)

    # Write the rows
    documents = es_result['search_results']
    for document_index, document in enumerate(documents):
        # Loop over the columns
        for column_index, column_name in enumerate(column_names):
            column_value = _get_field_value(column_name, document['source'])
            worksheet.write(document_index+1, column_index, column_value)

    # Close the workbook before streaming the data.
    workbook.close()

    # Rewind the buffer.
    output.seek(0)

    return output


def _get_column_headers_and_names(document_source, column_mapping):
    """
    Creates ordered lists of column headers (the 'titles of the columns), and column names (the name of the fields in
    the ElasticSearch results, given the column mapping.

    So, for example, given the following column_mapping:
    {
      "עיר": "address.city"
      "תקציב": "details.budget"
    }
    The returned values will be:
    - column_headers: ['עיר', 'תקציב']
    - column_names: ['address.city', 'details.budget']

    If there is no column_mapping sent, the column_headers will be the same as the column_names

    :param document_source: A single document from the Elasticsearch search result
    :param column_mapping (dict): A dict mapping the column names in the original data, to the desired column headers
    for the output file (see example above)

    :return (tuple): A tuple consisting of column_headers and column_names, both lists (see example above)
    """

    # In case of column mapping, set the order of the columns and then get the headers list and the names list
    if column_mapping:
        ordered_columns = [(field, column_mapping[field]) for field in column_mapping]
        column_headers = [(field[1]) for field in ordered_columns]
        column_names = [(field[0]) for field in ordered_columns]

    # If no column_mapping is sent, simply set the order, and the column_names are the same as the column_headers
    else:
        column_headers = [field for field in document_source]
        column_names = column_headers

    return column_headers, column_names


def _get_field_value(field_to_get, object_to_search):
    """
    Gets the value of a nested object, for example, if we search
    'address.city.neighborhood' in the object:
    {'address':
        {'city':
            {'neighborhood': 'shapira'}
        }
    }
    The returned value will be 'shapira'

    :param field_to_get:
    :param object_to_search:
    :return (string): The value of the field that needs to be searched
    """

    # Split the field to get up in parts (in the example above: ['address', 'city', 'neighborhood']
    field_parts = field_to_get.split('.')

    # Keep popping field parts and retrieving the nested objects until the last part is reached.
    while len(field_parts) > 0:
        # Get the first 'field part' (in the example above, 'address') and pop it from the parts
        field_part = field_parts.pop(0)
        # Get the nested object from the object (in the example above: {'address': {'...':{'...': {}}}}
        object_to_search = object_to_search.get(field_part, '')

    return object_to_search
