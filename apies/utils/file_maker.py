import csv
import xlwt
import xlsxwriter

from io import BytesIO, StringIO


def get_csv(es_result, column_mapping):
    """
    Creates a string for a csv file through the Python csv writer and StringIo

    :param (dict): The result of the ElasticSearch search
    :param column_mapping (dict): A

    :return column_mapping (dict): A dict mapping the column names in the original data, to the desired column headers
    for the output file
    """

    # Create a string stream and writer object, passing the stram
    file_stream = StringIO()
    writer = csv.writer(file_stream)

    # Get a list of column headers for the first row, and a list of the names (keys) of the columns
    column_names, column_list = _get_column_headers_and_names(es_result, column_mapping)

    # Write the headers
    writer.writerow(column_names)

    # Write the rows
    for document in es_result['search_results']:
        # For every document, loop over the column names and find the value for the column
        column_values = []
        for column_name in column_list:
            column_values.append(document['source'][column_name])

        # Write the document row
        writer.writerow(column_values)

    return file_stream.getvalue()


def get_xls(es_result, column_mapping):
    """
    Creates a stream with the Excel file, the column headers, and the result rows

    :param es_result: The result of the Elasticsearch search
    :param column_mapping (dict): A dict mapping the column names in the original data, to the desired column headers
    for the output file

    :return (BytesIO): A stream with the Excel file
    """

    # Create a bytes stream
    file_stream = BytesIO()

    # Create an excel workbook and sheet
    workbook = xlwt.Workbook()
    worksheet = workbook.add_sheet('result')

    # Get a list of column names and a list of the names (keys) of the columns
    column_names, column_list = _get_column_headers_and_names(es_result, column_mapping)

    # Write the first row of column names
    for index, column_name in enumerate(column_names):
        worksheet.write(0, index, column_name)

    # Write the rows
    for document_index, document in enumerate(es_result['search_results']):
        for column_index, column_name in enumerate(column_list):
            column_value = document['source'][column_name]
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

    # Get a list of column names and a list of the names (keys) of the columns
    column_names, column_list = _get_column_headers_and_names(es_result, column_mapping)

    # Write the first row of column names
    for index, column_name in enumerate(column_names):
        worksheet.write(0, index, column_name)

    # Write the rows
    for document_index, document in enumerate(es_result['search_results']):
        for column_index, column_name in enumerate(column_list):
            column_value = document['source'][column_name]
            worksheet.write(document_index+1, column_index, column_value)

    # Close the workbook before streaming the data.
    workbook.close()

    # Rewind the buffer.
    output.seek(0)

    return output


def _get_column_headers_and_names(es_result, column_mapping):
    """
    Sets the order of the columns, creates a list of the column headers, and a list of the column names, given the
    column mapping.

    So, for example, given the following column_mapping:
    {
      "עיר": "city"
      "תקציב": "budget"
    }
    The returned values will be:
    - column_headers: ['עיר', 'תקציב']
    - column_names: ['city', 'budget']

    If there is no column_mapping sent, the column_headers will be the same as the column_names

    :param es_result: The result of the Elasticsearch search
    :param column_mapping (dict): A dict mapping the column names in the original data, to the desired column headers
    for the output file

    :return (tuple): A tuple consisting of column_headers and column_names, both lists (see above)
    """

    # Swap the 'destination name' and the 'original name' of the column mapping
    if column_mapping:
        column_mapping = {y: x for x, y in column_mapping.items()}

    # Create a list that defines the order of the columns, and a list of the column names
    column_list = []
    column_names = []
    for field in es_result['search_results'][0]['source']:

        # If there is column mapping, check if the field name is present, if so, take the 'destination name' for the
        # column header. If the field is not present in the column map, it should not be added to the file
        if column_mapping:
            if field not in column_mapping:
                continue
            column_names.append(column_mapping[field])

        else:
            column_names.append(field)

        column_list.append(field)

    return column_names, column_list
