# apies

[![Travis](https://img.shields.io/travis/OpenBudget/apies/master.svg)](https://travis-ci.org/datahq/apies)
[![Coveralls](http://img.shields.io/coveralls/OpenBudget/apies.svg?branch=master)](https://coveralls.io/r/OpenBudget/apies?branch=master)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/apies.svg)

apies is a flask blueprint providing an API for accessing and searching an ElasticSearch index created from source datapackages.

## endpoints

### `/get/<doc-id>`

Fetches a document from the index.

Query parameters that can be used:
- **type**: The type of the document to fetch (if not `docs`)

### `/search/count`

### `/search/<doc-types>`

Performs a search on the index.

`doc-types` is a comma separated list of document types to search.

Query parameters that can be used:
- **q**: The full text search textual query

- **filter**: A JSON object with filters to apply to the search. These are applied to the query but don't affect the scoring of the results.
  Filters should be an array of objects, each object depicting a single filter. All filters are combined with an `OR` operator. For example:
    ```
    [
        {
            "first-name": "John",
            "last-name": "Watson"
        },
        {
            "first-name": "Sherlock",
            "last-name": "Holmes"
        }
    ]
    ```
  Each object contains a set of rules that all must match. Each rule is a key-value pair, where the key is the field name and the value is the value to match. The value can be a string or an array of strings. If the value is an array, the rule will match if any of the values in the array match. For example:
    ```
    {
        "first-name": ["Emily", "Charlotte"],
        "last-name": "Bronte"
    }
    ```
  Field names can be appended with two underscores and an operator to convey other relations other than equality. For example:
    ```
    {
        "first-name": "Emily",
        "last-name": "Bronte",
        "age__gt": 30,
    }
    ```
  Allowed operators are:
  ('gt', 'gte', 'lt', 'lte', 'eq', 'not', 'like', 'bounded', 'all'):
    - `gt`: greater than
    - `gte`: greater than or equal to
    - `lt`: less than
    - `lte`: less than or equal to
    - `eq`: equal to
    - `not`: not equal to
    - `like`: like (textual match)
    - `bounded`: bounded (geospatial match to a bounding box)
    - `all`: all (for arrays - all values in the array must exist in the target)

  If multiple operators are needed for the same field, the field can also be suffixed by a hashtag and a number. For example:
    ```
    {
        "city": "San Francisco",
        "price__lt": 300000,
        "bedrooms__gt": 4,
        "amenities": "garage",
        "amenities#1": ["pool", "back yard"],
    }
    ```
    The above filter will match all documents where the `city` is "San Francisco", `price` is less than 300000, more than 4 `bedrooms`, the `amenities` field contains 'garage' and at least one of "pool" and "back yard".

- **lookup**: A JSON object with lookup filters to apply to the search. These filter the results, but also affect the scoring of the results.
- **context**: A textual context to search in (i.e. run the search in a subset of results matching the full-text-search query provided in this field)

- **extra**: Extra information that's passed to library extensions

- **size**: Number of results to fetch (default: 10)
- **offset**: Offset of first result to fetch (default: 0)
- **order**: Order results by (default: _score)

- **highlight**: Commas separated list of fields to highlight
- **snippets**: Commas separated list of fields to fetch snippets from

- **match_type**: ElasticSearch match type (default: most_fields)
- **match_operator**: ElasticSearch match operator (default: and)
- **minscore**: Minimum score for a result to be returned (default: 0.0)

### `download/<doctypes>`

Downloads search results in either csv, xls or xlsx format.

Query parameters that can be used:
- **types_formatted**: The type of the documents to search
- **search_term**: The Elastic search query
- **size**: Number of hits to return
- **offset**: Whether or not term offsets should be returned
- **filters**: What offset to use for the pagination
- **dont_highlight**:
- **from_date**: If there should be a date range applied to the search, and from what date
- **to_date**: If there should be a date range applied to the search, and until what date
- **order**:
- **file_format**: The format of the file to be returned, either 'csv', 'xls' or 'xlsx'.
If not passed the file format will be xlsx
- **file_name**: The name of the file to be returned, by default the name will be 'search_results'
- **column_mapping**: If the columns should get a different name then in the
original data, a column map can be send, for example:
```
{
  "עיר": "address.city",
  "תקציב": "details.budget"
}
```

For example, get a csv file with column mapping:
```
http://localhost:5000/api/download/jobs?q=engineering&size=2&file_format=csv&file_name=my_results&column_mapping={%22mispar%22:%22Job%20ID%22}
```

Or get an xslx file without column mapping:
```
http://localhost:5000/api/download/jobs?q=engineering&size=2&file_format=xlsx&file_name=my_results
```

## configuration

Flask configuration for this blueprint:


```python

    from apies import apies_blueprint
    import elasticsearch

    app.register_blueprint(
        apies_blueprint(['path/to/datapackage.json', Package(), ...],
                        elasticsearch.Elasticsearch(...), 
                        {'doc-type-1': 'index-for-doc-type-1', ...}, 
                        'index-for-documents',
                        dont_highlight=['fields', 'not.to', 'highlight'],
                        text_field_rules=lambda schema_field: [], # list of tuples: ('exact'/'inexact'/'natural', <field-name>)
                        multi_match_type='most_fields',
                        multi_match_operator='and'),
        url_prefix='/search/'
    )
```

## local development

You can start a local development server by following these steps:

1. Install Dependencies:
    
    a. Install Docker locally
    
    b. Install Python dependencies:

    ```bash
    $ pip install dataflows dataflows-elasticsearch
    $ pip install -e .
    ```
2. Go to the `sample/` directory
3. Start ElasticSearch locally:
   ```bash
   $ ./start_elasticsearch.sh
   ```

   This script will wait and poll the server until it's up and running.
   You can test it yourself by running:
   ```bash
   $ curl -s http://localhost:9200
        {
        "name" : "99cd2db44924",
        "cluster_name" : "docker-cluster",
        "cluster_uuid" : "nF9fuwRyRYSzyQrcH9RCnA",
        "version" : {
            "number" : "7.4.2",
            "build_flavor" : "default",
            "build_type" : "docker",
            "build_hash" : "2f90bbf7b93631e52bafb59b3b049cb44ec25e96",
            "build_date" : "2019-10-28T20:40:44.881551Z",
            "build_snapshot" : false,
            "lucene_version" : "8.2.0",
            "minimum_wire_compatibility_version" : "6.8.0",
            "minimum_index_compatibility_version" : "6.0.0-beta1"
        },
        "tagline" : "You Know, for Search"
        }
   ```
4. Load data into the database
   ```bash
   $ DATAFLOWS_ELASTICSEARCH=localhost:9200 python load_fixtures.py
   ```
   You can test that data was loaded:
   ```bash
   $ curl -s http://localhost:9200/jobs-job/_count?pretty
    {
        "count" : 1757,
        "_shards" : {
            "total" : 1,
            "successful" : 1,
            "skipped" : 0,
            "failed" : 0
        }
    }
   ```
5. Start the sample server
   ```bash
   $ python server.py 
    * Serving Flask app "server" (lazy loading)
    * Environment: production
    WARNING: Do not use the development server in a production environment.
    Use a production WSGI server instead.
    * Debug mode: off
    * Running on http://127.0.0.1:5000/ (Press CTRL+C to quit)
   ```  
6. Now you can hit the server's endpoints, for example:
   ```bash
        $ curl -s 'localhost:5000/api/search/jobs?q=engineering&size=2' | jq
        127.0.0.1 - - [26/Jun/2019 10:45:31] "GET /api/search/jobs?q=engineering&size=2 HTTP/1.1" 200 -
        {
            "search_counts": {
                "_current": {
                "total_overall": 617
                }
            },
            "search_results": [
                {
                "score": 18.812,
                "source": {
                    "# Of Positions": "5",
                    "Additional Information": "TO BE APPOINTED TO ANY CIVIL <em>ENGINEERING</em> POSITION IN BRIDGES, CANDIDATES MUST POSSESS ONE YEAR OF CIVIL <em>ENGINEERING</em> EXPERIENCE IN BRIDGE DESIGN, BRIDGE CONSTRUCTION, BRIDGE MAINTENANCE OR BRIDGE INSPECTION.",
                    "Agency": "DEPARTMENT OF TRANSPORTATION",
                    "Business Title": "Civil Engineer 2",
                    "Civil Service Title": "CIVIL ENGINEER",
                    "Division/Work Unit": "<em>Engineering</em> Review & Support",
            ...
        }
    ```