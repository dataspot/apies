# apies

[![Travis](https://img.shields.io/travis/OpenBudget/apies/master.svg)](https://travis-ci.org/datahq/apies)
[![Coveralls](http://img.shields.io/coveralls/OpenBudget/apies.svg?branch=master)](https://coveralls.io/r/OpenBudget/apies?branch=master)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/apies.svg)

apies is a flask blueprint providing an API for accessing and searching an ElasticSearch index created from source datapackages.

## endpoints

TBD

## configuration

Flask configuration for this blueprint:


```python

    from apies import apies_blueprint
    import elasticsearch

    app.register_blueprint(
        apies_blueprint(['path/to/datapackage.json', Package(), ...],
                        elasticsearch.Elasticsearch(...), 
                        'index-to-search-in', 
                        document_doctype='document',
                        dont_highlight=['fields', 'not.to', 'highlight']),
        url_prefix='/search/'
    )
```

## local development

You can start a local development server by following these steps:

1. Install Dependencies:
    
    a. Install Docker locally
    
    b. Install Python dependencies:

    ```bash
    $ pip install dataflows datapackage-pipelines-elasticsearch
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
        "name" : "DTsRT6T",
        "cluster_name" : "elasticsearch",
        "cluster_uuid" : "QnLVHaOYTkmJZzkCG3Hong",
        "version" : {
            "number" : "5.5.2",
            "build_hash" : "b2f0c09",
            "build_date" : "2017-08-14T12:33:14.154Z",
            "build_snapshot" : false,
            "lucene_version" : "6.6.0"
        },
        "tagline" : "You Know, for Search"
    }
   ```
4. Load data into the database
   ```bash
   $ python load_fixtures.py
   ```
   You can test that data was loaded:
   ```bash
   $ curl -s http://localhost:9200/jobs/_count?pretty
    {
        "count" : 3516,
        "_shards" : {
                "total" : 5,
                "successful" : 5,
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