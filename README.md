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