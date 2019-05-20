apies
=====

.. image:: https://travis-ci.org/OpenBudget/apies.svg?branch=master
    :target: https://travis-ci.org/OpenBudget/apies

.. image:: http://img.shields.io/coveralls/OpenBudget/apies.svg?branch=master
    :target: https://coveralls.io/r/OpenBudget/apies?branch=master

apies is a flask blueprint providing an API for accessing and searching an ElasticSearch index created from source datapackages.

endpoints
---------

TBD

configuration
-------------

Flask configuration for this blueprint:


.. code-block:: python

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
