import os

import elasticsearch

from flask import Flask
from flask_cors import CORS

from apies import apies_blueprint

DATAPACKAGE_BASE = 'http://next.obudget.org/datapackages/budgetkey/{}/datapackage.json'
ES_HOST = os.environ.get('ES_HOST', 'localhost')
ES_PORT = int(os.environ.get('ES_PORT', '9200'))
INDEX_NAME = os.environ.get('INDEX_NAME', 'budgetkey')

app = Flask(__name__)
CORS(app)
blueprint = apies_blueprint(app,
    [
        DATAPACKAGE_BASE.format(doctype)
        for doctype in [
            'people',
            'tenders',
            'entities',
            'contract-spending',
            'national-budget-changes',
            'supports',
            'reports',
            'budget',
            'gov_decisions',
        ]
    ],
    elasticsearch.Elasticsearch([dict(host=ES_HOST, port=ES_PORT)], timeout=60),
    'budgetkey',
    dont_highlight={
        'kind',
        'kind_he',
        'budget_code',
        'entity_kind',
        'entity_id',
        'code',
    },
)
app.register_blueprint(blueprint, url_prefix='/search/')

if __name__ == '__main__':
    app.run()
