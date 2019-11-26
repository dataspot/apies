import os

import elasticsearch

from flask import Flask
from flask_cors import CORS

from apies import apies_blueprint

DATAPACKAGE = 'data/datapackage.json'
ES_HOST = os.environ.get('ES_HOST', 'localhost')
ES_PORT = int(os.environ.get('ES_PORT', '9200'))
INDEX_NAME = 'jobs'

app = Flask(__name__)
CORS(app)
blueprint = apies_blueprint(
    app,
    [DATAPACKAGE],
    elasticsearch.Elasticsearch([dict(host=ES_HOST, port=ES_PORT)], timeout=60),
    dict(jobs='jobs-job'),
    'jobs-document',
    dont_highlight={
        'Salary Frequency',
    },
    multi_match_type='most_fields',
    multi_match_operator='or',
    debug_queries=True,
    text_field_rules=lambda x: [('inexact', '^2')] if x['name'] in ('Residency Requirement', 'Job Description', 'Business Title') else [('inexact', '')]
)
app.register_blueprint(blueprint, url_prefix='/api/')

if __name__ == '__main__':
    app.run()
