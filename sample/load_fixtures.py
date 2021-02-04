from tableschema_elasticsearch.mappers import MappingGenerator
import dataflows as DF
from dataflows_elasticsearch import dump_to_es

class SampleMappingGenerator(MappingGenerator):

    def __init__(self):
        super().__init__(base={})

    @classmethod
    def _convert_type(cls, schema_type, field, prefix):
        prop = super()._convert_type(schema_type, field, prefix)
        if field.get('es:keyword'):
            prop['type'] = 'keyword'
        elif schema_type in ('number', 'integer'):
            prop['index'] = True
        return prop


if __name__ == '__main__':
    DF.Flow(
        DF.load('new-york-city-current-job-postings.zip', filename='nyc-jobs.csv', name='jobs'),
        DF.add_field('doc_id', 'string', default=lambda row: 'job/{Job ID}'.format(**row)),
        DF.add_field('score', 'integer', default=1),
        DF.set_type('Salary Frequency', **{'es:keyword': True}),
        DF.set_primary_key(['doc_id']),
        dump_to_es(indexes={
            'jobs-job': [
                {
                    'resource-name': 'jobs',
                }
            ]
        }, mapper_cls=SampleMappingGenerator),
        DF.dump_to_path('data'),
        DF.add_field('value', 'object',
                    default=lambda row: dict((k, v) for k, v in row.items() if k not in ('doc_id', 'score')),
                    **{'es:index': False}),
        DF.select_fields(['doc_id', 'value']),
        dump_to_es(indexes={
            'jobs-document': [
                {
                    'resource-name': 'jobs',
                }
            ]
        }),
        DF.printer(fields=['doc_id'])
    ).process()
