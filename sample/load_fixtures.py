from datapackage_pipelines_elasticsearch.processors.dump.to_index import ESDumper
from tableschema_elasticsearch.mappers import MappingGenerator
import dataflows as DF


class DumpToElasticSearch(ESDumper):
    def __init__(self, indexes, **parameters):
        parameters['indexes'] = indexes
        parameters['engine'] = 'localhost:9200'
        self.mapper_cls = MappingGenerator
        self.index_settings = {'index.mapping.coerce': True}
        self.__params = parameters
        self.stats = {}

    def __call__(self):

        def step(package):
            self.initialize(self.__params)
            self.__datapackage = self.prepare_datapackage(package.pkg.descriptor, self.__params)
            yield package.pkg
            for resource in package:
                resource.spec = resource.res.descriptor
                ret = self.handle_resource(self.schema_validator(resource),
                                           resource.res.descriptor, self.__params, package.pkg.descriptor)
                yield ret
            self.finalize()

        return step

    def initialize(self, parameters):
        parameters['reindex'] = False
        return super(DumpToElasticSearch, self).initialize(parameters)


if __name__ == '__main__':
    DF.Flow(
        DF.load('new-york-city-current-job-postings.zip', filename='nyc-jobs.csv', name='jobs'),
        DF.add_field('doc_id', 'string', default=lambda row: 'job/{Job ID}'.format(**row)),
        DF.add_field('score', 'integer', default=1),
        DF.set_primary_key(['doc_id']),
        DumpToElasticSearch({
            'jobs': [
                {
                    'resource-name': 'jobs',
                    'doc-type': 'jobs'
                }
            ]
        })(),
        DF.dump_to_path('data'),
        DF.add_field('value', 'object',
                    default=lambda row: dict((k, v) for k, v in row.items() if k not in ('doc_id', 'score')),
                    **{'es:index': False}),
        DF.select_fields(['doc_id', 'value']),
        DumpToElasticSearch({
            'jobs': [
                {
                    'resource-name': 'jobs',
                    'doc-type': 'document'
                }
            ]
        })(),
        DF.printer(fields=['doc_id'])
    ).process()
