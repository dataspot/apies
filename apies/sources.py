from copy import copy

from datapackage import Package, Resource
from tableschema import Field


def _process_field(field: Field, rules, ret, prefix):
    schema_type = field['type']
    if schema_type == 'array':
        field = copy(field)
        field['type'] = field['es:itemType']
        return _process_field(field, rules, ret, prefix)
    enabled = field.get('es:index', True) and not field.get('es:exclude', False)
    subschema = {'fields': []}
    if enabled:
        if schema_type == 'object':
            subschema = field['es:schema']
            _process_schema(subschema, rules, ret, prefix + field['name'] + '.')
        elif schema_type == 'string':
            search_field = prefix + field['name']
            for kind, suffix in rules(field):
                ret.append((kind, search_field + suffix))


def _process_schema(schema, rules, ret=[], prefix=''):
    fields = schema['fields']
    for f in fields:
        _process_field(f, rules, ret, prefix)
    return ret


def extract_text_fields(sources, text_field_rules):

    sources = [src if isinstance(src, Package) else Package(src)
               for src in sources]

    ret = {}
    source: Package
    for source in sources:
        resource: Resource = source.resources[0]
        type_name = resource.name
        schema = resource.schema.descriptor
        text_fields = _process_schema(schema, text_field_rules, ret=[])
        ret[type_name] = text_fields

    return ret
