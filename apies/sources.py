from copy import copy

from datapackage import Package, Resource
from tableschema import Field

from .logger import logger


def _process_field(field: Field, rules, field_select, ret, prefix):
    schema_type = field['type']
    if schema_type == 'array':
        field = copy(field)
        field['type'] = field['es:itemType']
        return _process_field(field, rules, field_select, ret, prefix)
    enabled = field.get('es:index', True) and not field.get('es:exclude', False)
    subschema = {'fields': []}
    if enabled:
        if schema_type == 'object':
            subschema = field['es:schema']
            _process_schema(subschema, rules, field_select, ret, prefix + field['name'] + '.')
        elif schema_type == 'string':
            search_field = prefix + field['name']
            if field_select is None or search_field in field_select:
                for kind, suffix in rules(field):
                    ret.append((kind, search_field + suffix))


def _process_schema(schema, rules, field_select, ret=[], prefix=''):
    fields = schema['fields']
    for f in fields:
        _process_field(f, rules, field_select, ret, prefix)
    return ret


def extract_text_fields(sources, text_field_rules, text_field_select, debug=False):

    sources = [src if isinstance(src, Package) else Package(src)
               for src in sources]

    ret = {}
    source: Package
    for source in sources:
        resource: Resource = source.resources[0]
        type_name = resource.name
        type_text_field_select = text_field_select.get(type_name) if text_field_select else None
        schema = resource.schema.descriptor
        text_fields = _process_schema(schema, text_field_rules, type_text_field_select, ret=[])
        ret[type_name] = text_fields
        if debug:
            logger.info('TEXT FIELDS (for %s):\n%s', type_name, ', '.join(map(str, text_fields)))


    return ret
