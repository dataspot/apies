from apies.controllers import Controllers

DONT_HIGHLIGHT = {
    'kind',
    'kind_he',
    'budget_code',
    'entity_kind',
    'entity_id',
    'code',
}


def test_highlight_merge():

    c = Controllers()

    source = {
        'a': 'a simple string',
        'ax': 'an unrelated string',
        'ab': True,
        'ai': 5,
        'b': {
            'prop': 'a simple internal property',
            'propx': 'an unrelated internal property',
            'propn': None,
            'propi': 8,
            'propf': 8.0,
        },
        'c': [
            {'arrayprop': 'simple'},
            {'arrayprop': 'unrelated'},
        ],
        'd': ['simple', 'unrelated', 'simple'],
        'dd': [['simple', 'unrelated'], ['simple']]
    }

    highlights = {
        'a': ['<em>simple</em>'],
        'b.prop': ['<em>simple</em>'],
        'c.arrayprop': ['<em>simple</em>'],
        'd': ['<em>simple</em>'],
        'dd': ['<em>simple</em>'],
    }

    source = c._merge_highlight_into_source(source, highlights, DONT_HIGHLIGHT)

    assert source == {
        'a': 'a <em>simple</em> string',
        'ax': 'an unrelated string',
        'ab': True,
        'ai': 5,
        'b': {
            'prop': 'a <em>simple</em> internal property',
            'propx': 'an unrelated internal property',
            'propn': None,
            'propi': 8,
            'propf': 8.0,
        },
        'c': [
            {'arrayprop': '<em>simple</em>'},
            {'arrayprop': 'unrelated'},
        ],
        'd': ['<em>simple</em>', 'unrelated', '<em>simple</em>'],
        'dd': [['<em>simple</em>', 'unrelated'], ['<em>simple</em>']]
    }