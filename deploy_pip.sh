#!/bin/sh
rm -rf dist/ || true
python setup.py sdist bdist_wheel
python -m twine upload --config-file .pypirc --repository apies --verbose dist/*