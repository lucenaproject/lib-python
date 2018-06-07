#!/usr/bin/env bash

rm -rf ./dist
rm -rf ./build
rm -rf ./*.egg_info
python3 setup.py sdist
python3 setup.py bdist_wheel
twine upload dist/*
