# -*- coding: utf-8 -*-
from setuptools import setup

setup(
    name='lucena',
    version='0.5',
    description='Microservices library for Lucena Project',
    url='https://github.com/lucenaproject/lucena.py',
    author='Fernando Miranda',
    author_email=''.join(['fcmiranda', '@', 'gmail', '.', 'com']),
    license='MIT',
    install_requires=['pyzmq'],
    python_requires='>=3.6',
    classifiers=[
        # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'Topic :: Software Development :: Object Brokering',
        'License :: OSI Approved :: MIT License',
    ],
    packages=['lucena'],
)
