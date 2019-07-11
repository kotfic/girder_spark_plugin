#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

requirements = [
    'girder>=3.0.0a1',
    'pyspark==2.4.3',
    'marshmallow==3.0.0b10'
]

setup(
    author="Chris Kotfila",
    author_email='chris.kotfila@kitware.com',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7'
    ],
    description='A plugin for launching spark jobs through girder',
    install_requires=requirements,
    license='Apache Software License 2.0',
    long_description=readme,
    long_description_content_type='text/x-rst',
    include_package_data=True,
    keywords='girder-plugin, girder_spark_plugin',
    name='girder_spark_plugin',
    packages=find_packages(exclude=['test', 'test.*']),
    url='https://github.com/girder/girder_spark_plugin',
    version='0.1.0',
    zip_safe=False,
    entry_points={
        'girder.plugin': [
            'girder_spark_plugin = girder_spark_plugin.girder:GirderPlugin'
        ],
        'girder_worker_plugins': [
            'girder_spark_plugin = girder_spark_plugin.worker:GirderWorkerPlugin',
        ]
    }
)
