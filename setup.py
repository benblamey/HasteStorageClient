#!/usr/bin/env python

from setuptools import setup

setup(name='haste_storage_client',
      packages=['haste_storage_client',
                'haste_storage_client.models'],
      install_requires=[
          'pymongo',
          'python-swiftclient',
          'keystoneauth1',
          'future',
      ],
      test_requires=[
          'pytest'
      ]
      )
