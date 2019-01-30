#!/usr/bin/env python

from setuptools import setup

setup(name='haste_storage_client',
      version='0.12',
      packages=['haste_storage_client',
                'haste_storage_client.models',
                'haste_storage_client.storage'
                ],
      description='Client for the HASTE storage plaform: http://haste.research.it.uu.se',
      author='Ben Blamey',
      author_email='ben.blamey@it.uu.se',
      url='http://haste.research.it.uu.se',
      install_requires=[
          'pymongo',
          'python-swiftclient',
          'keystoneauth1',
          'future',
          # TODO: add Pachyderm (when its fixed)
      ],
      test_requires=[
          'pytest'
      ]
      )
