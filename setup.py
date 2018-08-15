#!/usr/bin/env python

from setuptools import setup

setup(name='haste_storage_client',
      packages=['haste_storage_client'],
      install_requires=[
          'pymongo',
          'python-swiftclient',
          'keystoneauth1',
      ],
      )
