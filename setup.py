#!/usr/bin/env python

from distutils.core import setup

setup(name='haste_storage_client',
      version='0.2',
      py_modules=['haste_storage_client'],
      install_requires=[
          'pymongo',
          'python-swiftclient',
          'keystoneauth1',
      ],
      )
