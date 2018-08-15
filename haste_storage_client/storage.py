from __future__ import print_function
from future.utils import with_metaclass
from keystoneauth1 import session
from keystoneauth1.identity import v3
import time
import abc
import swiftclient.client


class Storage(with_metaclass(object, abc.ABCMeta)):
    @abc.abstractmethod
    def save_blob(self, blob_bytes, blob_id):
        raise NotImplementedError('users must define method to use base class')

    @abc.abstractmethod
    def close(self):
        raise NotImplementedError('users must define method to use base class')


# The auth token expires after 24 hours by default, but refresh more frequently:
OS_SWIFT_CONN_MAX_LIFETIME_SECONDS = 60 * 60


class OsSwiftStorage(Storage):

    def __init__(self, config):
        self.config = config
        self.conn = None
        self.conn_timestamp_connected = None
        # Try to connect now, to fail fast:
        self.__reauthenticate_if_needed()

    def save_blob(self, blob_bytes, blob_id):
        self.__reauthenticate_if_needed()

        if isinstance(blob_bytes, bytearray):
            # Workaround a bug in the OpenStack client - 'bytearray' is not properly handled as a content.
            # (see https://bugs.launchpad.net/python-swiftclient/+bug/1741991)
            blob_bytes = bytes(blob_bytes)

        self.conn.put_object('Haste_Stream_Storage', blob_id, blob_bytes)

    def close(self):
        if self.conn is not None:
            self.conn.close()

    def __reauthenticate_if_needed(self):
        if self.conn is None \
                or self.conn_timestamp_connected is None \
                or self.conn_timestamp_connected + OS_SWIFT_CONN_MAX_LIFETIME_SECONDS < time.time():
            print('HasteStorageClient: (re)connecting os_swift...')

            if self.conn is not None:
                self.conn.close()
            self.conn = None
            self.conn_timestamp_connected = None

            auth = v3.Password(**self.config)
            keystone_session = session.Session(auth=auth)
            self.conn = swiftclient.client.Connection(session=keystone_session)
            self.conn_timestamp_connected = time.time()
