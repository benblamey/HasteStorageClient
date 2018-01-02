from keystoneauth1 import session
from keystoneauth1.identity import v3
import time
import abc
import swiftclient.client


class Storage(object, metaclass=abc.ABCMeta):
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
        print('save_blob', flush=True)
        self.__reauthenticate_if_needed()
        print(blob_id)
        print(blob_bytes)
        self.conn.put_object('Haste_Stream_Storage2', blob_id, blob_bytes)


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
