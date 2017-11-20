from pymongo import MongoClient
from keystoneauth1 import session
import swiftclient.client


class HasteStorageClient:
    def __init__(self,
                 stream_id,
                 host,
                 port,
                 keystone_auth):
        """
        :param stream_id: String ID for the stream session - used to group all the data
        (unique for each execution of the experiment)
        :param host: Hostname/IP of database server.
        :param port: Database server port. Usually 27017.
        :param keystone_auth: OpenCloud keystone auth v3 password object,
        see: https://docs.openstack.org/keystoneauth/latest/api/keystoneauth1.identity.v3.html#module-keystoneauth1.identity.v3.password
        """
        self.mongo_client = MongoClient(host, port)
        self.mongo_db = self.mongo_client.streams
        self.stream_id = stream_id

        keystone_session = session.Session(auth=keystone_auth)
        self.swift_conn = swiftclient.client.Connection(session=keystone_session)

    def save(self,
             unix_timestamp,
             location,
             blob,
             metadata):
        """
        :param unix_timestamp: should come from the cloud edge (eg. microscope). floating point.
        :param location: n-tuple representing spatial information (eg. (x,y)).
        :param blob: binary blob (eg. image).
        :param metadata: dictionary containing extracted metadata (eg. image features).
        """
        blob_name = 'strm_' + self.stream_id + '_ts_' + str(unix_timestamp)
        blob_location = 'swift'

        result = self.mongo_db['strm_' + self.stream_id].insert({
            'timestamp': unix_timestamp,
            'location': location,
            'blob_id': blob_name,
            'blob_location': blob_location,
            'metadata': metadata,
        })

        self.swift_conn.put_object('Haste_Stream_Storage', blob_name, blob)

    def close(self):
        self.mongo_client.close()
        self.swift_conn.close()
