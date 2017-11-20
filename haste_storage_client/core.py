from pymongo import MongoClient
from keystoneauth1 import session
import swiftclient.client


class HasteStorageClient:
    INTERESTINGNESS_DEFAULT = 1.0

    def __init__(self,
                 stream_id,
                 host,
                 port,
                 keystone_auth,
                 interestingness_model=None):
        """
        :param stream_id: String ID for the stream session - used to group all the data
        (unique for each execution of the experiment)
        :param host: Hostname/IP of database server.
        :param port: Database server port. Usually 27017.
        :param keystone_auth: OpenCloud keystone auth v3 password object,
        see: https://docs.openstack.org/keystoneauth/latest/api/keystoneauth1.identity.v3.html#module-keystoneauth1.identity.v3.password
        :param interestingness_model: InterestingnessModel to determine interestingness of the document,
        and hence the intended storage class.
        """
        self.mongo_client = MongoClient(host, port)
        self.mongo_db = self.mongo_client.streams
        self.stream_id = stream_id
        self.interestingness_model = interestingness_model

        keystone_session = session.Session(auth=keystone_auth)
        self.swift_conn = swiftclient.client.Connection(session=keystone_session)

    def save(self,
             unix_timestamp,
             location,
             blob,
             metadata):
        """
        :param unix_timestamp: should come from the cloud edge (eg. microscope). floating point. uniquely identifies the
        document.
        :param location: n-tuple representing spatial information (eg. (x,y)).
        :param blob: binary blob (eg. image).
        :param metadata: dictionary containing extracted metadata (eg. image features).
        """

        interestingness = self.__interestingness(location, metadata, unix_timestamp)

        blob_id, blob_location = self.__save_blob(blob, interestingness, unix_timestamp)

        result = self.mongo_db['strm_' + self.stream_id].insert({
            'timestamp': unix_timestamp,
            'location': location,
            'interestingness': interestingness,
            'blob_id': blob_id,
            'blob_location': blob_location,
            'metadata': metadata,
        })

    def close(self):
        self.mongo_client.close()
        self.swift_conn.close()

    def __save_blob(self, blob, interestingness, unix_timestamp):
        if interestingness > 0.1:
            blob_id = 'strm_' + self.stream_id + '_ts_' + str(unix_timestamp)
            blob_location = 'swift'
            self.swift_conn.put_object('Haste_Stream_Storage', blob_id, blob)
        else:
            blob_id = None
            blob_location = '(deleted)'
        return blob_id, blob_location

    def __interestingness(self, location, metadata, unix_timestamp):
        if self.interestingness_model is not None:
            try:
                result = self.interestingness_model.interestingness(unix_timestamp,
                                                                    location,
                                                                    metadata)
                interestingness = result['interestingness']
            except Exception as ex:
                print(ex)
                print('falling back to ' + str(self.INTERESTINGNESS_DEFAULT))
                interestingness = self.INTERESTINGNESS_DEFAULT
        else:
            interestingness = self.INTERESTINGNESS_DEFAULT
        return interestingness
