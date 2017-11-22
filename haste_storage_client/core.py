from pymongo import MongoClient
from keystoneauth1 import session
from keystoneauth1.identity import v3
import swiftclient.client
from os.path import expanduser
import json

class HasteStorageClient:
    INTERESTINGNESS_DEFAULT = 1.0

    def __init__(self,
                 stream_id,
                 config = None,
                 interestingness_model=None,
                 storage_policy=None,
                 default_storage_class='swift'):
        """
        :param stream_id: String ID for the stream session - used to group all the data
        (unique for each execution of the experiment)
        :param host: Hostname/IP of database server.
        :param port: Database server port. Usually 27017.
        :param keystone_auth: OpenCloud keystone auth v3 password object,
        see: https://docs.openstack.org/keystoneauth/latest/api/keystoneauth1.identity.v3.html#module-keystoneauth1.identity.v3.password
        :param interestingness_model: InterestingnessModel to determine interestingness of the document,
        and hence the intended storage class.
        :param storage_policy: policy mapping interestingness to storage class(es). Supported are 'swift' and
        None (meaning discard).
        :param default_storage_class: default storage class if no matches in policy.
        None means the document will be discarded.
        """
         
        if config is None:
            try:
                config=self._get_config()
            except:
                raise ValueError("If config is None, provide a configuration file.")
                
        if default_storage_class is None:
            raise ValueError("default_storage_location cannot be None - did you mean 'trash'?")

            
        self.mongo_client = MongoClient(config["haste_metadata_db_server"], int(config["haste_metadata_db_port"]))
        self.mongo_db = self.mongo_client.streams
        self.stream_id = stream_id
        self.interestingness_model = interestingness_model
        self.storage_policy = storage_policy
        self.default_storage_class = default_storage_class
        
        # Establish a connection to the OpenStack Swift storage backend
        self.swift_conn = self._get_os_swift_connection(config["os_swift_auth_credentials"])

        
    def _get_config(self):
        home = expanduser("~")
        default_config_dir = home+"/.haste" 
        with open(default_config_dir+"/haste_storage_client_config.json") as fh:
            haste_storage_client_config = json.load(fh)
        return haste_storage_client_config
            
    def _get_os_swift_connection(self, swift_auth_credentials):
        auth = v3.Password(**swift_auth_credentials)
        keystone_session = session.Session(auth=auth)
        return swiftclient.client.Connection(session=keystone_session)

    def save(self,
             unix_timestamp,
             location,
             blob,
             metadata):
        """
        :param unix_timestamp: should come from the cloud edge (eg. microscope). floating point. uniquely identifies the
        document.
        :param location: n-tuple representing spatial information (eg. (bsc,y)).
        :param blob: binary blob (eg. image).
        :param metadata: dictionary containing extracted metadata (eg. image features).
        """

        interestingness = self.__interestingness(location, metadata, unix_timestamp)
        blob_id = 'strm_' + self.stream_id + '_ts_' + str(unix_timestamp)
        blob_storage_classes = self.__save_blob(blob_id, blob, interestingness)

        blob_storage_classes = ['Trash' if bsc is None else bsc for bsc in blob_storage_classes]

        result = self.mongo_db['strm_' + self.stream_id].insert({
            'timestamp': unix_timestamp,
            'location': location,
            'interestingness': interestingness,
            'blob_id': blob_id,
            'blob_storage_classes': blob_storage_classes,
            'metadata': metadata,
        })

    def close(self):
        self.mongo_client.close()
        self.swift_conn.close()

    def __save_blob(self, blob_id, blob, interestingness):
        blob_locations = []
        if self.storage_policy is not None:
            for interestingness_lambda, storage_class in self.storage_policy:
                if interestingness_lambda(interestingness):
                    self.__save_blob_to_class(blob, blob_id, storage_class)
                    blob_locations.append(storage_class)

        if blob_locations.count == 0:
            self.__save_blob_to_class(blob, blob_id, self.default_storage_class)
            blob_locations.append(self.default_storage_class)

        return blob_locations

    def __save_blob_to_class(self, blob, blob_id, storage_class):
        if storage_class is None:
            # Implies discard.
            pass
        if storage_class == 'swift':
            self.swift_conn.put_object('Haste_Stream_Storage', blob_id, blob)
        else:
            raise ValueError('unknown storage class')

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
