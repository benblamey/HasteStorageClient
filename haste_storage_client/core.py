from pymongo import MongoClient
from os.path import expanduser
from .storage import OsSwiftStorage
import json

OS_SWIFT_STORAGE = 'os_swift'
TRASH = 'trash'
INTERESTINGNESS_DEFAULT = 1.0


class HasteStorageClient:

    def __init__(self,
                 stream_id,
                 config=None,
                 interestingness_model=None,
                 storage_policy=None,
                 default_storage=OS_SWIFT_STORAGE):
        """
        :param stream_id (str): ID for the stream session - used to group all the data for that streaming session.
            *unique for each execution of the experiment*.
        :param config (dict): dictionary of credentials and hostname for metadata server and storage,
            see haste_storage_client_config.json for structure.
            MongoDB connection string format, see: https://docs.mongodb.com/manual/reference/connection-string/
            If `None`, will be read from ~/.haste/haste_storage_client_config.json
        :param interestingness_model (InterestingnessModel): determines interestingness of the document,
            and hence the intended storage platform(s) for the blob. 
            `None` implies all documents will have interestingness=1.0
        :param storage_policy (list): policy mapping (closed) intervals of interestingness to storage platforms, eg.:
            [(0.5, 1.0, 'os_swift')]
            Overlapping intervals mean that the blob will be saved to multiple classes. 
            Valid storage platforms are: 'os_swift'
            If `None`, `default_storage` will be used.
        :param default_storage (str): storage platform if no policy matches the interestingness level. 
            valid platforms are those for `storage_policy`, and 'trash' meaning discard the blob.
        """

        if config is None:
            try:
                config = self.__read_config_file()
            except:
                raise ValueError('If config is None, provide a configuration file.')

        if default_storage is None:
            raise ValueError("default_storage_location cannot be None - did you mean 'trash'?")

        self.mongo_client = MongoClient(config['haste_metadata_server']['connection_string'])
        self.mongo_db = self.mongo_client.streams
        self.stream_id = stream_id
        self.interestingness_model = interestingness_model
        self.storage_policy = storage_policy
        self.default_storage = default_storage
        self.os_swift_storage = OsSwiftStorage(config[OS_SWIFT_STORAGE])

    @staticmethod
    def __read_config_file():
        with open(expanduser('~/.haste/haste_storage_client_config.json')) as fh:
            haste_storage_client_config = json.load(fh)
        return haste_storage_client_config

    def save(self,
             timestamp,
             location,
             substream_id,
             blob_bytes,
             metadata):
        """
        :param timestamp (numeric): should come from the cloud edge (eg. microscope). integer or floating point.
            *Uniquely identifies the document within the streaming session*.
        :param location (tuple): spatial information (eg. (x,y)).
        :param substream_id (string): ID for grouping of documents in stream (eg. microscopy well ID), or 'None'.
        :param blob_bytes (byte array): binary blob (eg. image).
        :param metadata (dict): extracted metadata (eg. image features).
        """

        mongo_collection = self.mongo_db['strm_' + self.stream_id]

        interestingness = self.__get_interestingness(timestamp=timestamp,
                                                     location=location,
                                                     substream_id=substream_id,
                                                     metadata=metadata,
                                                     mongo_collection=mongo_collection)
        blob_id = 'strm_' + self.stream_id + '_ts_' + str(timestamp)
        blob_storage_platforms = self.__save_blob(blob_id, blob_bytes, interestingness)
        if len(blob_storage_platforms) == 0:
            blob_id = ''

        document = {'timestamp': timestamp,
                    'location': location,
                    'substream_id': substream_id,
                    'interestingness': interestingness,
                    'blob_id': blob_id,
                    'blob_storage_platforms': blob_storage_platforms,
                    'metadata': metadata, }
        result = mongo_collection.insert(document)

        return document

    def close(self):
        self.mongo_client.close()
        self.os_swift_storage.close()

    def __save_blob(self, blob_id, blob_bytes, interestingness):
        storage_platforms = []
        if self.storage_policy is not None:
            for min_interestingness, max_interestingness, storage in self.storage_policy:
                if min_interestingness <= interestingness <= max_interestingness and (storage not in storage_platforms):
                    self.__save_blob_to_platform(blob_bytes, blob_id, storage)
                    storage_platforms.append(storage)

        if len(storage_platforms) == 0 and self.default_storage != TRASH:
            self.__save_blob_to_platform(blob_bytes, blob_id, self.default_storage)
            storage_platforms.append(self.default_storage)

        return storage_platforms

    def __save_blob_to_platform(self, blob_bytes, blob_id, storage_platform):
        if storage_platform == OS_SWIFT_STORAGE:
            self.os_swift_storage.save_blob(blob_bytes, blob_id)
        elif storage_platform == TRASH:
            raise ValueError('trash cannot be specified in a storage policy, only as a default')
        else:
            raise ValueError('unknown storage platform')

    def __get_interestingness(self,
                              timestamp=None,
                              location=None,
                              substream_id=None,
                              metadata=None,
                              mongo_collection=None):
        if self.interestingness_model is not None:
            try:
                result = self.interestingness_model.interestingness(timestamp=timestamp,
                                                                    location=location,
                                                                    substream_id=substream_id,
                                                                    metadata=metadata,
                                                                    stream_id=self.stream_id,
                                                                    mongo_collection=mongo_collection)
                interestingness = result['interestingness']
            except Exception as ex:
                print(ex)
                print('interestingness exception - falling back to ' + str(INTERESTINGNESS_DEFAULT))
                interestingness = INTERESTINGNESS_DEFAULT
        else:
            interestingness = INTERESTINGNESS_DEFAULT
        return interestingness
