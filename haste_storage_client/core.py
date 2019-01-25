import logging
from pymongo import MongoClient
from os.path import expanduser
from haste_storage_client.storage import storage
import json

# These are deprecated, use the new-style config with custom IDs.
OS_SWIFT_STORAGE = 'os_swift'
TRASH = 'trash'

INTERESTINGNESS_DEFAULT = 1.0


class HasteStorageClient:

    def __init__(self,
                 stream_id,
                 config=None,
                 interestingness_model=None,
                 storage_policy=[],
                 default_storage=None):
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
        :param storage_policy (list): policy mapping closed intervals of interestingness values to storage platform IDs, eg.:
            [(0.5, 1.0, 'my_os_swift')]
            Overlapping intervals mean that the blob will be saved to multiple classes. 
            Storage platform IDs need to be included in the config.
            If no storage class matches, object will not be written.
        :param default_storage (str): deprecated. Now, blobs which don't match the policy are not saved.

        """

        if config is None:
            try:
                config = self.__read_config_file()
            except:
                raise ValueError('If config is None, provide a configuration file.')

        if 'os_swift' in config:  # In the old-style config, class names were hard-coded.
            logging.warning('old (pre 2019) style configuration detected, please update.')
            config_new = {
                'haste_metadata_server': config['haste_metadata_server'],
                'targets': [{
                    'id': 'os_swift',
                    'class': 'OsSwiftStorage',
                    'config': config[OS_SWIFT_STORAGE]
                }]
            }
            config = config_new

        if len(storage_policy) == 0:
            logging.warning('storage policy empty, no blobs will be saved.')

        if default_storage is not None:
            logging.warning('default_storage is deprecated. Now, blobs which dont match the policy are not saved.')

        self.stream_id = stream_id
        self.interestingness_model = interestingness_model
        self.storage_policy = storage_policy

        self.targets = {t['id']: getattr(storage, t['class'])(t['config'], t['id']) for t in config['targets']}

        self.mongo_client = MongoClient(config['haste_metadata_server']['connection_string'])
        self.mongo_collection = self.mongo_client.get_database()['strm_' + self.stream_id]

        # ensure indices (idempotent)
        self.mongo_collection.create_index('substream_id')
        self.mongo_collection.create_index('timestamp')
        self.mongo_collection.create_index('location')

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

        interestingness = self.__get_interestingness(timestamp=timestamp,
                                                     location=location,
                                                     substream_id=substream_id,
                                                     metadata=metadata)
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
        result = self.mongo_collection.insert(document)

        return document

    def close(self):
        self.mongo_client.close()
        for key, storage_plaform in self.targets.items():
            storage_plaform.close()

    def __save_blob(self, blob_id, blob_bytes, interestingness):
        storage_platforms = []
        if self.storage_policy is not None:
            for min_interestingness, max_interestingness, storage_id in self.storage_policy:
                if min_interestingness <= interestingness <= max_interestingness and (storage_id not in storage_platforms):
                    self.__save_blob_to_platform(blob_bytes, blob_id, storage_id)
                    storage_platforms.append(storage_id)

        if len(storage_platforms) == 0:
            logging.info('no storage platform matched in policy for blob with ID: {}, interestingness: {}', blob_id, interestingness)

        return storage_platforms

    def __save_blob_to_platform(self, blob_bytes, blob_id, storage_platform_id):
        if storage_platform_id in self.targets:
            self.targets[storage_platform_id].save_blob(blob_bytes, blob_id)
        else:
            raise ValueError('unknown storage platform')

    def __get_interestingness(self,
                              timestamp=None,
                              location=None,
                              substream_id=None,
                              metadata=None):
        if self.interestingness_model is not None:
            try:
                result = self.interestingness_model.interestingness(timestamp=timestamp,
                                                                    location=location,
                                                                    substream_id=substream_id,
                                                                    metadata=metadata,
                                                                    stream_id=self.stream_id,
                                                                    mongo_collection=self.mongo_collection)
                interestingness = result['interestingness']
            except Exception as ex:
                logging.error(ex)
                logging.error('interestingness exception - falling back to ' + str(INTERESTINGNESS_DEFAULT))
                interestingness = INTERESTINGNESS_DEFAULT
        else:
            interestingness = INTERESTINGNESS_DEFAULT
        return interestingness
