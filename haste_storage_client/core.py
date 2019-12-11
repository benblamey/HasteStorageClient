from abc import ABCMeta, abstractmethod

from pymongo import MongoClient
from os.path import expanduser
import os
import json
import logging

LOGGING_FORMAT_DATE = '%Y-%m-%d %H:%M:%S.%d3'
LOGGING_FORMAT = '%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'

logging.basicConfig(level=logging.INFO,  # Can be overridden in config file.
                    format=LOGGING_FORMAT,
                    datefmt=LOGGING_FORMAT_DATE)

# These are deprecated, use the new-style config with custom IDs.
OS_SWIFT_STORAGE = 'os_swift'
TRASH = 'trash'

INTERESTINGNESS_DEFAULT = 1.0


class HasteClient():
    """
    Base class for various HASTE clients -- e.g. those managing (tiered) storage, and prioritized processing queues.
    """

    @abstractmethod
    def save(self, timestamp, location, substream_id, blob_bytes, metadata):
        """
        :param timestamp (numeric): should come from the cloud edge (eg. microscope). integer or floating point.
            *Uniquely identifies the document within the streaming session*.
        :param location (tuple): spatial information (eg. (x,y)).
        :param substream_id (string): ID for grouping of documents in stream (eg. microscopy well ID), or 'None'.
        :param blob_bytes (byte array): binary blob (eg. image).
        :param metadata (dict): extracted metadata (eg. image features).
        """
        pass

    @abstractmethod
    def close(self):
        pass


class HasteTieredClient(HasteClient):

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
        :param storage_policy (list): policy mapping (closed lower bound, open upper bound) intervals of interestingness values to storage platform IDs, eg.:
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
                    'class': 'haste_storage_client.storage.storage.OsSwiftStorage',
                    'config': config[OS_SWIFT_STORAGE]
                }]
            }
            config = config_new

        if 'log_level' in config:
            log_level = config['log_level']
            logging.getLogger().setLevel(log_level.upper())
            logging.info('Log level set to: ' + log_level)

        if len(storage_policy) == 0:
            logging.warning('storage policy empty, no blobs will be saved.')

        if default_storage is not None:
            logging.warning('default_storage is deprecated. Now, blobs which dont match the policy are not saved.')

        self.stream_id = stream_id
        self.interestingness_model = interestingness_model
        self.storage_policy = storage_policy
        self.targets = {t['id']: self.instantiate_target(t) for t in config['targets']}

        if os.getenv('DUMMY_MONGODB_HOST', '').lower() == 'true':
            # We're running in a unit test, use a short server timeout.
            mongo_kwargs = {'serverSelectionTimeoutMS': 100}
        else:
            mongo_kwargs = {}

        self.mongo_client = MongoClient(config['haste_metadata_server']['connection_string'], **mongo_kwargs)
        self.mongo_collection = self.mongo_client.get_database()['strm_' + self.stream_id]

        # ensure indices (idempotent)
        self.mongo_collection.create_index('substream_id')
        self.mongo_collection.create_index('timestamp')
        self.mongo_collection.create_index('location')
        self.mongo_collection.create_index('metadata.original_filename')

    def my_import(self, name):
        components = name.split('.')
        print('importing: ' + str(components))
        mod = __import__('.'.join(components[0:-1]))
        for comp in components[1:]:
            mod = getattr(mod, comp)
        return mod

    def instantiate_target(self, tgt):
        klass = self.my_import(tgt['class'])
        return klass(tgt['config'], tgt['id'])

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
        blob_storage_platforms = self.__save_blob(blob_id, blob_bytes, interestingness, metadata)
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

    def __save_blob(self, blob_id, blob_bytes, interestingness, metadata):
        storage_platforms = []
        if self.storage_policy is not None:
            for min_interestingness, max_interestingness, storage_id in self.storage_policy:
                if min_interestingness < interestingness <= max_interestingness and (
                        storage_id not in storage_platforms):
                    self.__save_blob_to_platform(blob_bytes, blob_id, storage_id, metadata)
                    storage_platforms.append(storage_id)

        if len(storage_platforms) == 0:
            logging.info('no storage platform matched in policy for blob with ID: %s, interestingness: %f', blob_id,
                         interestingness)

        return storage_platforms

    def __save_blob_to_platform(self, blob_bytes, blob_id, storage_platform_id, metadata):
        if storage_platform_id in self.targets:
            self.targets[storage_platform_id].save_blob(blob_bytes, blob_id, self.stream_id, metadata)
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


# Alias for backwards compatibility
HasteStorageClient = HasteTieredClient
