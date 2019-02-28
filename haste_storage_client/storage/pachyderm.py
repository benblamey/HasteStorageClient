import python_pachyderm
from python_pachyderm.client.pfs.pfs_pb2 import RepoInfo
import logging
from .storage import Storage


class PachydermStorage(Storage):

    def __init__(self, config, id):
        self.config = config
        self.id = id

        # Note, auth is only available in enterprise. Currently not supported by the HSC.
        # http://docs.pachyderm.io/en/latest/enterprise/auth.html#understanding-pachyderm-access-controls

        self.repo_name = config['repo']
        self.branch = config['branch']

        self.client = python_pachyderm.PfsClient(host=config['host'], port=config['port'])

        self.confirmed_repo_exists = False

    def save_blob(self, blob_bytes, blob_id, stream_id, metadata):
        # Ensure the target repo exists.
        # As there may be many instances, to be safe just try to create it and check the error:
        if not self.confirmed_repo_exists:
            try:
                logging.debug('Creating pachyderm repo to check it exists...')
                self.client.create_repo(self.repo_name)
                logging.debug('Pachyderm repo created.')
            except Exception as ex:
                if 'already exists' not in str(ex).lower():
                    logging.debug('Pachyderm repo already exists.')
                    raise ex

            self.confirmed_repo_exists = True

        with self.client.commit(self.repo_name, self.branch) as cmt:
            self.client.put_file_bytes(cmt, stream_id + '/' + blob_id, blob_bytes)
            logging.debug('File put in pachyderm successfully')

    def close(self):
        pass
