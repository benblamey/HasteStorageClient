import python_pachyderm

from .storage import Storage


class PachydermStorage(Storage):

    def __init__(self, config, id):
        self.config = config
        self.id = id

        # Note, auth is only available in enterprise
        # http://docs.pachyderm.io/en/latest/enterprise/auth.html#understanding-pachyderm-access-controls

        self.repo_name = config['repo']
        self.branch = config['branch']

        self.client = python_pachyderm.PfsClient(host=config['host'], port=config['port'])

    def save_blob(self, blob_bytes, blob_id, stream_id):
        if not self.created_repo:
            self.client.create_repo(self.repo_name)
            self.created_repo = True

        with self.client.commit(self.repo_name, self.branch) as cmt:
            self.client.put_file_bytes(cmt, stream_id + '/' + blob_id, blob_bytes)

    def close(self):
        pass
