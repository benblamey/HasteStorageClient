import time
import datetime
from haste_storage_client.core import HasteStorageClient, OS_SWIFT_STORAGE, TRASH
from haste_storage_client.models.rest_interestingness_model import RestInterestingnessModel

haste_storage_client_config = {
    'haste_metadata_server': {
        # See: https://docs.mongodb.com/manual/reference/connection-string/
        # Note that the name of the database is fixed.
        # In later versions of MongoDB (3?), it needs to be specified in the connection string.
        'connection_string': 'mongodb://130.xxx.yy.zz:27017/streams'
    },
    "log_level": "DEBUG",  # Optional, defaults to 'INFO'. See https://docs.python.org/3/library/logging.html#levels for possible values.
    # Note the structure changed here in January 2019:
    'targets': [
        {
            'id': 'os_swift',  # ID used by the policy. User-defined. Needs to consistent between sessions.
            'class': 'haste_storage_client.storage.storage.OsSwiftStorage',  # Needs to match a class in haste_storage_client.storage
            'config': {
                # See: https://docs.openstack.org/keystoneauth/latest/
                #   api/keystoneauth1.identity.v3.html#module-keystoneauth1.identity.v3.password
                'username': 'xxxxx',
                'password': 'xxxx',
                'project_name': 'xxxxx',
                'user_domain_name': 'xxxx',
                'auth_url': 'xxxxx',
                'project_domain_name': 'xxxx'
            }
        },
        {
            "id": "my-pachyderm-setup",  # ID to use in the storage policy
            "class": "haste_storage_client.storage.pachyderm.PachydermStorage",
            "config": {
                "host": "myhost",  # pachyderm hostname
                "port": 1234,  # pachyderm port
                "repo": "myrepo",
                "branch": "master"
            }
        }
    ]
}

# Identifies both the experiment, and the session (ie. unique each time the stream starts),
# for example, this would be a good format - this needs to be generated at the stream edge.
initials = 'anna_exampleson'
stream_id = datetime.datetime.today().strftime('%Y_%m_%d__%H_%M_%S') + '_exp1_' + initials

print('stream ID is: %s' % stream_id)

# Optionally, specify REST server with interesting model:
interestingness_model = RestInterestingnessModel('http://localhost:5000/model/api/v0.1/evaluate')

client = HasteStorageClient(stream_id,
                            config=haste_storage_client_config,
                            interestingness_model=interestingness_model,
                            storage_policy=[(0.5, 1.0, 'spjuth-lab-pachyderm')],  # map 0.5<=interestingness<=1.0 to OS swift, discard others.
                            # storage_policy=[(0.5, 1.0, 'spjuth-lab-pachyderm')],  # map 0.5<=interestingness<=1.0 to Pachyderm, discard others.
                            )

blob_bytes = b'this is a binary blob eg. image data.'
timestamp_cloud_edge = time.time()
substream_id = 'B13'  # Group by microscopy well ID.

client.save(timestamp_cloud_edge,
            (12.34, 56.78),
            substream_id,
            blob_bytes,
            {'image_height_pixels': 300,  # bag of extracted features here
             'image_width_pixels': 300,
             'number_of_green_pixels': 1234})

client.close()
