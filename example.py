import time
import datetime
from haste_storage_client.core import HasteStorageClient
from keystoneauth1.identity import v3

# Create a password auth plugin
# See: https://docs.openstack.org/keystoneauth/latest/api/keystoneauth1.identity.v3.html#module-keystoneauth1.identity.v3.password
auth = v3.Password(auth_url='https://foo.se:5000/v3/',
                   username='my_snic_username',
                   password='my_snic_password',
                   user_domain_name='foo',
                   project_name='my_project',
                   project_domain_name='some_domain')

# Identifies both the experiment, and the session (ie. unique each time the stream starts),
# for example, this would be a good format - this needs to be generated at the stream edge.
stream_id = "strm_" + datetime.datetime.today().strftime('%Y-%m-%d--%H-%M-%S') + "_exp1"

client = HasteStorageClient(stream_id,
                            'localhost',  # IP address of database server.
                            27017,
                            auth)

blob = b'this is a binary blob eg. image data.'
timestamp_cloud_edge = time.time()

client.save(timestamp_cloud_edge,
            blob,
            {'image_height_pixels': 300,  # bag of extracted features here
             'image_width_pixels': 300,
             'number_of_green_pixels': 1234})

client.close()
