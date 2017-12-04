import time
import datetime
from haste_storage_client.core import HasteStorageClient, OS_SWIFT_STORAGE, TRASH
from haste_storage_client.interestingness_model import RestInterestingnessModel


haste_storage_client_config = {
    'haste_metadata_server': {
        'host': '130.xxx.yy.zz',
        'port': 27017
    },
    'os_swift': {
        # See: https://docs.openstack.org/keystoneauth/latest/
        #   api/keystoneauth1.identity.v3.html#module-keystoneauth1.identity.v3.password
        'username': 'xxxxx',
        'password': 'xxxx',
        'project_name': 'xxxxx',
        'user_domain_name': 'xxxx',
        'auth_url': 'xxxxx',
        'project_domain_name': 'xxxx'
    }
}

# Identifies both the experiment, and the session (ie. unique each time the stream starts),
# for example, this would be a good format - this needs to be generated at the stream edge.
initials = 'jb'
stream_id = datetime.datetime.today().strftime('%Y_%m_%d__%H_%M_%S') + '_exp1_' + initials

# Optionally, specify REST server with interesting model:
interestingness_model = RestInterestingnessModel('http://localhost:5000/model/api/v0.1/evaluate')


client = HasteStorageClient(stream_id,
                            config=haste_storage_client_config,
                            interestingness_model=interestingness_model,
                            storage_policy=[(0.5, 1.0, OS_SWIFT_STORAGE)],  # map 0.5<=interestingness<=1.0 to OS swift.
                            default_storage=TRASH)  # discard blobs which don't match the policy above.

blob = b'this is a binary blob eg. image data.'
timestamp_cloud_edge = time.time()

client.save(timestamp_cloud_edge,
            (12.34, 56.78),
            blob,
            {'image_height_pixels': 300,  # bag of extracted features here
             'image_width_pixels': 300,
             'number_of_green_pixels': 1234})

client.close()
