import datetime
import os
import time

import pytest

from haste_storage_client.core import HasteTieredClient

# Skip by default -- requires mongodb
@pytest.mark.skip
def test_instantiate_and_save_movetodir():
    os.environ['DUMMY_MONGODB_HOST'] = 'True'  # We use a dummy hostname, use short timeouts.

    print(os.getcwd())

    os.system('cp scratch/blob.txt scratch/src/')
    os.system('rm -rf scratch/dst/**')

    target_dir = os.getcwd() + '/scratch/dst/'
    src_dir = os.getcwd() + '/scratch/src/'

    assert os.path.isfile(src_dir + 'blob.txt')
    assert not os.path.isfile(target_dir + 'blob.txt')

    haste_storage_client_config = {
        "haste_metadata_server": {
            "connection_string": "mongodb://???:?????@metadata-db-prod:27017/streams"
        },
        "log_level": "DEBUG",
        "targets": [
            {
                "id": "move-to-my-dir",
                "class": "haste_storage_client.storage.storage.MoveToDir",
                "config": {
                    "source_dir": src_dir,
                    "target_dir": target_dir
                }
            }
        ]
    }

    stream_id = datetime.datetime.today().strftime('%Y_%m_%d__%H_%M_%S') + '_exp1_' + 'deleteme'
    print('stream ID is: %s' % stream_id)

    timestamp_cloud_edge = time.time()

    # Optionally, specify REST server with interesting model:

    client = HasteTieredClient(stream_id,
                               config=haste_storage_client_config,
                               interestingness_model=None,
                               storage_policy=[(0.5, 1.0, 'move-to-my-dir')])

    client.save(timestamp_cloud_edge,
                (12.34, 56.78),
                'B13',
                bytearray(),
                {'image_height_pixels': 300,  # bag of extracted features here
                 'image_width_pixels': 300,
                 'original_filename': 'blob.txt'})

    client.close()

    assert not os.path.isfile(src_dir + 'blob.txt')
    assert os.path.isfile(target_dir + stream_id + '/blob.txt')
