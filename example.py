import time

from hasteStorage import HasteStorageClient


def example():
    client = HasteStorageClient('localhost', # IP address of database server.
                            10000, # should be 27017 from inside the cloud.
                            'stream_'+str(int(time.time()))) # ID for the stream session (unique each time we start/stop the simulator)

    client.save(time.time(),  # fake timestamp
                b'this is image data',
                {'image_height_pixels': 300,  # bag of extracted features here
                 'image_width_pixels': 300,
                 'number_of_green_pixels': 1234})

    client.close()


if __name__ == 'main':
    example()