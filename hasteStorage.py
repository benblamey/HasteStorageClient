from pymongo import MongoClient


class HasteStorageClient:
    def __init__(self,
                 host,
                 port,
                 stream_id):
        """
        :param host: Hostname/IP of database server.
        :param port: Database server port. Usually 27017.
        :param stream_id: String ID for the stream session (unique each time we start/stop the simulator/microscope)
        """
        self.mongo_client = MongoClient(host, port)
        self.mongo_db = self.mongo_client.metadata
        self.stream_id = stream_id

    def save(self,
             timestamp,  # the original timestamp from the microscope
             image,
             metadata):
        """
        :param timestamp: for the image, originates at the microscope/cloud edge.
        :param image: binary data.
        :param metadata: dictionary containing extracted image features.
        """
        result = self.mongo_db['stream_' + self.stream_id].insert({
            'timestamp': timestamp,
            'metadata': metadata,
        })

        #TODO: save image to container

    def close(self):
        self.mongo_client.close()
