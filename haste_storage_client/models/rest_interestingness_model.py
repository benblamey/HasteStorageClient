import urllib
import json
import logging
from .interestingness_model import InterestingnessModel

class RestInterestingnessModel(InterestingnessModel):
    """
    An interestingness model which queries a REST server.
    """

    def __init__(self, url):
        self.url = url
        """
        :param url: should accept HTTP GET /foo?feature_1=1&feature_2=42
        ...and respond with:
        {
            "interestingness": 0.5
        }
        Where the interestingness is in the closed interval [0,1]
        """

    def interestingness(self,
                        stream_id=None,
                        timestamp=None,
                        location=None,
                        substream_id=None,
                        metadata=None,
                        mongo_collection=None):
        """
        :param stream_id (str): ID for the stream session - used to group all the data for that streaming session.
        :param timestamp (numeric): should come from the cloud edge (eg. microscope). integer or floating point.
            *Uniquely identifies the document within the streaming session*.
        :param location (tuple): spatial information (eg. (x,y)).
        :param substream_id (string): ID for grouping of documents in stream (eg. microscopy well ID), or 'None'.
        :param metadata (dict): extracted metadata (eg. image features).
        :param mongo_collection: collection in mongoDB allowing custom queries (this is a hack - best avoided!)
        """

        headers = {'User-Agent': 'haste_storage_client (0.x)',
                   'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        logging.debug('querying interestingness using REST server...')
        data = urllib.parse.urlencode(metadata)
        req = urllib.request.Request(self.url + '?' + data, headers=headers)
        with urllib.request.urlopen(req) as response:
            response = response.read().decode("utf-8")
            decoded = json.loads(response)
            return {'interestingness': float(decoded['interestingness'])}
