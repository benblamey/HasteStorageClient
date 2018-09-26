import json
import hashlib
from .interestingness_model import InterestingnessModel


class RandomInterestingnessModel(InterestingnessModel):
    """
    An interestingness model which computes a 'random', (by taking a hash of the dictionary).
    """

    def __init__(self):
        pass

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

        all_metadata_for_blob = {
            'timestamp': timestamp,
            'location': location,
            'substream_id': substream_id,
            'metadata': metadata
        }

        # dicts cannot be hashed directly. One approach is to use frozensets, but there may be nested dicts in the metadata anyway.
        # Convert to JSON, (little slower).

        metadata_json = json.dumps(all_metadata_for_blob)

        # pythons hash(..) is not consistent between processes!

        # Instead, take the MD5 of the utf-8 encoded string:
        md5_hash_bytes = hashlib.md5(metadata_json.encode('utf-8')).digest()
        interestingness = float(sum(list(md5_hash_bytes)) % 1000)/1000

        return {'interestingness': interestingness}
