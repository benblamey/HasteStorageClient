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

        # dicts cannot be hashed directly, convert them to frozensets:
        all_metadata_for_blob = frozenset({
            'timestamp': timestamp,
            'location': location,
            'substream_id': substream_id,
            'metadata': frozenset(metadata)
        })

        # bug: this will not give a consistent hash across machines, but will do for now.
        # bug: we don't handle nested dicts - try JSON instead.

        # hash can be the same modulo 1000, so take the hash of the hash.
        interestingness = (float(hash(hash(all_metadata_for_blob))) % 1000) / 1000

        return {'interestingness': interestingness}
