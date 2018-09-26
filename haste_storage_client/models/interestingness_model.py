from __future__ import print_function
import abc
import sys

if sys.version_info[0] == 2:
    import urllib
else:
    # For Python3 -- this package was split:
    import urllib.request
    import urllib.parse

import json


class InterestingnessModel:
    """
    Base class for models for computing the interestingness of BLOBs (based on their metadata).
    """

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
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
        return {'interestingness': 1}
