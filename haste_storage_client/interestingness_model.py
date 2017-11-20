import abc
import urllib.request
import urllib.parse
import json


class InterestingnessModel(object, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def interestingness(self,
                        unix_timestamp,
                        location,
                        metadata):
        """
        :param unix_timestamp: should come from the cloud edge (eg. microscope). floating point.
        :param location: n-tuple representing spatial information (eg. (x,y)).
        :param metadata: dictionary containing extracted metadata (eg. image features).
        """
        raise NotImplementedError('users must define interestingness(..) to use this base class')


class RestInterestingnessModel(InterestingnessModel):

    def __init__(self, url):
        self.url = url
        """
        :param url for Rest Service, should accept HTTP GET JSON of the format:
        {
            "unix_timestamp": 1234.5678,
            "location": [0.1,2.3,4.5],
            "metadata": {
                "feature_1":"foo",
                "feature_2":42
            }
        }
        ...and respond with:
        {
            "interestingness":0.5
        }
        Where the interestingness is in the closed interval [0,1] 
        """

    def interestingness(self,
                        unix_timestamp,
                        location,
                        metadata):
        """
        :param unix_timestamp: should come from the cloud edge (eg. microscope). floating point.
        :param location: n-tuple representing spatial information (eg. (x,y)).
        :param metadata: dictionary containing extracted metadata (eg. image features).
        """

        headers = {'User-Agent': 'haste_storage_client (0.x)',
                   'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        values = {'unix_timestamp': unix_timestamp,
                  'location': location,
                  'metadata': metadata}

        data = urllib.parse.urlencode(values)
        req = urllib.request.Request(self.url + '?' + data, headers=headers)
        with urllib.request.urlopen(req) as response:
            response = response.read().decode("utf-8")
            decoded = json.loads(response)
            return {'interestingness': float(decoded['interestingness'])}
