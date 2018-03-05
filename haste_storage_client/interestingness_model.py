import abc
import urllib.request
import urllib.parse
import json


class InterestingnessModel(object, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def interestingness(self,
                        metadata):
        """
        :param metadata: dictionary containing extracted metadata (eg. image features).
        """
        raise NotImplementedError('users must define interestingness(..) to use this base class')


class RestInterestingnessModel(InterestingnessModel):

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

    def interestingness(self, metadata):
        """
        :param metadata: dictionary containing extracted metadata (eg. image features).
        """

        headers = {'User-Agent': 'haste_storage_client (0.x)',
                   'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        print('querying interestingness using REST server...')
        data = urllib.parse.urlencode(metadata)
        req = urllib.request.Request(self.url + '?' + data, headers=headers)
        with urllib.request.urlopen(req) as response:
            response = response.read().decode("utf-8")
            decoded = json.loads(response)
            return {'interestingness': float(decoded['interestingness'])}
