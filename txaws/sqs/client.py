from urllib import quote
from datetime import datetime

from txaws.client.base import BaseClient
from txaws.sqs.connection import SQSConnection


def get_utf8_value(value):
    if not isinstance(value, str) and not isinstance(value, unicode):
        value = str(value)
    if isinstance(value, unicode):
        return value.encode('utf-8')
    else:
        return value


class Query(SQSConnection):

    SignatureVersion = '2'
    SignatureMethod = 'HmacSHA256'
    APIVersion = '2012-11-05'
    DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

    def __init__(self, creds, endpoint):
        super(Query, self).__init__()
        self.creds = creds
        self.endpoint = endpoint

    def sign(self, query):
        text = (self.endpoint.method + "\n" +
                self.endpoint.get_host() + "\n/\n" +
                query)
        return self.creds.sign(text)

    def get_query_string(self, params):
        keys = sorted(params.keys())
        pairs = []
        for key in keys:
            val = get_utf8_value(params[key])
            pairs.append(quote(key, safe='') + '=' + quote(val, safe='-_~'))
        query = '&'.join(pairs)
        return query

    def get_standard_headers(self):
        return {
            'SignatureVersion': self.SignatureVersion,
            'SignatureMethod': self.SignatureMethod,
            'Version': self.APIVersion,
            'AWSAccessKeyId': self.creds.access_key,
        }

    def submit(self, action, **params):
        params['Action'] = action
        params['Timestamp'] = datetime.utcnow().strftime(self.DATE_FORMAT)
        params.update(self.get_standard_headers())
        query = self.get_query_string(params)
        query += '&{}={}'.format('Signature', self.sign(query))
        url = '{}?{}'.format(self.endpoint.get_uri(), query)
        print url
        return self.call(url, responseFormat='xml')


class SQSClient(BaseClient):

    def __init__(self, creds=None, endpoint=None, query_factory=None):
        query_factory = Query(creds, endpoint)
        super(SQSClient, self).__init__(creds, endpoint, query_factory)

    def get_queue(self, queue_name):
        """
        Retrieves the queue with the given name, or ``None`` if no match
        was found.

        :param str queue_name: The name of the queue to retrieve.
        :rtype: :py:class:`txaws.sqs.queue.Queue` or ``None``
        :returns: The requested queue, or ``None`` if no match was found.
        """
        params = {'QueueName': queue_name}
        return self.query_factory.submit('GetQueueUrl', **params)


