import base64
from urllib import quote
from datetime import datetime

from txaws.client.base import BaseClient
from txaws.sqs.connection import SQSConnection
from txaws.sqs.errors import RequestParamError


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
                self.endpoint.get_host().lower() + "\n" +
                self.endpoint.path + "\n" +
                query)
        return self.creds.sign(text, hash_type="sha256")

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
        return self.call(url, responseFormat='xml')


class SQSClient(BaseClient):

    def __init__(self, creds=None, endpoint=None, query_factory=None,
                 owner_id=None, queue=None):
        query_factory = Query(creds, endpoint)
        super(SQSClient, self).__init__(creds, endpoint, query_factory)
        if owner_id and queue:
            self.queue = queue
            self.owner_id = owner_id
            self.endpoint.set_path('/{}/{}/'.format(owner_id, queue))

    def change_message_visibility(self, receipt_handle, timeout):
        params = {'ReceiptHandle': receipt_handle,
                  'VisibilityTimeout': str(timeout)}

        return self.query_factory.submit('ChangeMessageVisibility', **params)

    def change_message_visibility_batch(self, receipt_handles, timeout):
        """
            receipt_handle - list of receipt_handle;
            timeout - list if timeouts (accordingly to the order of
                      receipt_handle) or int value if it is common
                      for all messages.
        """
        if len(receipt_handles) > 10:
            raise RequestParamError('More than 10 not allowed.')
        params = {}
        prefix = 'ChangeMessageVisibilityBatchRequestEntry'
        if isinstance(timeout, int):
            timeout = [timeout for i in len(receipt_handles)]
        for i, param in enumerate(zip(receipt_handles, timeout), start=1):
            params['{}.{}.Id'.format(prefix, i)] = str(i)
            params['{}.{}.ReceiptHandle'.format(prefix, i)] = param[0]
            params['{}.{}.VisibilityTimeout'.format(prefix, i)] = str(param[1])

        return self.query_factory.submit('ChangeMessageVisibilityBatch', **params)

    def delete_message(self, receipt_handle):
        params = {'ReceiptHandle': receipt_handle}

        return self.query_factory.submit('DeleteMessage', **params)

    def delete_message_batch(self, receipt_handles):
        if len(receipt_handles) > 10:
            raise RequestParamError('More than 10 not allowed.')
        params = {}
        prefix = 'DeleteMessageBatchRequestEntry'
        for i, receipt in enumerate(receipt_handles, start=1):
            params['{}.{}.Id'.format(prefix, i)] = str(i)
            params['{}.{}.ReceiptHandle'.format(prefix, i)] = receipt

        return self.query_factory.submit('DeleteMessageBatch', **params)

    def receive_message(self, attribute_name=None, max_number_of_messages=None,
                        visibility_timeout=None, wait_time_seconds=None):
        params = {}
        if attribute_name:
            params['AttributeName'] = attribute_name
        if max_number_of_messages:
            params['MaxNumberOfMessages'] = max_number_of_messages
        if visibility_timeout:
            params['VisibilityTimeout'] = visibility_timeout
        if wait_time_seconds:
            params['WaitTimeSeconds'] = wait_time_seconds

        return self.query_factory.submit('ReceiveMessage', **params)

    def send_message(self, message, delay_seconds=None):
        params = {'MessageBody': base64.b64encode(message)}
        if delay_seconds:
            params['DelaySeconds'] = str(delay_seconds)

        return self.query_factory.submit('SendMessage', **params)

    def send_message_batch(self, messages, delay_seconds=None):
        """
            messages - list,
            delay_seconds = list of int values or int value if it's common
                            for all messages
        """
        if len(messages) > 10:
            raise RequestParamError('More than 10 not allowed.')
        params = {}
        if isinstance(delay_seconds, int):
            delay_seconds = [delay_seconds for i in len(receipt_handles)]
        prefix = 'SendMessageBatchRequestEntry'

        for i, msg in enumerate(messages, start=1):
            params['{}.{}.Id'.format(prefix, i)] = str(i)
            params['{}.{}.MessageBody'.format(prefix, i)] = base64.b64encode(msg)
            if delay_seconds:
                params['{}.{}.DelaySeconds'.format(prefix, i)] = str(delay_seconds[i - 1])

        return self.query_factory.submit('SendMessageBatch', **params)