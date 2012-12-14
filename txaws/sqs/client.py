import base64
from urllib import quote
from datetime import datetime

from txaws.util import hmac_sha256, get_utf8_value
from txaws.client.base import BaseClient
from txaws.service import AWSServiceEndpoint
from txaws.sqs.connection import SQSConnection
from txaws.sqs.errors import RequestParamError
from txaws.sqs.parser import (empty_check,
                              parse_send_message,
                              parse_send_message_batch,
                              parse_change_message_visibility_batch,
                              parse_delete_message_batch,
                              parse_receive_message)


class Signature(object):

    VERSION = '2'
    method = 'HmacSHA256'

    @staticmethod
    def sign(secret_key, endpoint, query):
        text = (endpoint.method + "\n" +
                endpoint.get_host().lower() + "\n" +
                endpoint.path + "\n" +
                query)
        return hmac_sha256(secret_key, text)

    @classmethod
    def get_query_string(cls, secret_key, endpoint, params):
        keys = sorted(params.keys())
        pairs = []
        for key in keys:
            val = get_utf8_value(params[key])
            pairs.append(quote(key, safe='') + '=' + quote(val, safe='-_~'))
        query = '&'.join(pairs)
        sign = cls.sign(secret_key, endpoint, query)
        query += '&{}={}'.format('Signature', sign)
        return query


class Query(SQSConnection):

    SIGNATURE_CLASS = Signature
    APIVersion = '2012-11-05'
    DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


    def __init__(self, creds, endpoint, agent=None):
        super(Query, self).__init__(agent)
        self.creds = creds
        self.endpoint = endpoint

    def get_standard_headers(self):
        return {
            'SignatureVersion': self.SIGNATURE_CLASS.VERSION,
            'SignatureMethod': self.SIGNATURE_CLASS.method,
            'Version': self.APIVersion,
            'AWSAccessKeyId': self.creds.access_key,
        }

    def submit(self, action, **params):
        params['Action'] = action
        params['Timestamp'] = datetime.utcnow().strftime(self.DATE_FORMAT)
        params.update(self.get_standard_headers())
        query = self.SIGNATURE_CLASS.get_query_string(self.creds.secret_key,
                                                      self.endpoint, params)
        url = '{}?{}'.format(self.endpoint.get_uri(), query)
        return self.call(url)


class SQSClient(BaseClient):
    """
        TODO:
            - CreateQueue;
            - GetQueueUrl;
            - ListQueues.
    """

    def __init__(self, creds=None, endpoint=None, query_factory=None):
        query_factory = Query(creds, endpoint)
        super(SQSClient, self).__init__(creds, endpoint, query_factory)

    def get_queue(self, owner_id, queue):
        endpoint = AWSServiceEndpoint(uri=self.endpoint.get_uri())
        endpoint.set_path('/{}/{}/'.format(owner_id, queue))
        query_factory = Query(self.creds, endpoint, self.query_factory.agent)
        return Queue(self.creds, endpoint, query_factory)


class Queue(object):
    """
        Requests are made with path set to "/owner_id/queue_name/?...".
        Share with SQSClient creds and agent with HTTPConnectionPool.
        TODO:
            - AddPermission;
            - DeleteQueue;
            - GetQueueAttributes;
            - RemovePermission;
            - SetQueueAttributes.
    """

    def __init__(self, creds, endpoint, query_factory):
        self.creds = creds
        self.endpoint = endpoint
        self.query_factory = query_factory

    def change_message_visibility(self, receipt_handle, timeout):
        params = {'ReceiptHandle': receipt_handle,
                  'VisibilityTimeout': str(timeout)}

        body = self.query_factory.submit('ChangeMessageVisibility', **params)
        body.addCallback(empty_check)

        return body

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

        body = self.query_factory.submit('ChangeMessageVisibilityBatch', **params)
        body.addCallback(parse_change_message_visibility_batch)

        return body

    def delete_message(self, receipt_handle):
        params = {'ReceiptHandle': receipt_handle}

        body = self.query_factory.submit('DeleteMessage', **params)
        body.addCallback(empty_check)

        return body

    def delete_message_batch(self, receipt_handles):
        if len(receipt_handles) > 10:
            raise RequestParamError('More than 10 not allowed.')
        params = {}
        prefix = 'DeleteMessageBatchRequestEntry'
        for i, receipt in enumerate(receipt_handles, start=1):
            params['{}.{}.Id'.format(prefix, i)] = str(i)
            params['{}.{}.ReceiptHandle'.format(prefix, i)] = receipt

        body = self.query_factory.submit('DeleteMessageBatch', **params)
        body.addCallback(parse_delete_message_batch)

        return body

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

        body = self.query_factory.submit('ReceiveMessage', **params)
        body.addCallback(parse_receive_message)

        return body

    def send_message(self, message, delay_seconds=None):
        params = {'MessageBody': base64.b64encode(message)}
        if delay_seconds:
            params['DelaySeconds'] = str(delay_seconds)

        body = self.query_factory.submit('SendMessage', **params)
        body.addCallback(parse_send_message)

        return body

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

        body = self.query_factory.submit('SendMessageBatch', **params)
        body.addCallback(parse_send_message_batch)

        return body