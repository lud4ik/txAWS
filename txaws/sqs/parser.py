# -*- coding: utf-8 -*-
import base64
from collections import namedtuple

from txaws.util import XML


Message = namedtuple('Message', 'receipt, body')


def empty_check(data):
    if isinstance(data, basestring):
        return True


def process_batch_result(data, root, success_tag):
    result = []
    element = XML(data).find(root)
    for i in element.getchildren():
        if i.tag == success_tag:
            result.append(True)
        else:
            result.append(False)
    return result


def parse_error_message(data):
    element = XML(data).find('Error')
    _type = element.findtext('Type').strip()
    message = element.findtext('Message').strip()
    return _type, message


def parse_change_message_visibility_batch(data):
    return process_batch_result(data,
                                'ChangeMessageVisibilityBatchResult',
                                'ChangeMessageVisibilityBatchResultEntry')


def parse_delete_message_batch(data):
    return process_batch_result(data,
                                'DeleteMessageBatchResult',
                                'DeleteMessageBatchResultEntry')


def parse_send_message_batch(data):
    return process_batch_result(data,
                                'SendMessageBatchResult',
                                'SendMessageBatchResultEntry')


def parse_receive_message(data):
    result = []
    element = XML(data).find('ReceiveMessageResult')
    for i in element.getchildren():
        receipt = i.findtext('ReceiptHandle').strip()
        body = base64.b64decode(i.findtext('Body'))
        result.append(Message(receipt, body))
    return result


def parse_get_queue_url(data):
    element = XML(data).find('GetQueueUrlResult')
    return element.findtext('QueueUrl').strip()


def parse_list_queues(data):
    result = []
    element = XML(data).find('ListQueuesResult')
    for tag in element.findall('QueueUrl'):
        result.append(tag.text.strip())
    return result


def parse_create_queue(data):
    element = XML(data).find('CreateQueueResult')
    url = element.findtext('QueueUrl').strip()
    return url


def parse_queue_attributes(data):
    result = {}
    str_attrs = ['Policy', 'QueueArn']
    element = XML(data).find('GetQueueAttributesResult')
    for i in element.getchildren():
        attr = i.findtext('Name').strip()
        value = i.findtext('Value').strip()
        if attr not in str_attrs:
            value = int(value)
        result[attr] = value
    return result