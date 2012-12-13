import base64
from collections import namedtuple

from txaws.util import XML


Message = namedtuple('Message', 'id, md5, receipt, body')


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


def parse_send_message(data):
    element = XML(data).find('SendMessageResult')
    md5 = element.findtext('MD5OfMessageBody').strip()
    msg_id = element.findtext('MessageId').strip()
    return Message(msg_id, md5, None, None)


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
        msg_id = i.findtext('MessageId').strip()
        receipt = i.findtext('ReceiptHandle').strip()
        md5 = i.findtext('MD5OfBody').strip()
        body = base64.b64decode(i.findtext('Body'))
        result.append(Message(msg_id, md5, receipt, body))
    return result