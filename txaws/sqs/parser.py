from txaws.util import XML


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
    return md5, msg_id


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
        receipt_handle = i.findtext('ReceiptHandle').strip()
        md5 = i.findtext('MD5OfBody').strip()
        body = i.findtext('Body').strip()
        result.append((msg_id, receipt_handle, md5, body))
    return result