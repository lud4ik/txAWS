from txaws.sqs.parser import parse_error_message


class RequestParamError(Exception):
    pass


class ApiError(Exception):

    def __init__(self, value, code=None):
        self.value = value
        self.code = code

    def __str__(self):
        return repr(self.value)


class ResponseError(ApiError):

    def __init__(self, value, code=None):
        super(ResponseError, self).__init__(value, code)
        self.set_response_values()

    def set_response_values(self):
        _type, message =  parse_error_message(self.value)
        self.type = _type
        self.message = message

    def __str__(self):
        return 'Code - {}. Type - {}. Message - {}'.format(self.code,
                                                           self.type,
                                                           self.message)
