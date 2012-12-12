# -*- coding: utf-8 -*-
from cStringIO import StringIO

from twisted.internet import reactor, ssl, defer, protocol
from twisted.web.client import Agent, HTTPConnectionPool
from twisted.web.http_headers import Headers


class ApiError(Exception):

    def __init__(self, value, code=None):
        self.value = value
        self.code = code

    def __str__(self):
        return repr(self.value)


class SSLClientContextFactory(ssl.ClientContextFactory):
    """
        SSL context factory to make necessary verification.
    """
    def getContext(self, hostname, port):
        return ssl.ClientContextFactory.getContext(self)


class BodyReceiver(protocol.Protocol):

    def __init__(self, finished, response, responseFormat):
        self.finished = finished
        self.data = StringIO()
        self.code = response.code
        self.formatter = getattr(
            self,
            'format_%s' % responseFormat,
            lambda x: x
        )

    def format_xml(self, data):
        # make parse xml later
        return data

    def dataReceived(self, data):
        self.data.write(data)

    def connectionLost(self, reason):
        print self.code
        self.data.seek(0, 0)
        data = self.data.read()
        try:
            data = self.formatter(data)
        except Exception as e:
            error = ApiError(repr(e))
            log.err(error)
            self.finished.errback(error)
        else:
            self.finished.callback(data)
        self.data.close()


class SQSConnection(object):

    def __init__(self):
        pool = HTTPConnectionPool(reactor)
        contextFactory = SSLClientContextFactory()
        self.agent = Agent(
            reactor,
            contextFactory=contextFactory,
            pool=pool
        )

    def call(self, url, method='GET', headers={}, responseFormat='xml'):
        headers = Headers({
            key: [value] for key, value in headers.items()
        })
        d = self.agent.request(
            method, url, headers, None
        )
        def cbRequest(response):
            finished = defer.Deferred()
            response.deliverBody(
                BodyReceiver(finished, response, responseFormat)
            )
            return finished
        d.addCallback(cbRequest)
        return d