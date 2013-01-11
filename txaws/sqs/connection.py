# -*- coding: utf-8 -*-
from cStringIO import StringIO

from twisted.python import log
from twisted.internet import reactor, defer, protocol
from twisted.web.client import Agent, HTTPConnectionPool
from twisted.web.http_headers import Headers

from txaws.client.ssl import VerifyingContextFactory
from txaws.sqs.errors import ApiError, ResponseError


class BodyReceiver(protocol.Protocol):

    def __init__(self, finished, response):
        self.finished = finished
        self.data = StringIO()
        self.code = response.code

    def dataReceived(self, data):
        self.data.write(data)

    def connectionLost(self, reason):
        self.data.seek(0, 0)
        data = self.data.read()
        if self.code == 200:
            self.finished.callback(data)
        else:
            error = ResponseError(data, self.code)
            self.finished.errback(error)
        self.data.close()


class SQSConnection(object):

    def __init__(self, host, agent=None):
        if agent is None:
            pool = HTTPConnectionPool(reactor)
            contextFactory = VerifyingContextFactory(host)
            agent = Agent(
                reactor,
                contextFactory=contextFactory,
                pool=pool
            )
        self.agent = agent

    def call(self, url, method='GET', headers={}):
        headers = Headers({
            key: [value] for key, value in headers.items()
        })
        d = self.agent.request(
            method, url, headers, None
        )
        def cbRequest(response):
            finished = defer.Deferred()
            response.deliverBody(
                BodyReceiver(finished, response)
            )
            return finished
        d.addCallback(cbRequest)
        return d