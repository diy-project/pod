from abc import abstractmethod
from collections import namedtuple

# For non-CONNECT requests:
#   [request] makes a request for a single URL
class AbstractRequestProxy(object):

    @abstractmethod
    def request(self, method, url, headers, body):
        pass

# For CONNECT requests:
#   [connect] is called initially
#   [stream] turns over control of the sockets to the proxy
class AbstractStreamProxy(object):

    class Connection(object):

        @abstractmethod
        def close(self):
            pass

    @abstractmethod
    def connect(self, host, port):
        """Return a Connection object"""
        pass

    @abstractmethod
    def stream(self, cliSock, servSock):
        pass

# This is the interface that the http handler expects for a proxy.
class ProxyInstance(object):

    def __init__(self, requestProxy, streamProxy):
        assert isinstance(requestProxy, AbstractRequestProxy)
        assert isinstance(streamProxy, AbstractStreamProxy)
        self.requestProxy = requestProxy
        self.streamProxy = streamProxy

    def request(self, *args):
        return self.requestProxy.request(*args)

    def connect(self, *args):
        return self.streamProxy.connect(*args)

    def stream(self, *args):
        return self.streamProxy.stream(*args)



ProxyResponse = namedtuple('Response', ['status_code', 'headers', 'content'])
