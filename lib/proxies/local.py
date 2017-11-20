import socket

from lib.proxy import AbstractRequestProxy, AbstractStreamProxy,\
    proxy_single_request, proxy_sockets


class LocalProxy(AbstractRequestProxy, AbstractStreamProxy):

    class Connection(AbstractStreamProxy.Connection):

        def __init__(self, sock):
            self.sock = sock

        def close(self):
            self.sock.close()

    def __init__(self, stats, maxIdleTimeout=60):
        self.__connIdleTimeout = maxIdleTimeout
        self.__proxyModel = stats.get_model('proxy')

    def request(self, *args):
        return proxy_single_request(*args)

    def connect(self, host, port):
        return LocalProxy.Connection(socket.create_connection((host, port)))

    def stream(self, cliSock, servConn):
        assert isinstance(servConn, LocalProxy.Connection)
        proxy_sockets(cliSock, servConn.sock, self.__connIdleTimeout,
                      self.__proxyModel)
