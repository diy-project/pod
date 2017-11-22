import logging
import socket

from lib.proxy import AbstractRequestProxy, AbstractStreamProxy,\
    proxy_single_request, proxy_sockets

logger = logging.getLogger(__name__)

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
        try:
            err, bytesDown, bytesUp = proxy_sockets(cliSock, servConn.sock,
                                                    self.__connIdleTimeout)
            if err is not None:
                logger.exception(err)
            self.__proxyModel.record_bytes_down(bytesDown)
            self.__proxyModel.record_bytes_up(bytesUp)
        except Exception as e:
            logger.exception(e)
        finally:
            servConn.close()
