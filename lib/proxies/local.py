import errno
import select
import socket

from lib.proxy import AbstractRequestProxy, AbstractStreamProxy, proxy_single_request


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

        servSock = servConn.sock
        rlist = [cliSock, servSock]
        wlist = []
        waitSecs = 1.0
        idleSecs = 0.0
        while True:
            idleSecs += waitSecs
            (ins, _, exs) = select.select(rlist, wlist, rlist, waitSecs)
            if exs: break
            if ins:
                for i in ins:
                    out = cliSock if i is servSock else servSock
                    data = i.recv(8192)
                    if data:
                        try:
                            out.send(data)
                            if out is cliSock:
                                self.__proxyModel.record_bytes_down(len(data))
                            else:
                                self.__proxyModel.record_bytes_up(len(data))
                        except IOError, e:
                            if e.errno == errno.EPIPE:
                                break
                            else:
                                raise
                    idleSecs = 0.0
            if idleSecs >= self.__connIdleTimeout: break

