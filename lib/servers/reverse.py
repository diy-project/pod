import logging
import time

from http.server import BaseHTTPRequestHandler
from threading import Thread, Lock, Condition

from lib.proxy import proxy_sockets
from lib.stats import EC2StatsModel
from lib.utils import ThreadedHTTPServer

logger = logging.getLogger(__name__)


class Message(object):

    def __init__(self, content):
        self.__content = content
        self.__receiveTime = time.time()

    @property
    def content(self):
        return self.__content

    @property
    def receiveTime(self):
        return self.__receiveTime


class Socket(object):

    def __init__(self, sock, idleTimeout):
        self.__sock = sock
        self.__idleTimeout = idleTimeout
        self.__openTime = time.time()

    @property
    def sock(self):
        return self.__sock

    @property
    def openTime(self):
        return self.__openTime

    @property
    def idleTimeout(self):
        return self.__idleTimeout

    def close(self):
        self.__sock.close()


class ReverseConnectionServer(object):

    def __init__(self, publicHostAndPort, messageTimeout=5, connTimeout=5):
        self.__messages = {}
        self.__messagesLock = Lock()

        self.__sockets = {}
        self.__socketsLock = Lock()
        self.__socketsCond = Condition(self.__socketsLock)
        self.__connTimeout = connTimeout

        self.__publicHostAndPort = publicHostAndPort

        self.__httpServer = None

        t = Thread(target=self.__timeout_sockets_and_messages,
                   args=(messageTimeout,))
        t.daemon = True
        t.start()

    def __timeout_sockets_and_messages(self, messageTimeout, frequency=1):
        while True:
            time.sleep(frequency)
            curTime = time.time()
            with self.__socketsLock:
                for socketId in self.__sockets.keys():
                    sockObj = self.__sockets[socketId]
                    if (curTime - sockObj.openTime > sockObj.idleTimeout):
                        sockObj.close()
                        del self.__sockets[socketId]
            with self.__messagesLock:
                for messageId in self.__messages.keys():
                    if (curTime - self.__messages[messageId].receiveTime
                            > messageTimeout):
                        del self.__messages[messageId]

    @property
    def publicHostAndPort(self):
        return self.__publicHostAndPort

    def take_ownership_of_socket(self, socketId, sock, idleTimeout):
        with self.__socketsLock:
            self.__sockets[socketId] = Socket(sock, idleTimeout)
            self.__socketsCond.notify_all()

    def get_socket(self, socketId):
        endTime = time.time() + self.__connTimeout
        with self.__socketsLock:
            while True:
                curTime = time.time()
                ret = self.__sockets.get(socketId)
                if ret is not None:
                    del self.__sockets[socketId]
                    break
                else:
                    self.__socketsCond.wait(endTime - curTime)
                if curTime > endTime: break
        return ret

    def get_message(self, messageId):
        with self.__messagesLock:
            ret = self.__messages.get(messageId)
            if ret is not None:
                del self.__messages[messageId]
        return ret

    def put_message(self, messageId, message):
        with self.__messagesLock:
            self.__messages[messageId] = message

    def register_http_server(self, httpServer):
        self.__httpServer = httpServer

    def shutdown(self):
        self.__httpServer.server_close()
        self.__httpServer.shutdown()


def start_reverse_connection_server(localPort, publicHostAndPort, stats):
    proxyModel = stats.get_model('proxy')
    if 'ec2' not in stats.models:
        stats.register_model('ec2', EC2StatsModel())
    ec2Model = stats.get_model('ec2')

    server = ReverseConnectionServer(publicHostAndPort)
    testLivenessResponse = 'Server is live!\n'

    class RequestHandler(BaseHTTPRequestHandler):

        def log_message(self, format, *args):
            """Override the default logging to not print ot stdout"""
            logger.info('%s - [%s] %s' %
                        (self.client_address[0],
                         self.log_date_time_string(),
                         format % args))

        def log_error(self, format, *args):
            """Override the default logging to not print ot stdout"""
            logger.error('%s - [%s] %s' %
                         (self.client_address[0],
                          self.log_date_time_string(),
                          format % args))

        def do_GET(self):
            self.send_response(200)
            self.send_header('Content-Length', str(len(testLivenessResponse)))
            self.end_headers()
            self.wfile.write(testLivenessResponse)

        def do_POST(self):
            messageId = self.path[1:]
            messageLength = int(self.headers['Content-Length'])
            messageBody = self.rfile.read(messageLength)
            logger.info('Received: %s (%dB)', messageId, len(messageBody))
            server.put_message(messageId, Message(messageBody))
            proxyModel.record_bytes_down(len(messageBody))
            self.send_response(204)
            self.send_header('Content-Length', '0')
            self.end_headers()

        def do_CONNECT(self):
            socketId = self.path[1:]
            logger.info('Connect: %s', socketId)
            socketRequest = server.get_socket(socketId)
            try:
                if socketRequest is not None:
                    self.send_response(200)
                    self.end_headers()
                else:
                    self.send_error(404, 'Resource not found')
                    self.end_headers()
                    return
                err, bytesDown, bytesUp = \
                    proxy_sockets(socketRequest.sock, self.connection,
                                  socketRequest.idleTimeout)
                if err is not None:
                    logger.exception(err)
                proxyModel.record_bytes_down(bytesDown)
                proxyModel.record_bytes_up(bytesUp)
                ec2Model.record_bytes_down(bytesDown)
                ec2Model.record_bytes_up(bytesUp)
            except Exception as e:
                logger.exception(e)
            finally:
                if socketRequest is not None:
                    socketRequest.close()

    httpServer = ThreadedHTTPServer(('', localPort), RequestHandler)
    server.register_http_server(httpServer)
    t = Thread(target=lambda: httpServer.serve_forever())
    t.daemon = True
    t.start()
    return server
