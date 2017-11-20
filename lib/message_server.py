import logging
import time

from BaseHTTPServer import BaseHTTPRequestHandler
from threading import Thread, Lock

from lib.utils import ThreadedHTTPServer

logger = logging.getLogger('messages')


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


class MessageServer(object):

    def __init__(self, publicHostAndPort, timeout=30):
        self.__messages = {}
        self.__messagesLock = Lock()
        self.__publicHostAndPort = publicHostAndPort

        self.__httpServer = None

        t = Thread(target=self.__timeout_messages, args=(timeout,))
        t.daemon = True
        t.start()

    def __timeout_messages(self, timeout, frequency=60):
        while True:
            time.sleep(frequency)
            curTime = time.time()
            with self.__messagesLock:
                for messageId in self.__messages.keys():
                    if (curTime - self.__messages[messageId].receiveTime
                            > timeout):
                        del self.__messages[messageId]

    @property
    def publicHostAndPort(self):
        return self.__publicHostAndPort

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


def start_local_message_server(localPort, publicHostAndPort):
    messageServer = MessageServer(publicHostAndPort)
    testLivenessResponse = 'Server is live!\n'

    class MessageHandler(BaseHTTPRequestHandler):

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
            messageServer.put_message(messageId, Message(messageBody))
            self.send_response(204)
            self.send_header('Content-Length', '0')
            self.end_headers()

    httpServer = ThreadedHTTPServer(('', localPort), MessageHandler)
    messageServer.register_http_server(httpServer)
    t = Thread(target=lambda: httpServer.serve_forever())
    t.daemon = True
    t.start()
    return messageServer
