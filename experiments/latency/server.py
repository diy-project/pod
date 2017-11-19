#!/usr/bin/env python

import argparse
import ssl
import time

from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
from SocketServer import ThreadingMixIn

DEFAULT_PORT = 8080

CERTFILE = 'server.cert.pem'
KEYFILE = 'server.key.pem'


print 'Preloading random data'
with open('/dev/random', 'rb') as ifs:
    ONE_KB = 1 << 10
    CACHED_RANDOM_KB = ifs.read(ONE_KB)
    print 'Done loading random data'


class RandomHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        try:
            sizeRequested = int(self.path[1:])
        except:
            self.send_error(400, 'Invalid request size')
            self.end_headers()
            return
        self.send_response(200)
        self.send_header('Content-Length', int(sizeRequested))
        # Prevent compression by sending binary
        self.send_header('Content-Type', 'text/text')
        self.end_headers()
        bytesSent = 0
        while bytesSent < sizeRequested:
            bytesToSend = min(sizeRequested - bytesSent, ONE_KB)
            self.wfile.write(CACHED_RANDOM_KB[:bytesToSend])
            bytesSent += bytesToSend
            print bytesSent


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    pass


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', '-p', type=int, default=DEFAULT_PORT)
    parser.add_argument('--https', '-s', action='store_true')
    return parser.parse_args()


def main(args):
    server = ThreadedHTTPServer(('', args.port), RandomHandler)
    if args.https:
        server.socket = ssl.wrap_socket(server.socket, KEYFILE, CERTFILE,
                                        server_side=True)
    print 'Serving on port', args.port
    server.serve_forever()


if __name__ == '__main__':
    main(get_args())
