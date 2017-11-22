#!/usr/bin/env python
"""A server that echos the client's IP address"""

import argparse
import time

from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
from SocketServer import ThreadingMixIn

DEFAULT_PORT = 8080
DEFAULT_DELAY = 1


def build_handler(delay):
    class EchoIpHandler(BaseHTTPRequestHandler):

        def do_GET(self):
            ip, port = self.client_address
            responseData = '%s:%d' % (ip, port)
            time.sleep(delay)
            self.send_response(200)
            self.send_header('Content-Length', len(responseData))
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(responseData)

    return EchoIpHandler


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    pass


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', '-p', type=int, default=DEFAULT_PORT)
    parser.add_argument('--delay', '-d', type=int, default=DEFAULT_DELAY,
                        help='Number of seconds to delay before responding')
    return parser.parse_args()


def main(args):
    handler =build_handler(args.delay)
    server = ThreadedHTTPServer(('', args.port), handler)
    print 'Serving on port', args.port
    server.serve_forever()


if __name__ == '__main__':
    main(get_args())
