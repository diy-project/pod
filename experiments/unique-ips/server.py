#!/usr/bin/env python
"""A server that echos the client's IP address"""

import argparse

from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
from SocketServer import ThreadingMixIn

DEFAULT_PORT = 8080


class EchoIpHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        ip, port = self.client_address
        responseData = '%s:%d' % (ip, port)
        self.send_response(200)
        self.send_header('Content-Length', len(responseData))
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()
        self.wfile.write(responseData)


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    pass


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', '-p', type=int, default=DEFAULT_PORT)
    return parser.parse_args()


def main(args):
    server = ThreadedHTTPServer(('', args.port), EchoIpHandler)
    print 'Serving on port', args.port
    server.serve_forever()


if __name__ == '__main__':
    main(get_args())
