from BaseHTTPServer import HTTPServer
from SocketServer import ThreadingMixIn


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle requests in a separate thread."""
