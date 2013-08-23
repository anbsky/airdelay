#encoding=utf-8

from __future__ import print_function

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import tornado.httpclient
import urllib
import json
import datetime
import time
from tornado.options import define, options
import tornado.gen
import functools

from parsers import parsers

define("port", default=8000, help="run on the given port", type=int)


class IndexHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self, iata_code, _type=None):
        client = tornado.httpclient.AsyncHTTPClient()
        try:
            parser = parsers.initialize(iata_code)
        except KeyError:
            self.write(json.dumps({'status': 'error', 'message': 'no such airport found'}))
            self.finish()
        else:
            records = []
            if parser.is_multi_url:
                for _type, url in parser.url.items():
                    records += list(parser.parse(parser.parse_html((
                        yield client.fetch(url, headers=parser.get_request_headers())
                    )), type=_type))
                parser.sleep()
            else:
                records = list(parser.parse(parser.parse_html((
                    yield client.fetch(parser.url, headers=parser.get_request_headers())
                )), type=_type))

            self.write(parser.to_json(records))
            self.set_header('Content-Type', 'application/json')
            self.finish()


if __name__ == '__main__':
    tornado.options.parse_command_line()
    app = tornado.web.Application(handlers=[
        (r'/airports/(.+?)/(?:(.+?)/)?$', IndexHandler),
    ], debug=True)
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()