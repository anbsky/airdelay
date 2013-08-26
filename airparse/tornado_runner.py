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


class AirportsHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self, iata_code, _type=None):
        client = tornado.httpclient.AsyncHTTPClient()
        try:
            parser = parsers.initialize(iata_code)
        except TypeError:
            self.set_status(404)
            self.write({
                'status': 'error',
                'message': 'Airport {} not found'.format(iata_code)
            })
            self.finish()
        else:
            records = yield parser.run_async()
            self.write(records.to_json())

            self.set_header('Content-Type', 'application/json')
            self.finish()


if __name__ == '__main__':
    tornado.options.parse_command_line()
    app = tornado.web.Application(handlers=[
        (r'/airports/(.+?)/(?:(.+?)/)?$', AirportsHandler),
    ], debug=True)
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()