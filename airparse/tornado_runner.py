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
import os

from parsers import registry


define("port", default=8000, help="run on the given port", type=int)
static_root = os.path.join(os.path.dirname(__file__), '..', 'static')
template_root = os.path.join(os.path.dirname(__file__), '..', 'templates')


class HomeHandler(tornado.web.RequestHandler):
    def get(self):
        self.render('index.html')


class AirportsHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self, iata_code, _type=None):
        client = tornado.httpclient.AsyncHTTPClient()
        try:
            parser = registry.initialize(iata_code)
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


app = tornado.web.Application(handlers=[
    (r'/airports/(.+?)/(?:(.+?)/)?$', AirportsHandler),
    (r'/', HomeHandler),
], template_path=template_root, static_path=static_root, debug=True)


if __name__ == '__main__':
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()