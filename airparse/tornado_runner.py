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

from parsers import registry, TimetableRow

define("port", default=8000, help="run on the given port", type=int)


class IndexHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self, iata_code, _type=None):
        client = tornado.httpclient.AsyncHTTPClient()
#        client.fetch("http://search.twitter.com/search.json?" +\
#                     urllib.urlencode({"q": query, "result_type": "recent", "rpp": 100}),
        try:
            crawler = registry.get(iata_code)
        except KeyError:
            self.write(json.dumps({'status': 'error', 'message': 'no such airport found'}))
            self.finish()
        else:
            response = yield tornado.gen.Task(client.fetch, crawler.url)
            self.content_type = 'application/json'
            flights = []
            our_response = {
                'status': 'success',
                'iata_code': iata_code,
                'flights': flights
            }
            for record in crawler.process(response.body):
                flights.append(record)
            self.write(json.dumps(our_response, default=TimetableRow.json_handler))
            self.finish()


class SyncIndexHandler(tornado.web.RequestHandler):
    def get(self, iata_code, _type=None):
        client = tornado.httpclient.HTTPClient()
        try:
            crawler = registry.get(iata_code)
        except KeyError:
            self.write(json.dumps({'status': 'error', 'message': 'no such airport found'}))
        else:
            response = client.fetch(crawler.url)
            self.content_type = 'application/json'
            flights = []
            our_response = {
                'status': 'success',
                'iata_code': iata_code,
                'flights': flights
            }
            for record in crawler.process(response.body):
                flights.append(record)
            self.write(json.dumps(our_response, default=TimetableRow.json_handler))


if __name__ == '__main__':
    tornado.options.parse_command_line()
    app = tornado.web.Application(handlers=[
        (r'/airports/(.+?)/(?:(.+?)/)?$', IndexHandler),
        (r'/airports/(.+?)/(?:(.+?)/)?sync/$', SyncIndexHandler),
    ])
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()