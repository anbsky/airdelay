# encoding=utf-8

from __future__ import print_function

import json
from flask import Flask, after_this_request

from parsers import parsers


app = Flask(__name__)

@app.route('/airports/<iata_code>/')
def timetable(iata_code):
    @after_this_request
    def add_header(response):
        response.headers['Content-type'] = 'application/json'
        return response

    parser = parsers.initialize(iata_code)
    records = parser.run()
    return records.to_json()


if __name__ == "__main__":
    app.run(debug=True)