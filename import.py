#!/usr/bin/env python
#encoding=utf-8

import re
import requests
from bs4 import BeautifulSoup
from dateutil import parser

from models import FlightStatus, Flight, Airport, FlightType


class _BaseCrawler(object):
    def __init__(self):
        pass

    def fetch(self):
        r = requests.get(self.url)
        return self.parse(r.content)


class DMEDepartureCrawler(_BaseCrawler):
    airport_code = 'DME'
    flights_type = FlightType.OUTBOUND
    url = 'http://www.domodedovo.ru/onlinetablo/default.aspx?tabloname=TabloDeparture_R'
    targets = {
        'FL_NUM_PUB': 'code',
        'ORG': 'peer_airport_name',
        'TIM_P': 'scheduled',
        'TIM_L': 'actual',
        'STATUS': 'raw_status',
    }
    _months = {
        u'янв': 'january',
        u'фев': 'february',
        u'мар': 'march',
        u'апр': 'april',
        u'май': 'may',
        u'июн': 'june',
        u'июл': 'july',
        u'авг': 'august',
        u'сен': 'september',
        u'окт': 'october',
        u'ноя': 'november',
        u'дек': 'december',
    }
    _statuses = [
        (re.compile(r'tablo/4\.gif'), FlightStatus.SCHEDULED),
        (re.compile(r'tablo/6\.gif'), FlightStatus.DELAYED),
        (re.compile(r'tablo/5\.gif'), FlightStatus.CANCELLED),
        (re.compile(r'tablo/[81]\.gif'), FlightStatus.DEPARTED),
    ]
    codeshare = re.compile(ur'совмещен с ')

    def parse(self, content):
        soup = BeautifulSoup(content)

        for row in soup.find(id='onlinetablo').find_all('tr'):
            if 'tabloheader' in row['class']:
                continue
            parsed_row = {}
            for cell in row.find_all('td'):
                if cell['class'][0] in self.targets:
                    attr_name = self.targets[cell['class'][0]]
                    if attr_name.startswith('raw_'):
                        parsed_row[attr_name] = str(cell)
                    else:
                        parsed_row[attr_name] = cell.string.strip()
            if self.codeshare.search(parsed_row['code']):
                parsed_row['codeshare'] = 1
            parsed_row['scheduled'] = self.parse_time(parsed_row['scheduled'])
            parsed_row['actual'] = self.parse_time(parsed_row['actual'])
            parsed_row['status'] = self.parse_status(parsed_row['raw_status'])
            yield parsed_row

    def parse_time(self, time):
        assert isinstance(time, basestring)
        for rus, eng in self._months.items():
            time = time.replace(rus, eng)
        return parser.parse(time)

    def parse_status(self, status):
        for check, value in self._statuses:
            if check.search(status):
                return value


def do_import():
    crawler = DMEDepartureCrawler()
    for r in crawler.fetch():
        airport = Airport.objects.filter(code=crawler.airport_code).first()
        if not airport:
            airport = Airport(code=crawler.airport_code)
            airport.save()
        flight = Flight(type=crawler.flights_type, airport=airport, **r)
        flight.save()

if __name__ == '__main__':
    do_import()