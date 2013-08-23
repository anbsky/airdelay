#!/usr/bin/env python
#encoding=utf-8

import re
from bs4 import BeautifulSoup
from dateutil import parser
from datetime import datetime
from collections import namedtuple
from operator import itemgetter
from itertools import chain
from random import randint
import json
import requests
import time

from codes import find_airport_code, find_airport_name


class ParserRegistry(dict):
    def add(self, iata_code, klass):
        if iata_code in self.keys():
            raise NameError('Crawler for {} already registered'.format(iata_code))
        assert getattr(klass, 'url'), 'Cannot register a crawler without URL'
        self[iata_code] = klass

    def initialize(self, iata_code):
        try:
            klass = self.get(iata_code)
            return klass(iata_code)
        except KeyError:
            raise Exception('airport {} not found'.format(iata_code))

parsers = ParserRegistry()


class FlightStatus(object):
    SCHEDULED = 'scheduled'
    DELAYED = 'delayed'
    DEPARTED = 'departed'
    LANDED = 'landed'
    CANCELLED = 'cancelled'
    ARRIVED = 'arrived'


class FlightEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()

        return super(FlightEncoder, self).default(o)


class Flight(dict):
    fields = ['origin', 'origin_name', 'destination', 'destination_name', 'number', 'airline',
            'date_scheduled', 'date_actual', 'date_retrieved', 'status',
            'is_codeshare', 'source']

    def __init__(self, **kwargs):
        kwargs.setdefault('date_retrieved', datetime.now())
        clean_data = self._cleanup(self._clean_kwargs(kwargs))
        strict_data = {f: clean_data[f] for f in set(self.fields) & set(clean_data.keys())}
        super(Flight, self).__init__(**strict_data)

    def __getattr__(self, item):
        if item in self.fields:
            return self.get(item, None)
        else:
            raise AttributeError(item)

    def __setattr__(self, key, value):
        if key in self.fields:
            self[key] = value
        else:
            raise AttributeError(key)

    def set_origin(self, name, iata_code=None):
        self.origin_name = name
        self.origin = iata_code or find_airport_code(name)

    def set_destination(self, name, iata_code=None):
        self.destination_name = name
        self.destination = iata_code or find_airport_code(name)

    @staticmethod
    def _clean_kwargs(kwargs):
        return dict(filter(lambda item: not(item[1] == ''), kwargs.items()))

    @staticmethod
    def _cleanup(data):
        return data


_agents = [
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Win64; x64; Trident/6.0)',
    'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Win64; x64; Trident/4.0; .NET CLR 2.0.50727; SLCC2; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; Tablet PC 2.0)',
    'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; GTB6.4; .NET CLR 1.1.4322; FDM; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/534.57.2 (KHTML, like Gecko) Version/5.1.7 Safari/534.57.2',
    'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/531.21.8 (KHTML, like Gecko) Version/4.0.4 Safari/531.21.10',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20100101 Firefox/21.0',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:20.0) Gecko/20100101 Firefox/20.0',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.31 (KHTML, like Gecko) Chrome/26.0.1410.64 Safari/537.31',
    'Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.22 (KHTML, like Gecko) Ubuntu Chromium/25.0.1364.160 Chrome/25.0.1364.160 Safari/537.22',
]

class BaseParser(object):
    iata_code = None
    name = None
    url = None
    request_headers = {
        'Accept-Language': 'en-US',
    }

    def __init__(self, iata_code, delay=2):
        self.records = []
        self.status = None
        self.delay = delay
        self.iata_code = iata_code
        self.name = find_airport_name(iata_code)

        self.metadata = {
            'status': None,
            'iata_code': iata_code,
            'name': self.name,
            'flights': self.records
        }

    def get_request_headers(self):
        headers = self.request_headers
        headers['User-Agent'] = _agents[randint(0, len(_agents) - 1)]
        return headers

    def set_status(self, value):
        self.metadata['status'] = value

    def run(self):
        # Airport website has separate pages for departure/arrival
        if isinstance(self.url, dict):
            for _type, url in self.url.items():
                self.records += list(self.parse(
                    BeautifulSoup(requests.get(url, headers=self.get_request_headers()).content),
                    type=_type
                ))
                time.sleep(self.delay)
        # Single page
        else:
            self.records += list(self.parse(BeautifulSoup(
                requests.get(self.url, headers=self.get_request_headers()).content)))
        self.set_status('OK')
        return self.records

    def parse(self, content, **defaults):
        raise NotImplementedError

    def to_json(self):
        return json.dumps(self.records, cls=FlightEncoder)


class DMEParser(BaseParser):
    url = {
        'outbound': 'http://www.domodedovo.ru/onlinetablo/default.aspx?tabloname=TabloDeparture_E',
        'inbound': 'http://www.domodedovo.ru/onlinetablo/default.aspx?tabloname=TabloArrival_E'
    }
    _targets = {
        'FL_NUM_PUB': 'number',
        'ORG': 'peer',
        'TIM_P': 'date_scheduled',
        'TIM_L': 'date_actual',
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
        (re.compile(r'tablo/7\.gif'), FlightStatus.LANDED),
    ]
    _codeshare_re = re.compile(ur'совмещен с ')

    def parse(self, soup, **defaults):
        for row in soup.find(id='onlinetablo').find_all('tr'):
            parsed_row = {}

            if row.find('th'):
                continue

            for cell in row.find_all('td'):
                if cell['class'][0] in self._targets:
                    attr_name = self._targets[cell['class'][0]]
                    if attr_name.startswith('raw_'):
                        parsed_row[attr_name] = str(cell)
                    else:
                        parsed_row[attr_name] = cell.string.strip()
            is_codeshare = bool(self._codeshare_re.search(parsed_row['number']))

            date_scheduled = self._parse_time(parsed_row['date_scheduled'])
            date_actual = self._parse_time(parsed_row['date_actual'])
            status = self._parse_status(parsed_row['raw_status'])
            peer = re.sub(r'\(.+\)', '', parsed_row['peer'])

            f = Flight(
                source=self.iata_code,
                date_scheduled=date_scheduled,
                date_actual=date_actual,
                status=status,
                number=parsed_row['number'],
                is_codeshare=is_codeshare
            )

            if defaults['type'] == 'inbound':
                f.set_destination(self.name, self.iata_code)
                f.set_origin(peer)
            else:
                f.set_destination(peer)
                f.set_origin(self.name, self.iata_code)

            yield f

    def _parse_time(self, time):
        assert isinstance(time, basestring)
        for rus, eng in self._months.items():
            time = time.replace(rus, eng)
        return parser.parse(time, dayfirst=True)

    def _parse_status(self, status):
        for check, value in self._statuses:
            if check.search(status):
                return value


class SVOParser(BaseParser):
    url = 'http://svo.aero/en/tt/'
    _statuses = {
        'sL': FlightStatus.LANDED,
        'sE': FlightStatus.DELAYED,
        'sC': FlightStatus.CANCELLED,
        'sK': FlightStatus.DELAYED
    }

    def parse(self, soup, **defaults):
        for row in soup.find('div', {'class': 'timetable'}).find(
            'div', {'class': 'table'}
        ).find('table').find_all('tr'):
            row_cls = row.get('class', [])
            if 'sA' in row_cls:
                _type = 'arrival'
            elif 'sD' in row_cls:
                _type = 'departure'
            else:
                continue
            raw_cells = row.find_all('td')
            raw = [cell.string for cell in raw_cells]

            f = Flight(
                date_scheduled=self._parse_time(' '.join(raw[:2])),
                date_actual=self._parse_actual(raw[7]),
                status=self._parse_status(raw_cells[7]),
                number=' '.join(raw[2:4]),
                source=self.iata_code
            )
            if _type == 'arrival':
                f.set_destination(self.name, self.iata_code)
                f.set_origin(raw[5])
            else:
                f.set_destination(raw[5])
                f.set_origin(self.name, self.iata_code)
            yield f

    def _parse_time(self, time):
        assert isinstance(time, basestring)
        time = time.replace(u'\xa0', ' ')
        return parser.parse(time, dayfirst=True)

    def _parse_actual(self, actual):
        if not actual:
            return None
        time_part = actual.split()[-1]
        if not time_part.find(':') == 2:
            return None
        return parser.parse(time_part)

    def _parse_status(self, cell):
        for cls, status in self._statuses.items():
            if cls in cell.get('class', []):
                return status


class VKOParser(BaseParser):
    url = {
        'outbound': 'http://vnukovo.ru/eng/for-passengers/board/index.wbp?time-table.direction=1',
        'inbound': 'http://vnukovo.ru/eng/for-passengers/board/index.wbp?time-table.direction=0'
    }
    _statuses = {
        'departed': FlightStatus.DEPARTED,
        'arrived': FlightStatus.LANDED,
        'has not departed': FlightStatus.DELAYED,
    }

    def parse(self, soup, **defaults):
        for row in soup.find('table', {'id': 'TimeTable'}).find('tbody').find_all('tr'):
            raw_cells = row.find_all('td')
            raw = [cell.string for cell in raw_cells]
            airport = ''
            if defaults.get('type') == 'outbound':
                airport = raw[3]
            elif defaults.get('type') == 'inbound':
                airport = raw[2]
            airport = re.sub(r'\s?\(.+\)', '', airport.replace('\n', ' ')).strip().capitalize()

            f = Flight(
                source=self.iata_code,
                date_scheduled=self._parse_time(raw_cells[5]),
                date_actual=self._parse_time(raw_cells[6]),
                status=self._parse_status(raw[4]),
                number=raw[0].strip(),
                destination=self.iata_code,
                airline=raw[1].strip().capitalize(),
                **defaults
            )

            if defaults['type'] == 'outbound':
                f.set_origin(self.name, self.iata_code)
                f.set_destination(airport)
            else:
                f.set_origin(airport)
                f.set_destination(self.name, self.iata_code)

            yield f


    def _parse_time(self, cell):
        time_str = ' '.join(list(cell.stripped_strings))
        if len(time_str) < 11:
            return None
        time_norm = '{}.{}'.format(time_str, datetime.now().year)
        return parser.parse(time_norm, dayfirst=True)

    def _parse_status(self, value):
        return self._statuses.get(value)


parsers.add('DME', DMEParser)
parsers.add('SVO', SVOParser)
parsers.add('VKO', VKOParser)


def do_import():
    crawler = DMEParser()
    for r in crawler.fetch():
        pass

if __name__ == '__main__':
    do_import()