#!/usr/bin/env python
#encoding=utf-8

import re
from bs4 import BeautifulSoup
from dateutil import parser
from datetime import datetime
from collections import namedtuple
from operator import itemgetter
import json
import requests
import time


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
    fields = ['origin', 'destination', 'number', 'airline',
            'date_scheduled', 'date_actual', 'date_retrieved', 'status',
            'is_codeshare']

    def __init__(self, **kwargs):
        kwargs.setdefault('date_retrieved', datetime.now())
        super(Flight, self).__init__(**self._clean_kwargs(kwargs))
        for f in self.fields:
            setattr(self, f, property(itemgetter(f)))

    @staticmethod
    def _clean_kwargs(kwargs):
        return dict(filter(lambda item: not(item[1] == ''), kwargs.items()))


# class Flight(object):
#     def __init__(self, *args, **kwargs):
#         pass
#
#     def __setattr__(self, key, value):
#         pass
#
#     def __getattr__(self, instance, owner):
#         pass


class BaseParser(object):
    iata_code = None
    url = None

    def __init__(self, iata_code, delay=2):
        self.records = []
        self.status = None
        self.metadata = {
            'status': None,
            'iata_code': iata_code,
            'flights': self.records
        }
        # To prevent banning
        self.delay = delay
        self.iata_code = iata_code

    def set_status(self, value):
        self.metadata['status'] = value

    def run(self):
        # Airport website has separate pages for departure/arrival
        if isinstance(self.url, dict):
            for _type, url in self.url.items():
                self.records += list(self.parse(
                    BeautifulSoup(requests.get(url).content),
                    type=_type
                ))
                time.sleep(self.delay)
        # Single page
        else:
            self.records += list(self.parse(BeautifulSoup(requests.get(self.url).content)))
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
        'ORG': 'origin',
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
            val = self._parse_row(row)
            if val:
                yield val

    def _parse_row(self, row):
        parsed_row = {}

        if row.find('th'):
            return

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

        return Flight(
            date_scheduled=date_scheduled,
            date_actual=date_actual,
            status=status,
            number=parsed_row['number'],
            origin=parsed_row['origin'],
            destination=self.iata_code,
            is_codeshare=is_codeshare
        )

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

            yield Flight(
                date_scheduled=self._parse_time(' '.join(raw[:2])),
                date_actual=self._parse_actual(raw[7]),
                status=self._parse_status(raw_cells[7]),
                number=' '.join(raw[2:4]),
                origin=raw[5],
                destination=self.iata_code
            )

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
            airport = airport.replace('\n', ' ')

            yield Flight(
                date_scheduled=self._parse_time(raw_cells[5]),
                date_actual=self._parse_time(raw_cells[6]),
                status=self._parse_status(raw[4]),
                number=raw[0].strip(),
                origin=airport,
                destination=self.iata_code,
                airline=raw[1].strip(),
                **defaults
            )


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