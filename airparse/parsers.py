#!/usr/bin/env python
#encoding=utf-8

import re
from bs4 import BeautifulSoup
from dateutil import parser
from datetime import datetime
import json
import requests
import time


class ParserRegistry(dict):
    def add(self, iata_code, klass):
        if iata_code in self.keys():
            raise NameError('Crawler for {} already registered'.format(iata_code))
        assert getattr(klass, 'url'), 'Cannot register a crawler without URL'
        self[iata_code] = klass

    def get(self, iata_code):
        return self[iata_code]()

parsers = ParserRegistry()


class FlightStatus(object):
    SCHEDULED = 'scheduled'
    DELAYED = 'delayed'
    DEPARTED = 'departed'
    LANDED = 'landed'
    CANCELLED = 'cancelled'
    ARRIVED = 'arrived'


def json_record_handler(obj):
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    else:
        raise TypeError, 'Object of type %s with value of %s is not JSON serializable' % (type(obj), repr(obj))


class BaseParser(object):
    def __init__(self, delay=0.5):
        self.records = []
        self.status = None
        self.metadata = {
            'status': None,
            'iata_code': self.iata_code,
            'flights': self.records
        }
        # To prevent banning
        self.delay = delay

    def set_status(self, value):
        self.metadata['status'] = value

    def run(self):
        # Airport website has separate pages for departure/arrival
        if isinstance(self.url, dict):
            for _type, url in self.url.items():
                self.records += list(self.parse(
                    BeautifulSoup(requests.get(url).content),
                    {'type': _type}
                ))
                print 'got url ' + url
                time.sleep(self.delay)
        # Single page
        else:
            self.records += list(self.parse(BeautifulSoup(requests.get(self.url).content)))
        self.set_status('OK')
        return self.records

    def clean_row(self, parsed_row):
        for key, value in parsed_row.items():
            if not value:
                del parsed_row[key]
        return parsed_row


    def to_json(self):
        return json.dumps(self.metadata, default=json_record_handler)


class DMEParser(BaseParser):
    url = {
        'departure': 'http://www.domodedovo.ru/onlinetablo/default.aspx?tabloname=TabloDeparture_E',
        'arrival': 'http://www.domodedovo.ru/onlinetablo/default.aspx?tabloname=TabloArrival_E'
    }
    _targets = {
        'FL_NUM_PUB': 'code',
        'ORG': 'peer_airport_name',
        'TIM_P': 'time_scheduled',
        'TIM_L': 'time_actual',
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

    def parse(self, soup, preset=None):
        for row in soup.find(id='onlinetablo').find_all('tr'):
            val = self.row_parser(row, preset)
            if val:
                yield val

    def row_parser(self, row, preset):
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
        if self._codeshare_re.search(parsed_row['code']):
            parsed_row['is_codeshare'] = True
        parsed_row['time_scheduled'] = self._parse_time(parsed_row['time_scheduled'])
        parsed_row['time_actual'] = self._parse_time(parsed_row['time_actual'])
        parsed_row['status'] = self._parse_status(parsed_row['raw_status'])
        del parsed_row['raw_status']
        if preset:
            parsed_row.update(preset)
        return self.clean_row(parsed_row)

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

    def parse(self, soup, preset=None):
        for row in soup.find('div', {'class': 'timetable'}).find(
            'div', {'class': 'table'}
        ).find('table').find_all('tr'):
            parsed_row = {}
            row_cls = row.get('class', [])
            if 'sA' in row_cls:
                parsed_row['type'] = 'arrival'
            elif 'sD' in row_cls:
                parsed_row['type'] = 'departure'
            else:
                continue
            raw_cells = row.find_all('td')
            raw = [cell.string for cell in raw_cells]
            parsed_row['time_scheduled'] = self._parse_time(' '.join(raw[:2]))
            parsed_row['time_actual'] = self._parse_actual(raw[7])
            parsed_row['code'] = ' '.join(raw[2:4])
            parsed_row['peer_airport_name'] = raw[5]
            parsed_row['status'] = self._parse_status(raw_cells[7])
            yield self.clean_row(parsed_row)

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
        'departure': 'http://vnukovo.ru/eng/for-passengers/board/index.wbp?time-table.direction=1',
        'arrival': 'http://vnukovo.ru/eng/for-passengers/board/index.wbp?time-table.direction=0'
    }
    _statuses = {
        'departed': FlightStatus.DEPARTED,
        'arrived': FlightStatus.LANDED,
        'has not departed': FlightStatus.DELAYED,
    }

    def parse(self, soup, preset=None):
        for row in soup.find('table', {'id': 'TimeTable'}).find('tbody').find_all('tr'):
            parsed_row = {}
            raw_cells = row.find_all('td')
            raw = [cell.string for cell in raw_cells]
            parsed_row['code'] = raw[0].strip()
            parsed_row['airline'] = raw[1].strip()
            airport = ''
            if preset:
                if preset.get('type') == 'departure':
                    airport = raw[3]
                elif preset.get('type') == 'arrival':
                    airport = raw[2]
            airport = airport.replace('\n', ' ')
            parsed_row['peer_airport_name'] = airport
            parsed_row['status'] = self._parse_status(raw[4])
            parsed_row['time_scheduled'] = self._parse_time(raw_cells[5])
            parsed_row['time_actual'] = self._parse_time(raw_cells[6])
            yield self.clean_row(parsed_row)

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