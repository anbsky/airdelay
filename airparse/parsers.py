#!/usr/bin/env python
#encoding=utf-8

import re
from datetime import datetime

from dateutil import parser
import redis

from engine import FlightStatus, Flight, BaseParser


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

registry = ParserRegistry()


class DMEParser(BaseParser):
    url = {
        'outbound': 'http://www.domodedovo.ru/onlinetablo/default.aspx?tabloname=TabloDeparture_E',
        'inbound': 'http://www.domodedovo.ru/onlinetablo/default.aspx?tabloname=TabloArrival_E'
    }
    _targets = {
        'FL_NUM_PUB': 'number',
        'ORG': 'peer',
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

            time_scheduled = self._parse_time(parsed_row['time_scheduled'])
            time_actual = self._parse_time(parsed_row['time_actual'])
            status = self._parse_status(parsed_row['raw_status'])
            peer = re.sub(r'\(.+\)', '', parsed_row['peer'])

            f = Flight(
                source=self.iata_code,
                time_scheduled=time_scheduled,
                time_actual=time_actual,
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
                time_scheduled=self._parse_time(' '.join(raw[:2])),
                time_actual=self._parse_actual(raw[7]),
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
            airport = re.sub(r'\s?\(.+\)', '', airport.replace('\n', ' ')).strip().title()

            f = Flight(
                source=self.iata_code,
                time_scheduled=self._parse_time(raw_cells[5]),
                time_actual=self._parse_time(raw_cells[6]),
                status=self._parse_status(raw[4]),
                number=raw[0].strip(),
                destination=self.iata_code,
                airline=raw[1].strip().title(),
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


class LEDParser(BaseParser):
    url = {
        'outbound': [
            'http://www.pulkovoairport.ru/eng/online_serves/online_timetable/departures/',
            'http://www.pulkovoairport.ru/eng/online_serves/online_timetable/departures/?p=2'
        ],
        'inbound': [
            'http://www.pulkovoairport.ru/eng/online_serves/online_timetable/arrivals/',
            'http://www.pulkovoairport.ru/eng/online_serves/online_timetable/arrivals/?p=2',
        ]
    }

    def parse(self, soup, **defaults):
        re_airport = re.compile(r'(\w+)\s+\((\w+)\)')
        statuses = {
            'departed': FlightStatus.DEPARTED,
            'arrived': FlightStatus.LANDED,
            'cancelled': FlightStatus.CANCELLED,
        }

        def parse_time(time_str):
            if not time_str:
                return None
            return parser.parse(time_str, dayfirst=True)

        for row in soup.find('table', {'class': 'tabloBigNew'}).find_all('tr', recursive=False):
            if 'bigTableTitle' in row.get('class', []) or 'onlineDetailTr' in row.get('class', []):
                continue
            raw_cells = row.find_all('td')
            raw_strings = map(lambda s: s.strip() if s else s, [cell.string for cell in raw_cells])
            if not len(raw_strings) >= 6:
                continue

            f = Flight(
                source=self.iata_code,
                time_scheduled=parse_time(raw_strings[2]),
                time_actual=parse_time(raw_strings[3]),
                status=statuses.get(raw_strings[4]),
                number=raw_strings[0],
                destination=self.iata_code,
                airline=raw_strings[5],
                **defaults
            )

            airport_match = re_airport.match(raw_strings[1])
            if airport_match:
                airport_city, airport_iata_code = airport_match.groups()
            else:
                airport_city, airport_iata_code = raw_strings[1], None

            if defaults['type'] == 'outbound':
                f.set_origin(self.name, self.iata_code)
                f.set_destination(airport_city, airport_iata_code)
            else:
                f.set_origin(airport_city, airport_iata_code)
                f.set_destination(self.name, self.iata_code)

            yield f


parsers.add('DME', DMEParser)
parsers.add('SVO', SVOParser)
parsers.add('VKO', VKOParser)
parsers.add('LED', LEDParser)


def do_import():
    crawler = DMEParser()
    for r in crawler.fetch():
        pass

if __name__ == '__main__':
    do_import()