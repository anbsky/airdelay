#encoding=utf-8

from __future__ import print_function, unicode_literals
from itertools import groupby

from airdelay.models import Flight


if __name__ == '__main__':
    records = {}
    loads = {}
    loads_dates, loads_values = [], []
    for date, flights in groupby(
        Flight.objects.filter(
#            status=Flight.status_list.DELAYED,
            codeshare=0
        ).order('created_at'), lambda f: f.created_at_compressed):
        records.setdefault(date, [])
        flight_weights = [(f.delay_weight, f.delay_minutes, unicode(f)) for f in flights]
        records[date].append(flight_weights)
        load = 0
        delay_total = 0
        delay_count = 0
        for weight, minutes, code in flight_weights:
            load += weight
            if minutes > 0:
                delay_total += minutes
                delay_count += 1
        loads[date] = load, delay_total / delay_count, delay_count, len(flight_weights)
        loads_dates.append(date)
        loads_values.append([load, delay_total / delay_count, delay_count, len(flight_weights)])
