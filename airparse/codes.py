# encoding=utf-8

import csv


_airports_cache = None


def find_airport_code(name):
    port = find_airport_by_name(name)
    return port['iata_code'] if port else None


def find_airport_by_name(name):
    name = name.lower()
    try:
        return filter(lambda p: name == p['city'].lower() or name == p['name'].lower(), get_airports())[0]
    except IndexError:
        return None


def get_airports():
    global _airports_cache
    if _airports_cache is None:
        _airports_cache = list(load_airports())
    return _airports_cache


def load_airports(filename='airports.dat'):
    fields = 'id name city country iata_code icao_code latitude longitude altitude_ft timezone dst'.split()

    with open(filename, 'rb') as airports_file:
        airports_csv = csv.DictReader(airports_file, fieldnames=fields, delimiter=',')
        for port in airports_csv:
            yield port