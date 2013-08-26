from fabric.api import *

from airparse import codes


def load_airports():
    codes.reload_airports_cache()