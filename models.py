#encoding=utf-8

from __future__ import print_function
from redisco import models


class Airport(models.Model):
    code = models.Attribute(required=True)
    title = models.Attribute(required=True)

    @property
    def flights(self):
        return False


class Status(object):
    _list = []

    @classmethod
    def lend_to_class(cls, klass, field='status'):
        for status in cls._list:
            property_name = 'is_{}'.format(status.lower())
            status_value = getattr(cls, status)
            def setx(self, value, _status=status_value):
                assert isinstance(value, bool)
                if value:
                    setattr(self, field, _status)
                else:
                    setattr(self, field, None)
            def getx(self, _status=status_value):
                return getattr(self, field) == _status
            setattr(klass, property_name, property(getx, setx))


class FlightStatus(Status):
    _list = 'SCHEDULED', 'DELAYED', 'DEPARTED', 'LANDED'

    SCHEDULED = 10
    DELAYED = 20
    DEPARTED = 30
    LANDED = 35


class Flight(models.Model):
    airport = models.ReferenceField(Airport, required=True)
    peer_airport = models.Attribute(required=True)
    type = models.IntegerField(required=True)
    created_at = models.DateTimeField(auto_now_add=True)
    scheduled = models.DateTimeField(required=True)
    actual = models.DateTimeField(required=True)
    status = models.IntegerField(required=True)

FlightStatus.lend_to_class(Flight)
