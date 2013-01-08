#encoding=utf-8

from redisco import models


class Airport(models.Model):
    code = models.Attribute(required=True)
    title = models.Attribute(required=True)


class FlightInfo(models.Model):
    airport = models.ReferenceField(Airport, required=True)
    peer_airport = models.Attribute(required=True)
    type = models.IntegerField(required=True)
    created_at = models.DateTimeField(auto_now_add=True)
    scheduled = models.DateTimeField(required=True)
    actual = models.DateTimeField(required=True)
    status = models.IntegerField(required=True)