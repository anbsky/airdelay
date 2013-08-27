# Parser for airport websites, written in Python.

Works as a Tornado application, spits out JSON. Tries to run asynchronously whenever possible by using *concurrent.futures* (backport to Python 2.x).
Results are cached in Redis so it doesn't hit ariport website on every request.

## Example output

```
$ curl http://airdelay.info/airports/SVO/

{
   "iata_code": "SVO",
   "time_retrieved": "2013-08-27T13:25:37",
   "name": "Sheremetyevo",
   "flights": [
      {
         "status": "landed",
         "origin": "LCA",
         "origin_name": "Larnaca",
         "destination": "SVO",
         "destination_name": "Sheremetyevo",
         "number": "CY 1774",
         "time_actual": "2013-08-27T23:52:00",
         "time_scheduled": "2013-08-27T00:05:00"
      },
      ...
   ]
}
```

Currently supports:
* Domodedovo, Moscow (DME)
* Sheremetyevo, Moscow (SVO)
* Vnukovo, Moscow (VKO)
* Pulkovo, Saint-Petersburg (LED)

Returns a bit more than airport websites report, e.g. origin and destination airports IATA codes.

*Intended as a part of a project researching the nature of airport delays*