Parser for airport websites, written in Python.
Works as an asynchronous Tornado service, returns JSON.

Currently supports:
* Pulkovo, Saint-Petersburg (LED)
* Domodedovo, Moscow (DME)
* Sheremetyevo, Moscow (SVO)
* Vnukovo, Moscow (VKO)

Returns a bit more than airport websites report, e.g. origin and destination airports IATA codes.

*Intended as a part of a project researching the nature of airport delays*