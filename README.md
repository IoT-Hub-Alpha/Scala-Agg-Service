## Scala Telemetry Aggrigation Service.

A telemetry aggregation service based on scala.
- Read telemetry from telemetry.raw (kafka)
- Retain telemetry data for a preset window time (1hour default)
- Return average/min/max and mean per device per timeframe.
- Accessable only from inside docker (Access point from teletry API)

Usage: 
``` 
GET .../agg
```
Query parameters:
|Parameter|Definition|Example|Required|
|---------|----------|-------|--------|
|device|Device serial number to search by|SN-001-PWR|True|
|operation| avg/min/max/mean requested result|avg|False (avg default)|
|window|Window to search (in seconds)|30|False(30 Default)|

Example:

``` 
GET .../agg?device=SN-001-PWR&operation=mean&window=60
```

Gathers average from SN-001-PWR during the last 60 seconds from request time.
