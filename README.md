# RedisTimeseriesManager

RedisTimeseriesManager is a redis timeseries management system that enhance redis timeseries with features including multi-line data, built-in timeframes, data classifiers and convenient data accessors. This is achieved by maintaing a set of timeseries that are tied together(called ***lines***) and interact with them as a whole. 
As a result, multiple timeseries values can be refered with a timestamp as if they are stored in a table with a timestamp and multiple columns.

This library supports two levels of data classifiers(c1, c2) to interact with data, plus support for timeframes and data compression(downsampling).

## About Redis
Redis is an open source (BSD licensed), in-memory data structure store used as a database, cache, message broker, and streaming engine.

## About RedisTimeSeries
RedisTimeSeries is a Redis module that adds a time series data structure to Redis. `RedisTimeseriesManager` uses RedisTimeSeries to store timeseries data.

## Installation

To install RedisTimeseriesManager, run the following command:

```pip install --upgrade redis_timeseries_manager```


## Usage

## Basic Example

To get started, simply create a class that inherits from `RedisTimeseriesManager`. Then set the properties `_name`, `_lines`, and `_timeframes`.

```python
class Test(RedisTimeseriesManager):
    _name = 'test'
    _lines = ['l1', 'l2']
    _timeframes = {
        'raw': {'retention_secs': 60*60*24}, # retention 1 day
    }
```

You can think of `_lines` as the columns in a relational database. You can have as many lines as your requirements. They can be added or removed at any time using `add_line()` or `delete_line()` methods.

At least one timeframe must be provided. Even if you simply want to store timeseries data without separate timeframes, add a default timeframe and it will be used seamlessly. Also a unique `_name` must be provided for each class.

## Creating the object

```python
t = Test(host, port, db, password)
```

## Adding data  to series

The method `add()` is used to add data to the series. The syntax is:

```python
t.add(data, c1, c2, create_inplace=False)
```

The format of data is as follows:

```python
[timestamp, l1, l2, ...]
```

`c1` and `c2` are the classifiers. You can classify data in two levels using these classifiers. In our [sensors example](examples/sensor_data.ipynb) we have used `c1` to identify the location of sensor and `c2` for the sensor itself.

Before adding data, we have to prepare the timeseries with the classifiers. To achieve this, we use the `create()` method. For the sensors example:
    
```python
t.create(
    c1='building1',
    c2='sensor1',
)
```

Now we can add data for sensor 1:

```python
t.add(
    data=[
        [123456, 1, 2],
        [123457, 3, 4],
        [123458, 5, 6],
    ],
    c1='building1',
    c2='sensor1',
)
```

For the sensor2, we don't want to prepare series explicitly, instead we set `create_inplace` to True and the series will be prepared with the new classifiers implicitely while inserting data:

```python
t.add(
    data=[
        [123456, 7, 8],
        [123457, 9, 10],
        [123458, 11, 12],
    ],
    c1='building1',
    c2='sensor2',
    create_inplace=True,
)
```

## Reading the data

The method `read()` is used to read data from the series. In our example we can read the data for sensor1 as follows:

```python
t.read(
    c1='building1',
    c2='sensor1',
)
```
    [[123456, 1.0, 2.0], [123457, 3.0, 4.0], [123458, 5.0, 6.0]]

There are also some other methods to read or investigate about the data. `read_last_n_records()`, `read_last_nth_record()`, `read_last()` and `find_last()` are those methods. 

Since v2.0, pandas dataframes are supported. You can choose the format of output data when calling read methods. Supported formats are 'list' (python `list`, default format), 'df'(pandas dataframe) or 'raw' which is the raw data read from the timeseries.

Some additional parameters can also be used to control what data are read, they include `from_timestamp`, `to_timestamp` and `extra_records`. Refer to the documentations of each method for details.




# Advanced Usage Examples

## Example 1: Sensor Data

In this example, we are going to maintain data of two imaginary sensors. Each sensor provides two measurements: `temperature` and `humidity`.

The data is collected with the resolution of one minute. Then we compress(downsample) the data to hourly and daily resolutions. To compress the data, we consider the average value of temperature and the maximum value of humidity in each time frame.

We also want to keep 1-minute sensor data for just one week, 1-hour data for one month and respectively 1-day data for a year.
In this Example, we use the classifier 1(c1) to identify the building where the sensor is located and the classifier 2(c2) for the sensor.

```python
import time, datetime, random
from pytz import timezone

from redis_timeseries_manager import RedisTimeseriesManager

settings = {
    'host': 'localhost',
    'port': 6379,
    'db': 13,
    'password': None,
}

class SensorData(RedisTimeseriesManager):
    _name = 'sensors'
    _lines = ['temp', 'hum']
    _timeframes = {
        'raw': {'retention_secs': 60*60*24*7}, # retention 7 day
        '1h': {'retention_secs': 60*60*24*30, 'bucket_size_secs': 60*60}, # retention 1 month; timeframe 3600 secs
        '1d': {'retention_secs': 60*60*24*365, 'bucket_size_secs': 60*60*24}, # retention 1 year; timeframe 86400 secs
    }

    #compaction rules
    def _create_rule(self, c1:str, c2:str, line:str, timeframe_name:str, timeframe_specs:str, source_key:str, dest_key:str):
        if line == 'temp':
            aggregation_type = 'avg'
        elif line == 'hum':
            aggregation_type = 'max'
        bucket_size_secs = timeframe_specs['bucket_size_secs']
        self._set_rule(source_key, dest_key, aggregation_type, bucket_size_secs)
    
    @staticmethod
    def print_data(data):
        for ts, temp, hum in data:
            print(f"{datetime.datetime.fromtimestamp(ts, tz=timezone('UTC')):%Y-%m-%d %H:%M:%S}, temp: {round(temp, 2)}, hum(max): {round(hum, 2)}")
```

> [***view the full example and usage***](examples/sensor_data.ipynb)

## Example 2: Market Data(OHLCV)

***Demonstrating a high-performance market price data downsampling mechanism using *redis* backend and `RedisTimeseriesManager`***


In this example, we are going to maintain the data of some financial markets. We have chosen the `cryptocurrency` and `irx` for our example. Each market contain several instruments that we refer to them as symbols and we collect OHLCV(`open`, `high`, `low`, `close`, `volume`) data for each symbol.

The raw data is directly collected from the market with the resolution of seconds and we insert them in `raw` timeframe. Then we compress(downsample) the data to timeframes of `1m`, `1h` and `1d`. As the names `open`, `high`, `low`, `close`, `volume` implies, we use the `FIRST` aggregator for `open`, `MAX` for `high`, `MIN` for `low`, `LAST` for `close` and the `SUM` aggregator for `volume` to compress the data and build the appropriate timeframes of data.

We also want to keep `1m` data for just one week, `1h` for one month and respectively `1d` data for a year.
In this Example, we use the classifier 1(c1) to identify the market(here `cryptocurrency` or `irx`) and the classifier 2(c2) for the symbols.

```python
import time, datetime, random
from pytz import timezone

from redis_timeseries_manager import RedisTimeseriesManager

settings = {
    'host': 'localhost',
    'port': 6379,
    'db': 13,
    'password': None,
}

class MarketData(RedisTimeseriesManager):
    _name = 'markets'
    _lines = ['open', 'high', 'low', 'close', 'volume']
    _timeframes = {
        'raw': {'retention_secs': 60*60*24*4}, # retention 4 days
        '1m': {'retention_secs': 60*60*24*7, 'bucket_size_secs': 60}, # retention 7 day; timeframe 60 secs
        '1h': {'retention_secs': 60*60*24*30, 'bucket_size_secs': 60*60}, # retention 1 month; timeframe 3600 secs
        '1d': {'retention_secs': 60*60*24*365, 'bucket_size_secs': 60*60*24}, # retention 1 year; timeframe 86400 secs
    }

    #compaction rules
    def _create_rule(self, c1:str, c2:str, line:str, timeframe_name:str, timeframe_specs:str, source_key:str, dest_key:str):
        if line == 'open':
            aggregation_type = 'first'
        elif line == 'close':
            aggregation_type = 'last'
        elif line == 'high':
            aggregation_type = 'max'
        elif line == 'low':
            aggregation_type = 'min'
        elif line == 'volume':
            aggregation_type = 'sum'
        else:
            return
        bucket_size_secs = timeframe_specs['bucket_size_secs']
        self._set_rule(source_key, dest_key, aggregation_type, bucket_size_secs)
    
    @staticmethod
    def print_data(data):
        for ts, open, high, low, close, volume in data:
            print(f"{datetime.datetime.fromtimestamp(ts, tz=timezone('UTC')):%Y-%m-%d %H:%M:%S}, open: {open}, high: {high}, low: {low}, close: {close}, volume: {volume}")
```

> [***view the full example and usage***](examples/market_data.ipynb)