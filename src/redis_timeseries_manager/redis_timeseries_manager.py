# MIT License

# Copyright (c) 2022 Ahmad Azizi
# https://github.com/ahmadazizi/redis_timeseries_manager

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import datetime, time, re, math
from typing import Union

import redis
import pandas as pd


class RedisTimeseriesManager:
    """RedisTimeseriesManager is a redis timeseries management system that enhance redis timeseries with features including multi-line data, built-in timeframes, data classifiers and convenient data accessors.
    This class can be used within context manager.
    """
    _name = None
    _lines = []
    _timeframes = {}

    def __init__(
            self,
            host:str,
            port,
            db:int,
            password:str,
        ) -> None:
        """Initialize RedisTimeseriesManager

        Args:
            host (str): host of redis server
            port (int): port of redis server
            db (int): database index of redis server
            password (str): password of redis server
        """
        self.client = redis.Redis(host=host, port=port, db=db, password=password)
        self.ts = self.client.ts()
        if not self._name:
            raise Exception("`_name` was not set. please set the `_name` class property.")
        if not self._lines:
            raise Exception("`_lines` is empty. please set at least one line in the `_lines` class property.")
        if not self._timeframes:
            raise Exception("`_timeframes` is empty. please set at least one timeframe in the `_timeframes` class property.")


    def __enter__(self):
        return self
    

    def __exit__(self, exc_type, exc_value, traceback):
        self.finish()


    def finish(self):
        if self.client:
            self.client.close()
    

    def get_lines(self):
        return self._lines
    

    def get_timeframes(self):
        return list(self._timeframes.keys())


    def create(self, c1:str, c2:str, extra_labels:dict=None):
        c1 = c1.lower()
        c2 = c2.lower()
        if self.map_exists(c1, c2):
            return True, f'{c2} already exists'
        try:
            self._create_tseries(c1, c2, None, extra_labels)
            self._iter_rules(c1, c2)
            return True, f'Create {c1}:{c2} success!'
        except Exception as e:
            return False, str(e)


    def _create_tseries(self, c1:str, c2:str, new_line:str=None, extra_labels:dict=None):
        lines = self._lines if not new_line else [new_line.lower()]
        for tf_name, tf_specs in self._timeframes.items():
            for line in lines:
                key_name = self._get_key_name(c1, c2, tf_name, line)
                main_labels = {
                    'tl': self._name,
                    'c1': c1,
                    'c2': c2,
                    'line': line,
                    'timeframe':tf_name
                }
                labels = {}
                if extra_labels and type(extra_labels) is dict:
                    labels.update(extra_labels)
                labels.update(main_labels)
                self._create_ts(key_name, tf_specs['retention_secs'], 'last', labels)


    def _create_ts(self, key_name:str, retention_secs:int=0, duplicate_policy:str='last', labels:dict=None):
        """Create timeseries
        If a key already exists, you get a normal Redis error 
        https://redis.io/commands/ts.create/
        Args:
            key_name (str): name of key
            retention_secs (int, optional): retention in seconds. Defaults to 0(unlimited).
            duplicate_policy (str, optional): Can be one of: block, first, last, min max. Defaults to 'last'.
            labels (dict, optional): metadata lebels. Defaults to None.
        """
        self.ts.create(
            key=key_name,
            retention_msecs=retention_secs * 1000,
            duplicate_policy=duplicate_policy,
            labels=labels
        )


    def _iter_rules(self, c1:str, c2:str, new_line:str=None):
        tf_list = list(self._timeframes.keys())
        lines = self._lines if not new_line else [new_line.lower()]
        for tf_name, tf_specs in self._timeframes.items():
            if tf_name in [tf_list[0]]: # skip first timeframe
                continue
            if 'ignore_rules' in tf_specs and tf_specs['ignore_rules']: # skip rule by flag
                continue
            for line in lines:
                specs = {
                    'c1': c1,
                    'c2': c2,
                    'line': line,
                    'timeframe_name': tf_name,
                    'timeframe_specs': tf_specs,
                    'source_key': self._get_key_name(c1, c2, tf_list[0], line),
                    'dest_key': self._get_key_name(c1, c2, tf_name, line),
                }
                self._create_rule(**specs)
    

    def _iter_rules_line(self, line:str):
        try:
            # rules must be created for all c1, c2
            index_info = self.query_index()
            c1_list = index_info[1]['c1']
            # looping per c1
            for c1 in c1_list:
                c1_index_info = self.query_index(c1=c1)
                c2_list = c1_index_info[1]['c2']
                # looping per c2
                for c2 in c2_list:
                    self._iter_rules(c1, c2, line)
            return True, f'Rules created for the line {line}'
        except Exception as e:
            return False, str(e)


    def _create_rule(self, c1:str, c2:str, line:str, timeframe_name:str, timeframe_specs:str, source_key:str, dest_key:str):
        """This method will be called upon creating a line of data.
        Rewrite this method as per your needs.
        Args:
            c1 (str): c1
            c2 (str): c2
            line (str): line
            timeframe_name (str): timeframe name
            timeframe_specs (str): timeframe specs
            source_key (str): source key
            dest_key (str): destination key
        """
        pass


    def _set_rule(self, source_key:str, dest_key:str, aggregation_type:str, bucket_size_secs:int):
        """Create a rule
        Args:
            source_key (str): source key
            dest_key (str): destination key
            aggregation_type (str): Can be one of: first, last, min, max, sum, count, avg, ...
            bucket_size_secs (int): bucket size in seconds
        """
        self.ts.createrule(
            source_key=source_key,
            dest_key=dest_key,
            aggregation_type=aggregation_type,
            bucket_size_msec=bucket_size_secs * 1000
        )


    def clear_data(self, c1:str, c2:str, from_timestamp=0, to_timestamp=None):
        """Clear all data in a range
        WARNING:
            This error may happen when the retention time in source_key is small:
            ----Exception("TSDB: Can't delete an event which is older than retention time, in such case no valid way to update the downsample")
            I'm not still sure about the way this can be fixed; and wether it is required to clear compressed timeframes(seems this happen automatically).
        """
        c1 = c1.lower()
        c2 = c2.lower()
        try:
            if not to_timestamp:
                to_timestamp = int(time.time())
            from_timestamp = from_timestamp * 1000
            to_timestamp = to_timestamp * 1000
            for tf_name, tf_specs in self._timeframes.items():
                for line in self._lines:
                    key = self._get_key_name(c1, c2, tf_name, line)
                    self.ts.delete(key, from_timestamp, to_timestamp)
            return True, f"Cleard data of {c1}:{c2} from {from_timestamp} to {to_timestamp}"
        except Exception as e:
            message = f"Error clearing data of key {key} from {from_timestamp} to {to_timestamp}; {e}"
            return False, message


    def delete_classifier_keys(self, c1:str, c2:str):
        """Delete keys based on classifiers
        If data related to some classifiers is not needed anymore, delete the related keys at all
        Args:
            c1 (str): _description_
            c2 (str): _description_
        Returns:
            _type_: _description_
        """
        c1 = c1.lower()
        c2 = c2.lower()
        try:
            for tf_name, tf_specs in self._timeframes.items():
                for line in self._lines:
                    key = self._get_key_name(c1, c2, tf_name, line)
                    self.client.delete(key)
            return True, 'Finished'
        except Exception as e:
            return False, str(e)
    

    def add_line(self, line:str, default_value:float=0.0, extra_labels:dict=None):
        """Add a line to the series
        Notice:
            - After runnig this method, add `line` name to `_lines`
        Args:
            line (str): line
            default_value (float, optional): default value, new line will be filled with this value. Defaults to 0.0.
            extra_labels(dict, optional): set extra labels for the new created line
        Returns:
            tuple: (success:bool, message:str)
        """
        line = line.lower()
        try:
            # line must be created for all c1, c2 and timeframe combinations
            index_info = self.query_index()
            c1_list = index_info[1]['c1']
            # looping per c1
            for c1 in c1_list:
                c1_index_info = self.query_index(c1=c1)
                c2_list = c1_index_info[1]['c2']
                # looping per c2
                for c2 in c2_list:
                    # creating lines for all timeframes
                    self._create_tseries(c1, c2, new_line=line, extra_labels=extra_labels)
                    # filling new lines with default value
                    self._reset_line_values(c1, c2, line, default_value)
            # creating rules for the new line
            self._iter_rules_line(line)
            return True, f'Create line {line} success!'
        except Exception as e:
            return False, str(e)


    def delete_line(self, line:str):
        """Delete a line
        Notice:
            - After runnig this method, remove `line` name from `_lines`
        Args:
            line (str): line
        Returns:
            tuple: (success:bool, message:str)
        """
        try:
            line = line.lower()
            keys = self.query_index(
                line=line,
                return_key_names=True,
            )
            for key in keys[1]:
                self.client.delete(key)
            return True, f'Deleted {len(keys[1])} keys'
        except Exception as e:
            return False, str(e)


    def add(self, data:list, c1:str, c2:str=None, c2_position:int=None, extra_labels:dict=None, timeframe:str=None, create_inplace:bool=False):
        """THIS METHOD IS OBSOLETE, USE insert() INSTEAD.
            Add data records
            If c2 is not provided, it means that it resides inside the data at position `c2_position`
        Args:
            data (list): list of lists containing data(To insert a single record, no need to be enclosed in a `list`). Each inner list is as [timestamp, [c2], line1, line2, ...]
            c1 (str): classifier 1
            c2 (str, optional): classifier 2. If c2 is not provided, it means that it resides inside data at position `c2_position`
            c2_position (int, optional): position of c2 inside data. Must be provided if c2 is not provided.
            extra_labels (dict, optional): Extra lables for the new created ts map when `create_implace` is True.
            timeframe (str, optional): timeframe. Defaults to 1st(shortest) timeframe. Please note that you shouldn't add data directly to `compressed` timeframes. So using this parameter is prohibited.
            create_inplace (bool, optional): Create ts map if does not exist. Defaults to False.
        Returns:
            tuple: (success:bool, insertedDataLength:int or error:str)
        """
        return self.insert(
            data=data,
            c1=c1,
            c2=c2,
            c2_position=c2_position,
            extra_labels=extra_labels,
            timeframe=timeframe,
            create_inplace=create_inplace
        )


    def insert(self, data:list, c1:str, c2:str=None, c2_position:int=None, extra_labels:dict=None, timeframe:str=None, create_inplace:bool=False):
        """Inserts record(s) of data into series
            If data with already existing timestamps are provided, the existing data will be overwritten
            If c2 is not provided, it means that it resides inside the data at position `c2_position`

        Args:
            data (list): list of lists containing data(To insert a single record, no need to be enclosed in a `list`). Each inner list is as [timestamp, [c2], line1, line2, ...]
            c1 (str): classifier 1
            c2 (str, optional): classifier 2. If c2 is not provided, it means that it resides inside data at position `c2_position`
            c2_position (int, optional): position of c2 inside data. Must be provided if c2 is not provided.
            extra_labels (dict, optional): Extra lables for the new created ts map when `create_implace` is True.
            timeframe (str, optional): timeframe. Defaults to 1st(shortest) timeframe. Please note that you shouldn't add data directly to `compressed` timeframes. So using this parameter is prohibited.
            create_inplace (bool, optional): Create ts map if does not exist. Defaults to False.
        
        Returns:
            tuple: (success:bool, insertedDataLength:int or error:str)
        """
        try:
            c1 = c1.lower()
            c2 = c2.lower() if c2 else None
            c2_inline = c2 is None
            timeframe= timeframe.lower() if timeframe else self._get_timeframe_at_position(0)
            if not c2 and c2_position is None:
                raise Exception(f"Either `c2` or `c2_position` must be set")
            output = []
            if c2 and not self.map_exists(c1, c2):
                if create_inplace:
                    self.create(c1, c2, extra_labels)
                else:
                    raise Exception(f"Map {c1}:{c2} does not exist.")
            if type(data[0]) not in [list, tuple]:
                data = [data]
            for d in data:
                if c2_inline:
                    c2 = d.pop(c2_position)
                    if not self.map_exists(c1, c2):
                        if create_inplace:
                            self.create(c1, c2, extra_labels)
                        else:
                            raise Exception(f"Map {c1}:{c2} does not exist.")
                for idx, line in enumerate(self._lines):
                    key = self._get_key_name(c1, c2, timeframe, line)
                    output.append((key, d[0]*1000, d[idx+1]))
            return True, len(self.ts.madd(output))
        except Exception as e:
            message = f"Failed to add to {c1}:{c2} -> {e}"
            return False, message


    def update(self, c1:str, c2:str, timestamp:int, values:dict):
        """Update values at an existing timestamp
        This will only update the provided values at an existing timestamp. Other values will be untouched.
        Update will be applied on the first timeframe, other timeframes will be updated by compaction rules(if any)

        Args:
            c1 (str): classifier 1
            c2 (str): classifier 2
            timestamp (int): timestamp with existing values
            values (dict): key-value pairs of updating data, Every key must be a valid line


        Returns:
            tuple: (success:bool, message:str)
        """
        try:
            # checking if all provided lines are valid
            for line in values.keys():
                if line not in self._lines:
                    raise Exception(f"The line `{line}` does not exist")
            success, result = self.read(
                c1=c1,
                c2=c2,
                from_timestamp=timestamp,
                to_timestamp=timestamp,
                from_timestamp_inclusive=True,
                return_as='list'
            )
            if not success:
                raise Exception(result)
            if len(result) != 1:
                raise Exception(f"Expected exacly 1 result, {len(result)} was returned")
            for line, value in values.items():
                key = self._get_key_name(c1=c1, c2=c2, timeframe=self._get_timeframe_at_position(0), line=line)
                self.ts.add(key=key, timestamp=timestamp*1000, value=value)
            return True, f"{len(values)} value(s) has been updated"
        except Exception as e:
            return False, str(e)


    def read(
            self,
            c1:Union[str, dict],
            c2:Union[str, dict],
            timeframe:str=None,
            from_timestamp:int=None,
            to_timestamp:int=None,
            extra_records:int=None,
            timestamp_minimum_boundary:int=None,
            from_timestamp_inclusive:bool = True,
            line_order:list=None,
            allow_multiple:bool=False,
            return_as:str = 'list'
        ):
        """Read records based on conditions
        Args:
            c1 (str|dict): c1 classifier or a dictionary containing label filters
            c2 (str|dict): c2 classifier or a dictionary containing label filters
            timeframe (str, optional): timeframe. Defaults to 1st timeframe.
            from_timestamp (int, optional): Defaults to 0.
            to_timestamp (int, optional): Defaults to timestamp of current time.
            extra_records (int, optional): Number of extra_records before from_timestamp. Defaults to None. [THIS FEATURE IS EXPERIMENTAL]
            timestamp_minimum_boundary (int, optional): When extra_records set, this limits how much from_timestamp can decline. Defaults to None. [THIS FEATURE IS EXPERIMENTAL]
            from_timestamp_inclusive(bool, optional): If enabled, the output range will include the from_timestamp data. Default enabled
            line_order(list, optional): Optional order(or filter) of output data. The default order is as the class `_lines` property. E.g. if _lines is ['x', 'y', 'z'] and you need only `z` and `y` in order, you can set `line_order` to ['z', 'y']
            allow_multiple(bool, optional) Allow combine multiple sets of data. This may results in multiple data-points with the same time. Defaults to False
            return_as(str, optional): Set the output format(default format is `list`). Available options are `raw`, `df`, `list`, `dict`, `sets-df`, `sets-list` and `sets-dict`
        Returns:
            Any: list|df|raw of data like [`timestamp(secs)`, `line1`, `line2`, ...]
        """
        try:
            timeframe= timeframe.lower() if timeframe else self._get_timeframe_at_position(0)
            labels = {
                'timeframe': timeframe
            }
            self._add_classifier_label(labels, 'c1', c1)
            self._add_classifier_label(labels, 'c2', c2)
            if from_timestamp and type(from_timestamp) is int:
                from_timestamp = from_timestamp*1000 if from_timestamp_inclusive else (from_timestamp+1)*1000
            else:
                from_timestamp = '-'
            to_timestamp = to_timestamp*1000 if to_timestamp and type(to_timestamp) is int else '+'
            if extra_records:
                adjust_key_name = self._get_key_name(c1, c2, timeframe, list(self._timeframes.keys()))[0]
                from_timestamp = self._adjust_from_timestamp(adjust_key_name, from_timestamp, extra_records, timestamp_minimum_boundary)
            raw = self.ts.mrange(
                from_time=from_timestamp,
                to_time=to_timestamp,
                with_labels=True,
                filters = self.create_filters(**labels)
            )
            # raw data is now collected; redis would already has raised exception in case of error
            return self._prepare_read_data(data=raw, line_order=line_order, allow_multiple=allow_multiple, return_as=return_as)
        except Exception as e:
            message = f"Failed to read from `{c1}:{c2}` -> {e}"
            return False, message


    def read_last_n_records(
            self,
            c1:Union[str, dict],
            c2:Union[str, dict],
            n:int,
            timeframe:str=None,
            minimum_timestamp:int=None,
            line_order:list=None,
            allow_multiple:bool=False,
            return_as:str = 'list'
        ):
        """read the last n records
        Args:
            c1 (str|dict): c1 classifier or a dictionary containing label filters
            c2 (str|dict): c2 classifier or a dictionary containing label filters
            n (int): The intended number of records from the last
            timeframe (str): timeframe. Defaults to 1st timeframe.
            minimum_timestamp (int|`optimized`, optional): The minimum timestamp(secs) of valid record. If not provided it means unlimited; Set to 'optimized' so that an optimized value will be chosen based on timeframe.
            line_order(list, optional): Optional order(or filter) of output data. The default order is as the class `_lines` property. E.g. if _lines is ['x', 'y', 'z'] and you need only `z` and `y` in order, you can set `line_order` to ['z', 'y']
            allow_multiple(bool, optional) Allow combine multiple sets of data. This may results in multiple data-points with the same time. Defaults to False
            return_as(str, optional): Set the output format(default format is `list`). Available options are `raw`, `df`, `list`, `dict`, `sets-df`, `sets-list` and `sets-dict`
        Returns:
            tuple: success(bool), records_are_enough(bool), records(list|df|raw)
        """
        try:
            timeframe = timeframe.lower() if timeframe else self._get_timeframe_at_position(0)
            labels = {
                'timeframe': timeframe,
            }
            self._add_classifier_label(labels, 'c1', c1)
            self._add_classifier_label(labels, 'c2', c2)
            if minimum_timestamp == 'optimized':
                minimum_timestamp = self.get_optimized_from_timestamp(timeframe, n)
            elif type(minimum_timestamp) is int and minimum_timestamp > 0:
                minimum_timestamp = minimum_timestamp * 1000
            else:
                minimum_timestamp = 0
            records_are_enough = False
            success, data_count, pivot_timestamp = self.find_last_nth_timestamp(
                filters=self.create_filters(**{**labels, 'line': self._lines[0]}),
                n=n,
                from_timestamp=minimum_timestamp,
            )
            if not success:
                raise Exception(pivot_timestamp)
            if data_count == n:
                records_are_enough = True
            # everything is ready to fetch actual data
            raw = self.ts.mrange(
                from_time=pivot_timestamp,
                to_time='+',
                with_labels=True,
                filters = self.create_filters(**labels)
            )
            # raw data is now collected; redis would already has raised exception in case of error
            success, data = self._prepare_read_data(data=raw, line_order=line_order, allow_multiple=allow_multiple, return_as=return_as)
            if not success:
                raise Exception(data)
            # data_len = len(data) if return_as != 'raw' else data_count # #todo fix: data_count shouldn't be used here
            # records_are_enough = True if data_len == n else False
            return True, records_are_enough, data
        except Exception as ex:
            return False, False, str(ex)


    def read_last_nth_record(
            self,
            c1:Union[str, dict],
            c2:Union[str, dict],
            n:int,
            timeframe:str=None,
            minimum_timestamp:int=None,
            line_order:list=None,
            allow_multiple:bool=False,
            return_as:str = 'list'
        ):
        """return the last nth record
        Args:
            c1 (str|dict): c1 classifier or a dictionary containing label filters
            c2 (str|dict): c2 classifier or a dictionary containing label filters
            n (int): The position
            timeframe (str): timeframe. Defaults to 1st timeframe.
            minimum_timestamp (int|`optimized`, optional): The minimum timestamp(secs) of valid record. If not provided it means unlimited; Set to 'optimized' so that an optimized value will be chosen based on timeframe.
            line_order(list, optional): Optional order(or filter) of output data. The default order is as the class `_lines` property. E.g. if _lines is ['x', 'y', 'z'] and you need only `z` and `y` in order, you can set `line_order` to ['z', 'y']
            allow_multiple(bool, optional) Allow combine multiple sets of data. This may results in multiple data-points with the same time. Defaults to False
            return_as(str, optional): Set the output format(default format is `list`). Available options are `raw`, `df`, `list`, `dict`, `sets-df`, `sets-list` and `sets-dict`

        Returns:
            tuple: position_exists(bool), record(list|df|raw)
        """
        try:
            timeframe = timeframe.lower() if timeframe else self._get_timeframe_at_position(0)
            labels = {
                'timeframe': timeframe,
            }
            self._add_classifier_label(labels, 'c1', c1)
            self._add_classifier_label(labels, 'c2', c2)
            if minimum_timestamp == 'optimized':
                minimum_timestamp = self.get_optimized_from_timestamp(timeframe, n)
            elif type(minimum_timestamp) is int and minimum_timestamp > 0:
                minimum_timestamp = minimum_timestamp * 1000
            else:
                minimum_timestamp = 0
            success, data_count, pivot_timestamp = self.find_last_nth_timestamp(
                filters=self.create_filters(**{**labels, 'line': self._lines[0]}),
                n=n,
                from_timestamp=minimum_timestamp,
            )
            if not success:
                raise Exception(pivot_timestamp)
            if data_count < 1 or data_count < n:
                raise Exception(f"Could not locate record at position {n} from last")
            # everything is ready to fetch actual data
            target_timestamp = int(pivot_timestamp / 1000)
            success, record = self.read(
                c1,
                c2,
                timeframe=timeframe,
                from_timestamp=target_timestamp,
                to_timestamp=target_timestamp,
                timestamp_minimum_boundary=0,
                from_timestamp_inclusive=True,
                line_order=line_order,
                allow_multiple=allow_multiple,
                return_as=return_as
            )
            if not success:
                raise Exception(record)
            record_length = self.get_read_length(record, return_as)
            if record_length != 1:
                raise Exception(f"Expected exactly one record, {record_length} was returned")
            if return_as != "raw":
                record = record[0] if return_as != 'df' else record.iloc[0]
            return True, record
        except Exception as e:
            return False, str(e)

    
    def read_last(self, c1:str=None, c2:str=None, timeframe:str=None, line_order:list=None, return_as:str='list'):
        """read the last record
            If exactly the last record is required, this performs faster than read_last_nth_record(n=1)
            Notice: This method only supports string classifiers right now; #TODO
        """
        try:
            timeframe = timeframe.lower() if timeframe else self._get_timeframe_at_position(0)
            if not c1 or not c2:
                last = self.last_record(c1, c2, timeframe)
                if not last[0]:
                    raise Exception(last[1])
                last = last[1]
                c1 = last['c1']
                c2 = last['c2']
            ref_key = self._get_key_name(c1, c2, timeframe, self._lines[0])
            ref = self.ts.get(ref_key)
            if ref is None:
                raise Exception("No data returned")
            timestamp = int(ref[0] / 1000)
            success, record = self.read(
                c1,
                c2,
                timeframe=timeframe,
                from_timestamp=timestamp,
                to_timestamp=timestamp,
                timestamp_minimum_boundary=0,
                line_order=line_order,
                return_as=return_as
            )
            if not success:
                raise Exception(record)
            if return_as != "raw":
                record = record[0] if return_as != 'df' else record.iloc[0]
            return True, record, {'c1': c1, 'c2': c2, 'timeframe': timeframe}
        except Exception as e:
            return False, str(e), {'c1': c1, 'c2': c2, 'timeframe': timeframe}


    def find_last(self, c1:str=None, c2:str=None, timeframe:str=None, line:str=None):
        """Find the last value inserted into timeseries; can be filtered by the parameters given
        This is implemented differently from read_last() and is based on labels; used primarily to check if a record already exists and fetch the intended value concurrently
        Note that this method only returns the value of a single line(and also the timestamp).
        Notice: This method only supports string classifiers right now; #TODO
        Args:
            c1 (str, optional): c1. Set c1 filter.
            c2 (str, optional): c1. Set c2 filter.
            timeframe (str, optional): timeframe. Set timeframe filter.
            line (str, optional): line. Set line filter.
        Returns:
            tuple: result_found(bool), result(dict)
        """
        try:
            filters = self.create_filters(
                c1=c1,
                c2=c2,
                line=line,
                timeframe=timeframe,
            )
            results = self.ts.mget(
                filters=filters,
                with_labels=True,
            )
            final = {'time': 0}
            for item in results:
                for key, values in item.items():
                    timestamp = values[1]
                    if timestamp and timestamp > final['time']:
                        final = {
                            'key': key,
                            'time': timestamp,
                            'value': values[2],
                            **values[0]
                        }
            if final['time'] > 0:
                final['time'] = int(final['time'] / 1000)
                return True, final
            return False, 'No record found'
        except Exception as e:
            return False, str(e)


    ########COMMON/PRIVATE METHODS############

    def find_last_nth_timestamp(self, filters:dict, n:int, from_timestamp:int=0):
        try:
            results = self.ts.mrevrange(
                from_time=from_timestamp,
                to_time='+',
                filters=filters,
                count=n,
                with_labels=True,
            )
            if len(results) > 1:
                raise Exception(f"Inadequate filters: Apply all labels on classifiers to filter out data properly")
            if len(results) < 1:
                raise Exception(f"Could not locate data based on provided filters")
            results = list(results[0].values())[0][1]
            pivot_ts = results[-1][0]
            return True, len(results), pivot_ts
        except Exception as ex:
            return False, False, str(ex)


    def query_index(self, c1:str=None, c2:str=None, line:str=None, timeframe:str=None, return_key_names:bool=False):
        """query the key index
        Args:
            c1 (str, optional): add c1 filter
            c2 (str, optional): add c2 filter
            line (str, optional): add line filter
            timeframe (str, optional): add timeframe filter
            return_key_names (bool, optional): return key names instead of values
        Returns:
            tuple: success(bool), mixed
        """
        output = {
            'c1': set(),
            'c2': set(),
            'timeframe': set(),
            'line': set(),
        }
        try:
            filters = self.create_filters(
                c1=c1,
                c2=c2,
                line=line,
                timeframe=timeframe,
            )
            keys = self.ts.queryindex(filters=filters)
            if return_key_names:
                return True, keys
            for key in keys:
                key_parts = self._extract_key_name(key)
                output['c1'].add(key_parts['c1'])
                output['c2'].add(key_parts['c2'])
                output['timeframe'].add(key_parts['timeframe'])
                output['line'].add(key_parts['line'])
            return True, output
        except Exception as e:
            return False, str(e)
    

    def _reset_line_values(self, c1:str, c2:str, line:str, value:float):
        """Set all values of a line to a given value
        Args:
            line (str): line
        """
        try:
            line = line.lower()
            # looping per timeframe
            for timeframe in self._timeframes.keys():
                # internally using the 1st line as ref line to loop all ts records
                ref_key = self._get_key_name(c1, c2, timeframe, self._lines[0])
                # use - + instead of timestamps #todo
                ref_data = self.ts.range(ref_key, 0, int(time.time())*1000)
                key = self._get_key_name(c1, c2, timeframe, line)
                for timestamp, vval in ref_data:
                    self.ts.add(key, timestamp, value)
            return True, "OK"
        except Exception as e:
            return False, str(e)


    def _adjust_from_timestamp(self, key_name:str, from_timestamp:int, extra_records:int=0, timestamp_minimum_boundary:int=None):
        result = self.ts.revrange(key_name, 0, from_timestamp, count=extra_records+1)
        if(len(result) == 0):
            return 0
        return result[-1][0]
        # #todo TIMESTAMP MINIMUM BOUNDARY


    def stats(self, c1:str, c2:str, timeframe:str, line:str=None):
        c1 = c1.lower()
        c2 = c2.lower()
        line = line.lower() if line else self._lines[0]
        timeframe = timeframe.lower()
        key = self._get_key_name(c1, c2, timeframe, line)
        if not self.client.exists(key):
                raise Exception(f'{key} does not exist')
        return self.ts.info(key)


    def map_exists(self, c1:str, c2:str):
        """Test wether ts map already exist or not.
        Args:
            c1 (str): classifier 1
            c2 (str): classifier 2
        Returns:
            bool: result
        """
        return self.client.exists(self._get_test_key_name(c1.lower(), c2.lower()))


    ######## CLASS & STATIC METHODS ################################

    @classmethod
    def create_filters(cls, **kwargs):
        filters = [f'tl={cls._name}']
        for filter_name, filter_value in kwargs.items():
            if filter_value is not None:
                filters.append(f"{filter_name}={filter_value}")
        return filters
    

    @staticmethod
    def _add_classifier_label(labels:dict, classifier:str, value:Union[str, dict]):
        if type(value) is str:
            labels[classifier] = value.lower()
        elif type(value) is dict:
            labels.update(value)


    @classmethod
    def _get_key_name(cls, c1, c2, timeframe, line):
        return f'{cls._name}:{c1}:{c2}:{timeframe}:{line}'

    
    @classmethod
    def _get_test_key_name(cls, c1, c2, line=None):
        timeframe = list(cls._timeframes.keys())[0]
        line = cls._lines[0] if not line else line
        return f'{cls._name}:{c1}:{c2}:{timeframe}:{line}'


    @classmethod
    def _get_timeframe_at_position(cls, position:int=0):
        return list(cls._timeframes.keys())[position]


    @classmethod
    def _prepare_read_data_basic(cls, data, line_order=None, return_as:str='list'):
        """DEPRECATED, WILL BE REMOVED
        """
        try:
            line_order = line_order or cls._lines
            if not data:
                return True, pd.DataFrame([], columns=['time', *line_order]) if return_as == 'df' else []
            data_lines = dict()
            for i in data:
                for k, v in i.items():
                    line = v[0]['line']
                    if line in data_lines:
                        raise Exception(f"Inadequate filters: Multiple lines with the same name are returned. This generally happens when label filters are used as classifiers and labels are not enough. Please add all labels to filter out data properly.")
                    data_lines[line] = v[1]
            if return_as == 'raw':
                return True, data_lines
            df:pd.DataFrame = None
            for line in line_order:
                if not line in data_lines:
                    raise Exception(f"Missing data for the line `{line}`")
                if df is None:
                    df = pd.DataFrame(data_lines[line], columns=['time', line])
                else:
                    df = pd.merge(df, pd.DataFrame(data_lines[line], columns=['time', line]), on='time')
            df['time'] = (df['time']/1000).astype(int)
            df.set_index('time', inplace=True)
            return (True, df) if return_as=="df" else (True, df.values.tolist())
        except Exception as e:
            return False, str(e)


    @classmethod
    def _prepare_read_data(cls, data, line_order=None, allow_multiple:bool=False, return_as:str='list'):
        try:
            line_order = line_order or cls._lines
            if not data:
                return True, pd.DataFrame([], columns=['time', *line_order]) if return_as == 'df' else []
            data_sets = dict()
            for d in data:
                for key, val in d.items():
                    labels:dict = val[0]
                    line = labels.pop('line')
                    data_points = val[1]
                    sig = f"{labels['c1']}_{labels['c2']}"
                    data_sets.setdefault(sig, {'count': 0, 'labels': labels, 'lines': {}})
                    data_sets[sig]['lines'][line] = data_points
                    data_sets[sig]['count'] += 1
                    if not allow_multiple and len(data_sets) > 1:
                        raise Exception(f"Inadequate filters: Multiple lines with the same name are returned. This generally happens when label filters are used as classifiers and labels are not enough. Please add all labels to filter out data properly or turn on the `allow_multiple` option")
            if return_as == 'raw':
                return True, data_sets
            # mergins lines
            for data_name, lines_data in data_sets.items():
                if(lines_data['count'] != len(cls._lines)):
                    raise Exception(f"Lines mismatch: {lines_data['count']} lines of data returned while {len(cls._lines)} lines are expected")
                data_sets[data_name]['df']:pd.DataFrame = None
                for line in line_order:
                    if not line in lines_data['lines']:
                        raise Exception(f"Missing data for the line `{line}`")
                    if data_sets[data_name]['df'] is None:
                        data_sets[data_name]['df'] = pd.DataFrame(lines_data['lines'][line], columns=['time', line])
                    else:
                        data_sets[data_name]['df'] = pd.merge(data_sets[data_name]['df'], pd.DataFrame(lines_data['lines'][line], columns=['time', line]), on='time')
            if 'sets' in return_as:
                output = []
                for s in data_sets.keys():
                    data_sets[s]['df']['time'] = (data_sets[s]['df']['time']/1000).astype(int)
                    if return_as == 'sets-df':
                        output.append((data_sets[s]['labels'], data_sets[s]['df']))
                    elif return_as == 'sets-dict':
                        output.append((data_sets[s]['labels'], data_sets[s]['df'].to_dict('list')))
                    else:
                        output.append((data_sets[s]['labels'], data_sets[s]['df'].values.tolist()))
                return True, output
            # concating data sets
            df:pd.DataFrame = None
            for name, dataset in data_sets.items():
                if df is None:
                    df = dataset['df']
                else:
                    df = pd.concat([df, dataset['df']])
            # finalizing output
            df['time'] = (df['time']/1000).astype(int)
            if len(data_sets) > 1:
                df.sort_values(by='time', inplace=True, ignore_index=True)
            # df.set_index('time', inplace=True) # the index column will not be included when converted to list; no need to set time as index
            if return_as == 'df':
                return True, df
            elif return_as == 'dict':
                return True, df.to_dict('list')
            return True, df.values.tolist()
        except Exception as e:
            return False, str(e)


    @staticmethod
    def get_read_length(data, data_type:str):
        try:
            if data_type == "raw":
                l = data[list(data.keys())[0]]['lines']
                return len(l[list(l.keys())[0]])
            elif 'sets' in data_type:
                if data_type == 'sets-dict':
                    return len(data[0][1]['time'])
                return len(data[0][1])
            elif data_type == 'dict':
                return len(data['time'])
            return len(data)
        except Exception as ex:
            return -1


    @staticmethod
    def _extract_key_name(key:str):
        match = re.match(r'(?P<tl>\w+):(?P<c1>\w+):(?P<c2>\w+):(?P<timeframe>\w+):(?P<line>\w+)', key)
        if match:
            return match.groupdict()
        return False


    @staticmethod
    def _get_timeframe_seconds(timeframe:str):
        if timeframe == 'raw':
            return 60
        unit_secs = 0
        match = re.match(r'(\d+)(\w+)', timeframe)
        numeric = int(match.group(1))
        unit = match.group(2)
        if unit == 'm':
            unit_secs = 60
        elif unit == 'h':
            unit_secs = 3600
        else:
            unit_secs = 3600 * 24
        return unit_secs * numeric
    

    @staticmethod
    def get_optimized_from_timestamp(timeframe:str, record_count:int):
        days = 1
        if timeframe in ['raw', '1m']:
            days = math.ceil(record_count / 24) * 2
        elif timeframe == '1h':
            days = math.ceil(record_count / 12) * 2
        elif timeframe == '1d':
            days = record_count * 2
        days = max(days, 5)
        initial_date = datetime.datetime.now()
        new_date = initial_date - datetime.timedelta(days=days)
        return int(new_date.timestamp()) * 1000



