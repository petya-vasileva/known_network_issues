import re
import socket
import ipaddress
import csv
import multiprocessing as mp
from functools import partial
from contextlib import contextmanager
from datetime import datetime, timedelta
import dateutil.relativedelta
import time
import requests 
import os
import pandas as pd
import dask.dataframe as dd
import numpy as np
import functools

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import getpass

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from functools import wraps

from parquet import Parquet



INDICES = ['ps_packetloss', 'ps_owd', 'ps_retransmits', 'ps_throughput', 'ps_trace']

user, passwd, mapboxtoken = None, None, None
with open("creds.key") as f:
    user = f.readline().strip()
    passwd = f.readline().strip()
    mapboxtoken = f.readline().strip()

def ConnectES():
    global user, passwd
    credentials = (user, passwd)

    try:
        es = Elasticsearch([{'host': 'atlas-kibana.mwt2.org', 'port': 9200, 'scheme': 'https'}],
                                timeout=240, http_auth=credentials, max_retries=10)
        print('Success' if es.ping()==True else 'Fail')
        return es
    except Exception as error:
        print (">>>>>> Elasticsearch Client Error:", error)

es = ConnectES()


def readDF(dftype, location):
    pq = Parquet()
    dd = pq.readSequenceOfFiles(location, dftype)
    dd = dd.reset_index(drop=True)

    if dftype == 'ps_throughput':
        dd['throughput_Mb'] = round(dd['throughput']*1e-6)

    dd['dt'] = pd.to_datetime(dd['timestamp'], unit='ms')
    dd['src_site'] = dd['src_site'].str.upper()
    dd['dest_site'] = dd['dest_site'].str.upper()

    return dd


def timer(func):
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start_time = time.perf_counter()
        value = func(*args, **kwargs)
        end_time = time.perf_counter()
        run_time = end_time - start_time
        print(f"Finished {func.__name__!r} in {run_time:.4f} secs")
        return value
    return wrapper_timer


def defaultTimeRange(days=3):
    now = roundTime(datetime.now()) # 1 hour
    defaultEnd = datetime.strftime(now, '%Y-%m-%d %H:%M')
    defaultStart = datetime.strftime(now - timedelta(days), '%Y-%m-%d %H:%M')
    return [defaultStart, defaultEnd]


def roundTime(dt=None, round_to=60*60):
    if dt == None:
        dt = datetime.utcnow()
    seconds = (dt - dt.min).seconds
    rounding = (seconds+round_to/2) // round_to * round_to
    return dt + timedelta(0,rounding-seconds,-dt.microsecond)


# Expected values: time in miliseconds or string (%Y-%m-%d %H:%M')
def FindPeriodDiff(dateFrom, dateTo):
    if (isinstance(dateFrom, int) and isinstance(dateTo, int)):
        d1 = datetime.fromtimestamp(dateTo/1000)
        d2 = datetime.fromtimestamp(dateFrom/1000)
        time_delta = (d1 - d2)
    else:
        fmt = '%Y-%m-%d %H:%M'
        d1 = datetime.strptime(dateFrom, fmt)
        d2 = datetime.strptime(dateTo, fmt)
        time_delta = d2-d1
    return time_delta


def GetTimeRanges(dateFrom, dateTo, intv=1):
    diff = FindPeriodDiff(dateFrom, dateTo) / intv
    t_format = "%Y-%m-%d %H:%M"
    tl = []
    for i in range(intv+1):
        if (isinstance(dateFrom, int)):
            t = (datetime.fromtimestamp(dateFrom/1000) + diff * i)
            tl.append(int(time.mktime(t.timetuple())*1000))
        else:
            t = (datetime.strptime(dateFrom,t_format) + diff * i).strftime(t_format)
            tl.append(int(time.mktime(datetime.strptime(t, t_format).timetuple())*1000))

    return tl


def CalcMinutes4Period(dateFrom, dateTo):
    time_delta = FindPeriodDiff(dateFrom, dateTo)
    return (time_delta.days*24*60 + time_delta.seconds//60)


def MakeChunks(minutes):
    if minutes < 60:
        return 1
    else:
        return round(minutes / 60)


def getValueField(idx):
    if idx == 'ps_packetloss':
        return 'packet_loss'
    elif idx == 'ps_owd':
        return 'delay_mean'
    elif idx == 'ps_retransmits':
        return 'retransmits'
    elif idx == 'ps_throughput':
        return 'throughput'

    return None