#!/usr/bin/env python3
#
#
#
#
# Kim Brugger (03 Apr 2019), contact: kim@brugger.dk

import argparse
import datetime
import os
import re
import sys
import requests

import kbr.config_utils as config_utils
import kbr.db_utils as db_utils
import kbr.timedate_utils as timedate_utils

points = []
url    = None
db     = None
dbuser = None
dbpass = None


def write_points(data):



    global url, db, dbuser, dbpass
    wurl = f"{url}/write?db={db}"

    if url is None:
        print( data )
        return

    try:
        res = requests.post(wurl, data=data, auth=(dbuser, dbpass))
        res.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print (e.response.text)



class Timerange:

    """Iterator that counts upward forever."""

    def __init__(self, start:str, end:str, interval:str):
        self._start = timedate_utils.datestr_to_ts(start)
        self._end   = timedate_utils.datestr_to_ts(end)
        self._timeframe = timedate_utils.timedelta_to_sec( interval )

    def __iter__(self):
        return self

    def __next__(self):
        ts = self._start
        self._start = self._start + datetime.timedelta(seconds = self._timeframe)
        if ts >= self._end:
            raise StopIteration  # signals "the end"
        return ts



def unix_time_nano(dt):
    epoch = datetime.datetime.utcfromtimestamp(0)
    return int((dt - epoch).total_seconds() * 1000000000)


DB = None

def date_range(start:str, end:str, timeframe:str) -> []:
    start = timedate_utils.datestr_to_ts(start)
    end   = timedate_utils.datestr_to_ts(end)
    timeframe = timedate_utils.timedelta_to_sec( timeframe )

    res = [start]
    while True:
        start = start + datetime.timedelta(seconds = timeframe)
#        print( start )
        if start >= end:
            break
        res.append( start )

    return res



def interval_type(interval:str) -> (int, str):
    ''' 1m, 3h, 2d, 1w --> now - delta as epoc secs '''

    try:
        g = re.match(r'(\d+)([hdM])', interval)
        num, range = g.groups(0)
        if range == 'h':
            return num, "hour"
        elif range == 'd':
            return num, "day"
        elif range == 'M':
            return num, "month"
    except Exception as e:
        print(f"timerange {interval} is invalid valid examples: 1d 2h 1w 1M")
        sys.exit(1)



def make_timeframe(start:str, end:str, interval:str):


    size, sort = interval_type(interval)
    timeframe = f"timeframe={sort},size={size}"
    delta_time = f"{size} {sort}"
    return timeframe, delta_time


def workflow_stats(start:str, end:str, interval:str, resolution:str="30s"):

    timeframe, delta_time = make_timeframe(start, end, interval)


    for i in Timerange(start, end, resolution):
        sql = f"select count(*) AS count  from workflow_invocation WHERE create_time AT TIME ZONE 'UTC' < '{i}' and create_time AT TIME ZONE 'UTC' > timestamp '{i}' - INTERVAL '{delta_time}'"
        ts = unix_time_nano(i)
        for entry in DB.get_as_dict(sql):
            if entry["count"] == None or entry["count"] == 0:
                continue
            l = f"workflows,{timeframe} count={entry['count']} {ts}"
            print(l)
            write_points(l)





def data_stats(start:str, end:str, interval, resolution:str="30s"):
    timeframe, delta_time = make_timeframe(start, end, interval)


    for i in Timerange(start, end, resolution):
        sql = f"SELECT sum(coalesce(dataset.total_size, dataset.file_size, 0)) AS size FROM dataset WHERE create_time AT TIME ZONE 'UTC' < '{i}' and create_time AT TIME ZONE 'UTC' > timestamp '{i}' - INTERVAL '{delta_time}'"
        ts = unix_time_nano(i)
        for entry in DB.get_as_dict(sql):
            if entry["size"] == None or entry["size"] == 0:
                continue
            l = f"data_growth,{timeframe} size={entry['size']} {ts}"
            #print(l)
            write_points(l)





def job_stats(start:str, end:str, interval, resolution:str="30s"):
    timeframe, delta_time = make_timeframe(start, end, interval)

    for i in Timerange(start, end, resolution):
        sql = f"SELECT state, count(*) AS count from job WHERE create_time AT TIME ZONE 'UTC' < '{i}' and create_time AT TIME ZONE 'UTC' > timestamp '{i}' - INTERVAL '{delta_time}' group by state"
        ts = unix_time_nano(i)
        for entry in DB.get_as_dict(sql):
            l = f"jobs,{timeframe},state={entry['state']} count={entry['count']} {ts}"
           #print(l)
            write_points(l)




def user_stats(start:str, end:str, interval:str, resolution:str="30s"):
    timeframe, delta_time = make_timeframe(start, end, interval)


    for i in Timerange(start, end, resolution):
        sql = f"select count(distinct(user_id)) as count  from job WHERE create_time AT TIME ZONE 'UTC' < '{i}' and create_time AT TIME ZONE 'UTC' > timestamp '{i}' - INTERVAL '{delta_time}'"
        ts = unix_time_nano(i)
        for entry in DB.get_as_dict(sql):
            if entry["count"] == None or entry["count"] == 0:
                continue
            l = f"galaxy-users,{timeframe} count={entry['count']} {ts}"
            #print(l)
            write_points(l)


def jobs_total(start:str, end:str, interval:str, resolution:str="30s"):

    timeframe = "timeframe=epoch"

    for i in Timerange(start, end, resolution):
        sql = f"SELECT state, count(*) AS count from job WHERE create_time AT TIME ZONE 'UTC' < '{i}' group by state"
        ts = unix_time_nano(i)
        for entry in DB.get_as_dict(sql):
            if entry["count"] == None or entry["count"] == 0:
                continue
            l = f"jobs,{timeframe},state={entry['state']} count={entry['count']} {ts}"
            write_points(l)

def datagrowth_total(start:str, end:str, interval:str, resolution:str="30s"):
    timeframe = "timeframe=epoch"

    for i in Timerange(start, end, resolution):
        sql = f"SELECT sum(coalesce(dataset.total_size, dataset.file_size, 0)) AS size FROM dataset WHERE create_time AT TIME ZONE 'UTC' < '{i}'"
        ts = unix_time_nano(i)
        for entry in DB.get_as_dict(sql):
            if entry["size"] == None or entry["size"] == 0:
                continue
            l = f"data_growth,{timeframe} size={entry['size']} {ts}"
        write_points(l)


def workflow_total(start:str, end:str, interval:str, resolution:str="30s"):

    timeframe = "timeframe=epoch"


    for i in Timerange(start, end, resolution):
        sql = f"select count(*) AS count  from workflow_invocation WHERE create_time AT TIME ZONE 'UTC' < '{i}'"
        ts = unix_time_nano(i)
        for entry in DB.get_as_dict(sql):
            if entry["count"] == None or entry["count"] == 0:
                continue
            l = f"workflows,{timeframe} count={entry['count']} {ts}"
            write_points(l)

def nels_export_total(start:str, end:str, interval:str, resolution:str="30s"):


    for i in Timerange(start, end, resolution):
        sql = f"SELECT count(*), instance FROM nels_export_tracking  WHERE create_time AT TIME ZONE 'UTC' < '{i}' GROUP BY instance"
        ts = unix_time_nano(i)
        for entry in DB.get_as_dict(sql):
            l = f"nels-exports,instance={entry['instance']} count={entry['count']} {ts}"
            write_points(l)

def nels_import_total(start:str, end:str, interval:str, resolution:str="30s"):


    for i in Timerange(start, end, resolution):
        sql = f"SELECT count(*) FROM nels_import_tracking  WHERE create_time AT TIME ZONE 'UTC' < '{i}'"
        ts = unix_time_nano(i)
        for entry in DB.get_as_dict(sql):
            l = f"nels-imports count={entry['count']} {ts}"
            write_points(l)





def main():
    parser = argparse.ArgumentParser(description='cbu galaxy admin tool')
    parser.add_argument('-c', '--config', default="galaxy.json", help="config file", required=True)
    parser.add_argument('-s', '--start', help="start time", required=True)
    parser.add_argument('-e', '--end',  help="end time", required=True)
    parser.add_argument('-i', '--interval', help="time interval", required=True)
    parser.add_argument('-r', '--resolution', default='5m', help="time resolution to pull data for")

    parser.add_argument('-U', '--url', help="time interval")
    parser.add_argument('-d', '--database', help="time interval")
    parser.add_argument('-u', '--user', help="time interval")
    parser.add_argument('-p', '--password', help="time interval")

    args = parser.parse_args()
#    workflow_stats(args.start, args.end, args.interval)

#    sys.exit()

    global url, db, dbuser, dbpass
    url    = args.url
    db     = args.database
    dbuser = args.user
    dbpass = args.password



    config = config_utils.readin_config_file(args.config)
    global DB
    if "db_url" in config:
        DB = db_utils.DB(config.db_url)
    elif "galaxy" in config and "database_connection" in config['galaxy']:
        DB = db_utils.DB(config['galaxy']['database_connection'])

#    workflow_stats(args.start, args.end, args.interval, args.resolution)
#    user_stats(args.start, args.end, args.interval, args.resolution)
#    data_stats(args.start, args.end, args.interval, args.resolution)
#    job_stats(args.start, args.end, args.interval, args.resolution)

    jobs_total(args.start, args.end, args.interval, args.resolution)
    datagrowth_total(args.start, args.end, args.interval, args.resolution)
    workflow_total(args.start, args.end, args.interval, args.resolution)
    nels_export_total(args.start, args.end, args.interval, args.resolution)
    nels_import_total(args.start, args.end, args.interval, args.resolution)

if __name__ == "__main__":
    main()
