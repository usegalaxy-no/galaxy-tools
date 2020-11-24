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

def write_points(url, database, user, password):

    global points
    url = f"{url}/write?db={database}"

    if user is not None and password is not None:
        try:
            res = requests.post(url, data="\n".join(points), auth=(user, password))
            res.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print (e.response.text)








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



def make_range(start:str, end:str, interval:str):


    range = date_range(start, end, interval)
    size, sort = interval_type(interval)
    timeframe = "timeframe={},size={}".format(sort,  size)

    return range, timeframe


def workflow_stats(start:str, end:str, interval:str):

    (timerange, timeframe) = make_range(start, end, interval)


    for i in range(0, len(timerange)-1):
        sql = f"select count(*) AS count  from workflow_invocation WHERE create_time AT TIME ZONE 'UTC' >= '{timerange[i]}' and create_time AT TIME ZONE 'UTC' < '{timerange[i+1]}'"
        entries = DB.get_as_dict(sql)
        for entry in entries:
            if entry["count"] == None:
                entry['count'] = 0
            timerange[i] = unix_time_nano(timerange[i])
            points.append("workflows,{timeframe} count={count} {time}".format(timeframe=timeframe, count=entry['count'], time=timerange[i]))





def data_stats(start:str, end:str, interval):
    timerange, timeframe = make_range(start, end, interval)


    for i in range(0, len(timerange)-1):
        sql = f"SELECT sum(coalesce(dataset.total_size, dataset.file_size, 0)) AS size FROM dataset WHERE create_time AT TIME ZONE 'UTC' >= '{timerange[i]}' and create_time AT TIME ZONE 'UTC' < '{timerange[i+1]}'"
        for entry in DB.get_as_dict(sql):
            if entry["size"] == None:
                entry['size'] = 0
            timerange[i] = unix_time_nano(timerange[i])
            points.append("data_growth,{} size={} {time}".format(timeframe, entry["size"], time=timerange[i]))





def job_stats(start:str, end:str, interval):
    timerange, timeframe = make_range(start, end, interval)

    sql = "SELECT state, count(*) from job "

    for i in range(0, len(timerange)-1):
        sql = f"SELECT state, count(*) AS count from job WHERE create_time AT TIME ZONE 'UTC' >= '{timerange[i]}' and create_time AT TIME ZONE 'UTC' < '{timerange[i+1]}' group by state"
        timerange[i] = unix_time_nano(timerange[i])
        for entry in DB.get_as_dict(sql):
            points.append("jobs,{},state={} count={} {time}".format(timeframe, entry['state'], entry['count'], time=timerange[i]))




def user_stats(start:str, end:str, interval:str):
    timerange, timeframe = make_range(start, end, interval)


    for i in range(0, len(timerange)-1):
        sql = f"select count(distinct(user_id)) as count  from job WHERE create_time AT TIME ZONE 'UTC' >= '{timerange[i]}' and create_time AT TIME ZONE 'UTC' < '{timerange[i+1]}'"
        for entry in DB.get_as_dict(sql):
            if entry["count"] == None:
                entry['count'] = 0
            timerange[i] = unix_time_nano(timerange[i])
            points.append("galaxy-users,{timeframe} count={count} {time}".format(timeframe=timeframe, count=entry['count'], time=timerange[i]))






def main():
    parser = argparse.ArgumentParser(description='cbu galaxy admin tool')
    parser.add_argument('-c', '--config', default="galaxy.json", help="config file", required=True)
    parser.add_argument('-s', '--start', help="start time", required=True)
    parser.add_argument('-e', '--end',  help="end time", required=True)
    parser.add_argument('-i', '--interval', help="time interval", required=True)

    parser.add_argument('-U', '--url', help="time interval")
    parser.add_argument('-d', '--database', help="time interval")
    parser.add_argument('-u', '--user', help="time interval")
    parser.add_argument('-p', '--password', help="time interval")


    args = parser.parse_args()
#    workflow_stats(args.start, args.end, args.interval)

    config = config_utils.readin_config_file(args.config)
    global DB
    if "db_url" in config:
        DB = db_utils.DB(config.db_url)
    elif "galaxy" in config and "database_connection" in config['galaxy']:
        DB = db_utils.DB(config['galaxy']['database_connection'])

    workflow_stats(args.start, args.end, args.interval)
    user_stats(args.start, args.end, args.interval)
    data_stats(args.start, args.end, args.interval)
    job_stats(args.start, args.end, args.interval)

    if args.url is not None:
        write_points( args.url, args.database, args.user, args.password)
    else:
        for p in points:
            print( p )

if __name__ == "__main__":
    main()
