#!/usr/bin/env python3
#
#
#
#
# Kim Brugger (03 Apr 2019), contact: kim@brugger.dk

import sys
import os
import pprint
pp = pprint.PrettyPrinter(indent=4)
import argparse

import datetime


import kbr.db_utils as db_utils
import kbr.args_utils as args_utils
import kbr.config_utils as config_utils
import kbr.string_utils as string_utils
import kbr.file_utils as file_utils

db = None

def disk_usage():
    pass

def get_job_stats(day:int=None, hour:int=None):
    q = "SELECT state, count(*) from job "

    timeframe = ""

    if hour is not None:
        q += "WHERE update_time > now() - INTERVAL '{} hour' ".format(hour)
        timeframe = "timeframe=hour,size={},".format( hour )
    elif day is not None:
        q += "WHERE update_time > now() - INTERVAL '{} day' ".format(day)
        timeframe = "timeframe=day,size={},".format( day )

    q += "GROUP BY state"

    total = 0

    for entry in db.get_as_dict( q ):
        print("jobs,{}state={}\tcount={}".format(timeframe,entry['state'], entry['count']))
        total += int(entry['count'])

    if total > 0:
        print("jobs,{}state={}\tcount={}".format(timeframe,"total", total))

def stats_jobs(args):
    if len( args.command) == 0:
        get_job_stats()
        get_job_stats(day=1)
        get_job_stats(hour=1)
        return

    commands = ['total', 'day', 'hour', 'help']
    command = args.command.pop( 0 )
    args_utils.valid_command(command, commands)

    if command == 'total':
        get_job_stats()
    elif command == 'day':
        offset = args_utils.get_or_default(args.command, 1)
        get_job_stats(day=offset)
    elif command == 'hour':
        offset = args_utils.get_or_default(args.command, 1)
        get_job_stats(hour=offset)
    else:
        print("stats job sub-commands: {}".format(", ".join(commands)))
        sys.exit()


def get_user_stats(year:int=None,month:str=None):

    # default we show for the current month
    today = datetime.datetime.today()
    datem = datetime.datetime(today.year, today.month, 1)
    where = "WHERE date_trunc('month', job.create_time AT TIME ZONE 'UTC') = '{}-{}-01'::date ".format( today.year, today.month )

    # or show for a full year
    if year is not None:
        where="WHERE date_trunc('year', job.create_time AT TIME ZONE 'UTC') = '{}-01-01'::date ".format( year)
    # or a given month (year-month)
    elif month is not None:
        where="WHERE date_trunc('month', job.create_time AT TIME ZONE 'UTC') = '{}-01'::date ".format( month )

    q  = "SELECT date_trunc('month', job.create_time AT TIME ZONE 'UTC')::date as month, "
    q += "count(distinct user_id) AS count FROM job {}".format( where )
    q += "GROUP BY month ORDER BY month DESC"

#    print( q )

    for entry in db.get_as_dict( q ):
        print("active-users,timeframe=month,size=1,date={}\tcount={}".format(entry['month'], entry['count']))



def stats_users(args):
    if len( args.command) == 0:
        get_user_stats()
        return

    commands = ['year', 'month', 'help']
    command = args.command.pop( 0 )
    args_utils.valid_command(command, commands)

    if command == 'year':
        args_utils.count(1, len(args.command), msg="stats year require a year")
        get_user_stats(year=args.command.pop(0))
    elif command == 'month':
        args_utils.count(1, len(args.command), msg="stats month require a year-month")
        get_user_stats(month=args.command.pop(0))
    else:
        print("stats users sub-commands: {}".format(", ".join(commands)))
        sys.exit()



def get_upload_stats(month:int=None,day:int=None,hour:int=None):

    q  = "SELECT coalesce(sum(dataset.total_size), 0) as size FROM job "
    q += "LEFT JOIN job_to_output_dataset ON job.id = job_to_output_dataset.job_id "
    q += "LEFT JOIN history_dataset_association ON "
    q += " job_to_output_dataset.dataset_id = history_dataset_association.id "
    q += "LEFT JOIN dataset ON history_dataset_association.dataset_id = dataset.id "
    q += "WHERE job.tool_id = 'upload1'"

    timeframe = "hour"
    size      = 1
    if month is not None:
        timeframe = "month"
        size = month
    elif day is not None:
        timeframe = "day"
        size = day
    elif hour is not None:
        timeframe = "hour"
        size = hour

    q += "AND job.create_time AT TIME ZONE 'UTC' > (now() - '{} {}s'::INTERVAL)".format(size, timeframe)

#    print( q )

    entry = db.get_as_dict( q )
    count = 0
    if entry is not None:
        count = float(entry[0]['size'])
        count /= 1e9

    print("data-upload,timeframe={},size=1,format=GB count={}".format( timeframe, count))


def stats_data(args):
    if len( args.command) == 0:
        get_upload_stats()
        return

    commands = ['month', 'day', 'hour','help']
    command = args.command.pop( 0 )
    args_utils.valid_command(command, commands)

    size = offset = args_utils.get_or_default(args.command, 1)
    if command == 'month':
        get_upload_stats(month=size)
    elif command == 'day':
        get_upload_stats(day=size)
    elif command == 'hour':
        get_upload_stats(hour=size)
    else:
        print("stats users sub-commands: {}".format(", ".join(commands)))
        sys.exit()


def stats_command(args) -> None:

    if len( args.command) == 0:
        stats_users(args)
        stats_data(args)
        stats_jobs(args)
        return

    commands = ['users', 'jobs', 'data', 'help']

    command = args.command.pop( 0 )
    args_utils.valid_command(command, commands)

    if command == 'users':
        stats_users(args)
    elif command == 'jobs':
        stats_jobs(args)
    elif command == 'data':
        stats_data(args)
    else:
        print("stat sub-commands: {}".format(", ".join(commands)))
        sys.exit()


def write_config_file():
    conf = '{"db_url": "postgresql://<USERNAME>:<PASSWORD>@<HOSTNAME>:<PORT>/<DATABASE>"}'

    file_utils.write("galaxy.json", conf)


def main():

    parser = argparse.ArgumentParser(description='cbu galaxy admin tool')
    parser.add_argument('-c', '--config', default="galaxy.json", help="config file")

    commands = ["stats", "bootstrap"]
    parser.add_argument('command', nargs='+', help="{}".format(",".join(commands)))

    args = parser.parse_args()


    command = args.command.pop(0)
    if command not in commands:
        parser.print_help()

    if command == 'bootstrap':
        write_config_file()
        sys.exit()


    config = config_utils.readin_config_file( args.config )
    global db
    db = db_utils.DB( config.db_url )

    if command == 'stats':
        stats_command(args)
    elif command == 'bootstrap':
        write_config_file()
    else:
        print("Unknown command: {} are allowed.".format(string_utils.comma_sep( commands )))
        sys.exit( 1 )





if __name__ == "__main__":
#    get_user_stats()
#    get_upload_stats()
    main()

