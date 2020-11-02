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

import kbr.args_utils as args_utils
import kbr.config_utils as config_utils
import kbr.db_utils as db_utils
import kbr.string_utils as string_utils

DB = None



def get_data_growth(month:int=None, day:int=None, hour:int=None):
    sql = "SELECT sum(coalesce(dataset.total_size, dataset.file_size, 0)) AS size FROM dataset  "

    timeframe = "timeframe=epoch,"


    if month is not None:
        sql += "WHERE update_time > now() - INTERVAL '{} month' ".format(month)
        timeframe = "timeframe=hour,size={},".format(month)
    elif hour is not None:
        sql += "WHERE update_time > now() - INTERVAL '{} hour' ".format(hour)
        timeframe = "timeframe=hour,size={},".format(hour)
    elif day is not None:
        sql += "WHERE update_time > now() - INTERVAL '{} day' ".format(day)
        timeframe = "timeframe=day,size={},".format(day)


    for entry in DB.get_as_dict(sql):
        print("data_growth,{}\tsize={}".format(timeframe, entry["size"]))


def stats_growth(args):
    if len(args.command) == 0:
        get_data_growth()
        get_data_growth(month=1)
        get_data_growth(day=1)
        get_data_growth(hour=1)
        return

    commands = ['total', 'month', 'day', 'hour', 'help']
    command = args.command.pop(0)
    args_utils.valid_command(command, commands)

    if command == 'total':
        get_data_growth()
    elif command == 'month':
        offset = args_utils.get_or_default(args.command, 1)
        get_data_growth(month=offset)
    elif command == 'day':
        offset = args_utils.get_or_default(args.command, 1)
        get_data_growth(day=offset)
    elif command == 'hour':
        offset = args_utils.get_or_default(args.command, 1)
        get_data_growth(hour=offset)
    else:
        print("stats data_growth sub-commands: {}".format(", ".join(commands)))
        sys.exit()



def get_job_stats(month: int= None, day: int = None, hour: int = None):
    sql = "SELECT state, count(*) from job "

    timeframe = "timeframe=epoch,"


    if month is not None:
        sql += "WHERE update_time > now() - INTERVAL '{} month' ".format(month)
        timeframe = "timeframe=hour,size={},".format(month)
    elif hour is not None:
        sql += "WHERE update_time > now() - INTERVAL '{} hour' ".format(hour)
        timeframe = "timeframe=hour,size={},".format(hour)
    elif day is not None:
        sql += "WHERE update_time > now() - INTERVAL '{} day' ".format(day)
        timeframe = "timeframe=day,size={},".format(day)

    sql += "GROUP BY state"

    total = 0

    for entry in DB.get_as_dict(sql):
        print("jobs,{}state={}\tcount={}".format(timeframe, entry['state'], entry['count']))
        total += int(entry['count'])

    if total > 0:
        print("jobs,{}state={}\tcount={}".format(timeframe, "total", total))


def stats_jobs(args):
    if len(args.command) == 0:
        get_job_stats()
        get_job_stats(day=1)
        get_job_stats(hour=1)
        get_job_stats(month=1)
        return

    commands = ['total', 'month', 'day', 'hour', 'help']
    command = args.command.pop(0)
    args_utils.valid_command(command, commands)

    if command == 'total':
        get_job_stats()
    elif command == 'month':
        offset = args_utils.get_or_default(args.command, 1)
        get_job_stats(month=offset)
    elif command == 'day':
        offset = args_utils.get_or_default(args.command, 1)
        get_job_stats(day=offset)
    elif command == 'hour':
        offset = args_utils.get_or_default(args.command, 1)
        get_job_stats(hour=offset)
    else:
        print("stats job sub-commands: {}".format(", ".join(commands)))
        sys.exit()


def get_user_stats(year: int = None, month: str = None):
    # default we show for the current month
    today = datetime.datetime.today()
    where = "WHERE date_trunc('month', job.create_time AT TIME ZONE 'UTC') = '{}-{}-01'::date ".format(today.year,
                                                                                                       today.month)

    # or show for a full year
    if year is not None:
        where = "WHERE date_trunc('year', job.create_time AT TIME ZONE 'UTC') = '{}-01-01'::date ".format(year)
    # or a given month (year-month)
    elif month is not None:
        where = "WHERE date_trunc('month', job.create_time AT TIME ZONE 'UTC') = '{}-01'::date ".format(month)

    sql = "SELECT date_trunc('month', job.create_time AT TIME ZONE 'UTC')::date as month, "
    sql += "count(distinct user_id) AS count FROM job {}".format(where)
    sql += "GROUP BY month ORDER BY month DESC"

    #    print( q )

    for entry in DB.get_as_dict(sql):
        print("active-users,timeframe=month,size=1,date={}\tcount={}".format(entry['month'], entry['count']))


def stats_users(args):
    if len(args.command) == 0:
        get_user_stats()
        return

    commands = ['year', 'month', 'help']
    command = args.command.pop(0)
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


def get_queue_stats():
    sql = "SELECT tool_id, state, count(*) as count FROM job "
    sql += "WHERE state in ('queued', 'running') "
    sql += "GROUP BY tool_id, state ORDER BY count desc"

    for entry in DB.get_as_dict(sql):
        entry['tool_id'] = re.sub(r'^.*repos/', '', entry['tool_id'])
        print("queue,tool_id={},state={} count={}".format(entry['tool_id'], entry['state'], entry['count']))


def stats_queue(args):
    get_queue_stats()


def get_upload_stats(month: int = None, day: int = None, hour: int = None):
    sql = "SELECT coalesce(sum(dataset.total_size), 0) as size FROM job "
    sql += "LEFT JOIN job_to_output_dataset ON job.id = job_to_output_dataset.job_id "
    sql += "LEFT JOIN history_dataset_association ON "
    sql += " job_to_output_dataset.dataset_id = history_dataset_association.id "
    sql += "LEFT JOIN dataset ON history_dataset_association.dataset_id = dataset.id "
    sql += "WHERE job.tool_id = 'upload1'"

    timeframe = "hour"
    size = 1
    if month is not None:
        timeframe = "month"
        size = month
    elif day is not None:
        timeframe = "day"
        size = day
    elif hour is not None:
        timeframe = "hour"
        size = hour

    sql += "AND job.create_time AT TIME ZONE 'UTC' > (now() - '{} {}s'::INTERVAL)".format(size, timeframe)

    #    print( q )

    entry = DB.get_as_dict(sql)
    count = 0
    if entry is not None:
        count = float(entry[0]['size'])
        count /= 1e9

    print("data-upload,timeframe={},size=1,format=GB count={}".format(timeframe, count))


def stats_data(args):
    if len(args.command) == 0:
        get_upload_stats()
        return

    commands = ['month', 'day', 'hour', 'help']
    command = args.command.pop(0)
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
    if len(args.command) == 0:
        stats_users(args)
#        stats_data(args)
        stats_jobs(args)
        stats_queue(args)
        stats_growth(args)
        return

    commands = ['users', 'jobs', 'queue', 'data', 'growth', 'help']

    command = args.command.pop(0)
    args_utils.valid_command(command, commands)

    if command == 'users':
        stats_users(args)
    elif command == 'jobs':
        stats_jobs(args)
    elif command == 'data':
        stats_data(args)
    elif command == 'queue':
        stats_queue(args)
    elif command == 'growth':
        stats_growth(args)
    else:
        print("stat sub-commands: {}".format(", ".join(commands)))
        sys.exit()


def print_tick_entry(config_file):
    interpreter_path = sys.executable
    script_path = os.path.realpath(__file__)
    config_file = os.path.abspath(config_file)

    cmd = "{} {} -c {}".format(interpreter_path, script_path, config_file)
    entry = """[[inputs.exec]]
   commands = ['{cmd} stats']
   timeout='10s'
   data_format = 'influx'
   interval = '1m'
   name_prefix='galaxy_' 
    """
    entry = entry.format(cmd=cmd)

    print(entry)


def main():
    parser = argparse.ArgumentParser(description='cbu galaxy admin tool')
    parser.add_argument('-c', '--config', default="galaxy.json", help="config file")

    commands = ["stats", "tick-config"]
    parser.add_argument('command', nargs='+', help="{}".format(",".join(commands)))

    args = parser.parse_args()

    command = args.command.pop(0)
    if command not in commands:
        parser.print_help()
        sys.exit()

    if command == 'tick-config':
        print_tick_entry(args.config)
        sys.exit()

    config = config_utils.readin_config_file(args.config)
    global DB
    if "db_url" in config:
        DB = db_utils.DB(config.db_url)
    elif "galaxy" in config and "database_connection" in config['galaxy']:
        DB = db_utils.DB(config['galaxy']['database_connection'])

    if command == 'stats':
        stats_command(args)
    elif command == 'tick':
        print_tick_entry()
    else:
        print("Unknown command: {} are allowed.".format(string_utils.comma_sep(commands)))
        sys.exit(1)


if __name__ == "__main__":
    main()
