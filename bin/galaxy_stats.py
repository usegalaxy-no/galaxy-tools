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

    print( q )

    total = 0


    for entry in db.get_as_dict( q ):
        print("jobs,{},state={}\tcount={}".format(timeframe,entry['state'], entry['count']))
        total += int(entry['count'])


    print("jobs,{},state={}\tcount={}".format(timeframe,"total", count))




def queue_overview(last_hours:int=None, last_days:int=None):
    q =  "SELECT  tool_id, tool_version, state, destination_id AS destination, job_runner_name, create_time, update_time FROM job "
    q += " WHERE state = 'running' OR state = 'queued' OR state = 'new' "

    if last_hours is not None:
        q += "AND update_time > now() - INTERVAL '{} hour' ".format(last_hours)
    elif last_days is not None:
        q += "AND update_time > now() - INTERVAL '{} day' ".format(last_days)

    print( q )


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



def stats_command(args) -> None:

    if len( args.command) == 0:
        default_stats(args)
        return

    commands = ['users', 'jobs', 'data', 'help']

    command = args.command.pop( 0 )
    args_utils.valid_command(command, commands)

    if command == 'users':
        stats_user(args)
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
    main()

