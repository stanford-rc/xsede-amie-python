#!/usr/bin/python

"""
Generate NPU AMIE packets to report XSEDE usage.

This script also uses a local SQLite3 DB file to record sent jobs to
avoid duplicates.
"""

import argparse
from datetime import datetime, timedelta
import csv
import logging
import os
import pwd
import pytz
import time
from StringIO import StringIO
import sqlite3
import sys

from ClusterShell.Task import task_self

from pyamie.task import AMIETask
from pyamie.packet_data import PacketData


TIMEZONE = 'US/Pacific'
TZ = pytz.timezone(TIMEZONE)
MACHINE_NAME = 'cluster.site.xsede'
AMIE_DSN = "dbname=amiedb user=amie"
AMIE_TIMEOUT = 30240
AMIE_LOCAL_SITE = 'ExampleSite'
AMIE_REMOTE_SITE = 'TGCDB'

ROOT_ACCOUNT = 'xsede'
SACCT_FIELDNAMES = ['Account', 'JobID', 'JobName', 'Partition', 'State',
                    'AllocNodes', 'AllocTRES', 'Submit', 'Start', 'End',
                    'Elapsed', 'User']

LOGGER = logging.getLogger(__name__)


def slurm_duration_to_seconds(slurm_duration):
    """Convert a slurm job duration to seconds"""
    days, elapsed = 0, str(slurm_duration)
    if '-' in elapsed:
        days, hms = elapsed.split('-')
        days = int(days)
    else:
        hms = elapsed
    t = time.strptime(hms, "%H:%M:%S")
    return days * 86400 + t.tm_hour * 3600 + t.tm_min * 60 + t.tm_sec

def slurm_parse_tres(tres):
    """Parse TRES string and create a dict of key/value TRES entries"""
    return dict(te.split('=') for te in tres.split(','))

def slurm_job_time(timestr):
    """Convert slurm job time format to datetime object with timezone"""
    dt_notz = datetime.strptime(timestr, "%Y-%m-%dT%H:%M:%S")
    return TZ.localize(dt_notz, is_dst=None).isoformat()

def get_username():
    """Get local username"""
    return pwd.getpwuid(os.getuid()).pw_name

def init_db(db_file):
    """Create a db file containing already reported job IDs"""
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    try:
        c.execute('CREATE TABLE jobs (id int, date text)')
        conn.commit()
    except sqlite3.OperationalError:
        pass
    return conn

def main():
    """Generate XStream NPU AMIE packets"""
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--start", required=True)
    parser.add_argument("--db-file", required=True)
    parser.add_argument("-d", "--debug", help="increase output verbosity",
                        action="store_true")
    pargs = parser.parse_args()

    if pargs.debug:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO

    logging.basicConfig(level=loglevel,
                        format='%(asctime)s %(name)s %(levelname)s %(message)s')

    # DB to keep track of sent packets
    conn = init_db(pargs.db_file)
    c = conn.cursor()

    amietask = AMIETask(AMIE_DSN, None, AMIE_TIMEOUT)

    # --start 1days, 2hours, etc. is supported
    if ':' not in pargs.start:
        unit = pargs.start.lstrip('0123456789')
        head = pargs.start[0:len(pargs.start) - len(unit)]
        deltadate = datetime.now() - timedelta(**{unit:int(head)})
        pargs.start = deltadate.strftime('%Y-%m-%dT%H:%M:%S')
        print >>sys.stderr, "Using start date: %s" % pargs.start

    task = task_self()
    task.run('for acct in $(sacctmgr -P list account parents=%s withassoc -n format=Account); do'
             '  sacct -a -n -s CA,CD,F,NF -P -X -A $acct --start %s --format %s; '
             'done;' % (ROOT_ACCOUNT, pargs.start, ','.join(SACCT_FIELDNAMES)),
             key='local', stderr=True)

    if task.max_retcode():
        print >>sys.stderr, str(task.key_error('local'))
        return task.max_retcode()

    reader = csv.DictReader(StringIO(str(task.key_buffer('local'))),
                            SACCT_FIELDNAMES,
                            delimiter='|',
                            quoting=csv.QUOTE_NONE)

    for row in reader:
        c.execute("SELECT * FROM jobs WHERE id=?", (row['JobID'],))
        if c.fetchone():
            continue

        # Forge NPU packet data
        npu_data = PacketData()
        npu_data['UsageType'] = 'normal'
        npu_data['ProjectID'] = row['Account'].upper()
        npu_data.append_record('JobIdentity', 'LocalJobID', row['JobID'])
        npu_data['JobName'] = row['JobName']
        npu_data['MachineName'] = MACHINE_NAME
        npu_data['NodeCount'] = row['AllocNodes']
        npu_data['Queue'] = row['Partition']

        now = datetime.now(TZ).replace(microsecond=0)
        npu_data.append_record('RecordIdentity', 'CreateTime',
                               now.isoformat())
        npu_data.append_record('RecordIdentity', 'RecordID',
                               "%s.%s@%s" % (row['JobID'], get_username(),
                                             MACHINE_NAME))
        # Job times
        npu_data['SubmitTime'] = slurm_job_time(row['Submit'])
        npu_data['StartTime'] = slurm_job_time(row['Start'])
        npu_data['EndTime'] = slurm_job_time(row['End'])

        seconds = slurm_duration_to_seconds(row['Elapsed'])
        npu_data['WallDuration'] = "PT%dS" % seconds

        # Get the number of allocated GPUs
        tres = slurm_parse_tres(row['AllocTRES'])
        if 'gres/gpu' not in tres:
            # Raise the following exception if you enforce GPU allocation for all jobs:
            #raise KeyError('gres/gpu not found in AllocTRES for job %s' % row['JobID'])
            npu_data['Processors'] = "1"
        else:
            npu_data['Processors'] = tres['gres/gpu']

        npu_data['UserLogin'] = row['User']

        # Charge is in SU or GPU.hour
        npu_data['Charge'] = "%f" % (float(seconds)/3600.0 *
                                     int(npu_data['Processors']))

        LOGGER.info("Creating NPU packet for project %s user %s jobid %s procs(gpus) %s charge %s",
                    npu_data['ProjectID'], npu_data['UserLogin'],
                    npu_data.getone('JobIdentity', 'LocalJobID'),
                    npu_data['Processors'], npu_data['Charge'])
        LOGGER.debug("Packet data: %s", npu_data)

        # Create a new AMIE transaction with a NPU packet
        trans = amietask.create_trans(AMIE_LOCAL_SITE, AMIE_REMOTE_SITE)
        c.execute("INSERT INTO jobs VALUES (?, ?)", (row['JobID'], now.isoformat()))
        trans.create_packet('notify_project_usage', npu_data, 'inform_transaction_complete')
        conn.commit()

    conn.close()


if __name__ == '__main__':
    main()
