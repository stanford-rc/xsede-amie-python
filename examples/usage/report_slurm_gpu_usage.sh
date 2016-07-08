#!/bin/bash

export PYTHONPATH=$HOME/xsede/pyamie/lib
export PATH=$PATH:/opt/local/slurm/default/bin

/usr/bin/python /home/amie/xsede/usage/report_slurm_gpu_usage.py --start 1hours --db-file /home/amie/xsede/usage/jobs.db

/home/amie/bin/amie 1>>/home/amie/xsede/logs/amie.log 2>&1
