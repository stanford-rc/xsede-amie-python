#!/bin/bash
#
# Crontab example script for AMIE packet processing
# Run every half hour
# 0,30 * * * * /home/amie/xsede/amie_example_cron.sh
#

export PYTHONPATH=$HOME/xsede/pyamie/lib
export PATH=$PATH:/opt/local/slurm/default/bin

/home/amie/bin/amie 1>>/home/amie/xsede/logs/amie.log 2>&1

/usr/bin/python /home/amie/xsede/amie_example_proc.py --config-file /home/amie/xsede/amie_example.yaml -o /home/amie/xsede/logs/amie_example_proc.log
