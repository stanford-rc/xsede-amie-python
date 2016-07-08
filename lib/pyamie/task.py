#!/usr/bin/python
#
# Copyright (C) 2016
#      The Board of Trustees of the Leland Stanford Junior University
#
# pyamie is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# In addition, as a special exception, the copyright holders give
# permission to link this program with the OpenSSL library (or with
# modified versions of OpenSSL that use the same license as OpenSSL),
# and distribute linked combinations including the two.
#
# You must obey the GNU Lesser General Public License in all respects for
# all of the code used other than OpenSSL.
#
# pyamie is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
# License for more details.


"""
AMIE Task

This module defines the AMIETask class that is used to process incoming
transaction or create new transaction using create_trans() method.

See also the provided example scripts.
"""

__author__ = 'sthiell@stanford.edu (Stephane Thiell)'

import logging

import psycopg2
from psycopg2.extras import DictCursor

from pyamie.trans import Trans


LOGGER = logging.getLogger(__name__)


class AMIETask(object):

    def __init__(self, dsn, packet_handler, timeout):
        self.dsn = dsn
        self.packet_handler = packet_handler
        self.timeout = timeout
        self.conn = psycopg2.connect(dsn, cursor_factory=DictCursor)

    def create_trans(self, local_site_name, remote_site_name):
        """Create new transaction"""
        with self.conn as db:
            with db.cursor() as cur:
                # Select incoming packets that are 'in-progress'
                cmd = """select coalesce(max(transaction_id),0)+1
                         from transaction_tbl"""
                cur.execute(cmd)
                trans_id = int(cur.fetchone()[0])
                state_name = 'in-progress'
                cmd = """insert into transaction_tbl
                         (originating_site_name, local_site_name,
                          remote_site_name, transaction_id, state_id)
                         select %s, %s, %s, %s, state_id
                         from state_des
                         where state_name = %s
                         returning trans_rec_id, originating_site_name,
                                   local_site_name, remote_site_name"""
                cur.execute(cmd, (local_site_name, local_site_name,
                                  remote_site_name, trans_id, state_name,))
                record = cur.fetchone()
                return Trans(record, state_name, self)

    def run(self, state='in-progress'):
        """run all transactions in specified state"""

        with self.conn as db:
            with db.cursor() as cur:
                LOGGER.info("Checking in-progress transactions")

                cmd = """select * from transaction_tbl trans
                         join state_des sd
                         on trans.state_id = sd.state_id
                         where sd.state_name = %s"""
                cur.execute(cmd, (state,))

                for record in cur:
                    trans_rec_id = record['trans_rec_id']
                    LOGGER.info("Found in-progress transaction: %s",
                                trans_rec_id)
                    # TODO: check transaction_depends_tbl
                    Trans(record, record['state_name'], self).run()
