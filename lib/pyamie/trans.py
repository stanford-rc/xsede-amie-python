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
AMIE Trans

This module defines the Trans class that represents an AMIE transaction.

A Trans object can either be run() to process incoming pending AMIE packets
associated with this transaction, or used to create new AMIE packet through
the create_packet() method.
"""

__author__ = 'sthiell@stanford.edu (Stephane Thiell)'

import logging

import psycopg2
from psycopg2.extras import DictCursor

from pyamie.packet import Packet


AMIE_VERSION = '1.0'
LOGGER = logging.getLogger(__name__)


class Trans(object):

    def __init__(self, record, state_name, amietask):
        """Initialize transaction object from DB record"""
        self.trans_rec_id = record['trans_rec_id']
        self.originating_site_name = record['originating_site_name']
        self.local_site_name = record['local_site_name']
        self.remote_site_name = record['remote_site_name']
        self.state_name = state_name
        self.amietask = amietask
        self.conn = psycopg2.connect(amietask.dsn, cursor_factory=DictCursor)

    def run(self):
        """Dispatch transaction"""
        with self.conn as db:
            with db.cursor() as cur:
                # Select incoming packets that are 'in-progress'
                cmd = """select p.packet_rec_id, p.packet_id, td.type_name,
                                sd.state_name, p.outgoing_flag
                         from packet_tbl p
                         left join type_des td on p.type_id = td.type_id
                         left join state_des sd on p.state_id = sd.state_id
                         where p.trans_rec_id = %s
                            and p.outgoing_flag = '0'
                            and sd.state_name = 'in-progress'"""
                cur.execute(cmd, (self.trans_rec_id,))
                for packet_rec in cur:
                    Packet(packet_rec, self).process()

    def _create_packet(self, type_name, response_data, reply_type_name=None,
                       orig_packet_rec_id=None):
        """Create a packet (low-level, within current context)"""
        with self.conn.cursor() as cur:
            cmd = """select coalesce(max(packet_id),0)+1 from packet_tbl
                     where trans_rec_id = %s"""
            cur.execute(cmd, (self.trans_rec_id,))
            packet_id = int(cur.fetchone()[0])
            cmd = """insert into packet_tbl
                     (trans_rec_id, packet_id, type_id, version, state_id,
                      outgoing_flag)
                     select %s, %s, td.type_id, %s, sd.state_id, 1
                     from type_des td, state_des sd
                     where td.type_name = %s
                     and sd.state_name = 'in-progress'
                     returning packet_rec_id"""
            cur.execute(cmd, (self.trans_rec_id, packet_id + 1, AMIE_VERSION,
                              type_name))

            new_packet_rec_id = cur.fetchone()[0]
            LOGGER.info("packet created '%s' rec_id=%s", type_name,
                        new_packet_rec_id)

            if not reply_type_name:
                LOGGER.info("not filling expected_reply_tbl for '%s' rec_id=%s",
                            type_name, new_packet_rec_id)
            else:
                cmd = """insert into expected_reply_tbl
                         (packet_rec_id, type_id, timeout)
                         select %s, type_id, %s
                         from type_des
                         where type_name = %s"""
                cur.execute(cmd, (new_packet_rec_id, self.amietask.timeout,
                                  reply_type_name))
                LOGGER.info("expected reply '%s' for rec_id=%s",
                            reply_type_name, new_packet_rec_id)

            # adding response data
            for tag, subtag, seq, value in response_data:
                cmd = """insert into data_tbl
                         (packet_rec_id, tag, subtag, seq, value)
                         values (%s, %s, %s, %s, %s)"""
                cur.execute(cmd, (new_packet_rec_id, tag, subtag, seq, value))

            if orig_packet_rec_id:
                # set origin packet completed if we're sending a reply packet
                cmd = """update packet_tbl
                         set state_id =
                            (select state_id
                             from state_des
                             where state_des.state_name = 'completed')
                         where packet_rec_id = %s;"""
                cur.execute(cmd, (orig_packet_rec_id,))

    def create_packet(self, type_name, response_data, reply_type_name=None):
        """Create an AMIE packet within this transaction"""
        with self.conn as db:
            self._create_packet(type_name, response_data, reply_type_name)
