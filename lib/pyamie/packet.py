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
AMIE Packet

This module defines the AMIE packet class and handles packet processing.
"""

__author__ = 'sthiell@stanford.edu (Stephane Thiell)'

import logging

from pyamie.packet_data import PacketData


LOGGER = logging.getLogger(__name__)

PACKET_REPLY_TYPE_NAME = {
    'request_project_create': 'notify_project_create',
    'notify_project_create': 'data_project_create',
    'data_project_create': 'inform_transaction_complete',
    'request_account_create': 'notify_account_create',
    'notify_account_create': 'data_account_create',
    'data_account_create': 'inform_transaction_complete',
    'request_project_inactivate': 'notify_project_inactivate',
    'notify_project_inactivate': 'inform_transaction_complete',
    'request_project_reactivate': 'notify_project_reactivate',
    'notify_project_reactivate': 'inform_transaction_complete',
    'request_account_inactivate': 'notify_account_inactivate',
    'notify_account_inactivate': 'inform_transaction_complete',
    'request_account_reactivate': 'notify_account_reactivate',
    'notify_account_reactivate': 'inform_transaction_complete',
    'request_user_modify': 'inform_transaction_complete',
    'inform_transaction_complete': None}


class PacketIgnoredException(Exception):
    """Raise this exception to explicitely ignore an AMIE packet"""

class PacketHandler(object):
    """Base class for packet handler"""

class Packet(object):

    def __init__(self, record, trans):
        """Initalize packet"""
        self.packet_rec_id = record['packet_rec_id']
        self.packet_id = record['packet_id']
        self.type_name = record['type_name'] # eg. request_project_create
        self.state_name = record['state_name']
        self.outgoing_flag = record['outgoing_flag']
        self.trans = trans

        with trans.conn.cursor() as cur:
            cmd = """select tag, subtag, seq, value from data_tbl
                     where packet_rec_id = %s order by seq"""
            cur.execute(cmd, (self.packet_rec_id,))
            self.data = PacketData(cur.fetchall())
            LOGGER.info("incoming packet %s with data %s", self.type_name,
                        str(self.data))

        # set packet handler
        self.handler = getattr(trans.amietask.packet_handler, self.type_name)

        # instantiate response dict
        self.response_data = PacketData()

    def __str__(self):
        return "%s %s %s %s %s %s" % (self.__class__.__name__,
                                      self.packet_rec_id, self.type_name,
                                      self.state_name, self.outgoing_flag,
                                      self.trans)

    def process(self):
        """Process packet"""
        LOGGER.info("Processing %s", self)

        try:
            LOGGER.debug("process: %s data_in=%s", self.type_name,
                         self.response_data)
            self.handler(self, self.data, self.response_data)
        except PacketIgnoredException:
            LOGGER.info("packet %s ignored", self.type_name)
            return

        LOGGER.info("packet %s handled", self.type_name)

        type_name = PACKET_REPLY_TYPE_NAME[self.type_name]
        reply_type_name = PACKET_REPLY_TYPE_NAME[type_name]

        LOGGER.info("reply: %s (data %s)", reply_type_name,
                    str(self.response_data))

        self.trans._create_packet(type_name, self.response_data,
                                  reply_type_name, self.packet_rec_id)
