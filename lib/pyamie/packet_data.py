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
AMIE PacketData

This module defines the PacketData class to represent data associated
to a AMIE Packet.
"""

__author__ = 'sthiell@stanford.edu (Stephane Thiell)'

from collections import defaultdict
import logging

LOGGER = logging.getLogger(__name__)


class PacketData(object):

    def __init__(self, records=list()):
        self._dict = defaultdict(list)
        for tag, subtag, seq, value in records:
            if seq > 0: # ensure seqs are in ascending order
                assert len(self._dict[(tag, subtag)]) == seq
            self._dict[(tag, subtag)].append(value)

    def copy(self):
        cpy = self.__class__()
        cpy._dict = self._dict.copy()
        return cpy

    __copy__ = copy

    def get(self, tag, subtag=None, seq=None):
        return self._dict.get((tag, subtag))

    def getone(self, tag, subtag=None, seq=None):
        val = self._dict.get((tag, subtag))
        if val:
            return val[0]
        return None

    def __getitem__(self, key):
        return self._dict[key, None][0]

    def __setitem__(self, key, value):
        self._dict[(key, None)].append(value)

    def append_record(self, tag, subtag, value):
        self._dict[(tag, subtag)].append(value)

    def __len__(self):
        return len(self._dict)

    def __str__(self):
        return "<%s %s>" % (self.__class__.__name__, str(self._dict))

    def __iter__(self):
        for (tag, subtag), vallist in self._dict.iteritems():
            for seq, value in enumerate(vallist):
                yield tag, subtag, seq, value
