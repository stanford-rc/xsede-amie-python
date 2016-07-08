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


from setuptools import setup, find_packages

VERSION = '0.1.0'

setup(name='pyamie',
      version=VERSION,
      package_dir={'': 'lib'},
      packages=find_packages('lib'),
      author='Stephane Thiell',
      author_email='sthiell@stanford.edu',
      license='GNU Lesser General Public License v3 or later (LGPLv3+)',
      url='https://github.com/stanford-rc/xsede-amie-python/',
      platforms=['GNU/Linux'],
      keywords=['XSEDE', 'AMIE'],
      description='AMIE database Python abstraction layer library',
     )
