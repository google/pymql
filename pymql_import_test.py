#!/usr/bin/python2.4
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Import unittest for pymql"""

__author__ = 'rtp@google.com (Tyler Pirtle)'

import google3
from google3.testing.pybase import googletest


class PymqlImportTest(googletest.TestCase):

  def canImport(self):
    import pymql

  def canInit(self):
    import pymql
    mql = pymql.MQLService(graphd_addrs=['localhost:8100'])

  def emqlCanImport(self):
    import pymql.emql.emql


if __name__ == '__main__':
  googletest.main()
