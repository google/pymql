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

# -*- coding: utf-8 -*-
#
"""Making sure new exceptions are called properly."""

__author__ = 'bobbyrullo@google.com (Bobby Rullo)'

import google3
import json
from pymql.mql import error
from pymql.test import mql_fixture

class MQLExceptionTest(mql_fixture.MQLTest):

  def setUp(self):
    super(MQLExceptionTest, self).setUp()
    self.env = {'user': '/user/mw_brendan'}


  def getFuzzKey(self, test_id):
    fuzz = self.getFuzz(test_id)
    fuzzKey = 'key_{0}'.format(fuzz[:fuzz.find('.')])
    return fuzzKey

  def newNode(self):
    query = json.dumps({
      "id": None,
      "create": "unconditional",
      })

    self.DoQuery(query, mqlwrite=True)
    new_id = self.mql_result.result['id']
    return new_id

  def testMQLValueAlreadyInUseError(self):
    key = self.getFuzzKey('alreadyInUse')

    new_id = self.newNode()

    query = {
        "id": new_id,
        "key": {
        "namespace": "/user/mw_brendan/default_domain",
        "value": key,
        "connect": "insert"
        }
      }

    self.DoQuery(json.dumps(query), mqlwrite=True)

    new_id = self.newNode()

    query['id'] = new_id

    self.DoQuery(json.dumps(query), mqlwrite=True,
                 exc_response = (
                     error.MQLValueAlreadyInUseError,
                     'This value is already in use. Please delete it first.'
                     ))


  def testMQLTooManyValuesForUniqueQuery(self):
    query = {
        "type": None,
        "id": "/en/sofia_coppola",
        "name": None
    }

    exc_response = (
        error.MQLTooManyValuesForUniqueQuery,
        "Unique query may have at most one result. Got 25"
    )
    self.DoQuery(json.dumps(query), exc_response=exc_response)


  def testMQLTooManyWrites(self):
    query = """
    {
      "create":"unconditional",
      "type":"/user/mw_brendan/default_domain/note",
      "name":"foobartoomanywrites",
      "id":null
    }
    """
    self.env = {
      'user': '/user/mw_brendan',
      'max_writes': {
        'limit': 0,
        'guid': '9202a8c04000641f80000000011af200'
      }
    }
    exc_response = (
        error.MQLWriteQuotaError,
        'Daily write limit of 0 was exceeded.'
    )
    self.DoQuery(query, mqlwrite=True, exc_response=exc_response)

if __name__ == '__main__':
  mql_fixture.main()
