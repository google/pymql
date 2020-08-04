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

#
"""Test misc. regressions."""

__author__ = 'bneutra@google.com (Brendan Neutra)'

import google3
from pymql.test import mql_fixture


class MQLTest(mql_fixture.MQLTest):

  def setUp(self):
    self.SetMockPath('data/regression_misc.yaml')
    super(MQLTest, self).setUp()
    self.env = {'as_of_time': '2009-10-01'}

  def testUtf8(self):
    """Regression test for issue 4970606."""

    query = u"""
      [{"name":"Beyonc\u00e9", "id": null}]
    """
    exp_response = u"""
    [
      {
        "id": "/en/beyonce",
        "name": "Beyonc\u00e9"
      },
      {
        "id": "/m/07ldnn6",
        "name": "Beyonc\u00e9"
      }
    ]
    """
    self.DoQuery(query.encode('utf-8'),
                 exp_response=exp_response.encode('utf-8'))

  def testCursor(self):
    """JIRA API-62 bug."""

    # not sure the bug is valid but I just wanted to capture
    # this style of query. the bug was that it timed out
    # but i can't reproduce that -brendan

    query = """
      [
        {
          "attribution": {
            "guid": null,
            "optional": true,
            "id": null
          },
          "reverse": null,
          "creator": {
            "guid": null,
            "optional": true,
            "id": null
          },
          "timestamp": null,
          "timestamp>=": "2012-01-01T20",
          "source": {
            "guid": null,
            "optional": true,
            "id": null
          },
          "valid": null,
          "limit": 1000,
          "master_property": null,
          "operation": null,
          "type": "/type/link",
          "target_value": null,
          "target": {
            "guid": null,
            "optional": true,
            "id": null
          }
        }
      ]
    """
    cursor = True
    while 1:
      self.env = {'cursor': cursor, 'as_of_time': '2012-01-02'}
      self.MQLQuerier(query)
      cursor = self.mql_result.cursor
      if cursor is False: break

  def testCursorComplex(self):
    """random hash ordering cursor bug b/8323666."""
    # TODO(bneutra) how to repro the bug, testing in process
    # doesn't tickle it.

    query = """
    [
      {
        "sort": "-timestamp",
        "type": "/type/link",
        "reverse": null,
        "creator": null,
        "timestamp": null,
        "source": {
          "mid": null
        },
        "a:creator": {
          "type": "/dataworld/provenance",
          "optional": "forbidden"
        },
        "valid": null,
        "limit": 10,
        "master_property": null,
        "operation": null,
        "target": {
          "mid": null
        },
        "target_value": null,
        "b:creator": {
          "usergroup": {
            "id|=": [
              "/freebase/bots",
              "/en/metaweb_staff",
              "/en/current_metaweb_staff"
            ],
            "optional": "forbidden"
          }
        }
      }
    ]
"""
    cursor = True
    i = 0
    while i < 30:
      i+=1
      self.env = {'cursor': cursor}
      self.MQLQuerier(query)
      self.assertEquals(len(self.mql_result.result), 10)
      # we should have a new cursor
      self.assertNotEquals(cursor, self.mql_result.cursor)
      cursor = self.mql_result.cursor
      # it should be a cursor
      self.assertNotEquals(cursor, False)

  def testCursorComplex2(self):
    """random hash ordering cursor bug b/8323666 freeq."""

    # TODO(bneutra) how to repro the bug, testing in process
    # doesn't tickle it.

    query = """
    [
      {
        "master_property": {
          "id": null,
          "reverse_property": null
        },
        "limit": 3,
        "type": "/type/link",
        "target": {
          "guid": null,
          "type": [],
          "id": "#9202a8c04000641f8000000003b50f85"
        },
        "source": {
          "guid": null,
          "type": [],
          "id": null
        }
      }
    ]
    """
    cursor = True
    i = 0
    while i < 30:
      i+=1
      self.env = {'cursor': cursor, 'as_of_time': '2013-03-01'}
      self.MQLQuerier(query)
      self.assertEquals(len(self.mql_result.result), 3)
      # we should have a new cursor
      self.assertNotEquals(cursor, self.mql_result.cursor)
      cursor = self.mql_result.cursor
      # it should be a cursor
      self.assertNotEquals(cursor, False)


if __name__ == '__main__':
  mql_fixture.main()
