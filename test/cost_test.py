#!/usr/bin/python2.6
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
"""mql cost tests."""

__author__ = 'bneutra@google.com (Brendan Neutra)'

import google3
from pymql.mql import error
from pymql.test import mql_fixture

# stuff we care about
FLOAT_COSTS = ['mql_stime',
    'mql_stime',
    'mql_utime',
    'mql_rtime',
    'mql_dbtime'
    ]
INT_COSTS = ['pf',
    'mql_dbtries',
    'tu',
    'ts',
    'te'
  ]

# important note: in mock replay mode, stored graph response costs
# are tallied. But mql_[x]time will be calculated in realtime
# so those costs will be quite different than when the mock was 
# recorded (they will be smaller, kinda the point of mocking)

class MQLTest(mql_fixture.MQLTest):
  """mql cost tests."""

  def setUp(self):
    self.SetMockPath('data/cost.yaml')
    super(MQLTest, self).setUp()
    self.env = {'as_of_time': '2010-05-01'}

  def testCost(self):
    """simple positive test."""

    query = """
    {
      "/people/person/place_of_birth": null,
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": "Duluth",
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)
    cost = self.mql_result.cost
    self.costs_exist(cost)
    self.assertGreater(cost['te'], 10, 'te cost should be something')
    self.assertEqual(cost['mql_dbreqs'], 4, 'four graphd requests')

  def testCostError(self):
    """a query that gets a GQL error."""

    query = """
    {
      "guid": "foobar"
    }
    """
    exc_response = (
        error.MQLParseError,
        'Can only use a hexadecimal guid here'
    )
    self.DoQuery(query, exc_response=exc_response)
    cost = self.mql_service.get_cost()
    self.costs_exist(cost)
    self.assertEqual(cost['mql_dbreqs'], 1, 'only one graphd request')

  def testCostComplex(self):
    """query that does a lot of GQL."""

    query = """
    [{
      "/people/person/date_of_birth" : [],
      "/music/artist/album" : [],
      "/film/actor/film" : [],
      "/film/director/film" : [],
      "/film/producer/film" : [],
      "/tv/tv_actor/starring_roles" : [],
      "/tv/tv_producer/programs_produced" : [],
      "type": "/music/artist",
      "b:type": "/film/actor",
      "c:type": "/film/director",
      "d:type": "/film/producer",
      "e:type": "/tv/tv_actor",
      "f:type": "/tv/tv_producer",
      "id": null
    }]
    """
    self.DoQuery(query)
    cost = self.mql_result.cost
    self.costs_exist(cost)
    self.assertEqual(cost['mql_dbreqs'], 12, '12 graphd requests')
    self.assertGreater(cost['tu'], 100, 'tu cost should be something')


  def testQueryTimeout(self):

    self.env['query_timeout_tu'] = 50
    query = """
    [{
      "type": "/people/person",
      "date_of_birth": null,
      "sort": "date_of_birth"
    }]
    """
    exc_response = (
        error.MQLTimeoutError,
        'Query too difficult.'
    )
    self.DoQuery(query, exc_response=exc_response)
    cost = self.mql_service.get_cost()
    self.costs_exist(cost)

  def testQueryTimeoutFloat(self):

    # float is allowed
    self.env['query_timeout_tu'] = 50.1
    query = """
    [{
      "type": "/people/person",
      "date_of_birth": null,
      "sort": "date_of_birth"
    }]
    """
    exc_response = (
        error.MQLTimeoutError,
        'Query too difficult.'
    )
    self.DoQuery(query, exc_response=exc_response)
    cost = self.mql_service.get_cost()
    self.costs_exist(cost)

  def costs_exist(self, cost):
    for c in FLOAT_COSTS:
      self.assertIsInstance(cost[c], float, 'cost %s exists' % c)
    for c in INT_COSTS:
      self.assertIsInstance(cost[c], int, 'cost %s exists' % c)

if __name__ == '__main__':
  mql_fixture.main()
