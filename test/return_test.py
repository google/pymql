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

# -*- coding: utf-8 -*-
#
"""mql return directive."""

__author__ = 'bneutra@google.com (Brendan Neutra)'

# thanks warren for these dimetests

import google3
from pymql.mql import error
from pymql.test import mql_fixture

class MQLTest(mql_fixture.MQLTest):
  """mql return directive."""

  def setUp(self):
    self.SetMockPath('data/return.yaml')
    super(MQLTest, self).setUp()
    self.env = {'as_of_time': '2010-05-01'}


  def testReturnCountOfObject(self):
    """return count of object."""

    query = """
    {
      "/people/person/children": {
        "count": null,
        "return": "count"
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/children": 6,
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReturnCountOfArray(self):
    """return count of array."""

    query = """
    {
      "/people/person/children": [
        {
          "count": null,
          "return": "count"
        }
      ],
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/children": [
        6
      ],
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReturnEstimateCountOfArray(self):
    """return estimate-count of array."""

    query = """
    {
      "/people/person/children": [
        {
          "return": "estimate-count",
          "estimate-count": null
        }
      ],
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/children": [
        6
      ],
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReturnCountNullWhenNone(self):
    """return count null when none."""

    query = """
    {
      "album": {
        "return": "count",
        "name": "Arrested"
      },
      "type": "/music/artist",
      "name": "The Police"
    }
    """
    exp_response = """
    null
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReturnCount0WhenNoneAndOptional(self):
    """return count 0 when none and optional."""

    query = """
    {
      "album": {
        "optional": true,
        "return": "count",
        "name": "Arrested"
      },
      "type": "/music/artist",
      "name": "The Police"
    }
    """
    exp_response = """
    {
      "album": 0,
      "type": "/music/artist",
      "name": "The Police"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReturnIgnoresOtherResultValues(self):
    """return ignores other result values."""

    query = """
    {
      "/people/person/children": [
        {
          "count": null,
          "nationality": {
            "id": "/en/united_states",
            "name": null
          },
          "return": "count",
          "id": null
        }
      ],
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/children": [
        2
      ],
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReturnImplicitCount(self):
    """return implicit count."""

    query = """
    {
      "/people/person/children": {
        "return": "count",
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/children": 6,
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReturnIdFail(self):
    """return id."""

    query = """
    {
      "/people/person/children": {
        "date_of_birth": null,
        "return": "id",
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exc_response = (
        error.MQLParseError,
	"'return' currently only supports 'count' and 'estimate-count'"
    )
    self.DoQuery(query, exc_response=exc_response)

if __name__ == '__main__':
  mql_fixture.main()
