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
"""mql sort tests."""

__author__ = 'bneutra@google.com (Brendan Neutra)'

import google3
from pymql.mql import error
from pymql.test import mql_fixture


class MQLTest(mql_fixture.MQLTest):
  """mql sort tests."""

  def setUp(self):
    self.SetMockPath('data/sort.yaml')
    super(MQLTest, self).setUp()
    self.env = {'as_of_time': '2010-05-01'}

  def testSortBySimpleValueProperty(self):
    """sort by simple value property."""

    query = """
    {
      "/people/person/children": [
        {
          "sort": "date_of_birth",
          "date_of_birth": null
        }
      ],
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/children": [
        {
          "date_of_birth": "1966-01-06"
        },
        {
          "date_of_birth": "1969-12-09"
        },
        {
          "date_of_birth": null
        },
        {
          "date_of_birth": null
        },
        {
          "date_of_birth": null
        },
        {
          "date_of_birth": null
        }
      ],
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReverseSortBySimpleValueProperty(self):
    """reverse sort by simple value property."""

    query = """
    {
      "/people/person/children": [
        {
          "sort": "-date_of_birth",
          "date_of_birth": null
        }
      ],
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/children": [
        {
          "date_of_birth": null
        },
        {
          "date_of_birth": null
        },
        {
          "date_of_birth": null
        },
        {
          "date_of_birth": null
        },
        {
          "date_of_birth": "1969-12-09"
        },
        {
          "date_of_birth": "1966-01-06"
        }
      ],
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testSortByTypeObjectValueProperty(self):
    """sort by /type/object/value property."""

    query = """
    {
      "/people/person/children": [
        {
          "sort": "date_of_birth.value",
          "date_of_birth": {
            "value": null
          }
        }
      ],
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/children": [
        {
          "date_of_birth": {
            "value": "1966-01-06"
          }
        },
        {
          "date_of_birth": {
            "value": "1969-12-09"
          }
        }
      ],
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testSortByDefaultProperty(self):
    """sort by default property."""

    query = """
    {
      "/people/person/children": [
        {
          "sort": "name",
          "name": null
        }
      ],
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/children": [
        {
          "name": "Anna Dylan"
        },
        {
          "name": "Desiree Gabrielle Dennis-Dylan"
        },
        {
          "name": "Jakob Dylan"
        },
        {
          "name": "Jesse Dylan"
        },
        {
          "name": "Maria Dylan"
        },
        {
          "name": "Sam Dylan"
        }
      ],
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testSortByMultipleProperties(self):
    """sort by multiple properties."""

    query = """
    {
      "/people/person/children": [
        {
          "sort": [
            "date_of_birth",
            "name"
          ],
          "date_of_birth": null,
          "name": null
        }
      ],
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/children": [
        {
          "date_of_birth": "1966-01-06",
          "name": "Jesse Dylan"
        },
        {
          "date_of_birth": "1969-12-09",
          "name": "Jakob Dylan"
        },
        {
          "date_of_birth": null,
          "name": "Anna Dylan"
        },
        {
          "date_of_birth": null,
          "name": "Desiree Gabrielle Dennis-Dylan"
        },
        {
          "date_of_birth": null,
          "name": "Maria Dylan"
        },
        {
          "date_of_birth": null,
          "name": "Sam Dylan"
        }
      ],
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testSortByObjectTimestamp(self):
    """sort by object timestamp."""

    query = """
    {
      "/people/person/children": [
        {
          "sort": "timestamp",
          "timestamp": null
        }
      ],
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/children": [
        {
          "timestamp": "2006-10-22T19:10:33.0053Z"
        },
        {
          "timestamp": "2006-10-23T04:49:44.0033Z"
        },
        {
          "timestamp": "2008-09-21T03:48:54.0001Z"
        },
        {
          "timestamp": "2008-09-21T03:48:54.0007Z"
        },
        {
          "timestamp": "2008-09-21T03:48:54.0013Z"
        },
        {
          "timestamp": "2008-09-21T03:48:54.0019Z"
        }
      ],
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testSortByCount(self):
    """sort by count."""

    query = """
    {
      "/people/person/children": [
        {
          "sort": "-parents.count",
          "parents": [
            {
              "count": null
            }
          ]
        }
      ],
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/children": [
        {
          "parents": [
            {
              "count": 2
            },
            {
              "count": 2
            }
          ]
        },
        {
          "parents": [
            {
              "count": 1
            }
          ]
        },
        {
          "parents": [
            {
              "count": 1
            }
          ]
        },
        {
          "parents": [
            {
              "count": 1
            }
          ]
        },
        {
          "parents": [
            {
              "count": 1
            }
          ]
        },
        {
          "parents": [
            {
              "count": 1
            }
          ]
        }
      ],
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadPropertiesRequiredBySort(self):
    """read properties required by sort."""

    query = """
    {
      "/people/person/children": [
        {
          "sort": [
            "profession",
            "date_of_birth",
            "place_of_birth"
          ]
        }
      ],
      "id": "/en/bob_dylan"
    }
    """
    exc_response = (
        error.MQLParseError,
        "Unable to locate profession from sort"
    )
    self.DoQuery(query, exc_response=exc_response)

  def testReadConstrainedPropertyRequiredBySort(self):
    """read constrained property required by sort."""

    query = """
    {
      "/people/person/children": [
        {
          "sort": "date_of_birth",
          "date_of_birth>": "1940"
        }
      ],
      "id": "/en/bob_dylan"
    }
    """
    exc_response = (
        error.MQLParseError,
        "Must sort on a single value, not at date_of_birth"
    )
    self.DoQuery(query, exc_response=exc_response)

  def testSortMatchingLabeledKey(self):
    """sort matching labeled key."""

    query = """
    {
      "/people/person/children": [
        {
          "sort": "foo:name",
          "foo:name": null
        }
      ],
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/children": [
        {
          "foo:name": "Anna Dylan"
        },
        {
          "foo:name": "Desiree Gabrielle Dennis-Dylan"
        },
        {
          "foo:name": "Jakob Dylan"
        },
        {
          "foo:name": "Jesse Dylan"
        },
        {
          "foo:name": "Maria Dylan"
        },
        {
          "foo:name": "Sam Dylan"
        }
      ],
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testSortByLinkClauseTimestamp(self):
    """sort by link clause timestamp."""

    query = """
    {
      "/people/person/children": [
        {
          "sort": "link.timestamp",
          "link": {
            "timestamp": null
          }
        }
      ],
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/children": [
        {
          "link": {
            "timestamp": "2008-03-05T00:48:02.0000Z"
          }
        },
        {
          "link": {
            "timestamp": "2008-09-21T03:48:54.0000Z"
          }
        },
        {
          "link": {
            "timestamp": "2008-09-21T03:48:54.0006Z"
          }
        },
        {
          "link": {
            "timestamp": "2008-09-21T03:48:54.0012Z"
          }
        },
        {
          "link": {
            "timestamp": "2008-09-21T03:48:54.0018Z"
          }
        },
        {
          "link": {
            "timestamp": "2008-09-21T03:48:54.0024Z"
          }
        }
      ],
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testSortByIndex(self):
    """sort by index."""

    query = """
    {
  "type": "/film/film",
  "starring": [{
    "limit": 3,
    "sort":  "index",
    "id":    null,
    "index": null
  }],
  "id":   "/en/blade_runner"
    }
    """
    exp_response = """
    {
    "id":   "/en/blade_runner",
    "starring": [
      {
        "id":    "/m/0jvhbm",
        "index": 0
      },
      {
        "id":    "/m/0jvhbs",
        "index": 1
      },
      {
        "id":    "/m/0jvhby",
        "index": 2
      }
    ],
    "type": "/film/film"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testSortByIndexReverse(self):
    """sort by index, reverse."""

    query = """
    {
  "type": "/film/film",
  "starring": [{
    "limit": 3,
    "sort":  "-index",
    "id":    null,
    "index": null
  }],
  "id":   "/en/blade_runner"
    }
    """
    exp_response = """
    {
    "id":   "/en/blade_runner",
    "starring": [
      {
        "id":    "/m/02nth5l",
        "index": 2
      },
      {
        "id":    "/m/02nth5x",
        "index": 1
      },
      {
        "id":    "/m/0jvhc2",
        "index": 0
      }
    ],
    "type": "/film/film"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testSortByGuidFail(self):
    """sort by guid (why not?)."""

    query = """
    [
      {
        "sort": "guid",
        "limit": 10,
        "type": "/people/person"
      }
    ]
    """
    exc_response = (
        error.MQLParseError,
        "Unable to locate guid from sort"
    )
    self.DoQuery(query, exc_response=exc_response)

if __name__ == '__main__':
  mql_fixture.main()
