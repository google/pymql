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
"""run some basic mqlread tests."""

# thanks to warren, these were dime tests

__author__ = 'bneutra@google.com (Brendan Neutra)'

import google3
from pymql.mql import error
from pymql.test import mql_fixture


class MQLTest(mql_fixture.MQLTest):
  """for testing basic mqlread queries."""

  def setUp(self):
    self.SetMockPath('data/basic_mql.yaml')
    super(MQLTest, self).setUp()
    self.env = {'as_of_time': '2009-10-01'}


  def testEcho(self):
    """echo."""

    query = """
    {
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadObjProperty(self):
    """read obj property."""

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

  def testReadObjPropertyWithSubProperty(self):
    """read obj property with sub-property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "name": "Duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "name": "Duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadValueProperty(self):
    """read value property."""

    query = """
    {
      "/people/person/date_of_birth": null,
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/date_of_birth": "1941-05-24",
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTextProperty(self):
    """read text property."""

    query = """
    {
      "id": "/en/bob_dylan",
      "name": null
    }
    """
    exp_response = """
    {
      "id": "/en/bob_dylan",
      "name": "Bob Dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTopLevelIdProperty(self):
    """read top-level id property."""

    query = """
    {
      "rv:id": null,
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "rv:id": "/en/bob_dylan",
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTopLevelGuidProperty(self):
    """read top-level guid property."""

    query = """
    {
      "guid": null,
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "guid": "#9202a8c04000641f8000000003abd178",
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testConstrainTopLevelGuidAndId(self):
    """constrain top-level guid and id."""

    query = """
    {
      "guid": "#9202a8c04000641f8000000003abd178",
      "id": "/en/bob_dylan"
    }
    """
    exc_response = (
        error.MQLParseError,
        "Can't specify an id more than once in a single clause"
    )
    self.DoQuery(query, exc_response=exc_response)

  def testReadTopLevelObjectTimestampProperty(self):
    """read top-level object timestamp property."""

    query = """
    {
      "timestamp": null,
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "timestamp": "2006-12-10T16:16:13.0307Z",
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTopLevelAttributionProperty(self):
    """read top-level attribution property."""

    query = """
    {
      "attribution": null,
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "attribution": "/user/mwcl_musicbrainz",
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testConstrainTopLevelObjectTimestampProperty(self):
    """constrain top-level object timestamp property."""

    query = """
    {
      "timestamp": "2006-12-10T16:16:13.0307Z",
      "name": "Bob Dylan"
    }
    """
    exp_response = """
    {
      "timestamp": "2006-12-10T16:16:13.0307Z",
      "name": "Bob Dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testConstrainTopLevelAttributionProperty(self):
    """constrain top-level attribution property."""

    query = """
    {
      "attribution": "/user/mwcl_musicbrainz",
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "attribution": "/user/mwcl_musicbrainz",
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadIdOfObjProperty(self):
    """read id of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "id": "/en/duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadGuidOfObjProperty(self):
    """read guid of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "guid": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "guid": "#9202a8c04000641f8000000000078626"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTimestampOfObjProperty(self):
    """read timestamp of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "timestamp": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "timestamp": "2006-10-22T10:09:35.0043Z"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadAttributionOfObjProperty(self):
    """read attribution of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "attribution": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "attribution": "/user/metaweb"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadValueOfValueProperty(self):
    """read value of value property."""

    query = """
    {
      "/people/person/date_of_birth": {
        "value": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/date_of_birth": {
        "value": "1941-05-24"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadValueOfTextProperty(self):
    """read value of text property."""

    query = """
    {
      "id": "/en/bob_dylan",
      "name": [
        {
          "value": null
        }
      ]
    }
    """
    exp_response = """
    {
      "id": "/en/bob_dylan",
      "name": [
        {
          "value": "Bob Dylan"
        },
        {
          "value": "\u0414\u0438\u043b\u0430\u043d, \u0411\u043e\u0431"
        },
        {
          "value": "\u0411\u043e\u0431 \u0414\u0456\u043b\u0430\u043d"
        },
        {
          "value": "\u30dc\u30d6\u30fb\u30c7\u30a3\u30e9\u30f3"
        },
        {
          "value": "\u9c8d\u52c3\u00b7\u8fea\u4f26"
        }
      ]
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testConstrainIdOfObjProperty(self):
    """constrain id of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "id": "/en/duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "id": "/en/duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testConstrainGuidOfObjProperty(self):
    """constrain guid of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "guid": "#9202a8c04000641f8000000000078626"
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "guid": "#9202a8c04000641f8000000000078626"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testConstrainTimestampOfObjProperty(self):
    """constrain timestamp of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "timestamp": "2006-10-22T10:09:35.0043Z"
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "timestamp": "2006-10-22T10:09:35.0043Z"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testConstrainAttributionOfObjProperty(self):
    """constrain attribution of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "attribution": "/user/metaweb"
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "attribution": "/user/metaweb"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testConstrainValueOfValueProperty(self):
    """constrain value of value property."""

    query = """
    {
      "/people/person/date_of_birth": {
        "value": "1941-05-24"
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/date_of_birth": {
        "value": "1941-05-24"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testConstrainValueOfTextProperty(self):
    """constrain value of text property."""

    query = """
    {
      "id": "/en/bob_dylan",
      "name": {
        "value": "Bob Dylan"
      }
    }
    """
    exp_response = """
    {
      "id": "/en/bob_dylan",
      "name": {
        "value": "Bob Dylan"
      }
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadStructuredIdOfObjProperty(self):
    """read structured id of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "id": {
          "value": null
        }
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "id": {
          "value": "/en/duluth"
        }
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadStructuredGuidOfObjProperty(self):
    """read structured guid of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "guid": {
          "value": null
        }
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "guid": {
          "value": "#9202a8c04000641f8000000000078626"
        }
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadStructuredTimestampOfObjProperty(self):
    """read structured timestamp of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "timestamp": {
          "value": null
        }
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "timestamp": {
          "value": "2006-10-22T10:09:35.0043Z"
        }
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testConstrainStructuredIdOfObjProperty(self):
    """constrain structured id of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "id": {
          "value": "/en/duluth"
        }
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "id": {
          "value": "/en/duluth"
        }
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testConstrainStructuredGuidOfObjProperty(self):
    """constrain structured guid of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "guid": {
          "value": "#9202a8c04000641f8000000000078626"
        }
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "guid": {
          "value": "#9202a8c04000641f8000000000078626"
        }
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testConstrainStructuredTimestampOfObjProperty(self):
    """constrain structured timestamp of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "timestamp": {
          "value": "2006-10-22T10:09:35.0043Z"
        }
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "timestamp": {
          "value": "2006-10-22T10:09:35.0043Z"
        }
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testConstrainLangOfTextProperty(self):
    """constrain lang of text property."""

    query = """
    {
      "id": "/en/bob_dylan",
      "name": {
        "lang": "/lang/ja",
        "value": null
      }
    }
    """
    exp_response = """
    {
      "id": "/en/bob_dylan",
      "name": {
        "lang": "/lang/ja",
        "value": "\u30dc\u30d6\u30fb\u30c7\u30a3\u30e9\u30f3"
      }
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadEnumeratedProperty(self):
    """read enumerated property."""

    query = """
    {
      "/type/user/userid": null,
      "id": "/user/warren"
    }
    """
    exp_response = """
    {
      "/type/user/userid": "warren",
      "id": "/user/warren"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testConstrainEnumeratedProperty(self):
    """constrain enumerated property."""

    query = """
    {
      "/type/user/userid": "warren",
      "id": "/user/warren"
    }
    """
    exp_response = """
    {
      "/type/user/userid": "warren",
      "id": "/user/warren"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadObjDefaultPropertiesOfObjProperty(self):
    """read obj default properties of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {},
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "type": [
          "/common/topic",
          "/location/location",
          "/location/citytown",
          "/user/joehughes/default_domain/transit_service_area",
          "/location/dated_location",
          "/location/statistical_region",
          "/government/governmental_jurisdiction"
        ],
        "id": "/en/duluth",
        "name": "Duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadObjDefaultPropertiesOfValueProperty(self):
    """read obj default properties of value property."""

    query = """
    {
      "/people/person/date_of_birth": {},
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/date_of_birth": {
        "type": "/type/datetime",
        "value": "1941-05-24"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadWithExpectedTypes1(self):
    """read with expected types 1."""

    query = """
    {
      "date_of_birth": null,
      "type": "/people/person",
      "id": "/en/bob_dylan",
      "place_of_birth": null
    }
    """
    exp_response = """
    {
      "date_of_birth": "1941-05-24",
      "type": "/people/person",
      "id": "/en/bob_dylan",
      "place_of_birth": "Duluth"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadWithExpectedTypes2(self):
    """read with expected types 2."""

    query = """
    {
      "date_of_birth": null,
      "type": "/people/person",
      "id": "/en/bob_dylan",
      "place_of_birth": {
        "geolocation": {
          "latitude": null,
          "longitude": null
        }
      }
    }
    """
    exp_response = """
    {
      "date_of_birth": "1941-05-24",
      "type": "/people/person",
      "id": "/en/bob_dylan",
      "place_of_birth": {
        "geolocation": {
          "latitude": 46.783299999999997,
          "longitude": -92.106399999999994
        }
      }
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadWithExpectedTypesOfReverseProperty(self):
    """read with expected types of reverse property."""

    query = """
    {
      "/people/profession/people_with_this_profession": [
        {
          "limit": 2,
          "profession": [],
          "name": null
        }
      ],
      "id": "/en/songwriter"
    }
    """
    exp_response = """
    {
      "/people/profession/people_with_this_profession": [
        {
          "profession": [
            "Singer",
            "Songwriter",
            "Record producer",
            "Bassist",
            "Composer",
            "Keyboard player"
          ],
          "name": "Brian Wilson"
        },
        {
          "profession": [
            "Songwriter"
          ],
          "name": "Diane Warren"
        }
      ],
      "id": "/en/songwriter"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadWithExpectedTypesOfReversedMasterProperty(self):
    """read with expected types of reversed master property."""

    query = """
    {
      "id": "/en/songwriter",
      "!/people/person/profession": [
        {
          "profession": [],
          "limit": 2,
          "name": null
        }
      ]
    }
    """
    exp_response = """
    {
      "id": "/en/songwriter",
      "!/people/person/profession": [
        {
          "profession": [
            "Singer",
            "Songwriter",
            "Record producer",
            "Bassist",
            "Composer",
            "Keyboard player"
          ],
          "name": "Brian Wilson"
        },
        {
          "profession": [
            "Songwriter"
          ],
          "name": "Diane Warren"
        }
      ]
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadWithExpectedTypesOfReversedReverseProperty(self):
    """read with expected types of reversed reverse property."""

    query = """
    {
      "!/people/profession/people_with_this_profession": [
        {
          "specialization_of": null,
          "limit": 2,
          "name": null
        }
      ],
      "type": "/people/person",
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "type": "/people/person",
      "!/people/profession/people_with_this_profession": [
        {
          "specialization_of": "Musician",
          "name": "Songwriter"
        },
        {
          "specialization_of": null,
          "name": "Writer"
        }
      ],
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadEmptyArrayOfObjProperty(self):
    """read empty array of obj property."""

    query = """
    {
      "religion": [],
      "type": "/people/person",
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "religion": [
        "Christianity",
        "Judaism"
      ],
      "type": "/people/person",
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadEmptyArrayOfValueProperty(self):
    """read empty array of value property."""

    query = """
    {
      "alias": [],
      "type": "/common/topic",
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "alias": [
        "Robert Zimmerman",
        "Blind Boy Grunt",
        "Robert Allen Zimmerman",
        "Boy Dylan",
        "Jack Frost",
        "Bob Allen Zimmerman",
        "Bobby Zimmerman",
        "Sergei Petrov",
        "Lucky Wilbury"
      ],
      "type": "/common/topic",
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadObjArrayOfObjProperty(self):
    """read obj array of obj property."""

    query = """
    {
      "religion": [
        {
          "id": null
        }
      ],
      "type": "/people/person",
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "religion": [
        {
          "id": "/en/christianity"
        },
        {
          "id": "/en/judaism"
        }
      ],
      "type": "/people/person",
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadObjArrayOfValueProperty(self):
    """read obj array of value property."""

    query = """
    {
      "date_of_birth": [
        {
          "value": null
        }
      ],
      "type": "/people/person",
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "date_of_birth": [
        {
          "value": "1941-05-24"
        }
      ],
      "type": "/people/person",
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadArrayOfTextPropertyWithDefaultLang(self):
    """read array of text property with default lang."""

    query = """
    {
      "id": "/en/bob_dylan",
      "name": [
        {
          "value": null
        }
      ]
    }
    """
    exp_response = """
    {
      "id": "/en/bob_dylan",
      "name": [
        {
          "value": "Bob Dylan"
        },
        {
          "value": "\u0414\u0438\u043b\u0430\u043d, \u0411\u043e\u0431"
        },
        {
          "value": "\u0411\u043e\u0431 \u0414\u0456\u043b\u0430\u043d"
        },
        {
          "value": "\u30dc\u30d6\u30fb\u30c7\u30a3\u30e9\u30f3"
        },
        {
          "value": "\u9c8d\u52c3\u00b7\u8fea\u4f26"
        }
      ]
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadArrayOfTextPropertyWithLang(self):
    """read array of text property with lang."""

    query = """
    {
      "id": "/en/bob_dylan",
      "name": [
        {
          "lang": null,
          "value": null
        }
      ]
    }
    """
    exp_response = """
    {
      "id": "/en/bob_dylan",
      "name": [
        {
          "lang": "/lang/en",
          "value": "Bob Dylan"
        },
        {
          "lang": "/lang/ru",
          "value": "\u0414\u0438\u043b\u0430\u043d, \u0411\u043e\u0431"
        },
        {
          "lang": "/lang/uk",
          "value": "\u0411\u043e\u0431 \u0414\u0456\u043b\u0430\u043d"
        },
        {
          "lang": "/lang/ja",
          "value": "\u30dc\u30d6\u30fb\u30c7\u30a3\u30e9\u30f3"
        },
        {
          "lang": "/lang/zh",
          "value": "\u9c8d\u52c3\u00b7\u8fea\u4f26"
        }
      ]
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadObjArrayWithLimit(self):
    """read obj array with limit."""

    query = """
    {
      "religion": [
        {
          "limit": 1,
          "id": null
        }
      ],
      "type": "/people/person",
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "religion": [
        {
          "id": "/en/christianity"
        }
      ],
      "type": "/people/person",
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadEmptyObjArrayDefaultPropertiesOfObjProperty(self):
    """read empty obj array default properties of obj property."""

    query = """
    {
      "religion": [
        {}
      ],
      "type": "/people/person",
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "religion": [
        {
          "type": [
            "/common/topic",
            "/religion/religion",
            "/broadcast/genre",
            "/exhibitions/exhibition_subject",
            "/book/book_subject",
            "/user/tsegaran/random/taxonomy_subject",
            "/people/profession",
            "/base/godbase/topic",
            "/base/popstra/sww_base",
            "/base/popstra/religion",
            "/base/popstra/topic",
            "/base/christianity/topic",
            "/base/argumentmaps/thing_of_disputed_value",
            "/base/argumentmaps/topic"
          ],
          "id": "/en/christianity",
          "name": "Christianity"
        },
        {
          "type": [
            "/common/topic",
            "/religion/religion",
            "/broadcast/genre",
            "/exhibitions/exhibition_subject",
            "/military/military_combatant",
            "/book/book_subject",
            "/user/tsegaran/random/taxonomy_subject",
            "/base/symbols/topic",
            "/base/symbols/symbolized_concept",
            "/organization/organization_sector",
            "/base/godbase/topic",
            "/m/05qry21",
            "/education/field_of_study",
            "/base/jewlib/topic",
            "/base/jewlib/jewish_studies_field",
            "/base/popstra/sww_base",
            "/base/popstra/religion",
            "/base/popstra/topic",
            "/fictional_universe/ethnicity_in_fiction",
            "/base/eating/practicer_of_diet"
          ],
          "id": "/en/judaism",
          "name": "Judaism"
        }
      ],
      "type": "/people/person",
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadEmptyObjArrayDefaultPropertiesOfValueProperty(self):
    """read empty obj array default properties of value property."""

    query = """
    {
      "alias": [
        {}
      ],
      "type": "/common/topic",
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "alias": [
        {
          "lang": "/lang/en",
          "type": "/type/text",
          "value": "Robert Zimmerman"
        },
        {
          "lang": "/lang/he",
          "type": "/type/text",
          "value": "\u05d1\u05d5\u05d1 \u05d3\u05d9\u05dc\u05df"
        },
        {
          "lang": "/lang/es",
          "type": "/type/text",
          "value": "Bob Dylan"
        },
        {
          "lang": "/lang/zh",
          "type": "/type/text",
          "value": "\u9c8d\u52c3\u00b7\u8fea\u4f26"
        },
        {
          "lang": "/lang/fr",
          "type": "/type/text",
          "value": "Bob Dylan"
        },
        {
          "lang": "/lang/it",
          "type": "/type/text",
          "value": "Bob Dylan"
        },
        {
          "lang": "/lang/de",
          "type": "/type/text",
          "value": "Bob Dylan"
        },
        {
          "lang": "/lang/sk",
          "type": "/type/text",
          "value": "Bob Dylan"
        },
        {
          "lang": "/lang/hu",
          "type": "/type/text",
          "value": "Bob Dylan"
        },
        {
          "lang": "/lang/id",
          "type": "/type/text",
          "value": "Bob Dylan"
        },
        {
          "lang": "/lang/ro",
          "type": "/type/text",
          "value": "Bob Dylan"
        },
        {
          "lang": "/lang/tr",
          "type": "/type/text",
          "value": "Bob Dylan"
        },
        {
          "lang": "/lang/ru",
          "type": "/type/text",
          "value": "\u0414\u0438\u043b\u0430\u043d, \u0411\u043e\u0431"
        },
        {
          "lang": "/lang/sr",
          "type": "/type/text",
          "value": "\u0411\u043e\u0431 \u0414\u0438\u043b\u0430\u043d"
        },
        {
          "lang": "/lang/ja",
          "type": "/type/text",
          "value": "\u30dc\u30d6\u30fb\u30c7\u30a3\u30e9\u30f3"
        },
        {
          "lang": "/lang/ca",
          "type": "/type/text",
          "value": "Bob Dylan"
        },
        {
          "lang": "/lang/sl",
          "type": "/type/text",
          "value": "Bob Dylan"
        },
        {
          "lang": "/lang/en",
          "type": "/type/text",
          "value": "Blind Boy Grunt"
        },
        {
          "lang": "/lang/en",
          "type": "/type/text",
          "value": "Robert Allen Zimmerman"
        },
        {
          "lang": "/lang/en",
          "type": "/type/text",
          "value": "Boy Dylan"
        },
        {
          "lang": "/lang/en",
          "type": "/type/text",
          "value": "Jack Frost"
        },
        {
          "lang": "/lang/en",
          "type": "/type/text",
          "value": "Bob Allen Zimmerman"
        },
        {
          "lang": "/lang/en",
          "type": "/type/text",
          "value": "Bobby Zimmerman"
        },
        {
          "lang": "/lang/en",
          "type": "/type/text",
          "value": "Sergei Petrov"
        },
        {
          "lang": "/lang/en",
          "type": "/type/text",
          "value": "Lucky Wilbury"
        }
      ],
      "type": "/common/topic",
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTypeAttributionAttributed(self):
    """read /type/attribution/attributed."""

    query = """
    {
      "/type/attribution/attributed": {
        "id": null
      },
      "id": "/user/warren"
    }
    """
    exp_response = """
    {
      "/type/attribution/attributed": {
        "id": "/m/07n73yp"
      },
      "id": "/user/warren"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testConstrainTypeAttributionAttributed(self):
    """constrain /type/attribution/attributed."""

    query = """
    {
      "/type/attribution/attributed": {
        "id": "/guid/9202a8c04000641f800000000f438fb5"
      },
      "id": "/user/warren"
    }
    """
    exp_response = """
    {
      "/type/attribution/attributed": {
        "id": "/guid/9202a8c04000641f800000000f438fb5"
      },
      "id": "/user/warren"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTypeAttributionAttributedAsBangTypeObjectAttribution(self):
    """read /type/attribution/attributed as !/type/object/attribution."""

    query = """
    {
      "!/type/object/attribution": {
        "id": null
      },
      "id": "/user/warren"
    }
    """
    exc_response = (
        error.MQLTypeError,
        "Can't reverse artificial property /type/object/attribution"
    )
    self.DoQuery(query, exc_response=exc_response)

  def testConstrainTypeAttributionAttributedAsBangTypeObjectAttribution(self):
    """constrain /type/attribution/attributed as !/type/object/attribution."""

    query = """
    {
      "!/type/object/attribution": {
        "id": "/guid/9202a8c04000641f800000000f438fb5"
      },
      "id": "/user/warren"
    }
    """
    exc_response = (
        error.MQLTypeError,
        "Can't reverse artificial property /type/object/attribution"
    )
    self.DoQuery(query, exc_response=exc_response)

  def testReadTypeObjectKeyProperties(self):
    """read /type/object/key properties."""

    query = """
    {
      "id": "/en/bob_dylan",
      "key": {
        "namespace": null,
        "value": "c2145f2d-fdd9-4e98-95b5-17d6bfd0b053"
      }
    }
    """
    exp_response = """
    {
      "id": "/en/bob_dylan",
      "key": {
        "namespace": "/authority/musicbrainz",
        "value": "c2145f2d-fdd9-4e98-95b5-17d6bfd0b053"
      }
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTypeNamespaceKeysProperties(self):
    """read /type/namespace/keys properties."""

    query = """
    {
      "/type/namespace/keys": [
        {
          "limit": 2,
          "namespace": null,
          "value": null
        }
      ],
      "id": "/authority/musicbrainz"
    }
    """
    exp_response = """
    {
      "/type/namespace/keys": [
        {
          "namespace": "/authority/musicbrainz/name",
          "value": "name"
        },
        {
          "namespace": "/en/extended_play",
          "value": "ALBUMTYPE3"
        }
      ],
      "id": "/authority/musicbrainz"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadStructuredTypeKeyNamespace(self):
    """read structured /type/key/namespace."""

    query = """
    {
      "/type/namespace/keys": [
        {
          "namespace": {
            "guid": null,
            "id": null
          },
          "limit": 2,
          "value": null
        }
      ],
      "id": "/authority/musicbrainz"
    }
    """
    exp_response = """
    {
      "/type/namespace/keys": [
        {
          "namespace": {
            "guid": "#9202a8c04000641f8000000001143432",
            "id": "/authority/musicbrainz/name"
          },
          "value": "name"
        },
        {
          "namespace": {
            "guid": "#9202a8c04000641f80000000012091b6",
            "id": "/en/extended_play"
          },
          "value": "ALBUMTYPE3"
        }
      ],
      "id": "/authority/musicbrainz"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testConstrainTypeObjectKeyProperties(self):
    """constrain /type/object/key properties."""

    query = """
    {
      "id": "/en/bob_dylan",
      "key": {
        "namespace": "/authority/musicbrainz",
        "value": "c2145f2d-fdd9-4e98-95b5-17d6bfd0b053"
      }
    }
    """
    exp_response = """
    {
      "id": "/en/bob_dylan",
      "key": {
        "namespace": "/authority/musicbrainz",
        "value": "c2145f2d-fdd9-4e98-95b5-17d6bfd0b053"
      }
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testConstrainTypeNamespaceKeysProperties(self):
    """constrain /type/namespace/keys properties."""

    query = """
    {
      "/type/namespace/keys": {
        "namespace": "/authority/musicbrainz/name",
        "value": "name"
      },
      "id": "/authority/musicbrainz"
    }
    """
    exp_response = """
    {
      "/type/namespace/keys": {
        "namespace": "/authority/musicbrainz/name",
        "value": "name"
      },
      "id": "/authority/musicbrainz"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testConstrainStructuredTypeKeyNamespace(self):
    """constrain structured /type/key/namespace."""

    query = """
    {
      "/type/namespace/keys": {
        "namespace": {
          "guid": "#9202a8c04000641f8000000001143432",
          "id": "/guid/9202a8c04000641f8000000001143432"
        },
        "value": "name"
      },
      "id": "/authority/musicbrainz"
    }
    """
    exc_response = (
        error.MQLParseError,
        "Can't specify an id more than once in a single clause"
    )
    self.DoQuery(query, exc_response=exc_response)

  def testConstrainIdOREqual(self):
    """constrain id|=."""

    query = """
    [
      {
        "id|=": [
          "/common/topic",
          "/tv/tv_actor",
          "/type/link",
          "/type/object"
        ],
        "name": null
      }
    ]
    """
    exp_response = """
    [
      {
        "name": "Object"
      },
      {
        "name": "Topic"
      },
      {
        "name": "Link"
      },
      {
        "name": "TV Actor"
      }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testConstrainPropertyOREqual(self):
    """constrain property |=."""

    query = """
    [
      {
        "album": [],
        "limit": 2,
        "album|=": [
          "Greatest Hits",
          "Super Hits"
        ],
        "type": "/music/artist",
        "name": null
      }
    ]
    """
    exp_response = """
    [
      {
        "album": [
          "Greatest Hits",
          "Greatest Hits",
          "Greatest Hits"
        ],
        "type": "/music/artist",
        "name": "Tupac Shakur"
      },
      {
        "album": [
          "Greatest Hits"
        ],
        "type": "/music/artist",
        "name": "Alice in Chains"
      }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testUtf8(self):
    """Regression test for issue 4970606"""

    query = """
      [{"name":"Beyonc\u00e9", "id": null}]
    """
    exp_response = """
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

  def testBeyonce(self):
    """Regression test for issue 4970606"""

    query = """
      [{"name":"Beyonc\u00e9", "id": null}]
    """
    exp_response = """
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
    self.DoQuery(query, exp_response=exp_response)

  def testBeyonce2(self):
    """Regression test for issue 4970606"""

    query = """
      [{"name":"Beyoncé", "id": null}]
    """
    exp_response = """
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
    self.DoQuery(query, exp_response=exp_response)


if __name__ == '__main__':
  mql_fixture.main()
