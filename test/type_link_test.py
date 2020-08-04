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
"""type link."""

__author__ = 'bneutra@google.com (Brendan Neutra)'

# thanks warren for these dimetests

import google3
from pymql.mql import error
from pymql.test import mql_fixture


class MQLTest(mql_fixture.MQLTest):
  """type link tests."""

  def setUp(self):
    self.SetMockPath('data/type_link.yaml')
    super(MQLTest, self).setUp()
    self.env = {'as_of_time': '2010-05-01'}

  def testLinkMasterProperty(self):
    """link:null (master_property) of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "link": null,
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "link": "/people/person/place_of_birth",
        "id": "/en/duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testLinkMasterValueProperty(self):
    """link:null (master_property) of value property."""

    query = """
    {
      "/people/person/date_of_birth": {
        "link": null,
        "value": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/date_of_birth": {
        "link": "/people/person/date_of_birth",
        "value": "1941-05-24"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTypeLinkMasterPropertyOfObjProperty(self):
    """read /type/link/master_property of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "master_property": null
        },
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "master_property": "/people/person/place_of_birth"
        },
        "id": "/en/duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTypeLinkTypeOfObjProperty(self):
    """read /type/link/type of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "type": null
        },
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "type": "/type/link"
        },
        "id": "/en/duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTypeLinkReverseOfObjProperty(self):
    """read /type/link/reverse of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "reverse": null
        },
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "reverse": false
        },
        "id": "/en/duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTypeLinkAttributionOfObjProperty(self):
    """read /type/link/attribution of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "attribution": null
        },
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "attribution": "/user/cvolkert"
        },
        "id": "/en/duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTypeLinkCreatorOfObjProperty(self):
    """read /type/link/creator of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "creator": null
        },
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "creator": "/user/cvolkert"
        },
        "id": "/en/duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTypeLinkTimestampOfObjProperty(self):
    """read /type/link/timestamp of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "timestamp": null
        },
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "timestamp": "2007-10-23T09:07:43.0024Z"
        },
        "id": "/en/duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTypeLinkSourceOfObjProperty(self):
    """read /type/link/source of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "source": null
        },
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "source": "Bob Dylan"
        },
        "id": "/en/duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTypeLinkTargetOfObjProperty(self):
    """read /type/link/target of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "target": null
        },
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "target": "Duluth"
        },
        "id": "/en/duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTypeLinkTargetOfObjArrayProperty(self):
    """read /type/link/target of obj array property."""

    query = """
    {
      "/people/person/children": [{
        "link": {
          "source": [
            {
              "id": null
            }
          ]
        },
        "id": null
      }],
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
    "/people/person/children": [
      {
        "id": "/en/jakob_dylan",
        "link": {
          "source": [{
            "id": "/en/jakob_dylan"
          }]
        }
      },
      {
        "id": "/en/jesse_dylan",
        "link": {
          "source": [{
            "id": "/en/jesse_dylan"
          }]
        }
      },
      {
        "id": "/en/desiree_gabrielle_dennis_dylan",
        "link": {
          "source": [{
            "id": "/en/desiree_gabrielle_dennis_dylan"
          }]
        }
      },
      {
        "id": "/en/maria_dylan",
        "link": {
          "source": [{
            "id": "/en/maria_dylan"
          }]
        }
      },
      {
        "id": "/en/sam_dylan",
        "link": {
          "source": [{
            "id": "/en/sam_dylan"
          }]
        }
      },
      {
        "id": "/en/anna_dylan",
        "link": {
          "source": [{
            "id": "/en/anna_dylan"
          }]
        }
      }
    ],
    "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTypeLinkTargetOfValueProperty(self):
    """read /type/link/target of value property."""

    query = """
    {
      "/people/person/date_of_birth": {
        "link": {
          "target": null
        },
        "value": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/date_of_birth": {
        "link": {
          "target": null
        },
        "value": "1941-05-24"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTypeLinkTargetValueOfObjProperty(self):
    """read /type/link/target_value of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "target_value": null
        },
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "target_value": null
        },
        "id": "/en/duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTypeLinkTargetValueOfValueProperty(self):
    """read /type/link/target_value of value property."""

    query = """
    {
      "/people/person/date_of_birth": {
        "link": {
          "target_value": null
        },
        "value": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/date_of_birth": {
        "link": {
          "target_value": "1941-05-24"
        },
        "value": "1941-05-24"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTypeLinkOperationOfObjProperty(self):
    """read /type/link/operation of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "operation": null
        }
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "operation": "insert"
        }
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTypeLinkOperationOfValueProperty(self):
    """read /type/link/operation of value property."""

    query = """
    {
      "/people/person/date_of_birth": {
        "link": {
          "operation": null
        }
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/date_of_birth": {
        "link": {
          "operation": "insert"
        }
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTypeLinkValidOfObjProperty(self):
    """read /type/link/valid of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "valid": null
        }
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "valid": true
        }
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTypeLinkValidOfValueProperty(self):
    """read /type/link/valid of value property."""

    query = """
    {
      "/people/person/date_of_birth": {
        "link": {
          "valid": null
        }
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/date_of_birth": {
        "link": {
          "valid": true
        }
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testConstrainTypeLinkMasterPropertyOfObjProperty(self):
    """constrain /type/link/master_property of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "master_property": "/people/person/place_of_birth"
        },
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "master_property": "/people/person/place_of_birth"
        },
        "id": "/en/duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testConstrainTypeLinkTypeOfObjProperty(self):
    """constrain /type/link/type of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "type": "/type/link"
        },
        "id": "/en/duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "type": "/type/link"
        },
        "id": "/en/duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testConstrainTypeLinkReverseOfObjProperty(self):
    """constrain /type/link/reverse of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "reverse": false
        },
        "id": "/en/duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    exc_response = (
        error.MQLParseError,
        "Can only ask for the value of 'reverse', not specify it"
    )

    self.DoQuery(query, exc_response=exc_response)

  def testConstrainTypeLinkAttributionOfObjProperty(self):
    """constrain /type/link/attribution of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "attribution": "/user/cvolkert"
        },
        "id": "/en/duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "attribution": "/user/cvolkert"
        },
        "id": "/en/duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testConstrainTypeLinkCreatorOfObjProperty(self):
    """constrain /type/link/creator of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "creator": "/user/cvolkert"
        },
        "id": "/en/duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "creator": "/user/cvolkert"
        },
        "id": "/en/duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testConstrainTypeLinkTimestampOfObjProperty(self):
    """constrain /type/link/timestamp of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "timestamp": "2007-10-23T09:07:43.0024Z"
        },
        "id": "/en/duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "timestamp": "2007-10-23T09:07:43.0024Z"
        },
        "id": "/en/duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testConstrainTypeLinkSourceOfObjProperty(self):
    """constrain /type/link/source of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "source": "Bob Dylan"
        },
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "source": "Bob Dylan"
        },
        "id": "/en/duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testConstrainTypeLinkTargetOfObjProperty(self):
    """constrain /type/link/target of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "target": "Duluth"
        },
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "target": "Duluth"
        },
        "id": "/en/duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testConstrainTypeLinkOperationOfObjProperty(self):
    """constrain /type/link/operation of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "operation": "insert"
        }
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "operation": "insert"
        }
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testConstrainTypeLinkValidOfObjProperty(self):
    """constrain /type/link/valid of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "valid": true
        }
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "valid": true
        }
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testSeveralTypeLinkPropertiesOfObjProperty(self):
    """several /type/link properties of obj property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "attribution": null,
          "reverse": null,
          "timestamp": null,
          "source": null,
          "target": null,
          "master_property": null,
          "type": null,
          "target_value": null
        },
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "attribution": "/user/cvolkert",
          "reverse": false,
          "timestamp": "2007-10-23T09:07:43.0024Z",
          "source": "Bob Dylan",
          "target": "Duluth",
          "master_property": "/people/person/place_of_birth",
          "type": "/type/link",
          "target_value": null
        },
        "id": "/en/duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testSeveralTypeLinkPropertiesOfValueProperty(self):
    """several /type/link properties of value property."""

    query = """
    {
      "/people/person/date_of_birth": {
        "link": {
          "attribution": null,
          "reverse": null,
          "timestamp": null,
          "source": null,
          "target": null,
          "master_property": null,
          "type": null,
          "target_value": null
        },
        "value": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/date_of_birth": {
        "link": {
          "attribution": "/user/mwcl_musicbrainz",
          "reverse": false,
          "timestamp": "2006-12-10T16:16:13.0316Z",
          "source": "Bob Dylan",
          "target": null,
          "master_property": "/people/person/date_of_birth",
          "type": "/type/link",
          "target_value": "1941-05-24"
        },
        "value": "1941-05-24"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadStructuredTypeLinkMasterProperty(self):
    """read structured /type/link/master_property."""

    query = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "master_property": {
            "name": null
          }
        },
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "master_property": {
            "name": "Place of birth"
          }
        },
        "id": "/en/duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadStructuredTypeLinkType(self):
    """read structured /type/link/type."""

    query = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "type": {
            "id": null
          }
        },
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exc_response = (
        error.MQLParseError,
        "Can't expand 'type' in a link clause (it is fixed as '/type/link')"
    )
    self.DoQuery(query, exc_response=exc_response)

  def testReadStructuredTypeLinkReverse(self):
    """read structured /type/link/reverse."""

    query = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "reverse": {
            "value": null
          }
        },
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "reverse": {
            "value": false
          }
        },
        "id": "/en/duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadStructuredTypeLinkAttribution(self):
    """read structured /type/link/attribution."""

    query = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "attribution": {
            "id": null
          }
        },
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "attribution": {
            "id": "/user/cvolkert"
          }
        },
        "id": "/en/duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadStructuredTypeLinkCreator(self):
    """read structured /type/link/creator."""

    query = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "creator": {
            "id": null
          }
        },
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "creator": {
            "id": "/user/cvolkert"
          }
        },
        "id": "/en/duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadStructuredTypeLinkSource(self):
    """read structured /type/link/source."""

    query = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "source": {
            "id": null
          }
        },
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "source": {
            "id": "/en/bob_dylan"
          }
        },
        "id": "/en/duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadStructuredTypeLinkTarget(self):
    """read structured /type/link/target."""

    query = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "target": {
            "id": null
          }
        },
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "target": {
            "id": "/en/duluth"
          }
        },
        "id": "/en/duluth"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTypeLinkMasterPropertyOfReverseProperty(self):
    """read /type/link/master_property of reverse property."""

    query = """
    {
      "/people/person/ethnicity": {
        "link": {
          "master_property": null
        },
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/ethnicity": {
        "link": {
          "master_property": "/people/ethnicity/people"
        },
        "id": "/en/ashkenazi_jews"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTypeLinkTypeOfReverseProperty(self):
    """read /type/link/type of reverse property."""

    query = """
    {
      "/people/person/ethnicity": {
        "link": {
          "type": null
        },
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/ethnicity": {
        "link": {
          "type": "/type/link"
        },
        "id": "/en/ashkenazi_jews"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTypeLinkReverseOfReverseProperty(self):
    """read /type/link/reverse of reverse property."""

    query = """
    {
      "/people/person/ethnicity": {
        "link": {
          "reverse": null
        },
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/ethnicity": {
        "link": {
          "reverse": true
        },
        "id": "/en/ashkenazi_jews"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTypeLinkAttributionOfReverseProperty(self):
    """read /type/link/attribution of reverse property."""

    query = """
    {
      "/people/person/ethnicity": {
        "link": {
          "attribution": null
        },
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/ethnicity": {
        "link": {
          "attribution": "/user/skud"
        },
        "id": "/en/ashkenazi_jews"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTypeLinkCreatorOfReverseProperty(self):
    """read /type/link/creator of reverse property."""

    query = """
    {
      "/people/person/ethnicity": {
        "link": {
          "creator": null
        },
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/ethnicity": {
        "link": {
          "creator": "/user/skud"
        },
        "id": "/en/ashkenazi_jews"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTypeLinkTimestampOfReverseProperty(self):
    """read /type/link/timestamp of reverse property."""

    query = """
    {
      "/people/person/ethnicity": {
        "link": {
          "timestamp": null
        },
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/ethnicity": {
        "link": {
          "timestamp": "2008-05-23T20:32:27.0008Z"
        },
        "id": "/en/ashkenazi_jews"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTypeLinkSourceOfReverseProperty(self):
    """read /type/link/source of reverse property."""

    query = """
    {
      "/people/person/ethnicity": {
        "link": {
          "source": null
        },
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/ethnicity": {
        "link": {
          "source": "Ashkenazi Jews"
        },
        "id": "/en/ashkenazi_jews"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTypeLinkTargetOfReverseProperty(self):
    """read /type/link/target of reverse property."""

    query = """
    {
      "/people/person/ethnicity": {
        "link": {
          "target": null
        },
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/ethnicity": {
        "link": {
          "target": "Bob Dylan"
        },
        "id": "/en/ashkenazi_jews"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTypeLinkTargetValueOfReverseProperty(self):
    """read /type/link/target_value of reverse property."""

    query = """
    {
      "/people/person/ethnicity": {
        "link": {
          "target_value": null
        },
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/ethnicity": {
        "link": {
          "target_value": null
        },
        "id": "/en/ashkenazi_jews"
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTypeLinkOperationOfReverseProperty(self):
    """read /type/link/operation of reverse property."""

    query = """
    {
      "/people/person/ethnicity": {
        "link": {
          "operation": null
        }
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/ethnicity": {
        "link": {
          "operation": "insert"
        }
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTypeLinkValidOfReverseProperty(self):
    """read /type/link/valid of reverse property."""

    query = """
    {
      "/people/person/ethnicity": {
        "link": {
          "valid": null
        }
      },
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "/people/person/ethnicity": {
        "link": {
          "valid": true
        }
      },
      "id": "/en/bob_dylan"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadBareTypeLinkMasterProperty(self):
    """read bare /type/link/master_property."""

    query = """
    [{
      "/people/person/place_of_birth": {
        "/type/link/master_property": null
      },
      "id": "/en/bob_dylan"
    }]
    """
    exc_response = (
        error.MQLTypeError,
        "Can't use /type/link properties on None"
    )
    self.DoQuery(query, exc_response=exc_response)

  def testReadTypePropertyLinks(self):
    """read /type/property/links."""

    query = """
    {
      "/type/property/links": [{
        "limit" : 1,
        "source": {
          "id": null
        },
        "target": {
          "id": null
        }
      }],
      "id": "/people/person/place_of_birth"
    }
    """
    exp_response = """
    {
      "/type/property/links": [{
        "source": {
          "id": "/en/james_caviezel"
        },
        "target": {
          "id": "/en/mount_vernon_washington"
        }
      }],
      "id": "/people/person/place_of_birth"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testConstrainTypePropertyLinks(self):
    """constrain /type/property/links."""

    query = """
    {
      "/type/property/links": [{
        "source": {
          "id": "/en/james_caviezel"
        },
        "target": {
          "id": "/en/mount_vernon_washington"
        }
      }],
      "id": "/people/person/place_of_birth"
    }
    """
    exp_response = """
   {
    "/type/property/links": [{
      "source": {
        "id": "/en/james_caviezel"
      },
      "target": {
        "id": "/en/mount_vernon_washington"
      }
    }],
    "id": "/people/person/place_of_birth"
   }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTypeLinkAttributionAsBangTypeAttributionLinks(self):
    """read /type/link/attribution as !/type/attribution/links."""

    query = """
    {
      "/people/person/place_of_birth": {
        "link": {
          "!/type/attribution/links": null
        },
        "id": null
      },
      "id": "/en/bob_dylan"
    }
    """
    exc_response = (
        error.MQLParseError,
        "Can't use reverse property queries in /type/link"
    )
    self.DoQuery(query, exc_response=exc_response)

  def testReadTypeLinkMasterPropertyAsBangTypePropertyLinks(self):
    """read /type/link/master_property as !/type/property/links."""

    query = """
    {
      "/people/person/place_of_birth": {
        "!/type/property/links": {
          "id": null
        }
      },
      "id": "/en/bob_dylan"
    }
    """
    exc_response = (
        error.MQLTypeError,
        "Can't reverse artificial property /type/property/links"
    )
    self.DoQuery(query, exc_response=exc_response)

  def testReadTypeAttributionLinks(self):
    """read /type/attribution/links."""

    query = """
    {
      "/type/attribution/links": [{
        "limit": 2,
        "source": {
          "id": null
        },
        "target": {
          "id": null
        }
      }],
      "id": "/user/warren"
    }
    """
    exp_response = """
    {
    "/type/attribution/links": [
      {
        "source": {
          "id": "/m/022q56s"
        },
        "target": {
          "id": "/common/document"
        }
      },
      {
        "source": {
          "id": "/m/022q56s"
        },
        "target": {
          "id": "/boot/all_permission"
        }
      }
    ],
    "id": "/user/warren"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testConstrainTypeAttributionLinks(self):
    """constrain /type/attribution/links."""

    query = """
    {
      "/type/attribution/links": {
        "source": {
          "id": "/guid/9202a8c04000641f80000000042b14d8"
        },
        "target": {
          "id": "/common/document"
        }
      },
      "id": "/user/warren"
    }
    """
    exp_response = """
    {
      "/type/attribution/links": {
        "source": {
          "id": "/guid/9202a8c04000641f80000000042b14d8"
        },
        "target": {
          "id": "/common/document"
        }
      },
      "id": "/user/warren"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testReadTimestampOfTopLevelTypeLinkQuery(self):
    """read timestamp of top-level /type/link query."""

    query = """
  [{
  "limit":     2,
  "timestamp": null,
  "type":      "/type/link"
  }]
    """
    exp_response = """
  [
    {
      "timestamp": "2006-10-22T07:34:24.0004Z",
      "type":      "/type/link"
    },
    {
      "timestamp": "2006-10-22T07:34:24.0005Z",
      "type":      "/type/link"
    }
  ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testTypeLinkQueryWithOptionalTargetValue(self):
    """/type/link query with optional target_value."""

    query = """
    [
      {
        "source": {
          "id": "/en/bob_dylan"
        },
        "limit": 2,
        "type": "/type/link",
        "target": {
          "id": null
        },
        "target_value": null
      }
    ]
    """
    exp_response = """
    [
      {
        "source": {
          "id": "/en/bob_dylan"
        },
        "type": "/type/link",
        "target": {
          "id": "/boot/all_permission"
        },
        "target_value": null
      },
      {
        "source": {
          "id": "/en/bob_dylan"
        },
        "type": "/type/link",
        "target": {
          "id": "/common/topic"
        },
        "target_value": null
      }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testTypeLinkQueryWithRequiredTargetValue(self):
    """/type/link query with required target_value."""

    query = """
    [
      {
        "source": {
          "id": "/en/bob_dylan"
        },
        "limit": 2,
        "type": "/type/link",
        "target": {
          "id": null
        },
        "target_value": {
          "value": null
        }
      }
    ]
    """
    exp_response = """
    [
      {
        "source": {
          "id": "/en/bob_dylan"
        },
        "type": "/type/link",
        "target": {
          "id": "/lang/en"
        },
        "target_value": {
          "value": "Robert Zimmerman"
        }
      },
      {
        "source": {
          "id": "/en/bob_dylan"
        },
        "type": "/type/link",
        "target": {
          "id": "/lang/he"
        },
        "target_value": {
          "value": "\u05d1\u05d5\u05d1 \u05d3\u05d9\u05dc\u05df"
        }
      }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

if __name__ == '__main__':
  mql_fixture.main()
