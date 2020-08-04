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

"""Tests /freebase/object_hints/best_hrid resolution.

/freebase/object_hints/best_hrid specifies a persistent HRID
for an entity. This should be favored over the earlier MQL
algorithm for choosing an HRID based on namespace traversal
and various heuristics.
"""
__author__ = 'nix@google.com (Nick Thompson)'

import json
import random
import string

import google3
from pymql.mql import error
from pymql.test import mql_fixture

class HRIDTest(mql_fixture.MQLTest):
  """Tests HRID queries using mqlread."""

  def setUp(self):
    # NOTE: the mock graphd support is broken, so there is no best_hrid.yaml
    #self.SetMockPath('data/best_hrid.yaml')
    super(HRIDTest, self).setUp()
    self.env = {'user': '/user/mw_brendan'}

  def newNodeWithHRID(self, best_hrid):
    query = """
    {
      "create":"unless_exists",
      "/freebase/object_hints/best_hrid": "%s",
      "guid":null
    }
    """ % best_hrid
    self.DoQuery(query, mqlwrite=True)
    self.assertEquals(self.mql_result.result["create"],
                      "created")
    return self.mql_result.result["guid"]

  def query_assert(self, q, r, exc_response=None, type="mqlread", asof=None):
    self.env = {}
    if asof is not None:
      self.env["as_of_time"] = asof
    self.DoQuery(q, exp_response=r, exc_response=exc_response)

  def test_missing_hrid(self):
    """Test that MQL still finds an id even if best_hrid is not present"""
    q= '{"id":null, "guid":"#9202a8c04000641f8000000000092a01", "mid":null}'
    r= ('{"guid": "#9202a8c04000641f8000000000092a01",'
        '"id": "/en/sting","mid":"/m/0lbj1"}')
    self.query_assert(q,r)

  def test_good_hrid(self):
    """Test /type/type, a best_hrid that agrees with the MQL heuristics"""
    #  /m/0j == /type/type
    q= '{"id":null, "mid":"/m/0j", "/freebase/object_hints/best_hrid":null}'
    r= ('{"id": "/type/type","mid":"/m/0j",'
        '"/freebase/object_hints/best_hrid":"/type/type"}')
    self.query_assert(q, r)

  def test_hrid_override(self):
    """Create a new node with a bogus best_hrid.

    The old MQL heuristics will fail; check that best_hrid works.
    """
    best_hrid = ('/user/nix/random_test_hrid/' +
                 ''.join(random.choice(string.ascii_lowercase)
                         for x in range(16)))
    guid = self.newNodeWithHRID(best_hrid)

    q= (('{"id":null, "guid":"%(guid)s",'
         '"/freebase/object_hints/best_hrid":null}' %
         {"guid":guid}))
    r= (('{"id": "%(best_hrid)s","guid":"%(guid)s",'
         '"/freebase/object_hints/best_hrid":"%(best_hrid)s"}') %
         {"guid":guid,"best_hrid":best_hrid})
    self.query_assert(q, r)

if __name__ == '__main__':
  mql_fixture.main()
