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


"""Query sorting unittest for pymql."""

__author__ = 'bneutra@google.com (Brendan Neutra)'

import collections
import json

import google3
import pymql

from google3.testing.pybase import googletest

testdictpart = collections.OrderedDict({
    'propd': None,
    'propc': 'foo',
    'propb': [],
    'prope': {},
    'propf': 1.1,
    11: False
})

testdict = testdictpart.copy()

testdict['propa'] = testdictpart.copy()
testdict['propg'] = [testdictpart.copy(), testdictpart.copy()]
testdict['propg'][1]['propa'] = testdictpart.copy()


def IsSorted(part):
  """Check that all keys are sorted."""
  if isinstance(part, list):
    for p in part:
      if IsSorted(p) is False:
        return False
  elif isinstance(part, dict):
    if sorted(part.keys()) != part.keys():
      return False
    for k, v in part.iteritems():
      if IsSorted(v) is False:
        return False

  return True


class PymqlSortTest(googletest.TestCase):

  def testSorting(self):
    """basic sorting test."""
    sorted_dict = pymql.sort_query_keys(testdict)
    self.assertTrue(IsSorted(sorted_dict))
    self.assertFalse(IsSorted(testdict))

    # the dict should not change in meaning
    # need to convert to dict first.
    converted_dict = json.loads(json.dumps(testdict))
    converted_sorted_dict = json.loads(json.dumps(sorted_dict))
    # nice helper function that's order independent
    self.assertDictEqual(converted_sorted_dict, converted_dict)


if __name__ == '__main__':
  googletest.main()
