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
"""test the test fixture."""

__author__ = 'bneutra@google.com (Brendan Neutra)'

import sys
import google3
from pymql.mql import error
from pymql.test import mql_fixture
from google3.pyglib import logging


class MQLTest(mql_fixture.MQLTest):
  """for testing basic mqlread queries."""

  def setUp(self):
    self.SetMockPath('data/mql_fixture.yaml')
    super(MQLTest, self).setUp()
    self.env = {'as_of_time': '2009-10-01'}

  def DoQueryException(self, query, expected, **kwargs):
    """expect a failure."""
    try:
      self.DoQuery(query, **kwargs)
    except AssertionError:
      msg = str(sys.exc_info()[1])
      if not expected in msg:
        self.fail('expected: %s\ngot: %s' % (expected, msg))
      else:
        logging.debug('assertion raised, as expected! got: %s', expected)

  def testPositive(self):
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

  def testUnexpectedResponse(self):
    query = """
    {
      "id": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "id": "/n/bob_dylan"
    }
    """
    self.DoQueryException(
        query,
        '!=',
        exp_response=exp_response
    )

  def testUnexpectedError(self):
    query = """
    {
      "invalidkey": "/en/bob_dylan"
    }
    """
    exp_response = """
    {
      "id": "/n/bob_dylan"
    }
    """
    self.DoQueryException(
        query,
        'exception. was not expected',
        exp_response=exp_response
    )

  def testExpectError(self):
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

  def testExpectNoError(self):
    query = """
    {
      "guid": "#9202a8c04000641f8000000003abd178",
      "id": "/en/bob_dylan"
    }
    """
    self.DoQueryException(
        query,
        'exception. was not expected',
        exp_response='whatev'
    )

  def testExpectOtherError(self):
    query = """
    {
      "guid": "#9202a8c04000641f8000000003abd178",
      "id": "/en/bob_dylan"
    }
    """
    self.DoQueryException(
        query,
        "MQLParseError'> != <type 'exceptions.KeyError'>",
        exc_response=(KeyError, 'whatev')
    )

if __name__ == '__main__':
  mql_fixture.main()
