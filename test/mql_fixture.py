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
"""mql query language test fixture.

Accomodates both simple query/expected tests using DoQuery
as well as more complex scenarios where different users, permissions
and writes are involved.
TODO(bneutra): Support user "login" and creation.

TODO(bneutra): mockmode is broken due to python random hash
ordering. default in config.cfg is --mockmode=nomock

In mock 'record' mode, the tests talk to a live (but not production)
graph. Fake, temporary users need to be created, those users
perform actual writes. All graph responses are recorded for future
test runs in mock mode.
"""

__author__ = 'bneutra@google.com (Brendan Neutra)'


from collections import OrderedDict
import os
import sys
import time

import google3

import pymql
from pymql.mql import error
from pymql.mql import graph
import simplejson
import yaml

from google3.pyglib import flags
from google3.pyglib import logging
from google3.pyglib import resources
from google3.testing.pybase import googletest

FLAGS = flags.FLAGS

# e.g. blade:freebase-graphd-sandbox (do not run tests against otg!)
flags.DEFINE_string('graphd_addr', None, 'gslb or /bns name of graphd service')
flags.DEFINE_enum('mockmode', 'replay', ['replay', 'record', 'nomock'],
                  '"record" or "replay" or "nomock"')


class MQLTest(googletest.TestCase):
  """test fixture for all mql tests.

  Attributes:
    env: dictionary passed to the MQLService
    mql_result: MQLService result object (includes response, costs, etc)
    json_query: query as a json dict
    json_response: just the query response
  """

  MOCK_DATA = None
  MOCKFILE_PATH = None
  STUBBY_CONN = None
  COSTS = {}

  @classmethod
  def _GetStubbyConnector(cls):
    if not cls.STUBBY_CONN:
      if not FLAGS.graphd_addr:
        raise Exception('Must specified graphd_addr to use StubbyConnector')

      cls.STUBBY_CONN = graph.StubbyGraphConnector(
          graphd_addr=FLAGS.graphd_addr
          )
      logging.info('init Stubby Connector')

    return cls.STUBBY_CONN

  @staticmethod
  def SetMockPath(path):
    # Only load the MOCK_DATA once per run.
    # Save it in the base test class
    if not MQLTest.MOCK_DATA:
      MQLTest.MOCKFILE_PATH = PrependBase(path)
      if FLAGS.mockmode == 'replay':
        logging.info('load mockfile data %s', MQLTest.MOCKFILE_PATH)
        MQLTest.MOCK_DATA = yaml.load(open(MQLTest.MOCKFILE_PATH).read())
      elif FLAGS.mockmode == 'record':
        MQLTest.MOCK_DATA = {}

  def getFuzz(self, testid):
    """to be used to uniquify query strings, optionally."""

    fuzz = str(round(time.time(), 1))
    if FLAGS.mockmode == 'replay':
      # grab the fuzz from the replay data
      fuzz = MQLTest.MOCK_DATA[testid]
    elif FLAGS.mockmode == 'record':
      # record the fuzz in the replay data
      MQLTest.MOCK_DATA[testid] = fuzz
    return fuzz

  def setUp(self):
    """all tests start with this."""

    if FLAGS.mockmode == 'replay':
      assert MQLTest.MOCK_DATA is not None, 'Must call SetMockPath()'
      logging.debug('mockmode: replay')
      self.conn = graph.MockReplayConnector(MQLTest.MOCK_DATA)

    elif FLAGS.mockmode == 'record':
      assert MQLTest.MOCK_DATA is not None, 'Must call SetMockPath()'
      logging.debug('mockmode: record')
      self.conn = graph.MockRecordConnector(MQLTest.MOCK_DATA,
                                            self._GetStubbyConnector())

    elif FLAGS.mockmode == 'nomock':
      self.conn = self._GetStubbyConnector()

    self.mql_service = pymql.MQLService(connector=self.conn)
    self.dateline = None
    self.env = {}
    self.mql_result = None
    self.json_query = None
    self._dumped_query = None
    self.json_response = None
    self._dumped_response = None

  def MQLQuerier(self, q, mqlwrite=False):
    """do a mqlread.

    Args:
      q: the mql query (json string)
      mqlwrite: boolean, else do mqlread
    """
    self.json_query = simplejson.loads(q, object_pairs_hook=OrderedDict)
    self._dumped_query = simplejson.dumps(self.json_query, indent=2)
    if self.dateline:
      logging.debug('including write_dateline %s', self.dateline)
      self.env['write_dateline'] = self.dateline
    logging.debug('mql query:\n%s', self._dumped_query)
    if mqlwrite is True:
      logging.debug('doing mqlwrite')
      self.mql_result = self.mql_service.write(self.json_query, **self.env)
      self.dateline = self.mql_result.dateline
    else:
      logging.debug('doing mqlread')
      self.mql_result = self.mql_service.read(self.json_query, **self.env)
    self._dumped_response = simplejson.dumps(self.mql_result.result, indent=2)
    # for some reason the when asserting on utf-8 unicode objects
    # the object returned by mql represents the string as hex utf-16
    # while the json expected object represents as utf-8
    # same underlying data...
    # this makes the comparison apples to apples
    self.json_response = simplejson.loads(self._dumped_response)
    logging.debug(
        'mql response:\n%s', self._dumped_response
    )

  def DoQuery(
      self,
      query,
      mqlwrite=False,
      exp_response=None,
      exc_response=None
  ):
    """test a query.

    Runs a mql query and asserts on the expected result
    or expected exception.

    Args:
      query: json string that is a mql query
      mqlwrite: boolean, else do mqlread
      exp_response: json string that matches the expected response
      exc_response: expected exception. tuple: exception class, msg

    Raises:
      AssertionError
    """
    exc = None
    msg = None
    try:
      self.MQLQuerier(query, mqlwrite)
    except (
        # add expected exceptions here
        error.MQLParameterizedError,
        error.MQLError,
        error.MQLParseError,
        error.MQLInternalError,
        error.MQLTypeError,
        error.MQLResultError,
        error.MQLInternalParseError,
        error.MQLAccessError,
        error.MQLTimeoutError,
        error.MQLGraphError,
        error.MQLDatelineInvalidError,
        error.MQLConnectionError,
        error.GraphIsSnapshottingError,
        error.MQLReadWriteError,
        error.NamespaceException
    ):
      exc = sys.exc_info()[0]
      msg = str(sys.exc_info()[1])
      exc_actual = (exc, msg)
      exc_str = 'exception encountered: %s msg: %s' % exc_actual
      logging.debug(exc_str)
      if not exc_response:
        self.fail(
            'exception. was not expected: %s' % exc_str
        )

    if exc_response:
      if not msg:
        self.fail(
            'we expected an exception but did not get one: %s %s'
            % exc_response
        )
      self.AssertErrorEqual(exc_actual, exc_response)

    elif exp_response:
      self.AssertMQLEqual(
          simplejson.loads(exp_response)
      )
    else:
      # the calling test should do some kind of assert
      logging.debug('no expected response was given for this query')

  def AssertMQLEqual(self, exp_response):
    """also log the query, response and expected in event of failure.

    Args:
      exp_response: expected mql response (dict, list, or None)

    Raises:
      AssertionError
    """
    try:
      response = self.json_response
      if isinstance(exp_response, list):
        response = {'response': response}
        exp_response = {'response': exp_response}
      if exp_response is None:
        self.assertEquals(response, None)
      else:
        self.assertDictEqual(
            response, exp_response
        )
    except AssertionError:
      msg = (
          'incorrect response\nquery:\n'
          '%s\nresponse:\n%s\nexpected response:\n%s' % (
              self._dumped_query,
              self._dumped_response,
              simplejson.dumps(exp_response, indent=2)
          )
      )
      logging.error(msg)
      raise

  def AssertErrorEqual(self, exc_actual, exc_response):
    """log the query, exception and expected exception in event of failure.

    Args:
      exc_actual: actual Exception class and msg string (tuple)
      exc_response: expected Exception class and msg string (tuple)

    Raises:
      AssertionError
    """
    try:
      logging.debug(
          'expected exception: %s %s',
          exc_response[0],
          exc_response[1]
      )
      self.assertEqual(exc_actual[0], exc_response[0])
      self.assertEqual(exc_actual[1], exc_response[1])
    except AssertionError:
      msg = (
          'expected a different error\nquery:\n'
          '%s\nexception:\n%s\nexpected:\n%s' % (
              self._dumped_query,
              exc_actual,
              exc_response
          )
      )
      logging.error(msg)
      raise

  def tearDown(self):
    """teardown."""

    logging.debug('teardown!')
    if FLAGS.mockmode == 'record':
      # It would be nice to only do this after the last testcase
      # but I'm not aware of a way to do that.
      logging.info('writing mockdata to %s', MQLTest.MOCKFILE_PATH)
      fl = open(MQLTest.MOCKFILE_PATH, 'w')
      fl.write(yaml.dump(self.conn.mockdata))
      fl.close()
    cost = self.mql_service.get_cost()
    if cost:
      for c in cost:
        if c in MQLTest.COSTS:
          MQLTest.COSTS[c] += cost[c]
        else:
          MQLTest.COSTS[c] = cost[c]

  @classmethod
  def tearDownClass(cls):
    logging.info('CUMULATIVE COSTS:%s', MQLTest.COSTS)


def PrependBase(rel_path):
  """Returns the absolute path to the requested file resource.

  Args:
    rel_path: string - Relative path to file
  Returns:
    string - Absolute path to resource.
      If resource startswith("/) then resource is returned
      else resource will have GetARootDirWithAllResources()
      prepended to it.

  Raises:
    ValueError: When constructed path does not exist. Or is not absolute.
  """
  rel_base = os.path.join(
      FLAGS.test_srcdir, 'google3', 'third_party', 'py', 'pymql', 'test'
  )
  resource = rel_base + '/' + rel_path
  if resource.startswith('/'):
    abs_path = resource
  else:
    base = resources.ParExtractAllFiles()
    if not base:
      base = resources.GetARootDirWithAllResources()
      abs_path = os.path.join(base, resource)

  if os.path.isabs(abs_path) and os.path.exists(abs_path):
    logging.debug('static file path found: %s', abs_path)
  else:
    raise ValueError(
        'File in pymql/test does not exist. Please create it. %s',
        rel_path
    )
  return abs_path


def main():
  flagfile = PrependBase('config.cfg')
  sys.argv.insert(1, '--flagfile=%s' % flagfile)
  googletest.main()
