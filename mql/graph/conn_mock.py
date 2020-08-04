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
"""Connector classes for mocked graphd query and response strings.

Use these connectors when using the pymql library.
See test/mql_fixture.py in pymql for a reference
of how to use the record and replay connectors.
"""

__author__ = 'bneutra@google.com (Brendan Neutra)'
import sys
import hashlib
import re
import time
from pymql.mql import error
from pymql.mql.graph.connector import GraphConnector
from pymql.mql.grparse import ReplyParser
from absl import logging


class GraphMockException(Exception):
  pass


TIMEOUT_POLICIES = {
    'default': {
        'timeout': 8.0,
        'stubby_deadline': 10.0,
        'fail_fast': False,
    },
    'bootstrap': {
        'timeout': 2.0,
        'stubby_deadline': 4.0,
        'fail_fast': False,
    },
}


class MockRecordConnector(GraphConnector):
  """Mock connector for recording graphd responses.

  This class will append to the mockdata dictionary that it
  is handed. It interacts with a slightly modified
  live connector that you specify
  (e.g. the mock stubby connector)
  See test/mql_fixture.py for an implentation example.
  """

  def __init__(self, mockdata, connector, **kwargs):

    if not kwargs.get('policy_map', None):
      kwargs['policy_map'] = TIMEOUT_POLICIES
    GraphConnector.__init__(self, **kwargs)
    self.mockdata = mockdata
    self._conn = connector
    self._conn._save_raw_response = True
    self._mocked = {}

  def open(self, policy=None):

    self._conn.open(policy)

  def transmit_query(self, q, policy, deadline, **kwargs):

    try:
      result = self._conn.transmit_query(q, policy, deadline)
    except error.MQLTimeoutError:
      self.gen_mock_data(q, self._conn._raw_response)
      self.totalcost = self._conn.totalcost
      raise

    self.gen_mock_data(q, self._conn._raw_response)
    self.totalcost = self._conn.totalcost
    return result

  def reset_cost(self):
    if hasattr(self, '_conn'):
      self._conn.reset_cost()

  def gen_mock_data(self, q, result):

    k, hsh = strip_mock_query(q)
    if hsh in self._mocked:
      # if a query has been seen before, assume it needs another
      # version of the response mocked.
      self._mocked[hsh] += 1
      hsh = hsh + '_' + str(self._mocked[hsh])
    else:
      self._mocked[hsh] = 0
    self.mockdata[hsh] = [k, result]


class MockReplayConnector(GraphConnector):
  """Mock connector for recording graphd responses.

  This class will read from the mockdata dictionary that it
  is handed. It doesn't connect or interact with graphd.
  It's faster and more reliable than talking to a live db.
  See test/mql_fixture.py for an implentation example.
  """

  def __init__(self, mockdata):
    # don't connect to a graph, do not call __init__
    self.no_timeouts = False
    self.totalcost = {}
    self.mockdata = mockdata
    self._mocked = {}

  def open(self, policy=None):
    pass

  def transmit_query(self, q, policy, deadline, **kwargs):
    start_time = time.time()
    logging.debug('mocking query: %s', q)
    k, hsh = strip_mock_query(q)

    if hsh in self._mocked:
      # we've seen this query before for this test
      # so increment as we did in record mode
      self._mocked[hsh] += 1
      hsh = hsh + '_' + str(self._mocked[hsh])
    else:
      self._mocked[hsh] = 0

    if hsh not in self.mockdata:
      msg = '%s NO MOCKED REPONSE for this query: %s' % (hsh, k)
      logging.error(msg)
      raise GraphMockException(msg)

    m = self.mockdata[hsh]
    msg = 'mock query found %s: %s' % (hsh, m[0])
    logging.debug(msg)
    logging.debug('mock response found: %s', m[1])
    rg = re.search(' dateline\=\"(\S+)\" ', m[1])
    self.dateline = None
    if rg:
      self.dateline = rg.groups()[0]

    reply_parser = ReplyParser()
    reply_parser.parse_full_reply(m[1])
    ret = reply_parser.get_reply()
    dbtime = time.time() - start_time
    self.add_graph_costs(ret.cost, dbtime, tries=1)
    return ret

  def _get_policy(self, policy=None):
    return None


def strip_mock_query(q):
  # strip off the id
  # note the query may be spread over multiple lines
  # but the directives should be on the first one.
  k = re.sub(' (id=\S+) ', ' ', q, count=1)

  # exception cases
  # timestamp stuff generated when creating mock responses is fine when it
  # comes time to replay, but mql does a scope query in realtime, not sure why
  # TODO(bneutra): why must MQL do this?
  p = re.compile('timestamp\>20\d\d-\d\d-\d\dT\d\d:\d\d:\d\d.\d+ ')
  if re.search(p, k):
    logging.debug('we saw a timestamp in the query %s', k)
  k = re.sub(p, 'timestamp>2010-09-23T00:00:00.000001 ', k)

  h = hashlib.sha1()
  h.update(k)
  hsh = h.hexdigest()
  return k, hsh
