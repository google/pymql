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

#
"""Legacy TCP GraphConnector.

This is a rewrite of the original GraphContext
which used TCP sockets to connect to graphd
"""

__author__ = 'nicholasv@google.com (Nicholas Veeser)'

# None of these method names are standard
# pylint: disable-msg=C6409

import random
import socket
import time

from pymql.error import GraphConnectionError
from pymql.log import LOG
from pymql.mql.error import GraphIsSnapshottingError
from pymql.mql.error import MQLConnectionError
from pymql.mql.error import MQLError
from pymql.mql.error import MQLReadWriteError
from pymql.mql.error import MQLTimeoutError
from pymql.mql.graph.connector import GraphConnector
from pymql.mql.grparse import coststr_to_dict
from pymql.mql.grparse import ReplyParser


class TcpGraphConnector(GraphConnector):
  """Class representing the original TCP connection to the graph."""

  BUILTIN_TIMEOUT_POLICIES = {
      # regular MQL queries
      'default': {
          'connect': 1.0,
          'timeout': 8.0,
          'down_interval': 300.0,
          'retry': [0.0, 0.0, 0.1],
          'dateline_retry': 5.0
      },
      'bootstrap': {
          'connect': 1.0,
          'timeout': 4.0,
          'down_interval': 10.0,
          'retry': [0.0, 0.1],
          'dateline_retry': 0.1
      },

      # you want a result instantly, or not at all.. and timing out
      # doesn't mark the server as out of commision
      'fast': {
          'connect': 1.0,
          'timeout': 0.2,
          'down_interval': 0.1,
          'retry': [0.0],
          'dateline_retry': 5.0
      },

      # autocomplete -- turns out we need to wait to get the
      # results, until we have an alternate way to link objects
      # together in the system
      'autocomplete': {
          'connect': 1.0,
          'timeout': 15.0,
          'down_interval': 300.0,
          'retry': [0.0, 0.0],
          'dateline_retry': 2.0
      },
      # batch -- don't break or timeout on regular looking queries
      'batch': {
          'connect': 8.0,
          'timeout': 60.0,
          'down_interval': 300.0,
          'retry': [0.0, 0.1, 0.3, 1.0, 3.0, 10.0, 30.0],
          'dateline_retry': 5.0
      },
      # crawl -- really spend a long time on queries
      'crawl': {
          'connect': 60.0,
          'timeout': 500.0,
          'down_interval': 300.0,
          'retry': [0.0, 0.1, 0.3, 1.0, 3.0, 10.0, 30.0, 90.0, 200.0, 600.0],
          'dateline_retry': 10.0,
      }
  }

  def __init__(self, addrs=None, **kwargs):
    if 'policy_map' not in kwargs:
      kwargs['policy_map'] = self.BUILTIN_TIMEOUT_POLICIES

    GraphConnector.__init__(self, **kwargs)

    if isinstance(addrs, (tuple, list)):
      self.addr_list = addrs
    else:
      raise GraphConnectionError(
          'Addresse(s) not provided.',
          http_code=500,
          app_code='/mql/backend/address_not_given')

    self.failures = {}
    self.open(policy=self.default_policy)

  def open(self, policy=None):
    """Open the first TCP connection."""

    if policy is None:
      policy = self.default_policy

    policy = self.timeout_policies[policy]

    self.dbretries = -1
    for retry_interval in policy['retry']:
      self.addr = self._pick_addr(policy)

      try:
        if retry_interval:
          time.sleep(retry_interval)

        self.dbretries += 1
        self.tcp_conn = TcpConnection(self.addr, policy['connect'])

        break  # got it

      except (MQLConnectionError, MQLTimeoutError), e:
        self._record_failure(self.addr)

    else:
      LOG.warning('graph.connect.error', str(e))
      raise e

    self.totalcost['gcr'] = self.dbretries
    LOG.debug('graph.connect', 'created and connected db', conn=self.tcp_conn)

  def _pick_addr(self, policy):
    """Pick a graph to make the next connect attempt to."""

    acceptable_time = (time.time() - policy['down_interval'])

    pick_list = [
        x for x in self.addr_list if self.failures.get(x, 0) < acceptable_time
    ]

    if not pick_list:
      # eek - everyone has failed in the past 5 minutes
      LOG.alert(
          'graph.connect.pick.failure',
          'All failed in past %d seconds' % policy['down_interval'],
          addresses=repr(self.addr_list))
      pick_list = self.addr_list  # open to the whole list

    i = random.randrange(len(pick_list))
    addr = pick_list[i]

    LOG.info('graph.connect.pick', addr)

    return addr

  def close(self):
    if self.tcp_conn is not None:
      self.tcp_conn.disconnect()

  def _record_failure(self, failed_addr):
    now = time.time()
    LOG.error('graph.connect.failed', failed_addr)
    self.failures[failed_addr] = now

  def _make_timeout(self, timeout, deadline):
    """Make a custom timeout based on a drop-dead time.

    No connections are allowed after the relative_deadline.

    Note that this should be used on a per-request basis, because
    as we make request that take time, the relative_deadline will
    change (well, it will approach 0 anyway)

    Args:
      timeout: timeout of the individual query
      deadline: of the whole query

    Returns:
      Value to timeout the next query for
    """

    if self.no_timeouts:
      return None

    if deadline is None:
      return timeout

    # new technique: if we have a deadline, then set the *socket*
    # timeout to that. tu= allowance will make sure that we don't
    # work too hard on a query
    return max(deadline - time.time(), 0)

  def transmit_query(self, msg, policy, deadline, **kwargs):
    """Transmit the query over TCP."""

    costs = []

    self.qretries = -1

    for retry_interval in policy['retry']:
      try:
        if retry_interval:
          time.sleep(retry_interval)

          if not self.tcp_conn:
            self.tcp_conn = TcpConnection(policy['connect'], self.addr)

          self.qretries += 1

          # Keep close to the connection.send
          LOG.notice('graph.request.start', '', policy=policy, addr=self.addr)

          start_time = time.time()

          timeout = self._make_timeout(policy['timeout'], deadline)

          self.tcp_conn.send_socket(msg + '\n', timeout)

          timeout = self._make_timeout(policy['timeout'], deadline)

          result = self.tcp_conn.wait_response(timeout)

          token_time = time.time()

          if result.cost is not None:
            request_cost = coststr_to_dict(result.cost)
            #request_cost['tg'] = (time.time() - start_time)
            request_cost['tg'] = (result.end_time - start_time)
            request_cost['tf'] = (token_time - start_time)

            # on success, the cost will be in req
            costs.append(request_cost)

          LOG.notice('graph.request.end', '')

          break

      except GraphIsSnapshottingError, e:
        # The connection is OK, but we need to break it
        # so that when we try again we'll reconnect somewhere else.
        self.tcp_conn.disconnect()
        self.tcp_conn = None
        self._record_failure(self.addr)
        cost = coststr_to_dict(e.cost)
        costs.append(cost)

      except MQLConnectionError, e:
        # only trap MQLConnectionError, not MQLTimeoutError.
        # most of the time a timeout error say "this query is too hard"
        # don't shop it around and force everyone else to timeout too.
        self._record_failure(self.addr)
        cost = coststr_to_dict(e.cost)
        costs.append(cost)

      except MQLError, e:
        # all other errors, collect the cost and reraise
        cost = coststr_to_dict(e.cost)
        costs.append(cost)
        raise

      finally:

        # accumulate all the costs from successes *and* failures
        for cost in costs:
          if cost:
            for k, v in cost.iteritems():
              self.totalcost[k] += v

    else:
      LOG.warning('graph.request.error', str(e))
      raise e

    self.nrequests += 1
    self.totalcost['gqr'] = self.qretries
    if 'mql_dbreqs' in self.totalcost:
      self.totalcost['mql_dbreqs'] += 1
    else:
      self.totalcost['mql_dbreqs'] = 1
    self.dateline = result.dateline

    return result


class TcpConnection(object):
  """TCP Connection to wrap a Unix Socket."""

  def __init__(self, addr, timeout):
    self.host, self.port = addr
    self.socket = None
    self.connect(timeout)

    self.pending = None
    self.reply_parser = ReplyParser()

  def connect(self, timeout):
    """Connect using the TCP Socket."""

    if timeout == 0:
      raise MQLTimeoutError(
          None,
          'No more time left to run queries in this request',
          host=self.host,
          port=self.port)
    if self.socket is not None:
      raise MQLConnectionError(
          None,
          'socket is already open',
          socket=str(self.socket),
          host=self.host,
          port=self.port)
    self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # turn off Nagle's algorithm (talk to Jutta)
    self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    # reset the timeout -- we must set it at least once per new socket
    self.socket.settimeout(timeout)

    try:
      self.socket.connect((self.host, self.port))

    except socket.gaierror, e:
      self.disconnect()
      raise MQLConnectionError(
          None,
          'Cannot resolve %(host)s:%(port)s',
          host=self.host,
          port=self.port,
          detail=list(e.args))
    except socket.error, e:
      self.disconnect()
      raise MQLConnectionError(
          None,
          'Cannot connect to %(host)s:%(port)s',
          host=self.host,
          port=self.port,
          detail=list(e.args))

  def disconnect(self):
    """Close the TCP socket."""

    if self.socket is None:
      return
    self.socket.close()
    self.socket = None

  def send_socket(self, s, timeout):
    """Send data s on socket.

    Send assumes the socket is open.
    the caller is responsible for calling connect() if necessary.

    Args:
       s: data to send
       timeout: socket timeout
    """

    if self.socket is None:
      raise MQLConnectionError(
          None, 'Send on disconnected socket', host=self.host, port=self.port)

    if timeout == 0:
      raise MQLTimeoutError(
          self.pending,
          'No more time in deadline to run queries',
          host=self.host,
          port=self.port)

      # sendall will block until the entire string is sent or timeout
    self.socket.settimeout(timeout)

    try:
      self.pending = s
      self.socket.sendall(s)

    except socket.timeout, e:
      self.disconnect()
      raise MQLConnectionError(
          self.pending,
          'Timeout sending query',
          host=self.host,
          port=self.port,
          detail=list(e.args))
    except socket.error, e:
      self.disconnect()
      raise MQLConnectionError(
          self.pending,
          'Error sending query',
          host=self.host,
          port=self.port,
          detail=list(e.args))

  def wait_response(self, timeout):
    """Wait for complete response from graphd.

    This may incur multiple socket reads.

    Args:
      timeout: socket timeout for read

    Returns:
      GRparser result.  See grparse.ReplyParser
    """
    if self.socket is None:
      raise MQLConnectionError(
          None, 'Write on disconnected socket', host=self.host, port=self.port)

    try:
      # bug 5826 -- sometimes we are ready the first time we call this. Why?
      if self.reply_parser.isready():
        reply = self.reply_parser.get_reply()
        reply.end_time = time.time()
        LOG.error(
            'graph.read.reply',
            'saw reply before first socket read',
            reply=reply)
        return reply

      self.socket.settimeout(timeout)

      while 1:
        try:
          b = self.socket.recv(8192)
        except socket.timeout, e:
          # we disconnect before we raise MQLConnectionError. That way if
          # the MQLConnectionError is not caught we don't leave a dangling
          # connection (with graphd still doing work behind it.)
          self.disconnect()
          raise MQLTimeoutError(
              self.pending,
              'Query timeout',
              host=self.host,
              port=self.port,
              detail=list(e.args))
        except socket.error, e:
          self.disconnect()
          raise MQLConnectionError(
              self.pending,
              'Error receiving response',
              host=self.host,
              port=self.port,
              detail=list(e.args))

        # weirdly enough, this is how python signals a closed nonblocking
        # socket.
        if not b:
          self.disconnect()
          raise MQLReadWriteError(
              self.pending,
              'Connection closed by graphd',
              host=self.host,
              port=self.port)

        else:
          # we may have got a \n -- record the time
          last_parse_time = time.time()
          self.reply_parser.parsestr(b)

        if self.reply_parser.isready():
          reply = self.reply_parser.get_reply()
          reply.end_time = last_parse_time
          return reply

    finally:
      self.pending = None
