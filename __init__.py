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
"""
 Client for Processing MQL queries.

This class exposes two main services, read() and write(), which perform
mql reads and mql writes. Both take any varenv params as keyword args.
(There are no envelopes here).

See http://www.freebase.com/docs/web_services for more information
"""
__author__ = "rtp@google.com (Tyler Pirtle)"

import collections
import copy
import logging
import time

from mql import error as mql_error
from mql.graph import TcpGraphConnector
from mql.hijson import HighQuery
from mql.lojson import LowQuery


class InvalidGraphAddr(Exception):
  pass


class MQLService(object):
  """Entry point for making MQL requests to the graph.

     see google3/metaweb/freebase/api, which provides
     the stubby interface for mql queries. It should be
     the only user of this library,

  """

  dollar_keys = [
      "user", "privileged", "lang", "permission", "authority", "attribution"
  ]

  MQLResult = collections.namedtuple("MQLResult", "result cost dateline cursor")

  def _fix_varenv(self, env):
    """Make a copy of self.varenv, update it with env."""
    dollared_env = dict([
        ("$" + k, v) for k, v in env.items() if k in self.dollar_keys
    ])

    not_dollared = dict([
        (k, v) for k, v in env.items() if k not in self.dollar_keys
    ])

    if "as_of_time" in not_dollared:
      not_dollared["asof"] = not_dollared["as_of_time"]
      del not_dollared["as_of_time"]

    # externally, it's "debug_token", pymql internal it's "tid",
    # in graphd it's the "id" field. legacy stuff.
    if "debug_token" in not_dollared:
      not_dollared["tid"] = not_dollared["debug_token"]
      del not_dollared["debug_token"]

    varenv = copy.deepcopy(self.varenv)
    varenv.update(dollared_env)
    varenv.update(not_dollared)
    # convert 'deadline' to an absolute
    # unix epoch deadline and set the var
    deadline = varenv.get("deadline")
    if deadline:
      varenv["epoch_deadline"] = time.time() + deadline
    return varenv

  def __init__(self, connector=None, graphd_addrs=None):
    """Initialize a MQLService with a connector."""
    self.varenv = {}

    if connector is not None:
      self.gc = connector
    elif graphd_addrs:
      addr_list = list(self._parse_graphaddr(graphd_addrs))
      self.gc = TcpGraphConnector(addr_list)

    else:
      raise Exception("Must supply an address list or connector")

    self.gc.open()

    low_querier = LowQuery(self.gc)
    self.high_querier = HighQuery(low_querier)

  def _parse_graphaddr(self, addrs):
    for g in addrs:
      if isinstance(g, str):
        addr = g.split(":")
        if len(addr) < 2:
          logging.warn("graph addr [%s] is malformed (missing :port)", g)
          continue
        yield (addr[0], int(addr[1]))

      elif isinstance(g, tuple):  # better be a tuple.
        yield g

      else:
        raise InvalidGraphAddr(g)

  def get_cost(self):
    return self.gc.totalcost

  def reset_costs(self):
    self.gc.reset_cost()
    self.high_querier.reset_cost()

  def read(self, query, **varenv):
    """Initiate a read of the specified query.

    Args:
      query: dict/json obj, mql query
      varenv: dict/json obj, options, key/vals: as_of_time (optional)
          timestamp string e.g. "2013-01-01T00:00:00.0000" or less precise.
            graph responses will be as though the query were made at this time.
            cursor (optional) None/True or string returned from a previous query
          (query with a "limit": n directive) which allows a paging mechanism
            for the database to provide the next set of n results. To request
            the first cursor use True. deadline (optional) float, timeout in
            seconds for this request, feeds into epoch_deadline escape
            (optional) boolean, default True (in effect) turns on cgi escaping
            of string values. lang (optional) string, lang id, default
            "/lang/en" project_id (optional) string, the project id that the
            request should use quota from. query_timeout_tu int or None, if
            provided, each resulting graph query will be cpu user-time
            constrained by this number of ms. Think
          of it as limiting the work done by the db. Note: a mql query can
            result in an arbitrarily large number of graph queries, so even a
            small value here could result in a lot of work done.
            uniqueness_failure (optional)
          string: 'hard' or 'soft', default 'hard'. If a query constraint is
            null or {}, 'soft' won't complain if a list is returned
            write_dateline string, which must be a valid dateline returned by a
            previous mql query. In proper practice, this should be a dateline
            returned by a mqlwrite, thus the name 'write_dateline'. This
            dateline is passed to the graph db replica and requires that the
            replica poll until it is caught up to the dateline that you provide
            (the dateline represents the primitive index count, i.e. the hex
            value of the latest guid + 1). It has the effect of ensuring the
            replica is up to date with the users last update to the database. If
            the replica is not up to date, it polls, until it gets there or
            times out (lagging graphs could timeout) The assumption is that the
            user only needs a level of freshness up to the last write that they
            did.
          So, the basic pattern is: use the write_dateline they provide for all
            reads, until they do a write and then
          return them a new dateline. see: go/graphd-dateline debug_token
            (optional)
            string: unique string to aid in debugging requests
        DEPRECATED: normalize, extended

      Returns:
        response: json object, query result
        cost: dict, of various cost key/val pairs
        dateline: string, see description of write_dateline above
          not sure how this, the latest dateline received, is
          being used. frapi doesn't pass it on. In proper use
          the user should only need a new dateline when doing
          a mqlwrite.
        cursor: string, a cursor to be used in subsequent paging
         queries.

      Raises: various exceptions
    """

    self.reset_costs()
    env = self._fix_varenv(varenv)
    if env.get("cursor"):
      query = sort_query_keys(query)
    logging.debug("pymql.read.start env: %s query: %s", env, query)

    r = self.high_querier.read(query, env)

    cost = self.get_cost()
    logging.debug("pymql.read.end env: %s cost: %s", env, cost.items())
    result = self.MQLResult(r, cost, env.get("dateline"), env.get("cursor"))
    return result

  def write(self, query, **varenv):
    """Initiate a write of the specified query using the GraphConnector.

    Args:
      query: dict/json obj, mql query
      varenv: dict/json obj, options, key/vals: attribution (optional) string,
        id of freebase attribution object This will be written as the
        attribution link for primitives written authority (optional) object,
        Allows requests to be made with the attribution of the current user, but
        using the permissions of the user specified by this param. Another
        dangerous one; improper exposure to the outside world could result in
        unwanted escalation of privileges. deadline (optional) float, timeout in
        seconds for this request, feeds into epoch_deadline escape (optional)
        boolean, default True (in effect) turns on cgi escaping of string
        values. lang (optional) string, lang id, default "/lang/en" permission
        (optional) string, id of freebase permission object This param should
        only be used for certain low-level operations by members of the
        Freebase/Metaweb team. privileged (optional) object, this object when
        passed as the privileged field enables you to pass another user id as
        the authority field the write will still be attributed to 'user', but
        'authority' permissions will be checked in addition to 'user'
        permissions. project_id (optional) string, the project id that the
        request should use quota from. query_timeout_tu int or None, if
        provided, all graph queries will be cpu user-time constrained by this
        number of ms. Think
          of it as limiting the work done by the db. Note: a mql query can
            result in an arbitrarily large number of graph queries, so even a
            small value here could result in a lot of work done. user (required)
            string, freebase user id e.g. "/user/brendan" write_dateline string,
            see description in read method, datelines have the same effect on
            writes, they ensure the db replica you are talking to is up to date
            with the user's last write state before doing the write. This is
            necessary since a many graph reads happen as part of a write i.e.
            the write may require data from a previous write in order to
            complete correctly. see go/graphd-dateline debug_token (optional)
            string: unique string to aid in debugging requests
        DEPRECATED: normalize, extended

      Returns:
        response: json object, query result
        cost: dict, of various cost key/val pairs
        dateline: string, see description of write_dateline above
        cursor: string, a cursor to be used in subsequent paging
         queries.

      Raises: various exceptions
    """

    self.reset_costs()
    env = self._fix_varenv(varenv)

    if not "$user" in env:
      raise mql_error.MQLAccessError(
          None, "You need to specify a user to write with.")
    logging.debug("pymql.write.start env: %s query: %s", env, query)
    r = self.high_querier.write(query, env)
    cost = self.get_cost()
    logging.debug("pymql.write.end env: %s cost: %s", env, cost.items())
    result = self.MQLResult(r, cost, env.get("write_dateline"),
                            env.get("cursor"))
    return result

  def normalize(self, query):
    """Normalize the specified query.  TODO(rtp) What does this actually do?"""
    self.reset_costs()
    r = self.read(query, normalize_only=True)
    result = self.MQLResult(r.result, r.cost, r.dateline, r.cursor)
    return result


def sort_query_keys(part):
  """sort keys in place.

  We do this to every mqlread with a cursor because
  graphd relies on GQL query string order to
  maintain the state of the cursor.

  This calls itself recursively sorting keys
  in any ply of the query that is a dict

  Args:
    part: any ply of your dict

  Returns:
    an OrderedDict where all keys have been sorted
    in every ply of the query.
  """

  if isinstance(part, list):
    new_d = []
    for item in part:
      new_d.append(sort_query_keys(item))
    return new_d
  elif isinstance(part, dict):
    new_d = collections.OrderedDict()

    for k in sorted(part.keys()):
      new_d[k] = sort_query_keys(part[k])
    return new_d
  else:
    return part
