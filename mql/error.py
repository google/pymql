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

from pymql.log import LOG, log_util
import string

__all__ = [
    'MQLError', 'MQLParseError', 'MQLInternalError', 'MQLTypeError',
    'MQLResultError', 'MQLInternalParseError', 'NamespaceException',
    'MQLAccessError', 'MQLWriteQuotaError'
]
"""MQL specific parameterized exception to break the umbilical for now until we can refactor MQL exceptions to conform to the new parameterized error"""


class MQLParameterizedError(Exception):

  def __init__(self, msg, *args, **kwds):
    msg = str(msg)
    Exception.__init__(self, msg, *args)

    self.msg = msg
    self.kwds = kwds

  def __str__(self):
    if self.kwds:
      try:
        return self.msg % self.kwds
      except KeyError, e:
        return '%s %s' % (self.msg, self.kwds)
      except TypeError, e:
        return '%s %s' % (self.msg, self.kwds)

    else:
      return self.msg


class MQLError(MQLParameterizedError):
  """
    this class takes a clause, key and message.
    it produces an error object which starts
    at the root of the query and contains

    "error_key": key
    "error_message": message
    "error_guid" : guid

    at the appropriate point.
    """
  error_types = ('PARSE', 'RESULT', 'INTERNAL', 'TYPE', 'NAMESPACE', 'ACCESS',
                 'UNKNOWN', 'INTERNAL_PARSE', 'TIMEOUT', 'GRAPH', 'CONNECTION',
                 'EMQL', 'READWRITE', 'DATELINE_INVALID', 'CURSOR_INVALID',
                 'WRITE_QUOTA')

  def __init__(self, error_type, clause, message, cost=None, *args, **kwds):

    MQLParameterizedError.__init__(self, message, error_type, clause, *args,
                                   **kwds)
    if error_type not in self.error_types:
      error_type = 'UNKNOWN'

    self.error = {}
    self.error_type = error_type
    self.cost = cost
    if clause is not None:
      self.set_query(clause)

    self.error['code'] = self.get_error_id()

    # make sure we don't get ReadMode in a JSON response
    for key in self.kwds:
      if not isinstance(
          self.kwds[key],
          (basestring, int, long, float, dict, list, bool, type(None))):
        self.kwds[key] = str(self.kwds[key])

    self.error['info'] = self.kwds
    self.error['message'] = str(self)

    if self.error_type in ('INTERNAL', 'ACCESS', 'WRITE_QUOTA', 'UNKNOWN'):
      level = log_util.CRIT
    else:
      level = log_util.WARNING

    if self.error_type in ('GRAPH', 'CONNECTION', 'TIMEOUT'):
      # these log the graph query
      # there is an idempotent write query that we do to get a dateline
      # that returns an error - but it's actually harmless and we dont
      # want to log-it.
      if self.error.get('info') and self.error['info'].get(
          'detail', None) != 'primitive tagged as unique already exist':
        LOG.error(
            error_type.lower() + '_error',
            repr(self.error),
            gql=repr(getattr(self, 'graph_query', None)))

    # there's no graph query otherwise
    elif self.error_type in ('TYPE', 'DATELINE_INVALID', 'CURSOR_INVALID'):
      # this is probably a developer-level error, no need to LOG.error
      LOG.warn(error_type.lower() + '_error', repr(self.error))
    else:
      LOG.error(error_type.lower() + '_error', repr(self.error))

  def get_error_id(self):
    # this is the centralized place for error naming
    return '/api/status/error/mql/%s' % string.lower(self.error_type)

  def get_error(self):
    # I have wrapped this because I may want to compute the values dynamically
    # rather than in the c'tor.
    return self.error

  def get_kwd(self, arg):
    # don't want to externalize this knowledge either; you just want to know what you passed into the c'tor
    # not the place the class chose to store it.
    return self.error['info'].get(arg, None)

  def set_query(self, clause):
    # these three lower level errors take the graph query as the first argument (if any)
    if self.error_type in ('GRAPH', 'CONNECTION', 'TIMEOUT'):
      self.graph_query = clause
    else:
      self.clause = clause
      self.root = self.find_root(self.clause)
      (self.orig_error, self.key,
       self.path) = self.get_error_location(self.clause)
      self.error['query'] = self.root
      self.error['path'] = self.path

  def add_error_inside(self):
    # no guarantees about the type or existance of orig_error at all,
    # so we should make sure this is possible before we try it.
    if hasattr(self, 'orig_error'):
      if isinstance(self.orig_error, dict):
        self.orig_error['error_inside'] = self.key

  def find_root(self, clause):
    # we might get a list rather than a dict in some obscure situations
    # (mostly internalerrors about multiple results in "@scope": [{}]
    # and "key": { "namespace": [] })
    if isinstance(clause, list):
      clause = clause[0]

    # if it's not a dict we're not going to make much progress...
    if not isinstance(clause, dict):
      return clause

    # are we a lojson query for a highjson query?
    if (hasattr(clause, 'original_query') and
        hasattr(clause.original_query, 'high_query')):
      clause = clause.original_query.high_query

    while clause.parent_clause is not None:
      clause = clause.parent_clause

    is_list = False
    if clause.list is not None:
      is_list = True

    if hasattr(clause, 'original_query'):
      clause = clause.original_query

    if is_list:
      clause = [clause]

    return clause

  def get_error_location(self, clause):
    ### XXX this is wrong because it alters 'query'. Correct behaviour is to completely duplicate query...

    # we might get a list rather than a dict in some obscure situations
    # (mostly internalerrors about multiple results in "@scope": [{}]
    # and "key": { "namespace": [] })
    if isinstance(clause, list):
      clause = clause[0]

    # if it's not a dict we're not going to make much progress...
    if not isinstance(clause, dict):
      return (clause, None, None)

    # are we a lojson query for a highjson query?
    if (hasattr(clause, 'original_query') and
        hasattr(clause.original_query, 'high_query')):
      clause = clause.original_query.high_query

    # we may be deep in some autogenerated clause
    trailing_keys = []
    while (not hasattr(clause, 'original_query') and
           clause.parent_clause is not None):
      if clause.key is not None:
        trailing_keys.append(clause.key)
      clause = clause.parent_clause

    if hasattr(clause, 'original_query'):
      if not isinstance(clause.original_query, dict):
        if clause.parent_clause is not None:
          if clause.key is not None:
            trailing_keys.append(clause.key)
          clause = clause.parent_clause

    path_keys = []
    path_clause = clause
    while (path_clause.parent_clause is not None):
      path_keys.append(path_clause.key)
      path_clause = path_clause.parent_clause

    # we still may need to go one level deeper
    if hasattr(clause, 'original_query'):
      clause = clause.original_query

    # give the user some clue about how we got here...
    if trailing_keys:
      trailing_keys.reverse()
      key = '.'.join(trailing_keys)
    else:
      key = '.'

    path_keys.reverse()
    path = '.'.join(path_keys + trailing_keys)

    return (clause, key, path)


# these are all convenience classes.
class MQLParseError(MQLError):

  def __init__(self, *args, **kws):
    MQLError.__init__(self, 'PARSE', *args, **kws)


class MQLInternalError(MQLError):

  def __init__(self, *args, **kws):
    MQLError.__init__(self, 'INTERNAL', *args, **kws)


class MQLTypeError(MQLError):

  def __init__(self, *args, **kws):
    MQLError.__init__(self, 'TYPE', *args, **kws)


# This was a valid query and I tried, but
# the graph didn't match what you wanted. Sorry.
class MQLResultError(MQLError):

  def __init__(self, *args, **kws):
    MQLError.__init__(self, 'RESULT', *args, **kws)


class MQLValueAlreadyInUseError(MQLResultError):

  def __init__(self, query, key, existing_value, new_value, update=True):
    if update:
      msg = 'Found existing value for unique property, try update.'
    else:
      msg = 'This value is already in use. Please delete it first.'
    MQLResultError.__init__(
        self, query, msg, existing_value=existing_value, new_value=new_value)


class MQLTooManyValuesForUniqueQuery(MQLResultError):

  def __init__(self, query, results, count):
    msg = 'Unique query may have at most one result. Got %(count)d'
    MQLResultError.__init__(self, query, msg, results=results, count=count)


# A parse error in lowjson - MQL should never generate code
# that causes these to raise, but just in case...
class MQLInternalParseError(MQLError):

  def __init__(self, *args, **kws):
    MQLError.__init__(self, 'INTERNAL_PARSE', *args, **kws)


# a permission failure
class MQLAccessError(MQLError):

  def __init__(self, *args, **kws):
    MQLError.__init__(self, 'ACCESS', *args, **kws)


# exceeded daily primitive write limit, specified in 'max_writes' env var.
class MQLWriteQuotaError(MQLError):

  def __init__(self, *args, **kws):
    MQLError.__init__(self, 'WRITE_QUOTA', *args, **kws)


# a timeout on the query
class MQLTimeoutError(MQLError):

  def __init__(self, *args, **kws):
    MQLError.__init__(self, 'TIMEOUT', *args, **kws)


# an error return from graphd
class MQLGraphError(MQLError):

  def __init__(self, *args, **kws):
    MQLError.__init__(self, 'GRAPH', *args, **kws)


# the cursor you supplied is bad, possible from another query.
class MQLCursorInvalidError(MQLError):

  def __init__(self, *args, **kws):
    MQLError.__init__(self, 'CURSOR_INVALID', *args, **kws)


# your dateline instance is messed up.
class MQLDatelineInvalidError(MQLError):

  def __init__(self, *args, **kws):
    MQLError.__init__(self, 'DATELINE_INVALID', *args, **kws)


# failed to connect to graphd - retry another graph,
# if available
class MQLConnectionError(MQLError):

  def __init__(self, *args, **kws):
    MQLError.__init__(self, 'CONNECTION', *args, **kws)


class GraphIsSnapshottingError(MQLConnectionError):

  def __init__(self, *args, **kws):
    MQLError.__init__(self, 'SNAPSHOTTING', *args, **kws)


# we're connected, but when we we sent or received the query, the
# graph barfed/crashed/etc. Don't retry this
class MQLReadWriteError(MQLError):

  def __init__(self, *args, **kwds):
    MQLError.__init__(self, 'READWRITE', *args, **kwds)


# this is the "can't find guid for name XXX" exception
class NamespaceException(MQLParameterizedError):
  pass
