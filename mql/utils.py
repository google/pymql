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

import string, re
import bisect

from collections import OrderedDict
from absl import logging as glogging
from pymql.log import LOG, log_util
#from pymql import json
import json
from error import MQLInternalParseError, MQLParseError
from base64 import urlsafe_b64encode, urlsafe_b64decode
from difflib import unified_diff
import zlib

__all__ = [
    "QueryDict", "QueryList", "ResultDict", "ResultList", "true", "false",
    "null", "Missing", "valid_relname", "valid_guid", "valid_timestamp",
    "valid_timestamp_op", "valid_value_op", "valid_comparison", "valid_idname",
    "valid_high_idname", "valid_key", "valid_precompiled_sort", "valid_mid",
    "is_direct_pointer", "follow_path", "dict_recurse", "elements", "element",
    "reserved_word"
]

true = True
false = False
null = None
Missing = object()

# these are the raw types used when we transform a json query into an
# annoted query tree. They are just like dicts and lists except that
# you can set attributes on them like .parent, etc


class QueryDict(OrderedDict):
  varenv = None  # the varenv for this query (why didn't I do this sooner?)
  terminal = None  # What type of terminal? D/L/V/N
  is_root = False  # Is this the root of a full query?
  parent_clause = None  # Containing clause to this one
  key = None  # key within our parent
  value = None  # hijson-only: value at terminal node
  list = None  # containing list, if any
  high_query = None  # lojson-only
  node = None  # lojson-only - QueryPrimitive instance for @
  link = None  # lojson-only - QueryPrimitive instance for :
  ordered = Missing  # lojson-only - QueryPrimitive instance for ? (order)
  unique_ns_check = None  # lojson-only - QueryPrimitive check for a unique namespace
  comparisons = None  # hijson-only - comparison operators
  property = None  # hijson-only - the corresponding property, if any
  implied = False  # hijson-only - is this a generated query clause?
  sort_var = None  # hijson-only - sorts on this node
  sort = None  #
  alternatives = None  # hijson-only - list of value alternatives for |= operator

  disconnected = False  # lojson-only - no matching query clause

  empty = False  # lojson-only -

  # these attributes are real attributes, but when they don't exist
  # that has a special meaning
  #default

  # these dont need a default and in fact accessing them before
  # they're written is an error
  #stype               # hijson-only - schema type

  # I think these are just debugging, but they aren't set by default
  #low
  #filter
  #sort

  def __init__(self, *args, **kwds):

    # pull out well-known attribute values
    self.__dict__.update(kwds)

    # pass *args to the dict c'tor. This allows
    # QueryDict({"a":"b","c":"d"},terminal="L") to work properly

    super(QueryDict, self).__init__(*args)

  def get_orig(self):
    # attempt to trace back to where we are.

    clause = self
    if (hasattr(clause, "original_query") and
        hasattr(clause.original_query, "high_query")):
      clause = clause.original_query.high_query

    while (not hasattr(clause, "original_query") and
           clause.parent_clause is not None):
      clause = clause.parent_clause

    if hasattr(clause, "original_query"):
      clause = clause.original_query

    return clause

  def high_elements(self, mode):
    return mode.high_elements_dict(self)


class QueryList(list):
  is_root = False
  ordered = None
  order_dict = None
  indexes = None

  def high_elements(self, mode):
    return mode.high_elements_list(self)


class ResultDict(dict):
  pass


class ResultList(list):
  pass


class ReadMode(object):

  def __str__(self):
    return "read"

  def high_elements_dict(self, query):
    return [query]

  def high_elements_list(self, query):
    if len(query) == 1 and isinstance(query[0], dict):
      return query

    raise MQLParseError(
        query,
        "Expected a dictionary or a list with one element in a %(mode)s (were you trying to write?)",
        mode="read")


class WriteMode(object):

  def __str__(self):
    return "write"

  def high_elements_dict(self, query):
    return [query]

  def high_elements_list(self, query):
    if len(query) > 0:
      for item in query:
        if not isinstance(item, dict):
          raise MQLParseError(
              query, "Expected a list of dictionaries", mode="write")
      return query

    raise MQLParseError(
        query,
        "Expected a dictionary or list of dictionaries here",
        mode="write")


class PrepareMode(object):

  def __str__(self):
    return "prepare"


class CheckMode(WriteMode):

  def __str__(self):
    return "check"


# now instanitate so that we get __eq__ and such
ReadMode = ReadMode()
WriteMode = WriteMode()
PrepareMode = PrepareMode()
CheckMode = CheckMode()


def encode_cursor(cursor):
  """
    dumbed down for now.. eventually we'd like to be smarter since
    cursors should always be ascii. Also, we need to sign cursors
    """
  if isinstance(cursor, basestring):
    return urlsafe_b64encode(zlib.compress(cursor, 9))
  return cursor


def decode_cursor(encoded_cursor):
  if isinstance(encoded_cursor, basestring):
    try:
      return zlib.decompress(urlsafe_b64decode(str(encoded_cursor)))
    except TypeError, e:
      # not base64
      raise MQLParseError(None, "Invalid cursor", cursor=encoded_cursor)
    except zlib.error:
      # base64, not zlib compressed.
      raise MQLParseError(None, "Invalid cursor", cursor=encoded_cursor)
  elif isinstance(encoded_cursor, bool):
    return encoded_cursor
  else:
    raise MQLParseError(
        None,
        "Invalid cursor -- must be a string, true (to start), or false (to finish)"
    )


# I don't necessarily use these, but I'd go insane it if the end user did!
reservedwords = frozenset(
    "meta typeguid left right datatype scope attribute relationship property link class future update insert delete replace create destroy default sort limit offset optional pagesize cursor index !index for while as in is if else return count function read write select var connect this self super xml sql mql any all macro estimate-count"
    .split())

# these are used in /type, but may not be used elsewhere
typeonlywords = frozenset(
    "guid id object domain name key type keys value timestamp creator permission namespace unique schema reverse"
    .split())


def reserved_word(word, allow_type=False):
  if word in reservedwords:
    return True
  elif not allow_type and word in typeonlywords:
    return True
  else:
    return False


# lojson valid comparisons
# verifying operators for certain slots
valid_value_op = re.compile(r"^value(\<\=|\>\=|\<|\>|\~\=)$").match
valid_timestamp_op = re.compile(r"^timestamp(\<\=|\>\=|\<|\>)$").match
valid_history_op = re.compile(r"^(newest|oldest)(\<\=|\>\=|\<|\>)$").match

# valid timestamps - some valid subset of 2006-06-06T17:26:50.123
__valid_timestamp_re = re.compile(
    r"^\d{4}(?:-\d\d(?:-\d\d(?:T\d\d(?:\:\d\d(?:\:\d\d(?:\.\d{1,4}Z?)?)?)?)?)?)?$"
).match


def valid_timestamp(timestamp):
  return __valid_timestamp_re(
      timestamp) and timestamp > "1970" and timestamp < "2100"


# validity checker for guids
valid_guid = re.compile("^#[" + string.hexdigits + "]{32}$").match

# and mids
valid_mid = re.compile("^/m/[0-9bcdfghjklmnpqrstvwxyz_]+$").match

# checker for relnames (ident with optional leading +-)
# Dae requested that guids (particularly typeguids) be allowed to stand in as relnames
# This way he can avoid constructions like
# "typeguid_18713342a356097bf2fc231" : { ":typeguid": "#187133...", ... }
# and use the more natural (still ugly)
# "#18713342a356097bf2fc231" : { ... }
# without :typeguid
__ident_str = r"(?:[A-Za-z0-9_]|\$[A-F0-9]{4})(?:(?:[A-Za-z0-9_-]|\$[A-F0-9]{4})*(?:[A-Za-z0-9_]|\$[A-F0-9]{4}))?"
valid_relname = re.compile("^(?:[-+!]|(?:" + __ident_str + "\\:))?(?:/?" +
                           __ident_str + "(?:/" + __ident_str + ")*|/|\\*|#[" +
                           string.hexdigits + "]{32})$").match

valid_key = re.compile("^" + __ident_str + "$").match

# an id is a list of / separated idents.
# XXX does it need to start with a / ?
# XXX When are ids valid versus relnames versus guids etc?
# is has_key/is_instance_of etc a valid idname or just a valid relname?
valid_idname = re.compile("^(?:/|/" + __ident_str + "(?:/" + __ident_str +
                          ")*)$").match

# property and type names have more restrictive rules that match the rules for javascript identifiers.
__high_ident_str = "[A-Za-z](?:_?[A-Za-z0-9])*"

# this is the validity checker for property and type names
valid_high_idname = re.compile("^(?:/|/" + __high_ident_str + "(?:/" +
                               __high_ident_str + ")*)$").match

# a high_idname with an optional prefix and optional leading ! for reversal.
valid_mql_key = re.compile("^(\\!)?(?:(" + __high_ident_str + ")\\:)?(/|/?" +
                           __high_ident_str + "(?:/" + __high_ident_str +
                           ")*)$").match


def key_compare(needed_key, query_key):
  if needed_key == query_key:
    return True
  m1 = valid_mql_key(needed_key)
  m2 = valid_mql_key(query_key)
  if not m1 or not m2:
    return False
  # test reverse flag:
  if m1.group(1) != m2.group(1):
    return False
  # test labels:
  if m1.group(2) and m2.group(2) and m1.group(2) != m2.group(2):
    return False
  if m1.group(2) and not m2.group(2):
    return False
  # test key name:
  return m1.group(3) == m2.group(3)


def find_key_in_query(key, query):
  for k, v in query.iteritems():
    if key_compare(key, k):
      return k
  return None


# MQL valid comparison - a high_idname with an optional prefix and a trailing comparison operator.
valid_comparison = re.compile("^(\\!)?(?:(" + __high_ident_str + ")\\:)?(/|/?" +
                              __high_ident_str + "(?:/" + __high_ident_str +
                              r")*)(\<\=|\>\=|\<|\>|\~\=|\|\=|\!\=)$").match


def make_comparison_truekey(reversed, prefix, idname):
  if prefix is not None:
    if reversed:
      truekey = reversed + prefix + ":" + idname
    else:
      truekey = prefix + ":" + idname
  else:
    if reversed:
      truekey = reversed + idname
    else:
      truekey = idname
  return truekey


# a direct pointer is a non-empty dict hung off @scope/:typeguid etc.
def is_direct_pointer(q):
  return isinstance(q, dict) and len(q) > 0


# how many components in this id? (0 for the root)
def id_length(id):
  return len([x for x in id.split("/") if len(x)])


# this follows a (very) basic path of the form
# \.?(ident)(\.(ident))*(\.?[@:](ident))?
# and returns the dict containing the final ident, and the value of that ident
#
# An exception will be thrown in cases of ambiguity.

# a key can include just "*"
__valid_sort_key = "[@:?]?[+-]?(?:" + __ident_str + "|\\*)"

__path_re = re.compile("^(?:([-+])\\.)?((?:(?:" + __valid_sort_key +
                       ")\\.)*)(" + __valid_sort_key + ")$")
__split_re = re.compile("\\.")

valid_precompiled_sort = re.compile("^[+-]\\$sort_\\d+$").match

__tilde_re = re.compile(r"((?:[^\"\\ ]|\\.)+)|\"((?:[^\"\\]|\\.)*)\"")


def parse_tilde_op(value):
  """
    Given a string like 'I "Love You" Fred' produce
    the three strings "I" "Love You" and "Fred".

    This allows us to fully use graphd ~= syntax even while we only accept one
    parameter.

    Understands that \" is literal and that an unmatched double quote should
    just be stripped.
    """
  result = []

  def get_res(m):
    if m.group(1):
      result.append(m.group(1))
    else:
      result.append(m.group(2))

  __tilde_re.sub(get_res, value)
  return result


def follow_path(q, path):
  """
    given a clause and a path, uses the path to walk down the clause. Also
    supports a leading +/- to specify direction (+ by default)

    some sample paths:
    -.key1.:value  - reversed sort, normal q["key1"][":value"]
    -.-key1.:value - reversed sort, reversed link q["-key1"][":value"]
    -key1.:value - normal sort, reversed key ["-key1"][":value"]
    """
  pathmatch = __path_re.match(path)
  if pathmatch is None:
    raise MQLInternalParseError(q, "%(path)s not a valid path", path=path)

  direction, identpart, valuepart = pathmatch.groups()
  if direction is None:
    direction = ""  # easier to deal with when direction
    # is always a string
  subq = q

  # now walk down identpart (i.e. "a.b.c.") walking down throug the
  # dictionaries or lists as we go.
  for ident in __split_re.split(identpart):
    if ident != "":
      subq = element(subq[ident])

  return subq, valuepart, direction


def dict_recurse(q):
  """
    yields flattened list of all dictionaries including q
    """
  if isinstance(q, dict):
    yield q
    for k, v in q.iteritems():
      for l in dict_recurse(v):
        yield l
  elif isinstance(q, list):
    for k in q:
      for l in dict_recurse(k):
        yield l


# glosses over the difference between a list of x's and a single x directly.
def elements(q, typeof):
  if isinstance(q, list):
    for k in q:
      if isinstance(k, typeof):
        yield k
      else:
        raise MQLInternalParseError(q, "Expected a list of dictionaries here")
  elif isinstance(q, typeof):
    yield q
  else:
    raise MQLInternalParseError(
        q,
        "Expected a dictionary or list here, got a %(t)s (%(q)s)",
        t=type(q),
        q=q)


# the item, or the first (and only) element in a list
def element(q):
  if isinstance(q, list):
    if len(q) == 1:
      return q[0]
    else:
      raise MQLInternalParseError(q, "Expected a single element here")
  else:
    return q


# this is grotty but used to do re-ordering by qprim:generate_new_order()
def incr_subseq(elems):
  # each elem is a list of 2+ items
  # item[0] - the sort order key
  # item[1] - what this algorithm will use
  # item[2+] - some reference to another struct
  best_subseq = []
  for elem in elems:
    elem[1] = None
    i = bisect.bisect(best_subseq, elem)
    if i > 0:
      elem[1] = best_subseq[i - 1]
    if i < len(best_subseq):
      best_subseq[i] = elem
    else:
      best_subseq.append(elem)

  rv = []
  if best_subseq:
    curr = best_subseq[-1]
    while curr:
      rv.append(curr)
      tmp = curr[1]
      curr[1] = None
      curr = tmp

  rv.reverse()

  return rv


EmptyListSingleton = []


def high_elements(query, mode):
  """
    Very fast way of iterating QueryDict and QueryList classes -
    dispatches based on type
    """
  return query.high_elements(mode)

  assert isinstance(
      query, (basestring, int, long,
              float)), "Expected primitive type, but got a %s" % type(query)

  return EmptyListSingleton


# In this case, 'one' is an original query, two is an already computed result.
# So we compute the other result here.
def mql_diff(query, (two, two_lbl), mss):
  result = mss.ctx.high_querier.read(query, mss.varenv)
  result_json = json.dumps(result, indent=True).splitlines()
  two_json = json.dumps(two, indent=True).splitlines()
  diff = "\\n".join(
      unified_diff(two_json, result_json, fromfile=two_lbl, tofile="pymql"))
  if diff:
    LOG.notice(
        "mql.compatability.diff",
        "found a diff",
        oriq_query=query,
        pymql=result,
        other_result=two,
        fromservice=two_lbl,
        diff=diff)
  else:
    LOG.notice("mql.compatability.nodiff", "output OK", query=query)
