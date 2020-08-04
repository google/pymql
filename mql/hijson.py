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

import sys, os, re

if __name__ == '__main__':
  sys.path.append(os.path.abspath('../..'))

from pymql.log import LOG, log_util, pprintlog, dumplog
import json
#from pymql import json

# these should probably be in a separate utility class
from utils import (QueryDict, QueryList, element, high_elements, ReadMode,
                   WriteMode, CheckMode, valid_comparison,
                   make_comparison_truekey, valid_high_idname, valid_mql_key,
                   reserved_word, valid_guid, valid_key, valid_mid, Missing)
import schema

from readqp import ReadQP

import pprint
import traceback
import copy
import time
from pymql.mql.env import Varenv, DeferredGuidLookup, DeferredGuidLookups, Guid, \
    FixedGuidList, DeferredGuidOfMidLookup, DeferredGuidOfMidLookups, \
    DeferredGuidOfMidOrGuidLookups

from pymql.mql.error import MQLError, MQLParseError, MQLInternalError, MQLTypeError

from pymql.mql import mid
import resource


class HighQuery(object):
  # class data
  reserved_keys = set(['type', 'name', 'key', 'value', 'id'])
  read_directives = set([
      'optional', 'sort', 'limit', 'macro', 'return', 'count', 'estimate-count'
  ])
  write_directives = set(['create', 'connect'])
  directives = read_directives | write_directives

  def __init__(self, lowq, transaction_id=None, cached_lowq=None):

    varenv = {'tid': transaction_id}

    self.querier = lowq
    if cached_lowq:
      self.cached_querier = cached_lowq
    else:
      self.cached_querier = lowq

    self.reset_cost()

    self._has_right_order = None
    self._has_left_order = None
    self._is_unique_namespace = None
    self._has_key = None
    self._schema_factory = None
    self._init_varenv = varenv

  # Lazily load these.
  @property
  def has_right_order(self):
    if not self._has_right_order:
      self._has_right_order = Guid(
          self.querier.lookup.lookup_guid('/boot/has_right_order',
                                          self._init_varenv))
    return self._has_right_order

  @property
  def is_unique_namespace(self):
    if not self._is_unique_namespace:
      self._is_unique_namespace = Guid(
          self.querier.lookup.lookup_guid('/boot/is_unique_namespace',
                                          self._init_varenv))
    return self._is_unique_namespace

  @property
  def has_key(self):
    if not self._has_key:
      self._has_key = Guid(
          self.querier.lookup.lookup_guid('/boot/has_key', self._init_varenv))
    return self._has_key

  @property
  def schema_factory(self):
    if not self._schema_factory:
      self._schema_factory = schema.SchemaFactory(self.cached_querier,
                                                  self._init_varenv)
    return self._schema_factory

  @property
  def has_left_order(self):
    if not self._has_left_order:
      self._has_left_order = Guid(
          self.querier.lookup.lookup_guid('/boot/has_left_order',
                                          self._init_varenv))
    return self._has_left_order

  def reset_cost(self):

    cost_keys = ('mql_utime', 'mql_stime', 'mql_rtime')
    # add them to other costs
    for m in cost_keys:
      self.querier.gc.totalcost[m] = 0

    self.rusage_start = self.rusage_end = None
    self.start_time = 0

  def cost_start(self):
    self.rusage_start = resource.getrusage(resource.RUSAGE_SELF)
    self.start_time = time.time()

  def cost_end(self):

    # it would be silly to call just cost_end().
    if self.rusage_start is not None:

      rusage_end = resource.getrusage(resource.RUSAGE_SELF)

      mql_utime = self.querier.gc.totalcost.get('mql_utime', 0)
      mql_stime = self.querier.gc.totalcost.get('mql_stime', 0)
      mql_rtime = self.querier.gc.totalcost.get('mql_rtime', 0)

      self.querier.gc.totalcost['mql_utime'] = mql_utime + (
          rusage_end.ru_utime - self.rusage_start.ru_utime)
      self.querier.gc.totalcost['mql_stime'] = mql_stime + (
          rusage_end.ru_stime - self.rusage_start.ru_stime)
      self.querier.gc.totalcost['mql_rtime'] = mql_rtime + (
          time.time() - self.start_time)
      LOG.debug('mql.cost %s', self.querier.gc.totalcost)

      # and reset start.
      self.rusage_start = None

  def write(self, orig_query, orig_varenv):
    return self.write_or_check(orig_query, WriteMode, orig_varenv)

  def check(self, orig_query, orig_varenv):
    return self.write_or_check(orig_query, CheckMode, orig_varenv)

  def write_or_check(self, orig_query, mode, orig_varenv):

    self.cost_start()

    varenv = Varenv(orig_varenv, self.querier.lookup)

    transaction_id = varenv.get('tid')

    pprintlog(
        'MQL_%s' % str(mode),
        orig_query,
        transaction_id=transaction_id,
        level=log_util.DEBUG)
    query = self.resolve_schema(orig_query, mode, varenv)

    dumplog('RESOLVED_%s' % str(mode), query)

    low_query = self.build_low_json_root(query, varenv, mode)

    dumplog('LOW_%s' % str(mode), low_query)

    low_result = self.querier.internal_write_or_check(low_query, varenv, mode)

    dumplog('LOW_%s_RESULT' % str(mode), low_result)

    self.lookup_all_ids(query, varenv)

    dumplog('GUID_DICT', varenv.lookup_manager.guid_dict)

    result = self.create_high_result(query, low_result, varenv, mode)

    pprintlog(
        'MQL_%s_RESULT' % str(mode),
        result,
        transaction_id=transaction_id,
        level=log_util.DEBUG,
        pop=True)

    # and you always find out what vars were used (even if none)
    varenv.export(('vars_used', 'dateline'))

    # only export these if we really did a write, not if we just pretended.
    if mode is WriteMode:
      varenv.export(('write_dateline', 'last_write_time'))

    self.cost_end()

    return result

  def read(self, orig_query, orig_varenv):

    self.cost_start()

    if orig_varenv.get('normalize_only'):
      return self.normalize(orig_query, orig_varenv)

    varenv = Varenv(orig_varenv, self.querier.lookup)

    transaction_id = varenv.get('tid')

    LOG.debug('mql.query', '', mql=orig_query)

    try:
      mquery, gquery = self.create_graph_query(orig_query, varenv,
                                               transaction_id)
      gresult = self.graph_read(gquery, varenv)
      high_result = self.create_mql_result(mquery, gresult, varenv)

      LOG.debug('mql.result', '', mql=high_result)

      # yuck -- I hate the cursor side-effect too!
      varenv.export(('cursor', 'vars_used', 'dateline', 'write_dateline'))

      self.cost_end()

      return high_result

    except Exception, e:

      # XXX
      # This should be consolidated with the gathering above, but the
      # structure of the end of this function doesn't easily lend itself
      # to simple reorganization.
      self.cost_end()
      raise

  def to_gql(self, tid, mql, varenv):
    """
        Return the GQL constraints for a MQL query
        """

    varenv = Varenv(varenv, self.querier.lookup)
    x, gql = self.create_graph_query(mql, varenv, tid)

    return gql

  def normalize(self, orig_query, orig_varenv):
    varenv = Varenv(orig_varenv, self.querier.lookup)
    query = self.resolve_schema(orig_query, ReadMode, varenv)
    #pprint.pprint(query)
    nquery = self.create_normalized_result(query, varenv)
    pprintlog('MQL_NORMALIZE', orig_query, level=log_util.DEBUG)
    return nquery

  def resolve_schema(self, orig_query, mode, varenv):
    try:
      query = self.make_orig(orig_query, varenv, True)
      self.resolve_names(query, varenv, mode)
      return query
    except MQLTypeError, e:
      # debug ME-907
      LOG.debug(
          'mql.hijson.HighQuery.resolve_schema',
          '',
          orig_query=orig_query,
          varenv=varenv)

      # XXX fairly nasty hack for bug #889 and bug #892. If we can't find a type or property,
      # maybe it is new and not in our type cache. If so, flush the type cache and try again...
      transaction_id = varenv.get('tid')

      # FIX:
      #
      # Signal the desired cache behavior
      # explicitly with something like:
      #
      # varenv['max-age'] = 0
      # -or-
      # varenv['no-cache'] = True
      #
      # rather than overloading the lwt mechanism.

      # make sure we miss the schema cache
      varenv['last_write_time'] = int(time.time())

      pprintlog(
          'CACHE_FLUSH_%s' % str(mode),
          str(e),
          transaction_id=transaction_id,
          level=log_util.DEBUG,
          push=True)
      self.querier.lookup.flush()
      self.schema_factory.flush(varenv)
      pprintlog(
          'CACHE_FLUSH_%s_COMPLETE' % str(mode),
          str(e),
          transaction_id=transaction_id,
          level=log_util.DEBUG)

      try:
        query = self.make_orig(orig_query, varenv, True)
        self.resolve_names(query, varenv, mode)

        # if we get to here without throwing an MQLTypeError again, it meant the
        # strategy of flush the cache worked. We should log this to see how helpful
        # this bugfix was.
        pprintlog(
            'CACHE_FLUSH_%s_SUCCESS' % str(mode),
            str(e),
            transaction_id=transaction_id,
            level=log_util.WARNING,
            pop=True)

        return query

      except MQLTypeError, mt:
        raise

  def create_graph_query(self, orig_query, varenv, transaction_id):
    query = self.resolve_schema(orig_query, ReadMode, varenv)

    self.add_query_primitive_root(element(query), varenv, ReadMode)

    # two round trips.
    varenv.lookup_manager.do_mid_to_guid_lookups()
    # and..
    varenv.lookup_manager.do_guid_lookups()

    graph_query = []
    qpush = graph_query.append
    element(query).node.generate_graph_query(qpush)
    gquery = ''.join(graph_query)
    #print gquery
    return (query, gquery)

  def graph_read(self, query, varenv):
    # this is the real mql query so it gets the special flag to generate asof, cursor= etc.
    varenv['mql_query'] = True

    graph_result = self.querier.gc.read_varenv(query, varenv)

    # perhaps should be using varenv.copy() but this is OK for now...
    del varenv['mql_query']

    return graph_result

  def create_mql_result(self, query, graph_result, varenv):
    high_result = element(query).node.parse_result_root(graph_result, varenv)

    varenv.lookup_manager.do_guid_to_mid_lookups()
    varenv.lookup_manager.do_id_lookups()

    # this is ugly, but what else can I do???
    high_result = varenv.lookup_manager.substitute_ids(high_result)
    high_result = varenv.lookup_manager.substitute_mids(high_result)

    return high_result

  # the possible values for a terminal are 'D', 'L', 'V' and 'N'
  # (drugs, language, violence and nudity?)

  def make_orig(self, query, varenv, root=False):
    if isinstance(query, dict):
      rv = QueryDict(original_query=query, varenv=varenv)

      if query == {}:
        rv.terminal = 'D'

      if root:
        rv.is_root = True

      for key in query:
        if not isinstance(key, str):
          if isinstance(key, unicode):
            try:
              key = key.encode('ascii')
            except UnicodeEncodeError:
              raise MQLParseError(
                  None, 'Keys must be ASCII strings', key=key.encode('utf-8'))
          else:
            raise MQLParseError(None, 'Keys must be strings', key=str(key))

        if key in self.directives:
          # do we want to deep-copy lists of directives?
          rv[key] = self.validate_directive(key, query[key])
        else:
          rv[key] = self.make_orig(query[key], varenv)
          if isinstance(rv[key], dict):
            rv[key].parent_clause = rv
            rv[key].key = key
          elif isinstance(rv[key], list):
            for elem in rv[key]:
              if isinstance(elem, dict):
                elem.key = key
                elem.parent_clause = rv
                elem.list = rv[key]

    elif isinstance(query, list):
      rv = QueryList()
      if query == []:
        if root:
          raise MQLParseError(
              None,
              'Queries must start with a dictionary or list of dictionaries')

        q = QueryDict(original_query=query, terminal='L')
        rv.append(q)

      for elem in query:
        rvelem = self.make_orig(elem, varenv, root)
        rv.append(rvelem)

        if isinstance(rvelem, dict):
          rvelem.list = rv

    elif root:
      raise MQLParseError(
          None, 'Queries must start with a dictionary or list of dictionaries')

    elif isinstance(query, (float, int, long, bool)):
      rv = QueryDict(original_query=query, terminal='V', value=query)
    elif isinstance(query, str):
      if len(query) > 4096:
        raise MQLParseError(
            None, 'String greater than 4096 bytes in length', value=query)

      rv = QueryDict(original_query=query, terminal='V', value=query)

    elif query is None:
      rv = QueryDict(original_query=query, terminal='N', value=None)
    elif isinstance(query, unicode):
      # support unicode and silently transcode into utf-8
      newstr = query.encode('utf-8')
      if len(newstr) > 4096:
        raise MQLParseError(
            None, 'String greater than 4096 bytes in length', value=query)

      rv = QueryDict(original_query=query, terminal='V', value=newstr)
    elif isinstance(query, long):
      raise MQLParseError(
          None, "Integer value out of range '%(value)s'", value=str(query))
    else:
      raise MQLParseError(
          None,
          'Unsupported object %(object)s in query',
          object=str(type(query)))

    return rv

  def validate_directive(self, key, value):

    # currently just normalizes unicode strings if necessary -- performs no actual parameter checks.
    if isinstance(value, (str, float, int, long, bool)) or value is None:
      # null isn't really valid, but we'd rather solve that problem at the individual directive level for now.
      return value
    elif isinstance(value, unicode):
      try:
        return value.encode('ascii')
      except UnicodeEncodeError:
        raise MQLParseError(
            None,
            "'%(key)s' directive values must be ASCII strings",
            key=key,
            value=value.encode('utf-8'))
    elif isinstance(value, list):
      rv = QueryList()
      for val in value:
        rv.append(self.validate_directive(key, val))
      return rv
    else:
      # try to raise a good error (got complaints on developers@ about these ones)
      raise MQLParseError(
          None,
          "Can't use a dictionary as a value for '%(key)s' directive",
          key=key)

  # property resolution is always unique - the first acceptable property found is the resolved property.
  def resolve_property(self, query, key, stype, varenv, mode):
    if key == 'link':
      return schema.LinkProperty

    key_match = valid_mql_key(key)
    if not key_match:
      raise MQLParseError(query, '%(key)s is not a valid MQL key', key=key)

    if (stype.category != 'object') and key_match.group(1) is not None:
      raise MQLParseError(
          query,
          "Can't use reverse property queries in %(type)s",
          type=stype.id,
          key=key)


#        if (stype.category != 'object') and key_match.group(2) is not None:
#            raise MQLParseError(query,"Can't use disambiguating prefixes in %(type)s",type=stype.id,key=key)

    if key_match.group(2) is not None and reserved_word(key_match.group(2)):
      raise MQLParseError(
          query,
          "Can't use reserved word %(prefix)s as a prefix",
          key=key,
          prefix=key_match.group(2))

    reversed = key_match.group(1)
    # need to distinguish between "x" and "/t/x" and "t/x"
    keylist = key_match.group(3).split('/')
    sprop = None

    if keylist[0] == '' and len(keylist) > 2:  # key[0] == '/'
      #            if stype.category != 'object':
      #                raise MQLParseError(query,"Can't use fully qualified property names in %(type)s",type=stype.id,key=key)

      # do this carefully so we can reload the type if necessary
      # where should this code be?? - it's icky and I don't think it belongs in get_or_add_type() or getprop() itself.
      # but maybe I am wrong..
      typepath = '/'.join(keylist[0:-1])
      propname = keylist[-1]
      try:
        sprop = self.schema_factory.get_or_add_type(typepath, varenv).getprop(
            propname, varenv, recurse=False)
        if reversed:
          sprop = sprop.get_reversed(varenv)
      except MQLTypeError, e:
        e.set_query(query)
        raise
    elif len(keylist) == 1:
      if reversed:
        raise MQLParseError(
            query,
            "Can't use unqualified propery names with ! reversing",
            type=stype.id,
            key=key)

      try:
        sprop = stype.getprop(keylist[0], varenv)
      except MQLTypeError, e:
        e.set_query(query)
        raise
    else:
      raise MQLParseError(query, "Didn't understand MQL key %(key)s", key=key)

    if sprop is None:
      raise MQLInternalError(
          query,
          "Couldn't locate a property for %(key)s",
          key=key,
          type=stype.id)

    if sprop.is_extension and mode is not ReadMode:
      raise MQLTypeError(
          query,
          "Can't use extension property %(id)s in a %(mode)s",
          id=sprop.id,
          mode=str(mode))

    if sprop.type.category == 'reflect' and stype.category == 'object':
      if mode is not ReadMode:
        raise MQLTypeError(
            query,
            "Can't use %(type)s in a %(mode)s",
            key=key,
            type=sprop.type.id,
            mode=str(mode))
      else:
        # this special case is OK - you can relect on objects in reads
        pass
    elif sprop.type.category != stype.category:
      raise MQLTypeError(
          query,
          "Can't use %(type)s properties on %(expected_type)s",
          key=key,
          expected_type=stype.id,
          type=sprop.type.id)
    elif sprop.type.category == 'link' and mode is not ReadMode:
      raise MQLTypeError(
          query,
          "Can't use %(type)s in a %(mode)s",
          key=key,
          type=sprop.type.id,
          mode=str(mode))

    return sprop

  def resolve_types(self, query, varenv, mode):
    # generic_concept is always the first stop for type resolution...
    stype = None
    # is the type constrained? It's hard to tell just from the presence of 'type'
    # (might be 'type': null or 'type': {} etc)

    if query.property is schema.LinkProperty:
      return self.schema_factory.gettype('/type/link')

    if 'type' in query:
      for typeobj in high_elements(query['type'], mode):
        if typeobj.value is None:
          continue

        typename = typeobj.value

        if not isinstance(typename, str) or not valid_high_idname(typename):
          raise MQLParseError(
              query, 'not a valid typename', key='type', id=typename)

        if typename[0] == '/':
          try:
            stype = self.schema_factory.get_or_add_type(typename, varenv)
          except MQLError, e:
            e.set_query(query)
            raise
        else:
          raise MQLTypeError(
              query, 'Unable to resolve type %(id)s', key='type', id=typename)

    if query.property is not None:
      expected_type = query.property.getothertype(varenv)
    else:
      # if we're totally unconstrained, at least we have generic_concept
      expected_type = self.schema_factory.get_or_add_type(
          '/type/object', varenv)

    if stype is None:
      # if we didn't manually constrain the type, look at .property
      stype = expected_type

    # type compatibility checks. Note that stype and expected type are now both set (although they may be the same)

    if stype.category != expected_type.category:
      # you can't put values in object slots or objects in value slots
      # except you can start a read query with /type/link
      if stype.category == 'link' and mode is ReadMode and query.is_root:
        query.is_root = 'link'
      else:
        raise MQLTypeError(
            query,
            "Can't use %(type)s (%(category)s) where %(expected_type)s (%(expected_category)s) is expected",
            type=stype.id,
            expected_type=expected_type.id,
            key='type',
            category=stype.category,
            expected_category=expected_type.category)

    # see bug 1009
    #if query.is_root and stype.category !:
    #    raise MQLTypeError(query,"Can't use a value type at the root of the query -- it must be an attribute",key='type',id=typename)

    # see bug 1596
    #if stype.id == '/type/value':
    #    raise MQLTypeError(query,"Can't instantiate %(type)s directly. Try another value type instead.",key='type',type=stype.id)

    return stype

  def resolve_names(self, query, varenv, mode):
    for subq in high_elements(query, mode):
      self.resolve_names_recurse(subq, varenv, mode)

  def resolve_terminal(self, query, varenv, mode):
    # what sort of terminal?
    terminal = query.terminal

    # hack - we determine this based on the "most likely" hrect.
    stype = query.stype

    # XXX should we use the derived type information here?
    # right now, we're just using the generic_concept
    # schema. (which we always guarantee to be first
    # by using resolve_types() to do the dirty work.)

    if mode is ReadMode and query.terminal == 'V' and query.list:
      raise MQLParseError(
          query,
          "Can't put raw values into a list, only dictionaries",
          value=query.value,
          key=query.key)

    # there's not much wrong with [ null ], but it duplicates [], so let's force a single standard.
    if query.list and query.terminal == 'N':
      raise MQLParseError(query, "Can't use [ null ]. Try [] instead.")

    if mode is not ReadMode and query.terminal in 'LD':
      raise MQLParseError(
          query, "Can't use [], [{}] or {} in a %(mode)s", mode=str(mode))

    if query.is_root:
      raise MQLParseError(
          query, "Can't use a default clause at the root of the query")

    # XXX this fails if one of the properties has multiple values (not that uncommon)
    # so we need a way to think about how that will work...
    if terminal == 'D':
      for prop in stype.get_basic_props(varenv):
        qdict = QueryDict(
            terminal='N',
            value=None,
            implied=True,
            key=prop.name,
            parent_clause=query)
        if prop.unique:
          query[prop.name] = qdict
        else:
          qdict.terminal = 'L'
          qlist = QueryList()
          qlist.append(qdict)
          qdict.list = qlist
          query[prop.name] = qlist

      # push comparison operators and sorts down.
      if query.comparisons is not None:
        query[stype.get_default_property_name()].comparisons = query.comparisons

      if query.sort_var is not None:
        query[stype.get_default_property_name()].sort_var = query.sort_var

    # here we just expand to asking for the default_property
    # we assume the default property is unique
    elif terminal in 'LNC':
      propname = stype.get_default_property_name()
      query.default = propname
      query[propname] = QueryDict(
          terminal='N',
          value=None,
          implied=True,
          key=propname,
          parent_clause=query)

      # push comparison operators down if this is not the true default.
      if query.comparisons is not None:
        query[propname].comparisons = query.comparisons

      if query.sort_var is not None:
        query[propname].sort_var = query.sort_var

    elif terminal == 'V':
      # push the value down one level - hopefully prop.get_other_type()
      # is a terminal type (or we will infinitely loop.)
      propname = stype.get_default_property_name()
      query.default = propname
      query[propname] = QueryDict(
          terminal='V',
          value=query.value,
          implied=True,
          key=propname,
          parent_clause=query)

    else:
      raise MQLInternalError(
          query, "Didn't understand terminal", terminal=terminal)
    # this applies to (almost) all cases
    # XXX nasty hack for the $lang default!
    if stype.id == '/type/text':
      if query.terminal == 'D' and query.list:
        # bug 5512, bug 5393 -- leave this case alone, so the user
        # sees things with all languages
        query['lang'] = QueryDict(
            terminal='N', implied=True, key='lang', parent_clause=query)
      else:
        lang_id = varenv.get_lang_id()
        if not lang_id:
          raise MQLParseError(
              query, 'Default language %(lang)s does not exist', lang=lang_id)

        query['lang'] = QueryDict(
            terminal='V',
            value=lang_id,
            implied=True,
            key='lang',
            parent_clause=query)
    # we will now go into resolve_names_recurse with this once we exit.

  def resolve_wildcard(self, query, varenv, stype, properties, mode):
    # XXX we modify the passed in "properties" array to add the newly generated properties.
    if mode is not ReadMode:
      raise MQLParseError(
          query,
          "Can't use '*' in a %(mode)s, only a read",
          mode=str(mode),
          key='*')

    # it's ok to use element() in a read.
    qdict = element(query['*'])

    if isinstance(qdict, dict) and qdict.terminal:
      for prop in stype.getprops(varenv):
        if prop.name not in query:
          # don't add properties the user already specified.
          properties.append(prop.name)
          # we need to copy qdict to every point. Thankfully it is a terminal (so it's only one level deep)
          myqdict = copy.copy(qdict)
          # but we're putting it in a different place in the query
          myqdict.key = prop.name
          # "*": {} in theory means "value": {}, which is an error. Let's hack around this for now -- it's useful
          # functionality.
          if ((prop.id == '/type/value/value') or
              (prop.id == '/type/value/type')):
            query[prop.name] = QueryDict(
                terminal='N',
                value=None,
                implied=True,
                key=prop.name,
                parent_clause=query)

          elif prop.unique:
            query[prop.name] = myqdict
          else:
            # avoid [ null ] error by pretending we generated []
            if myqdict.terminal == 'N':
              myqdict.terminal = 'L'
            myqlist = QueryList()
            myqlist.append(myqdict)
            myqdict.list = myqlist
            query[prop.name] = myqlist
    else:
      raise MQLParseError(query, "Can't put a complex query after '*'", key='*')

  def resolve_macro(self, query, varenv, mode):
    # XXX we modify the passed in "properties" array to add the newly generated properties.
    if mode is not ReadMode:
      raise MQLParseError(
          query,
          "Can't use 'macro' in a %(mode)s, only a read",
          mode=str(mode),
          key='macro')

    macro = query['macro']

    if not isinstance(macro, str):
      raise MQLParseError(
          query, 'Macro must be a string', key='macro', value=macro)
    if not valid_key(macro):
      raise MQLParseError(
          query, 'Macro must be a single key', key='macro', value=macro)
    if 'macro' not in varenv or macro not in varenv['macro']:
      raise MQLParseError(
          query, 'Macro %(macro)s is not defined', key='macro', macro=macro)

    macrodict = varenv['macro'][macro]

    if not isinstance(macrodict, dict):
      raise MQLParseError(
          query,
          'Macro %(macro)s must be a dictionary',
          key='macro',
          macro=macro)

    # OK, now expand a copy of the macro
    expanded_macro = self.make_orig(macrodict, varenv)

    # and copy it into the query if it doesn't duplicate
    for key, item in expanded_macro.iteritems():
      if key not in query:
        # the parent_clause changes, but not key or list
        item.parent_clause = query
        query[key] = item

  def resolve_sort(self, query, varenv, mode):
    # set the query sort number to begin at 0

    if isinstance(query['sort'], str):
      sortlist = [query['sort']]
    elif isinstance(query['sort'], list):
      sortlist = query['sort']
    else:
      raise MQLParseError(
          query,
          'sort directive must be a string or a list of strings',
          key='sort')

    newsortlist = []
    for sortitem in sortlist:
      if not isinstance(sortitem, str):
        raise MQLParseError(
            query,
            'sort directive must be a string or a list of strings',
            key='sort')

      direction = '+'
      if sortitem[0] in '+-':
        direction = sortitem[0]
        sortitem = sortitem[1:]

      subq = query

      # now walk down identpart (i.e. "a.b.c.") walking down throug the
      # dictionaries or lists as we go.
      for ident in re.compile('\\.').split(sortitem):
        if ident == 'count':
          if not (isinstance(subq, (QueryDict)) and \
                      ('return' in subq and subq['return'] == 'count' or
                       'count' in subq and not subq['count'])):
            raise MQLParseError(
                query,
                "Sort by 'count' specified, but \"return\":\"count\" or \"count\":null directive not present",
                subkey=ident,
                key='sort',
                value=sortitem)
          pass
        elif ident == 'estimate-count':
          if not (isinstance(subq, (QueryDict)) and \
                      ('return' in subq and subq['return'] == 'estimate-count' or
                       'estimate-count' in subq and not subq['estimate-count'])):
            raise MQLParseError(
                query,
                "Sort by 'estimate-count' specified, but \"return\":\"estimate-count\" or \"estimate-count\":null directive not present",
                subkey=ident,
                key='sort',
                value=sortitem)
          pass
        elif ident in self.directives:
          raise MQLParseError(
              query,
              "'%(subkey)s' is a directive and cannot be sorted on",
              subkey=ident,
              key='sort',
              value=sortitem)
        elif not valid_mql_key(ident):
          raise MQLParseError(
              query,
              "'%(subkey)s' is not a valid sort part component",
              subkey=ident,
              key='sort',
              value=sortitem)
        elif ident in subq:
          subq = element(subq[ident])
          if not isinstance(subq, (QueryDict, QueryList)):
            raise MQLInternalError(
                query,
                'Expected a query subprimitive, not %(obj)s',
                obj=type(subq).__name__)
        else:
          raise MQLParseError(
              query,
              'Unable to locate %(subkey)s from sort',
              subkey=ident,
              key='sort')

      # we're at the terminus. Make sure it is a terminal
      if not (ident == 'count' or ident == 'estimate-count') and (
          not subq.terminal or subq.terminal != 'N'):
        raise MQLParseError(
            query,
            'Must sort on a single value, not at %(value)s',
            key='sort',
            value=sortitem)

      # we may be sorting something else here already.
      if subq.sort_var is None:
        subq.sort_var = '$sort_%d' % varenv.sort_number
        varenv.sort_number += 1

      newsortlist.append(direction + subq.sort_var)

    query.sort = newsortlist

  def resolve_comparison(self, query, varenv, key, properties, mode):
    (reversed, prefix, idname, compop) = valid_comparison(key).groups()

    if compop == '|=':
      # we're expecting a list of values
      if not isinstance(query[key], list):
        raise MQLParseError(
            query,
            'Comparison operator %(operator)s takes a non-empty list of values',
            key=key,
            operator=compop)
      for item in query[key]:
        if not isinstance(item, dict):
          raise MQLParseError(
              query,
              'Comparison operator %(operator)s takes a non-empty list of values',
              key=key,
              operator=compop)
        if item.terminal != 'V':
          raise MQLParseError(
              query,
              'Comparison operator %(operator)s takes a non-empty list of values',
              key=key,
              operator=compop)
    else:
      # we can only deal with a single directly attached value.
      if not isinstance(query[key], dict):
        raise MQLParseError(
            query,
            'Comparison operator %(operator)s takes a single value',
            key=key,
            operator=compop)
      if query[key].terminal != 'V':
        raise MQLParseError(
            query,
            'Comparison operator %(operator)s takes a single value',
            key=key,
            operator=compop)

    if idname in self.directives:
      raise MQLParseError(
          query, "Can't use a comparison operator on a directive", key=key)

    if idname in ('index', '!index'):
      raise MQLParseError(
          query,
          "Can't use a comparison operator on '%(key)s'. Try sort and limit instead.",
          key=key)

    truekey = make_comparison_truekey(reversed, prefix, idname)
    if truekey not in query:
      query[truekey] = QueryDict(
          terminal='C', implied=True, key=truekey, parent_clause=query)
      # make sure we process this.
      properties.append(truekey)

    query_element = element(query[truekey])
    if (not query_element.terminal or query_element.terminal not in 'DLNC'):
      raise MQLParseError(
          query, "Can't use a comparison operator with a non-terminal", key=key)

    # this is why we need the second pass - we need to pick up the comp-ops in resolve_terminal[truekey]
    if query_element.comparisons is None:
      query_element.comparisons = {}

    if compop in query_element.comparisons:
      raise MQLInternalError(
          query,
          'Duplicate comparison operator %(operator)s',
          operator=compop,
          key=key)

    if compop == '|=':
      query_element.comparisons[compop] = [item.value for item in query[key]]
    else:
      query_element.comparisons[compop] = query[key].value

  def resolve_names_recurse(self, query, varenv, mode):
    # resolve macros first -- they may contain directives or 'type'
    if 'macro' in query:
      self.resolve_macro(query, varenv, mode)

    query.stype = self.resolve_types(query, varenv, mode)

    # we check that a terminal looks like a terminal...
    if query.terminal:
      self.resolve_terminal(query, varenv, mode)

    # we check that the clause is valid
    self.check_property_type(query, varenv, mode)

    # we have to do the properties after we have pushed any
    # comparison operators we found into them....
    # This is an ugly way to do it in two passes...
    properties = []
    for key in sorted(query.iterkeys()):
      if key in self.read_directives:
        # don't attempt to process directives here right now, just check they're not misused.
        if mode is not ReadMode:
          raise MQLParseError(
              query,
              "Can't use read directive %(key)s in a %(mode)s",
              key=key,
              mode=str(mode))

        if key == 'sort':
          self.resolve_sort(query, varenv, mode)

      elif key in self.write_directives:
        if mode not in (WriteMode, CheckMode):
          raise MQLParseError(
              query,
              "Can't use write directive %(key)s in a %(mode)s",
              key=key,
              mode=str(mode))
      elif key in ('index', '!index'):
        # another special case...
        # we do some basic sanity checks here, but no actual processing.
        if query.property and query.property.artificial and query.property.id != '/type/property/links':
          raise MQLParseError(
              query,
              "Can't ask for \"%(key)s\" on %(property)s",
              key=key,
              property=query.property.id)

        subq = element(query[key])
        if not isinstance(subq, (QueryDict, QueryList)):
          raise MQLInternalError(
              query,
              'Expected a query subprimitive, not %(obj)s',
              obj=type(subq).__name__)
        if query.is_root:
          raise MQLParseError(
              query,
              "Can't specify %(key)s at the root of the query, only in subclauses",
              key=key)
        elif not query.list:
          raise MQLParseError(
              query,
              'Must specify %(key)s in a list subquery, not a unique subquery',
              key=key)

        if mode is ReadMode:
          if subq.terminal != 'N' or subq.list:
            raise MQLParseError(
                query,
                'Only %(key)s: null is valid in a read',
                key=key,
                mode=str(mode))
        elif mode in (WriteMode, CheckMode):
          if subq.terminal != 'V' or subq.list:
            raise MQLParseError(
                query,
                'Must specify %(key)s in a write, cannot query it',
                key=key,
                mode=str(mode))
          elif not isinstance(subq.value, (long, int)) or subq.value < 0:
            raise MQLParseError(
                query,
                '%(key)s must be a non-negative integer, not %(value)s',
                key=key,
                value=subq.value)
      elif key == 'link':
        # do some checks, but otherwise we treat this like any other property (well mostly)
        if mode is not ReadMode:
          raise MQLParseError(
              query,
              "Can't use link in a %(mode)s, only in a read",
              key=key,
              mode=str(mode))

        if query.is_root:
          raise MQLParseError(
              query,
              "Can't specify %(key)s at the root of the query, only in subclauses",
              key=key)

        if query.property.artificial:
          # we probably should fake link here rather than have an arbitrary restriction
          # see bug 1896 for details.
          raise MQLParseError(
              query,
              "Can't ask for 'link' on %(property)s",
              key=key,
              property=query.property.id)

        properties.append(key)

      elif valid_mql_key(key):
        # have to do properties in the second pass
        properties.append(key)
      elif key == '*':
        if mode is ReadMode:
          self.resolve_wildcard(query, varenv, query.stype, properties, mode)
        else:
          raise MQLParseError(
              query,
              "Can't use wildcards in a %(mode)s, only in a read",
              key=key,
              mode=str(mode))

      elif valid_comparison(key):
        if mode is ReadMode:
          self.resolve_comparison(query, varenv, key, properties, mode)
        else:
          raise MQLParseError(
              query,
              "Can't use comparison operators in a %(mode)s, only in a read",
              key=key,
              mode=str(mode))

      else:
        raise MQLParseError(query, "Didn't understand key '%(key)s'", key=key)

    # { "return": "count" } is OK, all others are errors
    if len(properties) == 0 and 'return' not in query and \
           'count' not in query and 'estimate-count' not in query:
      raise MQLParseError(query,
                          'Clause must not be empty excluding directives')

    for key in properties:
      for subq in high_elements(query[key], mode):
        sprop = self.resolve_property(query, key, query.stype, varenv, mode)
        subq.property = sprop
        if ((sprop.id == '/type/value/value') or
            (sprop.id == '/type/value/type')):
          # we can't recurse from here. We fail if we try
          if not subq.terminal or subq.terminal not in 'LNVC' or (
              sprop.id == '/type/value/value' and subq.terminal == 'L'):
            raise MQLParseError(
                query,
                "value field '%(key)s' must be null or a value",
                key=key,
                property=sprop.id)

          # only "value" itself can have comparison operators.
          # XXX should describe this feature in schema.py somehow...
          if subq.comparisons and sprop.id != '/type/value/value':
            raise MQLParseError(
                query,
                'Comparison operators only valid on values, not on %(property)s',
                key=key,
                property=sprop.id)

          if subq.sort_var and sprop.id != '/type/value/value':
            raise MQLParseError(
                query,
                'Sort only valid on values, not id. Try sorting on timestamp instead.',
                key=key)
        else:
          # we recurse on types and objects
          self.resolve_names_recurse(subq, varenv, mode)

  def check_property_type(self, query, varenv, mode):
    # placeholder function to check things like
    # types are valid types (and not value types)
    # permissions are instances of /type/permission
    # etc
    sprop = query.property
    stype = query.stype

    if mode in (WriteMode, CheckMode) and not query.is_root:
      if (sprop.id == '/type/object/permission'):
        if 'connect' not in query or query['connect'] != 'update':
          raise MQLParseError(
              query,
              "Can't specify permission in a %(mode)s query",
              mode=str(mode))

        parent = query.parent_clause
        if 'create' in parent or 'create' in query:
          raise MQLParseError(
              query,
              "Can't specify permission in a %(mode)s query",
              mode=str(mode))

      if (sprop.id == '/type/object/type'):
        if 'create' in query:
          raise MQLParseError(
              query, "Can't create a type in the same write as you use it")

  def can_make_final(self, query):
    return (query.terminal and query.terminal in 'LVN' and
            query.stype.get_default_property_name() == 'id' and
            not (query.comparisons or query.sort_var))

  def add_query_primitive_root(self, query, varenv, mode):
    """
        This is the root (which is either an object or a link)
        """
    if query.is_root == 'link':
      query.node = ReadQP(query, 'directlink')
    else:
      query.node = ReadQP(query, 'node')

    # the root specially needs a pagesize
    query.node.pagesize = 100

    if varenv.get('cursor') is not None:
      query.node.cursor = varenv.get('cursor')

    self.add_read_directives(query, query.node, varenv)
    if query.is_root == 'link':
      self.add_query_primitive_link(query, varenv, mode)
    else:
      self.add_query_primitive_recurse(query, varenv, mode)

  def add_query_primitive_object(self, query, varenv, mode):
    """
        This is a non-root object
        """

    parent = query.parent_clause

    query.link = ReadQP(query, 'link')

    if query.terminal and query.terminal in 'DLN':
      # it's optional if we didn't constrain it in some way, but a comparison counts as a constraint.
      if query.comparisons is None:
        query.link.optional = True
    elif query.terminal == 'V':
      # it's a specified value -- let's make sure we only see one so we don't break uniqueness...
      # XXX we can't use :pagesize in a write. What should we do?
      query.link.pagesize = 1
    elif query.terminal == 'C':
      query.link.pagesize = 1
      query.link.category = 'constraint'

    # can we optimize the node away?
    if self.can_make_final(query):

      query.node = None

      guid = None
      if query.terminal == 'V':
        guid = DeferredGuidLookup(query.value, varenv.lookup_manager)
      query.link.add_final(query, query.property, guid)

      # hook it up to me and my parent and set the typeguid
      query.link.set_property(query.property, parent.node, None)

    else:
      query.node = ReadQP(query, 'node')

      # hook it up to me and my parent and set the typeguid
      query.link.set_property(query.property, parent.node, query.node)

      self.add_read_directives(query, query.link, varenv)

      self.add_query_primitive_recurse(query, varenv, mode)

  def add_query_primitive_value(self, query, varenv, mode):
    query.link = ReadQP(query, 'value')
    query.node = None

    if query.is_root:
      raise MQLInternalError(query,
                             "Can't start a query with a value at the root")

    if query.terminal and query.terminal in 'DLN' and query.comparisons is None:
      query.link.optional = True
    elif query.terminal == 'C':
      # this supresses results from this link
      query.link.pagesize = 1
      query.link.category = 'constraint'

    # add all the stuff to query.link (this code is shared with the
    # slightly different /type/link/target_value path)
    self.add_query_primitive_value_link(query, query.link, varenv, mode)

    if 'index' in query:
      self.add_query_primitive_index(query['index'], varenv, mode, False)
    if '!index' in query:
      self.add_query_primitive_index(query['index'], varenv, mode, True)

    if 'link' in query:
      self.add_query_primitive_link(query['link'], varenv, mode)

    stype = query.stype
    # now deal with the right (this has to be different for the /type/link/target_value case.
    rpn = stype.get_right_property_name(query)
    if rpn and rpn in query:
      # if we have a right PD, use it now as the query
      # as it goes at the same level, not one deeper

      child_query = element(query[rpn])

      # this is a special terminal optimization. We change
      # result=((contents)) right->(left=null right=null result=((guid)) )
      # into result=((right))
      # and
      # right->(left=null right=null guid=X result=(()) )
      # into right=X
      #
      if self.can_make_final(child_query):
        guid = None
        if child_query.terminal == 'V':
          guid = DeferredGuidLookup(child_query.value, varenv.lookup_manager)

        query.link.add_final(child_query, query.property, guid)
      else:
        # we need to go a level deeper to get the node...
        query.node = self.add_query_primitive_right(child_query, varenv, mode)
    elif query.terminal == 'D':
      # a nasty special case for bug 7133. Add a special finalQP
      # here so that just in case we see a value we can add
      # it to the result. Note that this child query doesn't even
      # have a good key!

      # but be careful -- enumerations shouldn't have something final
      # because they are just keys in disguise...
      if query.stype.id != '/type/enumeration':
        query.link.add_final(QueryDict(terminal='N'), query.property, None)

    if query.property.id == '/type/reflect/any_value':

      # /type/value does not include /type/key
      # (as a way to keep /type/reflect/any_value sane -- see bug 2353)
      if stype.id in ('/type/key', '/type/id', '/type/enumeration'):
        raise MQLTypeError(
            query,
            "Can't use %s with %s" % (stype.id, query.property.id),
            property=query.property.id,
            expected_type=stype.id)

      # make sure /type/key does not show up as /type/text by accident
      query.link.comparisons.append(('typeguid!=', self.has_key))

    # hook it up to me and my parent and set the typeguid
    query.link.set_property(query.property, query.parent_clause.node,
                            query.node)
    self.add_read_directives(query, query.link, varenv)

  def add_query_primitive_value_link(self, query, link, varenv, mode):
    stype = query.stype

    value = None
    # did the user specify it?
    dpn = stype.get_default_property_name()

    if dpn in query:
      if isinstance(query[dpn], list):
        raise MQLParseError(
            query,
            "Can't put raw values into a list without using the Or comparator operator, |=",
            value=query[dpn][0].value,
            key=query.key)
      value = query[dpn].value

    explicit_type = None
    if 'type' in query and element(query['type']).terminal == 'V':
      explicit_type = element(query['type']).value

    try:
      # we want to set datatype if either
      # "value" was set or "type" was set
      link.datatype = stype.get_datatype(
          value, comparison=None, explicit_type=explicit_type)
      link.value = stype.coerce_value(value)
      link.add_result('datatype')
      link.add_result('value')

    except MQLTypeError, e:
      e.set_query(query)
      raise

    if dpn in query and query[dpn].comparisons is not None:
      for (compop, compval) in query[dpn].comparisons.iteritems():

        if compop == '|=':
          # need to check this first since it comes with a list of values
          # rather than a single one.
          final_val = []

          for val in compval:
            if link.datatype is None:
              link.datatype = stype.get_datatype(val, compop)
            elif link.datatype != stype.get_datatype(val, compop):
              raise MQLTypeError(
                  query,
                  "Can't mix %(datatype)s and %(value)s with %(expected_type)s",
                  key=dpn,
                  value=val,
                  expected_type=stype.id,
                  datatype=link.datatype)

            if link.datatype == 'boolean':
              raise MQLTypeError(
                  query,
                  "Can't use comparison operators on boolean values",
                  key=dpn,
                  value=val,
                  expected_type=stype.id,
                  datatype=link.datatype)

            final_val.append(stype.coerce_value(val, compop))

          # note that the graph thinks this is value=(...), not value|=(...)
          link.comparisons.append(('value=', final_val))

        else:
          # force the datatype if we have comparisons -- otherwise lojson will force it for us
          # and will probably get it wrong (it will force string rather than timestamp or boolean)
          if link.datatype is None:
            link.datatype = stype.get_datatype(compval, compop)
          elif link.datatype != stype.get_datatype(compval, compop):
            raise MQLTypeError(
                query,
                "Can't mix %(datatype)s and %(value)s with %(expected_type)s",
                key=dpn,
                value=compval,
                expected_type=stype.id,
                datatype=link.datatype)

          if link.datatype == 'boolean':
            raise MQLTypeError(
                query,
                "Can't use comparison operators on boolean values",
                key=dpn,
                value=compval,
                expected_type=stype.id,
                datatype=link.datatype)

          elif compop == '~=' and link.datatype not in ('string', 'bytestring',
                                                        'url', 'key'):
            # see bug # 1067.
            raise MQLTypeError(
                query,
                "Can't use ~= on type %(expected_type)s",
                key=dpn,
                value=compval,
                expected_type=stype.id,
                datatype=link.datatype)

          # hack - MQL-573, if the link has the datatype, use the number comparator.
          if link.datatype in ('integer', 'float'):
            link.comparator = 'number'

          link.comparisons.append(
              ('value' + compop, stype.coerce_value(compval, compop)))

    if dpn in query and query[dpn].sort_var is not None:
      link.vars[query[dpn].sort_var] = 'value'

    comp = stype.get_comparator(varenv)
    if comp is not None:
      link.comparator = comp

  def add_query_primitive_recurse(self, query, varenv, mode):
    #for key, item in query.iteritems():
    for key in sorted(query.iterkeys()):
      item = query[key]
      if key == 'link':
        self.add_query_primitive_link(item, varenv, mode)
      elif key in ('index', '!index'):
        self.add_query_primitive_index(item, varenv, mode, key[0] == '!')
      elif key == 'value':
        raise MQLInternalError(
            query,
            "Can't expand a value clause as a reference subquery",
            key=key)
      elif key == '*':
        # this was handled earlier -- nothing to do in this phase
        pass
      elif valid_comparison(key):
        # this was handled earlier -- nothing to do in this phase
        pass
      elif key in self.directives:
        if key in self.read_directives and mode is ReadMode:
          # we do this in add_read_directives()
          pass
        else:
          raise MQLParseError(
              query,
              "Can't use directive %(key)s in %(mode)s",
              key=key,
              mode=str(mode))
      elif valid_mql_key(key):
        for item in high_elements(query[key], mode):

          sprop = item.property

          if ((sprop.id == '/type/value/value') or
              (sprop.id == '/type/value/type') or
              (sprop.type.id == '/type/link')):
            raise MQLInternalError(item,
                                   'We should not have recursed to this depth')

          elif (sprop.id == '/type/object/mid'):
            self.add_query_primitive_mid(item, varenv, mode)
          elif ((sprop.id == '/type/object/id') or
                (sprop.id == '/type/object/guid')):
            self.add_query_primitive_id(item, varenv, mode)
          elif (sprop.id == '/type/object/timestamp'):
            self.add_query_primitive_timestamp(item, varenv, mode)
          elif (sprop.id in ('/type/object/creator', '/type/object/attribution',
                             '/type/property/links', '/type/attribution/links',
                             '/type/attribution/attributed')):
            self.add_query_primitive_attached(item, varenv, mode)
          elif item.stype.category == 'object':
            self.add_query_primitive_object(item, varenv, mode)
          elif item.stype.category == 'value':
            self.add_query_primitive_value(item, varenv, mode)
          else:
            raise MQLInternalError(
                item,
                "Can't generate clause for %(type)s",
                type=item.stype.id,
                key=key)

      else:
        raise MQLInternalError(
            query,
            "Can't generate clause for %(type)s",
            type=item.stype.id,
            key=key)

  def add_query_primitive_attached(self, query, varenv, mode):
    tonode = True
    forward = True
    if query.property.id in ('/type/object/creator', '/type/object/attribution',
                             '/type/link/creator', '/type/link/attribution'):
      field = 'scope'
    elif query.property.id == '/type/link/master_property':
      field = 'typeguid'
    elif query.property.id == '/type/link/source':
      field = 'left'
    elif query.property.id == '/type/link/target':
      field = 'right'
    elif query.property.id == '/type/property/links':
      field = 'typeguid'
      tonode = False
    elif query.property.id == '/type/attribution/links':
      field = 'scope'
      tonode = False
    elif query.property.id == '/type/attribution/attributed':
      field = 'scope'
      forward = False
    else:
      raise MQLInternalError(
          query,
          "Can't call add_query_primitive_attached on %(property)s",
          property=query.property.id)

    # this is directly attached...a
    if tonode:
      query.node = ReadQP(query, 'attached')
    else:
      query.node = ReadQP(query, 'directlink')

    query.link = Missing

    if query.terminal and query.terminal in 'DLN':
      # it's optional if we didn't constrain it in some way, but a comparison counts as a constraint.
      if query.comparisons is None:
        query.node.optional = True
    elif query.terminal == 'C':
      # something to supress
      # no need to make the pagesize 1 as this is implied by direct
      # attachment
      query.node.category = 'constraint'

    # hook me up in the 'scope' slot.
    query.parent_clause.node.add_contents(query.node, field, forward and tonode)

    self.add_read_directives(query, query.node, varenv)

    # this isn't technically what tonode is for; it just happens that
    # the only two inward cases are exactly the /type/link cases.
    if tonode:
      self.add_query_primitive_recurse(query, varenv, mode)
    else:
      self.add_query_primitive_link(query, varenv, mode)

  def add_query_primitive_timestamp(self, query, varenv, mode):
    # note that if query is a link, query.node is actually query.link of the parent ...
    query = element(query)

    stype = query.stype

    value = None
    # the dpn of /type/datetime (required here) is always "value"

    underlying_node = query.parent_clause.node
    underlying_node.implied.append(query)

    dpn = stype.get_default_property_name()
    if dpn in query:
      value = query[dpn].value
      if query[dpn].sort_var is not None:
        underlying_node.vars[query[dpn].sort_var] = 'timestamp'

    if value is None:
      underlying_node.add_result('timestamp')
    elif stype.get_datatype(value) not in (None, 'timestamp'):
      raise MQLTypeError(
          query,
          "Can't ask for anything other than /type/datetime",
          key='timestamp',
          value=value)
    elif underlying_node.timestamp:
      raise MQLParseError(
          query,
          "Can't specify 'timestamp' more than once in a single clause",
          value=value,
          previous_value=underlying_node.timestamp)
    else:
      underlying_node.timestamp = value

    if dpn in query and query[dpn].comparisons is not None:
      for (compop, compval) in query[dpn].comparisons.iteritems():
        if compop in ['~=', '|=']:
          # see bug # 1067.
          raise MQLTypeError(
              query,
              "Can't use %(operator)s on type %(expected_type)s",
              key=dpn,
              value=value,
              expected_type=stype.id,
              datatype='timestamp',
              operator=compop)

        if stype.get_datatype(compval, compop) != 'timestamp':
          raise MQLTypeError(
              query,
              "Can't mix %(datatype)s and %(value)s with %(expected_type)s",
              key=dpn,
              value=value,
              expected_type=stype.id,
              datatype='timestamp')

        underlying_node.comparisons.append(
            ('timestamp' + compop, stype.coerce_value(compval, compop)))

  def add_query_primitive_right(self, query, varenv, mode):
    query.node = ReadQP(query, 'node')

    # this shares the same link as the parent
    query.link = Missing

    if query.terminal == 'C':
      query.node.category = 'constraint'

    self.add_query_primitive_recurse(query, varenv, mode)

    # XXX need to be very careful about the read directives we find in here, particularly "optional".
    # right now, I don't think any are meaningful, so none are processed.
    self.add_read_directives(query, query.node, varenv)

    return query.node

  def add_query_primitive_index(self, query, varenv, mode, inverse):
    query.link = ReadQP(query, '!index' if inverse else 'index')
    query.node = Missing

    if ((query.parent_clause.property.reverse and not inverse) or
        (inverse and not query.parent_clause.property.reverse)):
      query.link.typeguid = self.has_right_order
    else:
      query.link.typeguid = self.has_left_order

    query.link.add_result('value')
    # only need to find out if there is a duplicate
    # in theory we could make this 1 and have some errors go away.
    query.link.pagesize = 2

    if query.sort_var is not None:
      query.link.vars[query.sort_var] = 'value'

    link_clause = None
    if query.parent_clause.link is Missing:
      if query.parent_clause.node and query.parent_clause.node.category == 'directlink':
        link_clause = query.parent_clause.node
      else:
        raise MQLInternalError(
            query,
            "Can't use \"index\" on internal property %(property)s",
            property=query.parent_clause.property.id)
    else:
      link_clause = query.parent_clause.link

    link_clause.add_contents(query.link, 'left', False)

  def add_query_primitive_mid(self, query, varenv, mode):
    stype = query.stype
    sprop = query.property

    value = None
    dpn = stype.get_default_property_name()
    if dpn in query:
      value = query[dpn].value
      if query[dpn].sort_var is not None:
        raise MQLParseError(
            query,
            "Can't sort on %(property)s. Try sorting on /type/object/timestamp instead",
            property=sprop.id)

    underlying_node = query.parent_clause.node
    underlying_node.implied.append(query)

    # so if it's a |= : [] deal, value is none.
    #
    # ask about a single value without a comparator.
    if value is not None:
      if not valid_mid(value):
        raise MQLParseError(
            query, 'MID is invalid (failed to parse)', value=value)

      if underlying_node.guid is not None:
        # same deal, you already set a mid.
        raise MQLParseError(
            query,
            "Can't specify an id more than once in a single clause",
            value=value,
            previous_value=str(underlying_node.guid))

      underlying_node.guid = DeferredGuidOfMidLookup(value,
                                                     varenv.lookup_manager)

    # receive
    else:
      underlying_node.add_result('guid')
    # rubbish
    if dpn in query and query[dpn].comparisons is not None:
      for (compop, compval) in query[dpn].comparisons.iteritems():
        if underlying_node.guid is not None:
          raise MQLParseError(
              query,
              "Can't constrain with %(compop)s and specify a single value",
              key=dpn,
              value=value,
              expected_type='/type/id',
              datatype='guid',
              compop=compop)
      if compop == '|=':
        for compvalue in compval:

          #if (stype.get_datatype(compvalue) != "guid" or (sprop.id == "/type/object/mid" and not valid_mid(compvalue) or valid_mql_key(compvalue))):
          #    raise MQLTypeError(query, "Can't use an invalid mid in a |= mid list",
          #                        key=dpn,value=compvalue,expected_type="/type/id",datatype="mid")

          if stype.get_datatype(compvalue) != 'guid':
            raise MQLTypeError(query, 'Expected something of /type/id here')

          if sprop.id == '/type/object/mid':
            if not valid_mid(compvalue):
              if not valid_mql_key(compvalue):
                raise MQLTypeError(
                    query,
                    "Can't use an invalid id in a mid|=list",
                    key=dpn,
                    value=compvalue,
                    expected_type='/type/id',
                    datatype='mid')

        if sprop.id == '/type/object/mid':
          resolved_mids = DeferredGuidOfMidOrGuidLookups(
              compval, varenv.lookup_manager)
          query[dpn].alternatives = resolved_mids
          underlying_node.guid = resolved_mids
        else:
          raise MQLInternalError(
              query,
              "Didn't understand %(property)s in add_query_primitive_mid() fn",
              property=sprop.id,
              compop=compop)

      elif compop == '!=':
        if stype.get_datatype(compval) != 'guid' or (
            sprop.id == '/type/object/mid' and not valid_mid(compval)):
          raise MQLTypeError(
              query,
              '%(value)s is not a valid guid in !=',
              key=dpn,
              value=compval,
              expected_type=stype.id,
              datatype='guid')

        if (sprop.id == '/type/object/mid'):
          resolved_mid = DeferredGuidOfMidLookup(compval, varenv.lookup_manager)
          underlying_node.comparisons.append(('guid!=', resolved_mid))
        else:
          raise MQLInternalError(
              query,
              "Don't understand %(property)s in add_query_primitive_mid() fn",
              property=sprop.id,
              compop=compop)

      else:
        raise MQLTypeError(
            query,
            "Can't use comparison operator %(operator)s on /type/id",
            key=dpn,
            value=value,
            expected_type='/type/id',
            datatype='guid',
            operator=compop)

  def add_query_primitive_id(self, query, varenv, mode):
    stype = query.stype
    sprop = query.property

    value = None
    # the dpn of /type/id (required here) is always "value"
    dpn = stype.get_default_property_name()
    if dpn in query:
      value = query[dpn].value
      if query[dpn].sort_var is not None:
        raise MQLParseError(
            query,
            "Can't sort on %(property)s. Try sorting on /type/object/timestamp instead",
            property=sprop.id)

    underlying_node = query.parent_clause.node
    underlying_node.implied.append(query)

    # validate the guid
    if value is not None:
      if (sprop.id == '/type/object/guid' and
          (not isinstance(value, basestring) or not valid_guid(value))):
        raise MQLParseError(
            query, 'Can only use a hexadecimal guid here', value=value)
      elif stype.get_datatype(value) not in (None, 'guid'):
        raise MQLParseError(
            query, "Can't ask for anything other than /type/id", value=value)

      if underlying_node.guid is not None:
        # we've already specified the guid
        raise MQLParseError(
            query,
            "Can't specify an id more than once in a single clause",
            value=value,
            previous_value=str(underlying_node.guid))

      if valid_mid(value):
        underlying_node.guid = DeferredGuidOfMidLookup(value,
                                                       varenv.lookup_manager)
      else:
        underlying_node.guid = DeferredGuidLookup(value, varenv.lookup_manager)

    else:
      # we're asking for the result
      underlying_node.add_result('guid')

    # only |= is valid on id or guid
    if dpn in query and query[dpn].comparisons is not None:
      # can't use iteritems() as we are changing the dict
      for (compop, compval) in query[dpn].comparisons.iteritems():
        if underlying_node.guid is not None:
          raise MQLParseError(
              query,
              "Can't constrain with %(compop)s and specify a single value",
              key=dpn,
              value=value,
              expected_type='/type/id',
              datatype='guid',
              compop=compop)

        if compop == '|=':

          for compvalue in compval:
            if stype.get_datatype(compvalue) != 'guid' or (
                sprop.id == '/type/object/guid' and not valid_guid(compvalue)):
              raise MQLTypeError(
                  query,
                  "Can't use an invalid guid in a |= list",
                  key=dpn,
                  value=compvalue,
                  expected_type='/type/id',
                  datatype='guid')

          if (sprop.id == '/type/object/id'):
            # resolve all the ids at once. Keep a record of the resolutions so we can reverse them to put
            # the correct id next to each result.

            resolved_ids = DeferredGuidOfMidOrGuidLookups(
                compval, varenv.lookup_manager)

            # XXX ugly hack; can't think of a better place right now.
            query[dpn].alternatives = resolved_ids
            underlying_node.guid = resolved_ids

          elif (sprop.id == '/type/object/guid'):
            resolved_guids = FixedGuidList(compval, varenv)
            query[dpn].alternatives = resolved_guids
            underlying_node.guid = resolved_guids
          else:
            raise MQLInternalError(
                query,
                "Don't understand %(property)s in add_query_primitive_id() fn",
                property=sprop.id,
                compop=compop)

        elif compop == '!=':
          if stype.get_datatype(compval) != 'guid' or (
              sprop.id == '/type/object/guid' and not valid_guid(compval)):
            raise MQLTypeError(
                query,
                '%(value)s is not a valid guid in !=',
                key=dpn,
                value=compval,
                expected_type=stype.id,
                datatype='guid')

          if (sprop.id == '/type/object/id'):
            if valid_mid(compval):
              resolved_id = DeferredGuidOfMidLookup(compval,
                                                    varenv.lookup_manager)
            else:
              resolved_id = DeferredGuidLookup(compval, varenv.lookup_manager)
            underlying_node.comparisons.append(('guid!=', resolved_id))
          elif (sprop.id == '/type/object/guid'):
            underlying_node.comparisons.append(('guid!=', Guid(compval)))
          else:
            raise MQLInternalError(
                query,
                "Don't understand %(property)s in add_query_primitive_id() fn",
                property=sprop.id,
                compop=compop)

        else:
          raise MQLTypeError(
              query,
              "Can't use comparison operator %(operator)s on /type/id",
              key=dpn,
              value=value,
              expected_type='/type/id',
              datatype='guid',
              operator=compop)

  def add_query_primitive_link(self, query, varenv, mode):

    # note that here low_json is the outside clause,
    # whereas query is the 'link' subclause.
    query = element(query)

    if query.is_root or query.property.id in ('/type/property/links',
                                              '/type/attribution/links'):
      # special root-is-/type/link case
      # query.node is already defined in add_qp_root()
      query.node.comparisons.append(('left!=', Missing))

      # see bug 7504
      query.node.comparisons.append(('typeguid!=', self.has_left_order))
      query.node.comparisons.append(('typeguid!=', self.has_right_order))

    else:
      # we are on the link.
      query.node = query.parent_clause.link

    if mode is not ReadMode:
      raise MQLInternalError(
          query, "Use of 'link' in a %(mode)s", mode=str(mode))

    stype = self.schema_factory.gettype('/type/link')

    if 'operation' in query and 'valid' in query:
      op_item = high_elements(query['operation'], mode)[0]
      va_item = high_elements(query['valid'], mode)[0]

      op_value = self.get_link_fundamental_value(op_item)
      va_value = self.get_link_fundamental_value(va_item)

      query.node.add_result('next')
      query.node.add_result('previous')
      query.node.add_result('live')

      query.node.implied.append(op_item)
      query.node.implied.append(va_item)

      # validate
      if va_value not in [True, False, None]:
        raise MQLParseError(item, "'valid' requires a boolean argument")
      if op_value not in ['insert', 'update', 'delete', None]:
        raise MQLParseError(
            item,
            "Valid values for 'operation' are 'insert', 'delete' and 'update'")

      # operation == insert, value = {true, false, null}
      if op_value == 'insert' and va_value is True:
        query.node.comparisons.append(('live=', True))
        query.node.comparisons.append(('newest=', 0))
        query.node.comparisons.append(('oldest=', 0))
      elif op_value == 'insert' and va_value is False:
        query.node.comparisons.append(('live=', True))
        query.node.comparisons.append(('newest>', 0))
        query.node.comparisons.append(('oldest=', 0))
      elif op_value == 'insert' and va_value is None:
        query.node.comparisons.append(('live=', True))
        query.node.comparisons.append(('oldest=', 0))
        # if next is null it's valid, otherwise it's not.

      # operation == update, value = {true, false, null}
      if op_value == 'update' and va_value is True:
        query.node.comparisons.append(('live=', True))
        query.node.comparisons.append(('oldest>', 0))
        query.node.comparisons.append(('newest=', 0))
      elif op_value == 'update' and va_value is False:
        query.node.comparisons.append(('live=', True))
        query.node.comparisons.append(('oldest>', 0))
        query.node.comparisons.append(('newest>', 0))
      elif op_value == 'update' and va_value is None:
        query.node.comparisons.append(('live=', True))
        query.node.comparisons.append(('oldest>', 0))
        # same deal, if next is null it's valid, otherwise it's not.

      # operation == delete, value = {true, false, null}
      if op_value == 'delete' and va_value is True:
        raise MQLInternalError(
            query,
            'Cannot specify both operation: delete and valid: true, deleted means not valid'
        )
      elif op_value == 'delete' and va_value is False:
        query.node.comparisons.append(('live=', False))
        query.node.comparisons.append(('newest>=', 0))

      # This is silly, if it's deleted it's going to be false.
      # But, ok.
      elif op_value == 'delete' and va_value is None:
        query.node.comparisons.append(('live=', False))
        # >= in case we ever decide to show resurrections.
        query.node.comparisons.append(('newest>=', 0))

      # operation == null, value = {true, false, null}
      if op_value is None and va_value is True:
        # with an implied newest=0
        query.node.comparisons.append(('live=', True))

      # The operation can only be determined by looking at live and prev.
      # We have to filter out the case where
      # live = true and next = null, because those are valid.
      elif op_value is None and va_value is False:
        query.node.comparisons.append(('live=', 'dontcare'))
        query.node.comparisons.append(('newest>', 0))
      # just give me all of it, i'll sort it out.
      elif op_value is None and va_value is None:
        query.node.comparisons.append(('live=', 'dontcare'))
        query.node.comparisons.append(('newest>=', 0))

    elif 'operation' in query:
      op_item = high_elements(query['operation'], mode)[0]
      op_value = self.get_link_fundamental_value(op_item)

      if op_value not in ['insert', 'update', 'delete', None]:
        raise MQLParseError(
            item,
            "Valid values for 'operation' are 'insert', 'delete' and 'update'")

      query.node.add_result('next')
      query.node.add_result('previous')
      query.node.add_result('live')
      query.node.implied.append(op_item)

      if op_value == 'insert':
        query.node.comparisons.append(('newest=', 0))
        query.node.comparisons.append(('oldest=', 0))
      elif op_value == 'update':
        query.node.comparisons.append(('oldest>', 0))
      elif op_value == 'delete':
        query.node.comparisons.append(('live=', False))
      elif op_value is None:
        query.node.comparisons.append(('live=', 'dontcare'))
        query.node.comparisons.append(('newest>=', 0))

    elif 'valid' in query:
      va_item = high_elements(query['valid'], mode)[0]
      va_value = self.get_link_fundamental_value(va_item)

      if va_value not in [True, False, None]:
        raise MQLParseError(item, "'valid' requires a boolean argument")

      query.node.add_result('next')
      query.node.add_result('previous')
      query.node.add_result('live')
      query.node.implied.append(va_item)

      if va_value is True:
        query.node.comparisons.append(('newest=', 0))
      if va_value is False:
        query.node.comparisons.append(('newest>', 0))
        query.node.comparisons.append(('live=', 'dontcare'))
      if va_value is None:
        query.node.comparisons.append(('newest>=', 0))
        query.node.comparisons.append(('live=', 'dontcare'))

    for key in query:
      if key not in self.directives:
        # we want to use high_elements here, because we want the nice error if the user had multiple things in the list.
        item = high_elements(query[key], mode)[0]

        key_match = valid_mql_key(key)
        if key in ('index', '!index'):
          self.add_query_primitive_index(query[key], varenv, mode,
                                         key[0] == '!')
        elif key_match:
          # Now that we accept fully-qualified keys for "link" clauses, it's a little trickier to
          # determine the key name for comparison. resolve_property does this for us:
          sprop = self.resolve_property(None, key, stype, varenv, mode)
          key = sprop.name

        if key == 'timestamp':
          self.add_query_primitive_timestamp(item, varenv, mode)
        elif key in ('creator', 'attribution', 'source', 'target',
                     'master_property'):
          self.add_query_primitive_attached(item, varenv, mode)
        elif key == 'reverse':
          value = self.get_link_fundamental_value(item)
          query.node.implied.append(item)
          if value is not None:
            raise MQLParseError(
                item, "Can only ask for the value of 'reverse', not specify it")
          if item.sort_var:
            raise MQLParseError(
                item, "Can't sort on /type/link/reverse", key=key)
        elif key == 'type':
          query.node.implied.append(item)
          if not item.terminal:
            raise MQLParseError(
                item,
                "Can't expand 'type' in a link clause (it is fixed as '/type/link')"
            )
          value = item.value
          if item.comparisons:
            raise MQLParseError(
                item,
                "Can't use comparison operators on %(property)s",
                property=item.property.id)
          if value is not None and value != '/type/link':
            raise MQLParseError(
                item,
                "Only '/type/link' is valid as the type of a 'link' object'")
          if item.sort_var:
            raise MQLParseError(
                item, 'Meaningless to sort on /type/link/type', key=key)

        # these are OK, but handled earlier.
        elif key == 'operation':
          pass
        elif key == 'valid':
          pass

        elif key == 'target_value':
          self.add_query_primitive_target_value(item, query.node, varenv, mode)
        elif key == '*' or key in ('index', '!index') or valid_comparison(key):
          # this was handled earlier -- nothing to do in this phase
          pass
        else:
          raise MQLInternalError(
              query, "Unknown key %(key)s in 'link' clause", key=key)

    if query.node.category == 'directlink' and mode is ReadMode:
      self.add_read_directives(query, query.node, varenv)
    else:
      self.prohibit_all_directives(query, varenv, mode)

  def add_query_primitive_target_value(self, query, node, varenv, mode):
    self.add_query_primitive_value_link(query, node, varenv, mode)

    node.implied.append(query)
    node.add_result('typeguid')

    stype = query.stype
    rpn = stype.get_right_property_name(query)
    if rpn and rpn in query:
      # if we have a right PD, use it now as the query
      # as it goes at the same level, not one deeper

      child_query = element(query[rpn])
      if self.can_make_final(child_query):
        guid = None
        if child_query.terminal == 'V':
          guid = DeferredGuidLookup(child_query.value, varenv.lookup_manager)

        node.add_final(child_query, query.property, guid)
      else:
        raise MQLParseError(
            query,
            "Can't expand %(key)s directly inside %(property)s (this limitation may be removed in the future)",
            property=query.property.id,
            key=rpn)
    elif query.terminal == 'D':
      # more nasty fixes for bug 7133
      node.add_final(QueryDict(terminal='N'), query.property, None)

    # we need custom optional handling for target_value
    # which is mucky and somewhat cumbersome.
    if (query.terminal and query.terminal in 'DLN' and
        query.comparisons is None):
      # this is optional
      optionality = True
    elif self.get_optional_status(query) is False:
      # explicit optional: required or optional: false is OK.
      # (meaningless, but acceptable)
      optionality = False
    else:
      raise MQLParseError(
          query,
          "Can't use optional on /type/link/target_value (this limitation may be removed in the future)",
          key='optional',
          value=query['optional'])

    if query.stype.id in ('/type/id', '/type/enumeration'):
      raise MQLTypeError(
          query,
          "Can't use %s with %s" % (stype.id, query.property.id),
          property=query.property.id,
          expected_type=stype.id)

    if optionality == False:
      node.comparisons.append(('value!=', Missing))

      if query.stype.id == '/type/text':
        node.comparisons.append(('typeguid!=', self.has_key))
      elif query.stype.id == '/type/key':
        node.comparisons.append(('typeguid=', self.has_key))

  def get_optional_status(self, query):
    value = False
    if 'optional' in query:
      value = query['optional']
      if isinstance(value,
                    bool) or (isinstance(value, str) and
                              value in ('forbidden', 'optional', 'required')):
        if value == 'optional':
          value == True
        elif value == 'required':
          value = False
      else:
        raise MQLParseError(
            query,
            "'optional' takes true (or 'optional'),false (or 'required') or 'forbidden'",
            key='optional',
            value=value)

    return value

  def prohibit_all_directives(self, query, varenv, mode):
    for key in query:
      if key in self.read_directives:
        # what do we do here? There is always exactly one link (if it is legal to talk about link at all, which sometimes it is not)
        raise MQLParseError(
            query,
            "Can't use %(key)s inside a 'link' clause -- it would be meaningless as links exist exactly once",
            key=key)
      elif key in self.write_directives:
        raise MQLInternalError(
            query, "Can't use a write directive in a %(mode)s", mode=str(mode))

  def add_read_directives(self, query, qp, varenv):
    if 'optional' in query:
      value = query['optional']
      if isinstance(value,
                    bool) or (isinstance(value, str) and
                              value in ('forbidden', 'optional', 'required')):
        qp.optional = value
      else:
        raise MQLParseError(
            query,
            "'optional' takes true (or 'optional'),false (or 'required') or 'forbidden'",
            key='optional',
            value=value)

    if 'limit' in query:
      value = query['limit']
      # need to put @pagesize on the outermost part of the query, :pagesize elsewhere
      if isinstance(value,
                    (int, long)) and value >= 0 and not isinstance(value, bool):
        qp.pagesize = value
      else:
        raise MQLParseError(
            query,
            "'limit' must be a non-negative integer",
            key='limit',
            value=value)

    if 'return' in query:
      value = query['return']
      if value == 'count':
        qp.return_count = True
      elif value == 'estimate-count':
        qp.return_estimate_count = True
      else:
        raise MQLParseError(
            query,
            "'return' currently only supports 'count' and 'estimate-count'",
            key='return',
            value=value)

      # remove the limit unless it is explicitly specified.
      if 'limit' not in query:
        qp.pagesize = None

      # deal with sorting by count and estimate-count:
      if query.sort_var is not None:
        if query.link is not Missing:
          query.link.vars[query.sort_var] = value
        else:
          query.node.vars[query.sort_var] = value

    if 'count' in query:
      value = query['count']
      if value is not None:
        raise MQLParseError(
            query, "'count' directive must be null", key='count', value=value)
      qp.include_count = True
      # deal with sorting by count:
      if query.sort_var is not None:
        if query.link is not Missing:
          query.link.vars[query.sort_var] = 'count'
        else:
          query.node.vars[query.sort_var] = 'count'

    if 'estimate-count' in query:
      value = query['estimate-count']
      if value is not None:
        raise MQLParseError(
            query,
            "'estimate-count' directive must be null",
            key='estimate-count',
            value=value)
      qp.include_estimate_count = True
      # deal with sorting by estimate-count:
      if query.sort_var is not None:
        if query.link is not Missing:
          query.link.vars[query.sort_var] = 'estimate-count'
        else:
          query.node.vars[query.sort_var] = 'estimate-count'

    if query.sort and not qp.sort_comparator:
      qp.sort = query.sort

      # Resolve the sort directive (again), this time retrieving the lowest-level sort key's type.
      # Use this to figure out the sort's comparator:
      if isinstance(query['sort'], str):
        sortlist = [query['sort']]
      elif isinstance(query['sort'], list):
        sortlist = query['sort']
      else:
        # should have already been checked in resolve_sort
        raise MQLParseError(
            query,
            'sort directive must be a string or a list of strings',
            key='sort')
      for sortitem in sortlist:
        subq = query
        direction = '+'
        if sortitem[0] in '+-':
          direction = sortitem[0]
          sortitem = sortitem[1:]
        for ident in re.compile('\\.').split(sortitem):
          if ident == 'count' or ident == 'estimate-count' or \
                  ident == 'value' or ident == '/type/value/value':
            pass
          else:
            subq = element(subq[ident])
        if ident in ('count', 'estimate-count', 'index', '!index'):
          qp.sort_comparator += ['number']
        elif hasattr(subq, 'stype'):
          comp = subq.stype.get_comparator(varenv)
          if comp is None:
            comp = 'default'
          qp.sort_comparator += [comp]
        else:
          qp.sort_comparator += ['default']

  def get_link_fundamental_value(self, query):
    """ returns the value of this node (which may be a direct value or inside the dpn.

            If query is a list, it must have a single element, which is
            inspected for a
            value itself.
        """
    if isinstance(query, list):
      query = element(query)

    stype = query.stype

    value = None
    # the dpn is currently always 'value'
    dpn = stype.get_default_property_name()
    if dpn in query:
      value = query[dpn].value
      if query[dpn].comparisons is not None:
        raise MQLParseError(
            query,
            "Can't use a comparison operator on %(property)s",
            property=query.property.id)
      elif query[dpn].sort_var is not None:
        raise MQLParseError(
            query, "Can't sort on %(property)s", property=query.property.id)

    # we munge timestamps here if necessary.
    return stype.coerce_value(value)

  def build_low_json_root(self, query, varenv, mode):

    if isinstance(query, list):
      low_json = [
          self.build_low_json(x, varenv, mode)
          for x in high_elements(query, mode)
      ]
    else:
      low_json = self.build_low_json(query, varenv, mode)

    return low_json

  def build_low_json(self, query, varenv, mode):
    low_json = QueryDict(high_query=query)
    direct_attachment = False

    if query.property is not None:
      sprop = query.property
      # XXX need to handle -* and +*, not just +*
      if (sprop.has_id('/type/reflect/any_master') or
          sprop.has_id('/type/reflect/any_reverse') or
          sprop.has_id('/type/reflect/any_value')):

        if mode is not ReadMode:
          raise MQLInternalError(
              query,
              'Saw %(property)s in a %(mode)s',
              property=sprop.id,
              mode=str(mode))

        low_json[':type'] = '*'
        if (sprop.has_id('/type/reflect/any_master') or
            sprop.has_id('/type/reflect/any_reverse')):
          # must ask for the guid in case the user does { "link": null }
          # we don't want this to look like a value terminal.
          low_json['@guid'] = None
          # XXX hack until we get the real value=null semantics from gd/dev/35
          low_json[':datatype'] = 'null'

        if sprop.has_id('/type/reflect/any_reverse'):
          low_json[':reverse'] = True
        else:
          low_json[':reverse'] = False

      elif (sprop.has_id('/type/value/value') or
            sprop.has_id('/type/value/type') or
            sprop.has_id('/type/object/id') or
            sprop.has_id('/type/object/guid') or
            sprop.has_id('/type/object/timestamp') or
            sprop.type.has_id('/type/link')):
        raise MQLInternalError(query,
                               'We should not have recursed to this depth')
      elif (sprop.has_id('/type/object/creator') or
            sprop.has_id('/type/object/attribution')):
        # no :typeguid and :reverse for @scope
        direct_attachment = True
      else:
        low_json[':typeguid'] = sprop.typeguid
        low_json[':reverse'] = sprop.reverse

      # is this a unique property?
      # careful note: sprop may be /type/object/name (or some other unique text)
      # in that case we need to examine stype, but we do that in build_low_json_value()
      # where we overwrite the unique directive if necessary...
      if mode in (WriteMode, CheckMode):
        if sprop.is_master_unique() and sprop.is_reverse_unique():
          low_json[':unique'] = 'both'
        elif sprop.is_master_unique():
          low_json[':unique'] = 'right'
        elif sprop.is_reverse_unique():
          low_json[':unique'] = 'left'

    else:
      # we are at the root of the query
      pass

    if query.terminal and query.terminal in 'DLN':
      # it's optional if we didn't constrain it in some way, but a comparison counts as a constraint.
      if query.comparisons is None:
        # again, we need to treat @scope differently from anything else.
        if direct_attachment:
          low_json['@optional'] = True
        else:
          low_json[':optional'] = True
    elif query.terminal == 'V':
      # it's a specified value -- let's make sure we only see one so we don't break uniqueness...
      # XXX we can't use :pagesize in a write. What should we do?
      if not direct_attachment and mode is ReadMode:
        low_json[':pagesize'] = 1

    stype = query.stype
    if stype.get_category() == 'value':
      self.build_low_json_value(query, varenv, low_json, mode, stype)
    elif stype.get_category() == 'object':
      self.build_low_json_recurse(query, varenv, low_json, mode)
    else:
      raise MQLInternalError(
          query, "Can't generate clause for %(type)s", type=stype.id)

    query.low = low_json
    return low_json

  def build_low_json_link(self, query, varenv, low_json, mode):
    # links work more like values than objects.

    # note that here low_json is the outside clause,
    # whereas query is the 'link' subclause.
    query = element(query)

    for key in query:
      subq = query[key]
      # only null, a (direct) value
      if isinstance(subq,
                    list) or not subq.terminal or subq.terminal not in 'NV':
        raise MQLParseError(
            subq,
            'Can only use null or directly specified values in /type/link')

      # this is OK, as we must have the value here even if we pushed them down in resolve_property()
      value = subq.value

      if key == 'timestamp':
        stype = self.schema_factory.gettype('/type/datetime')
        low_json[':timestamp'] = stype.coerce_value(value)
        if subq.sort_var:
          low_json[':' + subq.sort_var] = 'timestamp'
      elif key == 'creator' or key == 'attribution':
        stype = self.schema_factory.gettype('/type/id')
        if value is not None:
          low_json[':scope'] = self.lookup_high_guid(
              stype.coerce_value(value), varenv)
        else:
          low_json[':scope'] = None

        if subq.sort_var:
          raise MQLParseError(subq, "Can't sort on %(key)s", key=key)
      elif key == 'master_property':
        stype = self.schema_factory.gettype('/type/id')
        if low_json.get(':typeguid') is not None and value is not None:
          raise MQLParseError(
              subq,
              "Can only specify 'master_property' if you use /type/reflect")
        elif low_json.get(':typeguid') is None and value is not None:
          del low_json[':type']
          low_json[':typeguid'] = self.lookup_high_guid(
              stype.coerce_value(value), varenv)
        elif low_json.get(':typeguid') is None:
          low_json[':typeguid'] = None
        else:
          # :typeguid is not None, value is None
          pass

        if subq.sort_var:
          raise MQLParseError(
              subq, "Can't sort on /type/link/master_property", key=key)

        #XXX need an else clause

      elif key == 'reverse':
        stype = self.schema_factory.gettype('/type/boolean')
        if value is not None:
          raise MQLParseError(
              subq, "Can only ask for the value of 'reverse', not specify it")
        if subq.sort_var:
          raise MQLParseError(subq, "Can't sort on /type/link/reverse", key=key)

      elif key == 'type':
        if value is not None and value != '/type/link':
          raise MQLParseError(
              subq,
              "Only '/type/link' is valid as the type of the 'link' pseudo-object'"
          )
        if subq.sort_var:
          raise MQLParseError(
              subq, 'Meaningless to sort on /type/link/type', key=key)

      elif key == 'operation':
        if value == 'insert':
          low_json[':oldest'] = 0
        elif value == 'update':
          low_json[':previous'] = None
          low_json[':oldest>'] = 0
        elif value == 'delete':
          low_json[':previous'] = None
          low_json[':live'] = False
        elif value is None:
          low_json[':previous'] = None
          low_json[':live'] = 'dontcare'
        else:
          raise MQLParseError(
              subq,
              "Valid values for 'operation' are 'insert', 'delete' and 'update'"
          )

        if subq.sort_var:
          raise MQLParseError(
              subq, "Can't sort on /type/link/operation", key=key)

      elif key == 'valid':
        stype = self.schema_factory.gettype('/type/boolean')
        if value is True:
          low_json[':newest'] = 0
        elif value is False:
          low_json[':next'] = None
          low_json[':newest>'] = 0
        elif value is None:
          low_json[':next'] = None
          low_json[':newest>='] = 0
        else:
          raise MQLParseError(subq, "'valid' requires a boolean argument")

        if subq.sort_var:
          raise MQLParseError(subq, "Can't sort on /type/link/valid", key=key)

      else:
        raise MQLParseError(
            query, 'property %(key)s does not exist in /type/link', key=key)

  def build_low_json_timestamp(self, query, varenv, low_json, mode):
    # note that we are passed the interior (query[key]) of the
    # timestamp directive, but the exterior of the low_json
    if mode in (WriteMode, CheckMode):
      raise MQLParseError(
          query,
          "Can't refer to timestamp in a %(mode)s query - it is automatically generated",
          key='timestamp',
          mode=str(mode))

    if isinstance(query, list):
      if len(query) == 1:
        query = query[0]
      else:
        # mode must be read, so the long list is a concern.
        raise MQLInternalError(query,
                               'Only expecting one item in the timestamp list')

    stype = query.stype

    value = None
    # the dpn of /type/datetime (required here) is always "value"
    dpn = stype.get_default_property_name()
    if dpn in query:
      value = query[dpn].value

    if value is None:
      if '@timestamp' not in low_json:
        low_json['@timestamp'] = None
    elif stype.get_datatype(value) not in (None, 'timestamp'):
      raise MQLTypeError(
          query,
          "Can't ask for anything other than /type/datetime",
          key='timestamp',
          value=value)
    elif low_json.get('@timestamp'):
      raise MQLParseError(
          query,
          "Can't specify 'timestamp' more than once in a single clause",
          value=value,
          previous_value=low_json['@timestamp'])
    else:
      low_json['@timestamp'] = value

    if dpn in query and query[dpn].comparisons is not None:
      for (compop, compval) in query[dpn].comparisons.iteritems():
        if compop in ['~=', '|=']:
          # see bug # 1067.
          raise MQLTypeError(
              query,
              "Can't use %(operator)s on type %(expected_type)s",
              key=dpn,
              value=value,
              expected_type=stype.id,
              datatype='timestamp',
              operator=compop)

        if stype.get_datatype(compval, compop) != 'timestamp':
          raise MQLTypeError(
              query,
              "Can't mix %(datatype)s and %(value)s with %(expected_type)s",
              key=dpn,
              value=value,
              expected_type=stype.id,
              datatype='timestamp')

        low_json['@timestamp' + compop] = stype.coerce_value(compval, compop)

    if dpn in query and query[dpn].sort_var is not None:
      low_json['@' + query[dpn].sort_var] = 'timestamp'

  def build_low_json_id(self, query, varenv, low_json, mode, wants_path_id):
    if isinstance(query, list):
      # treat multiple occurrences the same way as if we found them separately.
      for item in query:
        self.build_low_json_id(item, varenv, low_json, mode, wants_path_id)
      return

    stype = query.stype
    sprop = query.property

    value = None
    # the dpn of /type/id (required here) is always "value"
    dpn = stype.get_default_property_name()
    if dpn in query:
      value = query[dpn].value
      if query[dpn].sort_var is not None:
        raise MQLParseError(
            query,
            "Can't sort on %(property)s. Try sorting on /type/object/timestamp instead",
            property=sprop.id)

    if value is None:
      if '@guid' not in low_json:
        low_json['@guid'] = None
      if (wants_path_id and '@id' not in low_json):
        low_json['@id'] = None
    elif low_json.get('@guid') is not None:
      # we've already specified the guid
      raise MQLParseError(
          query,
          "Can't specify an id more than once in a single clause",
          value=value,
          previous_value=low_json['@guid'])
    elif (sprop.id == '/type/object/guid') and not valid_guid(value):
      raise MQLParseError(
          query, 'Can only use a hexadecimal guid here', value=value)
    elif stype.get_datatype(value) not in (None, 'guid'):
      raise MQLParseError(
          query, "Can't ask for anything other than /type/id", value=value)
    else:
      try:
        if sprop.id == '/type/object/id' or sprop.id == '/type/object/mid':
          value = self.lookup_high_guid(value, varenv)

        low_json['@guid'] = value
      except MQLError, e:
        e.set_query(query)
        raise e

    # only |= is valid on id or guid
    if dpn in query and query[dpn].comparisons is not None:
      # can't use iteritems() as we are changing the dict
      for (compop, compval) in query[dpn].comparisons.iteritems():
        if compop != '|=':
          raise MQLTypeError(
              query,
              "Can't use comparison operator %(operator)s on /type/id",
              key=dpn,
              value=value,
              expected_type='/type/id',
              datatype='guid',
              operator=compop)

        if low_json.get('@guid') is not None:
          raise MQLParseError(
              query,
              "Can't constrain with |= and specify a single value",
              key=dpn,
              value=value,
              expected_type='/type/id',
              datatype='guid')

        for compvalue in compval:
          if stype.get_datatype(compvalue) != 'guid' or (
              (sprop.id == '/type/object/guid') and not valid_guid(compvalue)):
            raise MQLTypeError(
                query,
                "Can't use an invalid id in a |= list",
                key=dpn,
                value=compvalue,
                expected_type='/type/id',
                datatype='guid')

        if sprop.id == '/type/object/id':
          # resolve all the ids at once. Keep a record of the resolutions so we can reverse them to put
          # the correct id next to each result.
          resolved_ids = self.querier.lookup.lookup_guids(compval, varenv)
          query[dpn].comparisons[compop] = resolved_ids

          # just need the values for lojson
          compval = resolved_ids.values()

        low_json['@guid'] = compval

  def build_low_json_scope(self, query, varenv, low_json, mode):
    if mode in (WriteMode, CheckMode):
      raise MQLParseError(
          query,
          "Can't refer to creator or attribution in a %(mode)s query - it is automatically generated",
          key='creator',
          mode=str(mode))

    # XXX OK, this is tough. We might get more than one 'creator' clause in a read.
    # they all (necessarily) point at the same place, and the graphd syntax supports
    # ( scope->(XXX) scope->(YYY) )
    # but lojson does not.
    if isinstance(query, list):
      if len(query) == 1:
        query = query[0]
      else:
        # mode must be read, so the long list is a concern.
        raise MQLInternalError(
            query, 'Only expecting one item in the creator or attribution list')

    # XXX here's the restriction. It's a bug but it's an edge case of an edge case...
    if '@scope' in low_json:
      raise MQLParseError(
          query,
          "Can't specify 'creator' or 'attribution' more than once in a single clause"
      )

    low_json['@scope'] = self.build_low_json(query, varenv, mode)

  def build_low_json_value(self, query, varenv, low_json, mode, stype):
    # the value property is always the default property
    value = None
    # did the user specify it?
    dpn = stype.get_default_property_name()
    if dpn in query:
      value = query[dpn].value

    if value is None and mode in (WriteMode, CheckMode):
      # note that we don't get timestamps or ids here. This is legal for them as they are generated by the write itself.
      raise MQLParseError(
          query,
          "Can't query for values of %(expected_type)s in a %(mode)s, only those of /type/id",
          key=dpn,
          value=value,
          expected_type=stype.id,
          mode=str(mode))

    if query.terminal == 'N' and query.comparisons is None:
      low_json[':optional'] = True

    explicit_type = None
    if 'type' in query and element(query['type']).terminal == 'V':
      explicit_type = element(query['type']).value

    if stype.id == '/type/value' and mode is not ReadMode:
      raise MQLParseError(
          query,
          "Can't use %(type)s in a %(mode)s",
          type=stype.id,
          mode=str(mode))

    try:
      # we want to set datatype if either
      # "value" was set or "type" was set
      low_json[':datatype'] = stype.get_datatype(
          value, comparison=None, explicit_type=explicit_type)
      low_json[':value'] = stype.coerce_value(value)

    except MQLTypeError, e:
      e.set_query(query)
      raise

    if dpn in query and query[dpn].comparisons is not None:
      for (compop, compval) in query[dpn].comparisons.iteritems():
        # need to check this first since it comes with a list of values we're not expecting here.
        if compop == '|=':
          raise MQLTypeError(
              query,
              "Can't use %(operator)s on %(expected_type)s, only on /type/id",
              key=dpn,
              value=value,
              expected_type=stype.id,
              operator=compop)

        # force the datatype if we have comparisons -- otherwise lojson will force it for us
        # and will probably get it wrong (it will force string rather than timestamp or boolean)
        if low_json[':datatype'] is None:
          low_json[':datatype'] = stype.get_datatype(compval, compop)
        elif low_json[':datatype'] != stype.get_datatype(compval, compop):
          raise MQLTypeError(
              query,
              "Can't mix %(datatype)s and %(value)s with %(expected_type)s",
              key=dpn,
              value=value,
              expected_type=stype.id,
              datatype=low_json[':datatype'])

        if low_json[':datatype'] == 'boolean':
          raise MQLTypeError(
              query,
              "Can't use comparison operators on boolean values",
              key=dpn,
              value=value,
              expected_type=stype.id,
              datatype=low_json[':datatype'])

        elif compop == '~=' and low_json[':datatype'] not in ('string',
                                                              'bytestring',
                                                              'url', 'key'):
          # see bug # 1067.
          raise MQLTypeError(
              query,
              "Can't use ~= on type %(expected_type)s",
              key=dpn,
              value=value,
              expected_type=stype.id,
              datatype=low_json[':datatype'])

        low_json[':value' + compop] = stype.coerce_value(compval, compop)

    if dpn in query and query[dpn].sort_var is not None:
      low_json[':' + query[dpn].sort_var] = 'value'

    if 'index' in query:
      index = element(query['index'])
      # value may be null or an integer
      low_json[':index'] = index.value
      if index.sort_var:
        low_json['?' + index.sort_var] = 'value'

    if 'link' in query:
      self.build_low_json_link(query['link'], varenv, low_json, mode)

    if stype.id in ['/type/key', '/type/enumeration']:
      # special case for uniqueness on /type/key
      low_json[':unique'] = 'key'

    elif ':unique' in low_json:
      # other value types which were stated to be unique, get value uniqueness.
      low_json[':unique'] = 'value'

    comp = stype.get_comparator(varenv)
    if comp is not None:
      low_json[':comparator'] = comp

    rpn = stype.get_right_property_name(query)
    if stype.id == '/type/enumeration':
      # we're an enumeration, so our left (we are reversed) is the enumerating namespace
      low_json['@guid'] = query.property.enumeration

    elif rpn and rpn in query:
      # if we have a right PD, use it now as the query
      # as it goes at the same level, not one deeper

      # if this is a list, it must have only one thing in it, regardless of mode.
      if isinstance(query[rpn], list) and len(query[rpn]) > 1:
        raise MQLParseError(
            query,
            '%(property)s is always unique. Specifying more than one value is impossible',
            property=stype.id + '/' + rpn,
            key=rpn)

      # element is OK now, because an RPN must be unique
      self.build_low_json_recurse(element(query[rpn]), varenv, low_json, mode)

    elif rpn:

      if mode is ReadMode:
        # we didn't ask for the RPN. But since we are /type/text or /type/key, we don't want to force
        # query.terminal and right=null in lojson. So let's ask for the guid anyway
        low_json['@guid'] = None
      else:
        raise MQLParseError(
            query,
            "Must specify '%(key)s' when using %(type)s in a %(mode)s",
            key=rpn,
            type=stype.id,
            mode=str(mode))

    elif stype.id == '/type/value':
      # values include keys and text, but not value=null

      low_json['@guid'] = None
      low_json['@optional'] = True
      # put in a bogus comparison operator if we don't already have one,
      # we really want "value!=null" but that doesn't exist...
      # note we need a hack in qprim.py:check_or_make_datatype()
      # to not make this force datatype=string...
      if ':value~=' not in low_json:
        low_json[':value~='] = '*'

    if mode in (WriteMode, CheckMode):
      self.handle_value_write_directives(query, varenv, low_json)
    elif mode is ReadMode:
      self.handle_value_read_directives(query, varenv, low_json)

  def handle_value_read_directives(self, query, varenv, low_json):
    if 'limit' in query:
      if isinstance(query['limit'], (int, long)) and query['limit'] >= 0:
        low_json[':pagesize'] = query['limit']
      else:
        raise MQLParseError(
            query,
            'limit must be a non-negative integer',
            key='limit',
            value=query['limit'])

    if 'optional' in query:
      if isinstance(query['optional'], bool):
        low_json[':optional'] = query['optional']
      else:
        raise MQLParseError(
            query,
            'optional takes a boolean argument',
            key='optional',
            value=query['optional'])

    if 'sort' in query:
      # handle_sort should have done the necessary validation
      low_json[':sort'] = query.sort

  def handle_value_write_directives(self, query, varenv, low_json):
    # XXX should we check for invalid properties at this level?
    if 'connect' in query:
      value = query['connect']
      if value == 'insert':
        low_json[':link'] = True
      elif value == 'delete':
        low_json[':unlink'] = True
      elif value in ('update', 'replace'):
        # update always works in the direction you are looking; the other end changes; so it must be unique
        is_unique = True

        if not query.property.is_master_unique():
          if query.property.reverse and query.property.enumeration and query.property.is_reverse_unique(
          ):
            # unique enumerated properties are OK. They are the only sort of unique reverse value properties
            pass
          elif query.property.id in [
              '/type/object/key', '/type/namespace/keys'
          ]:
            # these are special exceptions to the "only unique things can be updated" rule.
            pass
          elif value == 'update':
            raise MQLParseError(
                query,
                "Can't use 'connect': 'update' on a non-unique value",
                key='connect',
                value=value)
          else:
            # it's not unique but we're doing replace; that's OK.
            is_unique = False

        low_json[':link'] = True

        if is_unique:
          # all updates are case-sensitive so that you can change "freD SMIth" into "Fred Smith" if you need to.
          low_json[':comparator'] = 'octet'
          # if replace is actually going to do a replacement.

          # set the update field to the field we want to change, which may be left, right or value.
          if query.stype.category == 'value':
            # this is tricky; we want to update the key on a unique namespace (but we don't know if it is unique yet or not)
            # we have to distinguish this case from the namespace update itself (ick)
            # *** so this includes /type/namespace/keys and /type/object/key, but NOT /type/key/namespace! (which gets 'right')
            if query.stype.id == '/type/enumeration' or query.property.id in [
                '/type/namespace/keys', '/type/object/key'
            ]:
              if value == 'update':
                low_json[':update'] = 'key'
              else:
                # we have to defer the insert or update
                # decision until we can inspect the namespace
                # for uniqueness. bug 6949
                low_json[':update'] = 'keyreplace'
            else:
              low_json[':update'] = 'value'
          elif query.property.reverse:
            low_json[':update'] = 'left'
          else:
            # the default case
            low_json[':update'] = 'right'

      else:
        raise MQLParseError(
            query,
            "The valid arguments to 'connect' are 'insert','delete', 'update' or 'replace'",
            key='connect',
            value=value)
    elif 'create' in query:
      raise MQLParseError(
          query,
          "Only 'connect' is valid in value types, not 'create' as they are assumed to exist",
          key='create',
          value=query['create'])
    else:
      # no directive, do nothing
      pass

  def build_low_json_recurse(self, query, varenv, low_json, mode):
    for key in query:
      # this is not strictly necessary, but so easy to screw up the check is sane...
      if key == 'value':
        raise MQLInternalError(
            query,
            "Can't expand a value clause as a reference subquery",
            key=key)

      elif key == 'index':
        index = element(query[key])
        # value may be null or an integer
        low_json[':index'] = index.value
        if index.sort_var:
          low_json['?' + index.sort_var] = 'value'

      elif key == 'link':
        self.build_low_json_link(query[key], varenv, low_json, mode)

      elif key in self.write_directives and mode in (WriteMode, CheckMode):
        self.handle_write_directive(query, varenv, low_json, key)
      elif key in self.read_directives and mode is ReadMode:
        self.handle_read_directive(query, varenv, low_json, key)
      elif key in self.directives:
        raise MQLParseError(
            query,
            "Can't use directive %(key)s in %(mode)s",
            key=key,
            mode=str(mode))

      elif valid_comparison(key) and mode is ReadMode:
        # we've dealt with these in resolve_names, don't need to do them now.
        pass
      elif valid_mql_key(key):
        # for a write we will assume that the schema property is identical in all elements
        # This is currently true, but may not always be the case.
        if isinstance(query[key], list):
          sprop = query[key][0].property
        elif isinstance(query[key], dict):
          sprop = query[key].property

        # the three irregular cases for things directly attached to the object
        if sprop.id == '/type/object/id':
          self.build_low_json_id(query[key], varenv, low_json, mode, True)
        elif sprop.id == '/type/object/mid':
          self.build_low_json_id(query[key], varenv, low_json, mode, True)
        elif sprop.id == '/type/object/guid':
          self.build_low_json_id(query[key], varenv, low_json, mode, False)
        elif sprop.id == '/type/object/timestamp':
          self.build_low_json_timestamp(query[key], varenv, low_json, mode)
        elif sprop.id == '/type/object/creator' or sprop.id == '/type/object/attribution':
          self.build_low_json_scope(query[key], varenv, low_json, mode)

        elif isinstance(query[key], dict):
          low_json[key] = self.build_low_json(query[key], varenv, mode)
        elif isinstance(query[key], list):
          low_json[key] = [
              self.build_low_json(x, varenv, mode)
              for x in high_elements(query[key], mode)
          ]
        else:
          raise MQLInternalError(
              query, "Encountered '%(key)s' during build_low_json", key=key)
      elif key == '*' and mode is ReadMode:
        # we resolved the wildcard earlier
        pass
      else:
        raise MQLInternalError(
            query,
            "Encountered bogus key '%(key)s' during build_low_json",
            key=key)

    # do some last minute checks now we've built the whole array
    if mode in (WriteMode, CheckMode):
      # fix for bug 4590
      if low_json.get('@guid') is not None and 'create' in query:
        raise MQLParseError(
            query,
            "Can't specify the id or guid and also use 'create'",
            key='create')

  def handle_read_directive(self, query, varenv, low_json, key):
    value = query[key]

    if query.is_root:
      direct_attachment = True
    elif query.property.id in ('/type/object/creator',
                               '/type/object/attribution'):
      direct_attachment = True
    else:
      direct_attachment = False

    if key == 'optional':
      if isinstance(value, bool):
        if direct_attachment:
          low_json['@optional'] = value
        else:
          low_json[':optional'] = value
      else:
        raise MQLParseError(
            query, "'optional' takes a boolean argument", key=key, value=value)

    elif key == 'limit':
      # need to put @pagesize on the outermost part of the query, :pagesize elsewhere
      if isinstance(value, (int, long)) and value >= 0:
        if direct_attachment:
          low_json['@pagesize'] = value
        else:
          low_json[':pagesize'] = value
      else:
        raise MQLParseError(
            query,
            "'limit' must be a non-negative integer",
            key=key,
            value=value)

    elif key == 'sort':
      # handle_sort() has done validation of query.sort
      if direct_attachment:
        low_json['@sort'] = query.sort
      else:
        low_json[':sort'] = query.sort

    elif key == 'macro':
      # we've done this already
      pass

    else:
      raise MQLInternalError(
          query, "Didn't understand read directive", key=key, value=value)

  def handle_write_directive(self, query, varenv, low_json, key):
    value = query[key]
    if key == 'create':
      if value == 'unconditional':
        low_json['@insert'] = True
      elif value == 'unless_exists':
        if not query.is_root:
          low_json[':link'] = True
        low_json['@ensure'] = True
      elif value == 'unless_connected':
        if query.is_root:
          raise MQLParseError(
              query,
              "Can't use 'create': 'unless_connected' at the root of the query",
              key=key,
              value=value)
        low_json['@ensure'] = True
      else:
        raise MQLParseError(
            query,
            "The valid arguments to 'create' are 'unconditional','unless_exists' or 'unless_connected'",
            key=key,
            value=value)

    elif key == 'connect':
      if query.is_root:
        raise MQLParseError(
            query,
            "Can't use 'connect' at the root of the query",
            key=key,
            value=value)

      if value == 'insert':
        # see update - same logic applies...
        if 'create' not in query:
          low_json[':link'] = True

      elif value == 'delete':
        if 'create' in query:
          raise MQLParseError(
              query,
              "Can't use 'create' with 'connect': 'delete'",
              key=key,
              value=value)

        low_json[':unlink'] = True
      elif value in ('update', 'replace'):
        # update always works in the direction you are looking; the other end changes; so it must be unique
        is_unique = True

        if query.property.reverse and not query.property.is_reverse_unique():
          if value == 'update':
            raise MQLParseError(
                query,
                "Can't use 'connect': 'update' on a non-unique reverse property",
                key=key,
                value=value)
          else:
            is_unique = False

        if not query.property.reverse and not query.property.is_master_unique():
          if value == 'update':
            raise MQLParseError(
                query,
                "Can't use 'connect': 'update' on a non-unique master property",
                key=key,
                value=value)
          else:
            is_unique = False

        if query.property.id == '/type/text/lang':
          raise MQLParseError(
              query,
              "Can't update or replace the language of /type/text values, try delete and insert",
              key=key,
              value=value)

        if query.property.id == '/type/key/namespace':
          if query.parent_clause and (
              query.parent_clause.property.id == '/type/object/key'):
            if value == 'update':
              raise MQLParseError(
                  query,
                  "Can't update or replace parent namespaces, only children. Use /type/namespace/keys instead",
                  key=key,
                  value=value)
            else:
              is_unique = False

        # we only want to put in a :link clause if we have create: unless_exists, not otherwise.
        # so let create: unless_exists put the :link clause in itself.
        if 'create' not in query:
          low_json[':link'] = True

        if is_unique:
          if query.property.reverse:
            low_json[':update'] = 'left'
          else:
            low_json[':update'] = 'right'

      else:
        raise MQLParseError(
            query,
            "The valid arguments to 'connect' are 'insert','delete','update' or 'replace'",
            key=key,
            value=value)

    else:
      raise MQLInternalError(
          query, "Unimplemented write directive '%(key)s'", key=key)

  def get_terminal_result(self, query, result, varenv, mode):
    # these all want basic (string, int etc) terminals
    # where is the string?? - let's examine the prop to find out...
    stype = query.stype
    if stype.category == 'value':
      # we have a value type here, so we asked for :value directly

      # we completely failed to find an optional value
      if result is None:
        return None
      else:
        return stype.uncoerce_value(result[':value'], result[':datatype'],
                                    varenv.get('unicode_text'))
    elif stype.category == 'object':
      # we have an object type. We must have expanded this to 'name' or similar.
      # we stashed where to go in .default way back when...
      default = query.default

      # it's still OK to use  == 'id' here rather than has_id()
      # we know we only generate 'id', not '/type/object/id' or other BS craziness
      if default == 'id':
        # XXX this is a really bad hack!
        # XXX only try to get "good" ids for types that claim to have or need them
        # so user/lang/type/property get ids (if they have a default_property_name
        # of 'id'), everyone else just gets the guid...
        if query.value:
          return query.value

        elif query.comparisons and '|=' in query.comparisons:
          # if you gave me a list of ids, you get a result from that list.
          return self.lookup_high_id_from_map(result['@guid'],
                                              query.comparisons['|='], varenv)
        else:
          return self.lookup_high_id(result['@guid'], varenv)

      else:
        return self.get_terminal_result(query[default], result[default], varenv,
                                        mode)
    else:
      raise MQLInternalError(query, "Can't generate terminal for %(type)s",
                             query.stype.id)

  def get_value_result(self, query, result, varenv, mode):
    # we can assume this is a non-terminal query
    if query.terminal and query.terminal in 'LVN':
      raise MQLInternalError(
          query, "Don't handle terminals here", terminal=query.terminal)

    filter_result = {}
    stype = query.stype
    property_name = stype.get_default_property_name()
    for key, clause in query.iteritems():
      is_list = isinstance(clause, list)
      if key == property_name:
        # XXX default property is always value (better not ever change this)
        filter_result[key] = stype.uncoerce_value(result[':value'],
                                                  result[':datatype'],
                                                  varenv.get('unicode_text'))
      elif key == 'type':
        # XXX for orthogonality reasons you should be able to expand a value_type node. Right now
        # this is completely unsupported.
        # ZZZ really badly broken actually - should at least look at datatype.
        res = stype.get_value_type(result[':datatype'])
        if is_list:
          res = [res]
        filter_result[key] = res
      elif key == stype.get_right_property_name(query):
        # this is the interesting case
        # we descend into the query, but NOT the result.
        res = self.create_high_result(element(clause), result, varenv, mode)
        if is_list:
          res = [res]
        filter_result[key] = res
      elif key == 'index':
        filter_result[key] = result[':index']
      elif key == 'link':
        filter_result[key] = self.get_link_result(clause, result, varenv)
      elif key in self.write_directives and mode in (WriteMode, CheckMode):
        filter_result[key] = self.create_value_write_directive_result(
            query, result, varenv, key)
      elif key in self.read_directives and mode is ReadMode:
        pass
      elif key in self.directives:
        raise MQLInternalError(
            query,
            'Encountered unexpected directive %(key)s in %(mode)s',
            key=key,
            mode=str(mode))
      elif valid_comparison(key) and mode is ReadMode:
        pass
      elif key == '*' and mode is ReadMode:
        pass
      else:
        raise MQLInternalError(
            query,
            "Don't know about key %(key)s in value type %(expected_type)s",
            key=key,
            expected_type=stype.id)

    return filter_result

  def get_id_result(self, query, result, varenv, mode, wants_path_id):
    is_list = False
    if isinstance(query, list):
      is_list = True
      elem = query[0]
    else:
      elem = query

    # did we ourselves specify a value? If so, it's in elem.stype.get_default_property_name() (phew!)
    if elem.stype.get_default_property_name() in elem and elem[
        elem.stype.get_default_property_name()].value:
      returned_id = elem[elem.stype.get_default_property_name()].value

    elif wants_path_id and (
        elem[elem.stype.get_default_property_name()].comparisons and
        '|=' in elem[elem.stype.get_default_property_name()].comparisons):
      # if you gave me a list of ids, you get a result from that list.
      returned_id = self.lookup_high_id_from_map(
          result['@guid'],
          elem[elem.stype.get_default_property_name()].comparisons['|='],
          varenv)

    elif wants_path_id:
      # XXX we only want to produce a true id when we had a type that
      # needed to be named.
      returned_id = self.lookup_high_id(result['@guid'], varenv)

    else:
      returned_id = result['@guid']

    # now fake it the same way we do for timestamp
    fake_rv = {':value': returned_id, ':datatype': 'guid'}
    if is_list:
      fake_rv = [fake_rv]

    return self.create_high_result(query, fake_rv, varenv, mode)

  def get_link_result(self, query, result, varenv):
    is_list = False
    if isinstance(query, list):
      is_list = True
      query = element(query)

    if query.terminal and query.terminal == 'V':
      filter_result = query.value
    elif query.terminal and query.terminal in 'NL':
      filter_result = self.lookup_high_id(result[':typeguid'], varenv)
    else:
      filter_result = {}
      for key in query:
        if key == 'timestamp':
          stype = self.schema_factory.gettype('/type/datetime')
          filter_result[key] = \
              stype.uncoerce_value(result[':timestamp'],
                                   'timestamp',
                                   varenv.get('unicode_text'))
        elif key == 'creator' or key == 'attribution':
          # XXX need to resolve to an id if possible.
          filter_result[key] = self.lookup_high_id(result[':scope'], varenv)
        elif key == 'master_property':
          # XXX need to resolve to an id if possible.
          filter_result[key] = self.lookup_high_id(result[':typeguid'], varenv)
        elif key == 'reverse':
          # XXX need to resolve to an id if possible.
          filter_result[key] = result[':reverse']
        elif key == 'type':
          filter_result[key] = '/type/link'
        elif key == 'operation':
          res = 'insert'
          if result.get(':live', True) == False:
            res = 'delete'
          elif result.get(':previous') is not None:
            res = 'update'

          filter_result[key] = res

        elif key == 'valid':
          res = True
          if result.get(':next') is not None:
            res = False
          elif result.get(':live', True) == False:
            res = None

          filter_result[key] = res
        else:
          raise MQLInternalError(
              query,
              'Unexpected key %(key)s while processing link result',
              key=key)

    if is_list:
      filter_result = [filter_result]

    return filter_result

  def create_high_result(self, query, result, varenv, mode):
    # pin the result to the query.
    if isinstance(result, list):
      # This is different for reads and writes...
      if mode is ReadMode:
        if not isinstance(query, list) or len(query) != 1:
          raise MQLInternalError(
              query,
              'result query mismatch (%(mode)s list)',
              result=result,
              mode=str(mode))

        filter_result = []
        for elem in result:
          filter_result.append(
              self.create_high_result(query[0], elem, varenv, mode))

      elif mode in (WriteMode, CheckMode):
        if not isinstance(query, list) or len(query) != len(result):
          raise MQLInternalError(
              query,
              'result query mismatch (%(mode)s list)',
              result=result,
              mode=str(mode))

        filter_result = []
        for i in xrange(len(query)):
          filter_result.append(
              self.create_high_result(query[i], result[i], varenv, mode))

      else:
        raise MQLInternalError(query, 'Unknown mode %(mode)s', mode=str(mode))

    elif isinstance(result, dict):
      if not isinstance(query, dict):
        raise MQLInternalError(
            query,
            'result query mismatch (%(mode)s dict)',
            result=result,
            mode=str(mode))

      # we treat the empty dictionary as a non-terminal (it contains implicit terminals.)
      # terminal = D is a terminal only in the query - expansion in the result is expected.
      if query.terminal and query.terminal in 'LVN':
        filter_result = self.get_terminal_result(query, result, varenv, mode)
      # this is a non-terminal value query
      elif query.stype.category == 'value':
        filter_result = self.get_value_result(query, result, varenv, mode)
      elif query.stype.category == 'object':
        filter_result = {}
        query_property = query.stype.get_default_property_name()
        for key, clause in query.iteritems():
          if key == 'index':
            filter_result['index'] = result[':index']
          elif key == 'link':
            filter_result[key] = self.get_link_result(clause, result, varenv)

          elif key in self.write_directives and mode in (WriteMode, CheckMode):
            filter_result[key] = self.create_write_directive_result(
                query, result, varenv, key)
          elif key in self.read_directives and mode is ReadMode:
            pass
          elif key in self.directives:
            raise MQLInternalError(
                query,
                'Unexpected directive %(key)s in %(mode)s',
                key=key,
                mode=str(mode))
          elif key == '*' or valid_comparison(key):
            if mode is not ReadMode:
              raise MQLInternalError(
                  query,
                  'Unexpected operator in %(mode)s',
                  key=key,
                  mode=str(mode))
            # otherwise do nothing

          else:
            is_list = False
            if isinstance(clause, list):
              inner_property = clause[0].property
              is_list = True
            else:
              inner_property = clause.property

            if inner_property.id == '/type/object/id':
              filter_result[key] = self.get_id_result(clause, result, varenv,
                                                      mode, True)

            elif inner_property.id == '/type/object/guid':
              filter_result[key] = self.get_id_result(clause, result, varenv,
                                                      mode, False)

            elif inner_property.id == '/type/object/mid':
              filter_result[key] = mid.of_guid(result['@guid'][1:])

            elif inner_property.id == '/type/object/creator' or inner_property.id == '/type/object/attribution':
              fake_rv = result['@scope']
              if is_list:
                if fake_rv is not None:
                  fake_rv = [fake_rv]
                else:
                  fake_rv = []

              filter_result[key] = self.create_high_result(
                  clause, fake_rv, varenv, mode)

            elif inner_property.id == '/type/object/timestamp':
              # lets fake the entire result we would get back from any other datetime object!
              fake_rv = {
                  ':value': result['@timestamp'],
                  ':datatype': 'timestamp'
              }
              if is_list:
                fake_rv = [fake_rv]

              filter_result[key] = self.create_high_result(
                  clause, fake_rv, varenv, mode)

            elif key not in result:
              raise MQLInternalError(
                  query,
                  'key from query missing in result',
                  key=key,
                  result=result)
            else:
              filter_result[key] = self.create_high_result(
                  clause, result[key], varenv, mode)
      else:  # query.stype.category
        raise MQLInternalError(
            query,
            "Can't process %(type)s in create_high_result()",
            type=query.stype.id)

    elif result is None:
      # XXX this makes the exceptions go away, but I really don't know that it is always correct.
      filter_result = None
    else:
      raise MQLInternalError(
          query, 'unexpected token during result parsing', result=result)

    return filter_result

  def create_write_directive_result(self, query, result, varenv, key):
    if key == 'connect':
      value = query[key]
      if value == 'insert' and (result.get(':link') or result.get('@ensure') or
                                result.get('@insert')):
        filter_result = 'inserted'
      elif value == 'delete' and result[':unlink']:
        filter_result = 'deleted'
      elif value == 'update' and result[':update']:
        filter_result = 'updated'
      elif value == 'replace' and result.get(':update'):
        filter_result = 'updated'
      elif value in ('update', 'replace') and (result.get(':link') or
                                               result.get('@ensure') or
                                               result.get('@insert')):
        filter_result = 'inserted'
      elif value == 'delete':
        filter_result = 'absent'
      else:
        filter_result = 'present'
    elif key == 'create':
      value = query[key]
      if value == 'unconditional' and result['@insert']:
        filter_result = 'created'
      elif value == 'unless_exists' and result['@ensure']:
        filter_result = 'created'
      elif value == 'unless_connected' and result['@ensure']:
        filter_result = 'created'
      elif value == 'unless_exists' and result.get(':link', None):
        # the one special case - note that :link may not have been generated
        filter_result = 'connected'
      else:
        filter_result = 'existed'

    else:
      raise MQLInternalError(
          query,
          'Not prepared for directive while creating results',
          key=key,
          value=query[key])

    return filter_result

  def create_value_write_directive_result(self, query, result, varenv, key):
    if key == 'connect':
      value = query[key]
      if value == 'insert' and result[':link']:
        filter_result = 'inserted'
      elif value == 'delete' and result[':unlink']:
        filter_result = 'deleted'
      elif value in ('update', 'replace') and result.get(':update'):
        filter_result = 'updated'
      elif value in ('update', 'replace') and result[':link']:
        filter_result = 'inserted'
      elif value == 'delete':
        filter_result = 'absent'
      else:
        filter_result = 'present'
    else:
      # 'create' is illegal in a value.
      raise MQLInternalError(
          query,
          'Not prepared for directive while creating results',
          key=key,
          value=query[key])

    return filter_result

  def create_normalized_result(self, query, varenv):

    def update_key(old_key, new_key):
      old_match = valid_mql_key(old_key)
      new_match = valid_mql_key(new_key)
      if old_match and new_match:
        (old_rev, old_label, old_id) = old_match.groups()
        (new_rev, new_label, new_id) = new_match.groups()
        label = ''
        if new_label or old_label:
          label = (new_label or old_label) + ':'
        return (new_rev or old_rev or '') + label + (new_id or old_id)
      elif old_match:
        (old_rev, old_label, old_id) = old_match.groups()
        return (new_rev or old_rev) + (new_label or old_label) + (
            new_key or old_id)
      else:
        return new_key

    if isinstance(query, QueryDict):
      if query.terminal and query.terminal in 'VNL':
        # just echo back the original value - we don't normalize terminal values
        # anyway and otherwise we need to deal with decoding back to unicode
        # return query.value
        return query.get_orig()
      elif query.terminal and query.implied:
        return None
      else:
        rv = {}
        if query.is_root:
          stype = query.stype
        else:
          stype = query.property.type
        for k, v in query.iteritems():
          if k == '*':
            continue
          v2 = self.create_normalized_result(v, varenv)
          comp = valid_comparison(k)
          if comp:
            (reversed, prefix, idname, compop) = comp.groups()
            truekey = make_comparison_truekey(
                reversed, prefix,
                query.stype.getprop(idname, varenv).id)
            k = truekey + compop
          elif k == 'sort':
            if isinstance(v, str):
              sortlist = [v]
            elif isinstance(v, list):
              sortlist = v
            else:
              # should have already been checked in resolve_sort
              raise MQLParseError(
                  query,
                  'sort directive must be a string or a list of strings',
                  key='sort')
            resolved_sortitems = []
            for sortitem in sortlist:
              subq = query
              direction = '+'
              if sortitem[0] in '+-':
                direction = sortitem[0]
                sortitem = sortitem[1:]
              resolved = None
              for ident in re.compile('\\.').split(sortitem):
                if ident == 'count':
                  if resolved:
                    resolved = resolved + '.' + ident
                  else:
                    resolved = direction + ident
                else:
                  subq = element(subq[ident])
                  if not reserved_word(ident, True):
                    if subq.property and subq.property.id:
                      ident = update_key(ident, subq.property.id)
                  if resolved:
                    resolved = resolved + '.' + ident
                  else:
                    resolved = direction + ident
              resolved_sortitems.append(resolved)
            v2 = resolved_sortitems
          elif not reserved_word(k, True):
            if isinstance(v, QueryList):
              k2 = v[0].property.id
            else:
              k2 = v.property.id
            k = update_key(k, k2)
          if not rv.has_key(k):
            rv[k] = v2
          else:
            raise MQLParseError(
                query,
                'query with duplicate keys after normalization (use key prefix)',
                key=k)
        return rv
    elif isinstance(query, QueryList):
      rv = [self.create_normalized_result(elt, varenv) for elt in query]
      if rv in ([None], [[]]):
        return []
      return rv
    else:
      return query

  def lookup_all_ids(self, query, varenv):
    varenv.lookup_manager.guid_dict.update(
        self.querier.lookup.lookup_ids(varenv.lookup_manager.guid_list, varenv))

  def lookup_high_id(self, guid, varenv):
    # the schema factory maintains a cache of the ids it has previously seen.
    if self.schema_factory.lookup_id(guid, varenv):
      return self.schema_factory.lookup_id(guid, varenv)
    elif guid in varenv.lookup_manager.guid_dict:
      return varenv.lookup_manager.guid_dict[guid]
    elif guid is False:
      return False
    else:
      # we should never be here. But sometimes (bug 5911) the graph won't tell us about guids we should be seeing
      # because it's internal replication system hasn't caught up. An MQLInternalError is most unfriendly.

      # so let's have another try here...
      LOG.error(
          'mql.internal.guid.lookup',
          'Lookup of guid failed, retrying in lookup_high_id()',
          guid=guid)
      return self.querier.lookup.lookup_id(guid, varenv)

  def lookup_high_guid(self, id, varenv):
    guid = self.querier.lookup.lookup_guid(id, varenv)

    # later, we want to reverse this exactly.
    varenv.lookup_manager.guid_dict[guid] = id

    return guid

  def lookup_high_id_from_map(self, guid, mapping, varenv):
    # the mapping is id->guid, so we need to reverse it
    for (map_id, map_guid) in mapping.iteritems():
      if map_guid == guid:
        return map_id

    raise MQLInternalError(
        None, 'Unknown id from |= list', guid=guid, list=mapping.keys())


def cmdline_main():
  from mql.mql import cmdline
  op = cmdline.OP(usage='%prog [-g GRAPHD_ADDR] [...] <query>')

  op.add_option(
      '-f',
      '--file',
      dest='query_file',
      default=None,
      help='file containing a query')

  op.add_option(
      '-w',
      '--write',
      dest='write',
      default=False,
      action='store_true',
      help='perform a write query')

  op.add_option(
      '-c',
      '--check',
      dest='check',
      default=False,
      action='store_true',
      help='output what a write query will do')

  op.add_option(
      '-r',
      '--raw',
      dest='raw',
      default=False,
      action='store_true',
      help='use eval rather than mql.json to eval query')

  op.add_option(
      '-m',
      '--macro',
      dest='macro',
      default=None,
      help='substitute this dictionary as macros')

  op.add_option(
      '--escape', dest='escape', default='html', help='Choose escape type')

  op.add_option(
      '--write_dateline',
      dest='write_dateline',
      default=None,
      help='Specify a write dateline')

  op.add_option(
      '--asof', dest='asof', default=None, help='Run query asof a given guid')

  op.add_option(
      '-n',
      '--normalize',
      dest='normalize',
      default=False,
      action='store_true',
      help='normalize query (resolving schema only)')

  options, args = op.parse_args()

  queryfile = options.query_file
  if queryfile == '-':
    query = ''.join(sys.stdin.readlines())
    regex = re.compile('[\n\t]+')
    query = regex.sub(' ', query)
  elif queryfile is not None:
    qf = open(queryfile, 'r')
    query = ''.join(qf.readlines())
    regex = re.compile('[\n\t]+')
    query = regex.sub(' ', query)
    qf.close()
  elif len(args) == 1:
    query = args[0]
  else:
    op.error('at most one query argument allowed')

  macro = None
  if options.raw:
    query = eval(query)
    if options.macro:
      macro = eval(options.macro)

  else:
    # XXX should eventually use unicode, for now utf8
    query = json.loads(query)
    if options.macro:
      print options.macro
      macro = json.loads(options.macro)

  if macro:
    op.varenv['macro'] = macro

  if options.escape == 'false':
    options.escape = False
  op.varenv['escape'] = options.escape

  if options.write_dateline:
    op.varenv['write_dateline'] = options.write_dateline

  if options.asof:
    op.varenv['asof'] = options.asof

  try:
    if options.normalize:
      result = op.ctx.high_querier.normalize(query, op.varenv)
    elif options.write:
      result = op.ctx.high_querier.write(query, op.varenv)
    elif options.check:
      result = op.ctx.high_querier.check(query, op.varenv)
    else:
      result = op.ctx.high_querier.read(query, op.varenv)

    if options.cursor is not None:
      print 'cursor=%s' % repr(op.varenv['cursor'])

    if options.write_dateline:
      print 'dateline=%s' % repr(op.varenv['dateline'])

    if options.raw:
      pprint.pprint(result)
    else:
      print json.struct2json(result, sort=True)

  except MQLError, e:
    e.add_error_inside()

    if options.raw:
      pprint.pprint(e.error)
    else:
      print json.struct2json(e.error, sort=True)


if __name__ == '__main__':
  cmdline_main()
