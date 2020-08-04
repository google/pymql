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

import re

if __name__ == '__main__':
  #import site
  #site.addsitedir('../..')
  import sys, os
  sys.path.append(os.path.abspath('../..'))

from pymql.log import LOG, log_util, pprintlog, dumplog
from pymql.error import EmptyResult
#from pymql import json
import json

import scope
import lookup
import pprint
import cgi

from qprim import QueryPrimitive
from utils import (element, elements, dict_recurse, valid_relname, follow_path,
                   is_direct_pointer, valid_precompiled_sort, ReadMode,
                   WriteMode, PrepareMode, CheckMode, QueryDict, QueryList,
                   ResultDict, Missing)
from error import MQLResultError, MQLInternalError, MQLInternalParseError, MQLAccessError, MQLGraphError
from env import Varenv


class LowQuery(object):

  def __init__(self, gc):
    self.gc = gc
    self.lookup = lookup.NamespaceFactory(self)

  def make_orig(self, query):
    if isinstance(query, dict):
      rv = QueryDict(original_query=query)
      if hasattr(query, 'high_query'):
        rv.high_query = query.high_query
      for key, value in query.iteritems():
        rv[key] = self.make_orig(value)

    elif isinstance(query, (list, set)):
      rv = QueryList()
      rv.extend(self.make_orig(elem) for elem in query)
    elif isinstance(query, (str, float, int, long, bool, type(None))):
      rv = query
    elif isinstance(query, unicode):
      raise MQLInternalParseError(
          None,
          'Unicode strings not supported in direct interface. Use utf-8 instead.',
          value=query)
    else:
      raise MQLInternalParseError(
          query, 'unhandled type %s in descendent %s' % (type(query), query))

    return rv

  # adds parent_clause, key, list decorators
  def make_parents(self, query, root=False):
    # root applies to both the list and its immediate contents
    if root:
      query.is_root = True

    # need this special case - only runs on the very outside to set list
    if isinstance(query, list):
      for elem in query:
        if isinstance(elem, dict):
          elem.list = query
          self.make_parents(elem, root)

    elif isinstance(query, dict):
      # terminal implies that there are no '@' or 'display_name' style clauses
      # ie. that there is only a link here, no node.
      # note that ? is not counted as a non-terminal
      terminal = True
      for key, clause in query.iteritems():
        if key[0] not in '?:':
          terminal = False

        if isinstance(clause, dict):
          clause.parent_clause = query
          clause.key = key
          self.make_parents(clause)
        elif isinstance(clause, list):
          clause.parent_clause = query
          clause.key = key
          for elem in clause:
            # we allow [ {} ] and decorate {} only in this case.
            # so 'display_name': [ {} ] works (it just counts the display-names)
            if isinstance(elem, dict):
              elem.parent_clause = query
              elem.list = clause
              elem.key = key
              self.make_parents(elem)

      if terminal:
        # True??
        query.terminal = True

  def lookup_boot_guid(self, name, varenv):
    # this is just like lookup.lookup_guid except it takes "has_right_order" and returns
    # the guid for "/boot/has_right_order"
    if not isinstance(name, str):
      raise MQLInternalParseError(
          None, "didn't understand '%(id)s' as an id", id=str(name))
    if len(name) == 0:
      raise MQLInternalParseError(
          None, "didn't understand '%(id)s' as an id", id=name)

    if name[0] not in '#/$':
      name = '/boot/' + name

    return self.lookup.lookup_guid(name, varenv)

  # this only does the sort
  def lookup_sort(self, query, varenv):
    """
        Check for sort, annotating the query with sub-clauses that will
        be used later. Uses follow_path to sort clauses based on
        values in sub-clauses
        """

    if ':sort' in query:
      sortkey = ':sort'
    elif '@sort' in query and query.is_root:
      sortkey = '@sort'
    else:
      # no sort in the query!
      return

    # save what the sort used to look like, just because.
    query.sort = query[sortkey]
    newsort = []
    for elem in elements(query[sortkey], str):
      # hijson compiles the sort for us -- this is just for lojson convenience usage
      if valid_precompiled_sort(elem):
        directive = elem
      else:
        (subq, value, sort_direction) = follow_path(query, elem)
        if value == ':index':
          # :index is really ?value wherever it appears...

          sort_var = '$sort_%d' % varenv.sort_number
          directive = sort_direction + sort_var
          varenv.sort_number += 1

          subq['?' + sort_var] = 'value'
        elif subq == query and value[0] in ':@':
          # direct sorting based on value in current query
          directive = sort_direction + value[1:]
        else:
          # sort by a sub-clause. Generate a sort variable for
          # the graph.

          sort_var = '$sort_%d' % varenv.sort_number
          directive = sort_direction + sort_var
          varenv.sort_number += 1

          # now define that variable in the sub-clause
          # defines it as ':$sort_0' = 'value'
          # rather than '$sort_0' = ':value'
          # so that it is processed by "" handling later
          subq[value[0] + sort_var] = value[1:]

      newsort.append(directive)
    query[sortkey] = newsort

  # This is really crufty (and thus at least poorly designed, if not incorrect)
  #
  # all of which can occur in ':' and '@' varients. (Ick!!!)
  #
  def add_write_scope(self, query, varenv):
    # it's OK I suppose to ask '@scope': null, or ':scope': null during a write (you may have legit reasons to know)
    # asking anything more, or any sort of constraint is flat out illegal
    if query.get('@scope') is not None or query.get(':scope') is not None:
      raise MQLInternalParseError(
          query, "It's not legal to specify a scope during a write.")

    if '+has_permission' in query:
      raise MQLInternalParseError(
          query, "You can't specify the permission property in a write.")

    if not query.terminal:
      # this is the permission node...
      query['+has_permission'] = QueryDict(
          {
              ':guid': None,
              '@guid': None,
              '@default': varenv.default_permission_guid
          },
          key='+has_permission',
          parent_clause=query,
          implied=True)

      query['@scope'] = None

    if query.parent_clause is not None:
      query[':scope'] = None

  # look at dict_recurse and elements go!
  def add_implied_writes(self, query, varenv):
    for item in dict_recurse(query):
      # add the scope from the varenv if it exists
      # XXX this should be compulsory and non-overrideable when access control is mandatory
      if item.implied and item.key == '+has_permission':
        continue

      self.add_write_scope(item, varenv)

      # check the syntax of write directives - only allowed values of True, False or to be absent entirely.
      for directive in ('@insert', '@delete', '@ensure', '@ensurechild',
                        ':insert', ':delete', ':ensure', ':ensurechild',
                        ':link', ':unlink'):
        if item.get(directive) not in (True, False, None):
          raise MQLInternalParseError(
              query,
              'Invalid value for write directive %(key)s',
              key=directive,
              value=item.get(directive))

      if item.get('@insert'):
        if item.get('@delete') or item.get('@ensure'):
          raise MQLInternalParseError(query,
                                      "Can't mix insert with ensure or delete")
        elif item.get('@guid') is not None or item.get('@id') is not None:
          raise MQLInternalParseError(
              query,
              "Can't insert a node when the guid is specified",
              key='@insert',
              guid=item.get('@guid'),
              id=item.get('@id'))

        if item.parent_clause is not None:
          if item.get(':delete') or item.get(':unlink'):
            raise MQLInternalParseError(
                query, "Can't have inserts touching deletes or updates")

          item[':insert'] = True

        for key in item:
          if valid_relname(key):
            for subk in elements(item[key], dict):
              subk[':insert'] = True
      elif item.get('@delete'):
        if item.get('@insert') or item.get('@ensure'):
          raise MQLInternalParseError(query,
                                      "Can't mix delete with ensure or insert")

        if item.parent_clause is not None:
          if item.get(':insert') or item.get(':ensure') or item.get(
              ':link') or item.get(':ensurechild') or item.get(':update'):
            raise MQLInternalParseError(
                query, "Can't have inserts touching deletes or updates")

          item[':delete'] = True

        for key in item:
          if valid_relname(key):
            for subk in elements(item[key], dict):
              subk[':delete'] = True
      elif item.get('@ensure'):
        if item.get('@delete') or item.get('@insert'):
          raise MQLInternalParseError(query,
                                      "Can't mix insert with ensure or delete")
        elif item.get('@guid') or item.get('@id'):
          raise MQLInternalParseError(
              query,
              "Can't ensure a node when the guid or id is specified",
              key='@ensure',
              guid=item.get('@guid'),
              id=item.get('@id'))

        # Make sure not to add :ensure to the root of the query
        # or to anything that has :link
        if item.parent_clause is not None and not item.get(':link'):
          if item.get(':ensurechild'):
            # this is the case of create:unless_exists in the parent and create:unless_connected in the child.
            # so this clause is optional, but the node is not queried independently.
            del item[':ensurechild']

          item[':ensure'] = True
          del item['@ensure']
          item['@ensurechild'] = True

        # add "ensurechild" to the children - they will not be re-queried if the @ensure node fails.
        # nodes
        for key in item:
          if valid_relname(key):
            for subk in elements(item[key], dict):
              if not subk.get(':link'):
                subk[':ensurechild'] = True

  def add_query_primitives(self, query, varenv, mode):
    # the head has no link - don't try and create one
    if mode is ReadMode:
      query_head = element(query)
      if not isinstance(query_head, dict):
        raise MQLInternalParseError(
            query,
            'Queries must start with a single dict, optionally contained in a list'
        )

    # for both reads and writes...
    self.add_query_primitives_recurse(query, varenv, mode)

  def add_query_primitives_recurse(self, query, varenv, mode):
    for item in elements(query, dict):

      # see if we have any sorting to do here
      if mode is ReadMode:
        self.lookup_sort(item, varenv)

      # see if this is a terminal node.
      if item.terminal:
        item.node = Missing

      if item.is_root:
        item.link = Missing

      # add the :type directives (the type as inferred from the linking type)
      # also add :reverse if the type had a leading - sign

      # we may add @guid (from @id) and :typeguid (from :type) so the dictionary may increase in size.
      for key in sorted(item.iterkeys()):
        if valid_relname(key):
          if key[0] in '+-':
            newtype = key[1:]
          else:
            newtype = key

          # :typeguid rules over :type; :type rules over '[+-]xyz' or plain 'xyz'
          for subk in elements(item[key], dict):
            if (subk.get(':type') is None and subk.get(':typeguid') is None):
              subk[':type'] = newtype
              # is this reversed?
              if key[0] == '-':
                subk[':reverse'] = True
            else:
              if key[0] in '+-#':
                raise MQLInternalParseError(
                    item,
                    "Can't use :type or :typeguid with %(key)s as a relname",
                    key=key)

          self.add_query_primitives_recurse(item[key], varenv, mode)
        elif key[0] == ':' and item.link is Missing:
          raise MQLInternalParseError(
              item, 'Linking directives invalid in here', key=key)

        elif key[0] in '@:':
          if is_direct_pointer(item[key]):
            item[key].link = Missing
            self.add_query_primitives_recurse(item[key], varenv, mode)
          elif key == ':index':
            # This is an ordered primitive and part of an ordered list.
            item.ordered = True
          elif key[1:] == 'id':
            guidkey = key[0] + 'guid'
            # need to translate ids from their names.
            if item.get(key) is None:
              # we're just asking for the id (perhaps given the guid, perhaps not)
              pass
            elif item.get(guidkey) is None:
              if isinstance(item[key], str):
                item[guidkey] = self.lookup_boot_guid(item[key], varenv)
              elif isinstance(item[key], list):
                item[guidkey] = [
                    self.lookup_boot_guid(x, varenv) for x in item[key]
                ]
              else:
                raise MQLInternalParseError(
                    item, 'Invalid format for id', key=key)
            else:
              raise MQLInternalParseError(
                  item, 'You may not constrain both the id and the guid')
          elif key == ':type':
            # add an explicit typeguid if there is a non-* type
            if item.get(':typeguid') is None and item.get(':type') not in (None,
                                                                           '*'):
              item[':typeguid'] = self.lookup_boot_guid(item[':type'], varenv)
            elif item.get(':type') is None:
              raise MQLInternalParseError(
                  item, "Can't ask for :type, only specify it", key=key)
          elif key == ':value' and mode in (WriteMode, CheckMode):
            if item[key] is None:
              raise MQLInternalParseError(
                  item,
                  "Can't specify value == null in a %(mode)s",
                  key=key,
                  mode=str(mode))
          elif item[key] is None:
            pass
          elif key == ':update' and mode in (WriteMode, CheckMode):
            if item.get(':update') and not item.get(':unique'):
              raise MQLInternalParseError(
                  item,
                  "Can't use :update without :unique",
                  key=key,
                  mode=str(mode))
          elif key == ':unique' and mode in (WriteMode, CheckMode):
            if item[key] == 'key':
              # we need to check out the namespace
              if item.get(':reverse', False):
                # reversed so the namespace is the child which is here
                item.unique_ns_check = True
              else:
                # regular so the namespace is the parent
                item.parent_clause.unique_ns_check = True

        elif key[0] == '?':
          item.ordered = True
        else:
          raise MQLInternalParseError(
              item, "Couldn't parse key %(key)s", key=key)

      # now create the query primitives
      if item.node is None:
        item.node = QueryPrimitive('@', item, mode)

      if item.link is None:
        item.link = QueryPrimitive(':', item, mode)

      if item.ordered is True:
        # signal that we need to do ordered list processing on this list during link_query_primitives()
        if item.link.reverse:
          ordering_typeguid = self.lookup_boot_guid('/boot/has_right_order',
                                                    varenv)
        else:
          ordering_typeguid = self.lookup_boot_guid('/boot/has_left_order',
                                                    varenv)

        item.list.ordered = True
        item.update({
            '?typeguid': ordering_typeguid,
            '?value': None,
            '?datatype': 'float'
        })

        # we put the optional in for write in generate_prepare_instructions()
        if mode is ReadMode:
          item['?optional'] = True

        item.ordered = QueryPrimitive('?', item, mode)

  def link_query_primitives_root(self, query, varenv, mode):
    for item in elements(query, dict):
      self.link_query_primitives(item, varenv, mode)

  # this relies on the QueryPrimitives already having been created.
  def link_query_primitives(self, query, varenv, mode):
    item = element(query)
    node = item.node
    link = item.link
    if link is not Missing and node is not Missing:
      link.add_child(node)

    if mode in (WriteMode, CheckMode) and link is not Missing:
      link.add_unique_query_primitives(mode)

    # if this is a key link, or contains a key link we need to check the ns uniqueness
    if item.unique_ns_check is True:
      ns_check_tg = self.lookup_boot_guid('/boot/is_unique_namespace', varenv)
      item.unique_ns_check = QueryPrimitive(
          '~',
          QueryDict({
              '~typeguid': ns_check_tg,
              '~datatype': 'boolean',
              '~value': None,
              '~optional': True,
              '~pagesize': 2,
          }), mode)

      item.unique_ns_check.add_unique_namespace_check(node)

    if item.ordered is not Missing:
      # must come after the link.add_child() call. We already have a parent.
      item.ordered.link_order_query_primitive(link)

    for key in item:
      if valid_relname(key):
        if isinstance(
            item[key],
            list) and item[key].ordered and mode in (WriteMode, CheckMode):
          self.make_ordered_query_primitives(item[key], varenv, mode)

        for child in elements(item[key], dict):
          child.link.add_parent(node)
          self.link_query_primitives(child, varenv, mode)

      # XXX this seems very unclean [@:]scope and [@:]typeguid (@typeguid???)
      elif key[1:] in QueryPrimitive.pointers and is_direct_pointer(item[key]):
        if key[0] == '@':
          node.add_pointer(key[1:], item[key].node)
          self.link_query_primitives(item[key], varenv, mode)
        elif key[0] == ':' and link is not Missing:
          link.add_pointer(key[1:], item[key].node)
          self.link_query_primitives(item[key], varenv, mode)
        else:
          raise MQLInternalParseError(
              item, 'Invalid use of a direct slot for %(key)s', key=key)

  def make_ordered_query_primitives(self, qlist, varenv, mode):
    # note that qlist is the QueryList object at the point where we have the index statements
    # in clauses. It represents them as a group, not a particular individual one...

    # XXX There are a lot of questions about how strict we can/should be with what
    # is seen in an ordered set. Right now we enforce
    # - same typeguid
    # - same direction
    # which make things sane. For ease of understanding and use we also enforce
    # - indexes must be a permutation of 1..N (ie nothing missing, no duplicates, starts at 1)
    # we do not enforce
    # - everything has to have an index (ie you can order only half the list)

    if len(qlist) == 0:
      # we should have caught this earlier
      raise MQLInternalError(
          qlist.parent_clause, "Can't order an empty list", key=qlist.key)

    if qlist.is_root:
      raise MQLInternalParseError(qlist, "Can't order the root of a query")

    # everything must match the first item...
    typeguid = qlist[0][':typeguid']
    reverse = qlist[0].get(':reverse', False)

    indexes = {}
    maxindex = 0

    for item in qlist:
      if typeguid != item[':typeguid']:
        raise MQLInternalParseError(
            qlist.parent_clause,
            'Only homogenous lists may be ordered',
            key=qlist.key,
            guids=[typeguid, item[':typeguid']])
      if reverse != item.get(':reverse', False):
        raise MQLInternalParseError(
            qlist.parent_clause,
            'All items in an ordered list must point the same direction',
            key=qlist.key)

      if ':index' not in item:
        # this item is unordered and will play no further part in discussions...
        continue

      index = item[':index']
      if index in indexes:
        # not strictly strictly necessary (see NetFlix) but makes life so much easier.
        raise MQLInternalParseError(
            qlist.parent_clause,
            'Duplicate index %(index)s in ordered list',
            key=qlist.key,
            index=index)
      if not isinstance(index, (int, long)) or not index >= 0:
        raise MQLInternalParseError(
            qlist.parent_clause,
            'Illegal index %(index)s',
            key=qlist.key,
            index=index)

      indexes[index] = item
      maxindex = max(index, maxindex)

    # check we have every index between 0 and max-1. The pigeonhole principle is our friend.
    if maxindex + 1 != len(indexes):
      raise MQLInternalParseError(
          qlist.parent_clause,
          'Saw %(num)d indexes, but a maximum of %(index)d. Please fill in the gaps',
          key=qlist.key,
          num=len(indexes),
          index=maxindex)

    sort_directive = '$sort_%d' % varenv.sort_number
    varenv.sort_number += 1

    if reverse:
      ordering_typeguid = self.lookup_boot_guid('/boot/has_right_order', varenv)
    else:
      ordering_typeguid = self.lookup_boot_guid('/boot/has_left_order', varenv)

    # XXX no-one can possibly understand this...
    qlist.order_dict = QueryDict(
        {
            '&typeguid': typeguid,
            '&reverse': reverse,
            '&pagesize': maxindex + 2,
            '&optional': True,
            '&sort': sort_directive,
            '?' + sort_directive: 'value',
            '?typeguid': ordering_typeguid,
            '?value': None,
            '?datatype': 'float'
        },
        implied=True)
    qlist.indexes = indexes

    # this is a list of all the things that we saw in the new correct order
    # This is used in qprim.py:generate_new_order()
    new_order = [indexes[x].link for x in sorted(indexes.iterkeys())]

    # now build the QP object to query the indexes
    index_qp = QueryPrimitive('&', qlist.order_dict, mode)
    order_qp = QueryPrimitive('?', qlist.order_dict, mode)

    # link everything together...
    order_qp.link_order_query_primitive(index_qp)
    # and link to the underlying node
    index_qp.add_index_query(qlist.parent_clause.node, new_order)
    # attach it to the qlist
    qlist.ordered = index_qp

  def create_query_result(self, query, varenv, result, mode):
    """
        Create result JSON using the nested arrays from the graph
        """
    # [ { '@guid': null, 'a': [ { '@guid': null, ':value': null } ] } ]
    #
    # [ [ 1, [ [ 'a','b', [ [2] ]], [ 'a','c', [ [3] ]] ] ],
    #   [ 4, [ [ 'a','e', [ [5] ]], [ 'a','f', [ [6] ]] ] ] ]
    #
    # [ { '@guid':1, 'a': [ { '@guid':2, ':value':'b'}, { '@guid':3, ':value':'c'} ] },
    #   { '@guid':4, 'a': [ { '@guid':5, ':value':'e'}, { '@guid':6, ':value':'f'} ] } ]

    # at every stage, if we see a dict we take only the first result
    # (we print a complaint if we see another)
    # if we see a list we put all the results into that list
    if isinstance(query, dict):
      if len(result) == 0:
        return None
      elif len(result) > 1:
        guids = [('#' + x[0]) for x in result]
        raise MQLResultError(
            query,
            'Expected one result, got %(count)d',
            count=len(result),
            guids=guids)
      else:
        return query.node.create_results(result[0], ResultDict(), mode)
    elif isinstance(query, list):

      resultv = []
      for res in result:
        resultv.append(query[0].node.create_results(res, ResultDict(), mode))

      return resultv
    else:
      raise MQLInternalError(query, 'Query not dict or list')

  def sanitize_value(self, value, datatype, varenv):
    # this code is a copy of the code in env.py unquote_value()
    # which should be considered authorative.

    if varenv.get('escape', 'html') and isinstance(value, str):
      if datatype == 'url':
        if value.find('javascript:') == 0:
          value = 'unsafe-' + value
      else:
        value = cgi.escape(value)

    return value

  def filter_query_result(self, result, varenv):
    """
        make the result look like the query.

        note that the result is already structurally isomorphic to
        the query; this just converts guids and removes extra
        fields.
        """
    if isinstance(result, list):
      filter_result = []
      for elem in result:
        # need this pointer to get index results properly sorted.
        elem.list = result
        filter_result.append(self.filter_query_result(elem, varenv))
    elif isinstance(result, dict):
      filter_result = {}
      for key, asked in result.query.original_query.iteritems():
        if key[0] in '@:':
          basekey = key[1:]
          if basekey == 'id':
            filter_result[key] = asked
            # horrible hack to collect up the guids we care about...
            if asked is None:
              varenv.guid_list.append(result[key[0] + 'guid'])
          elif (basekey in QueryPrimitive.directives or
                basekey in QueryPrimitive.special):
            # should we output these?
            filter_result[key] = asked
          elif key[0] == '@' and result.query.get(
              '@optional') and key not in result:
            # XXX here we actually will give you an empty result
            # we could give you nothing at all
            filter_result[key] = None
          elif basekey == 'guid':
            filter_result[key] = result[key]
          elif basekey == 'value':
            # sanitize results.
            filter_result[key] = self.sanitize_value(
                result[key], result[key[0] + 'datatype'], varenv)
          elif basekey in QueryPrimitive.values:
            # this better be what you said!!!
            filter_result[key] = result[key]
          elif basekey == 'index':
            filter_result[key] = self.generate_index_read_result(result)
          elif basekey in QueryPrimitive.pointers:
            # might be direct sub-query or constraint, or query
            if isinstance(asked, dict):
              # sub-query, return it
              filter_result[key] = self.filter_query_result(result[key], varenv)
            else:
              if asked is None:
                # we'll be asking for the id of this thing, not just the guid.
                varenv.lookup_manager.guid_list.append(result[key])

              # just give back the guid
              filter_result[key] = result[key]
        elif valid_relname(key):
          # skip optional results we didn't get a value for.
          if result.query.get('@optional') and key not in result:
            # XXX should we give you None as a result rather than leaving it out completely?
            pass
          else:
            # is this a ResultError or an InternalError?
            if key not in result:
              raise MQLInternalError(
                  result.query, "No return result for '%(key)s'", key=key)
            else:
              filter_result[key] = self.filter_query_result(result[key], varenv)

        elif key[0] == '?':
          # it's possible that we didn't find any order information, so give back null in that case
          filter_result[key] = result.get(key, None)
        else:
          raise MQLInternalError(
              result.query,
              "Didn't expect to see %(key)s in original query while filtering",
              key=key)

      result.filter = filter_result
    elif result is None:
      # there's no result here even though we expected one.
      filter_result = result
    else:
      raise MQLInternalError(
          result.query, "Didn't understand result", result=result)

    return filter_result

  def generate_index_read_result(self, result):
    # bit of gymnastics to get the containing list of the result item we are passed

    if ':index' not in result:
      # must be the first element -- compute all the indexes.
      values = {}
      for elem in result.list:
        if '?value' in elem:
          if elem['?value'] not in values:
            values[elem['?value']] = elem
          else:
            raise MQLInternalError(
                result.query,
                'Duplicate ordering value found in list',
                value=elem['?value'])

        else:
          elem[':index'] = None

      i = 0
      for value in sorted(values.iterkeys()):
        values[value][':index'] = i
        i += 1

    return result[':index']

  def dispatch_prepares(self, query, varenv):
    if isinstance(query, dict):

      # eek
      def reader(graphq):
        dumplog('PREPARE', graphq)
        try:
          gresult = self.gc.read_varenv(graphq, varenv)
        except EmptyResult:
          # debug ME-907
          LOG.exception(
              'mql.lojson.LowQuery.dispatch_prepares()',
              graphq=graphq,
              varenv=varenv)

          # there's an implicit unescapable optional-ness at the root of every query
          gresult = []
        dumplog('PREPARE_RESULT', gresult)
        return gresult

      query.node.run_prepare(reader)

    elif isinstance(query, list):
      for subq in query:
        self.dispatch_prepares(subq, varenv)

  #
  # This code is too sloppy for security critical code -- I can't easily convince myself that
  # all paths into generate_write_query() must necessarily go through this code.
  # Probably we need to move this closer to generate_write_query() (which is not that
  # easy to verify even without the write access issues.
  #
  def check_write_access(self, head_query, varenv):
    # we have a prepared set of queries with attached results. We now check that you (the user) can create the
    # appropriate nodes and links. All the nodes will get the permission specified in varenv['$permission'].
    # all the links will get the permission of their left (if you are authorized in that permission)
    # if you are not authorized, then the write will fail with "unauthorized".

    scope.check_write_throttle(self, varenv)

    for query in dict_recurse(head_query):
      if query.key == '+has_permission':
        continue

      link = query.link
      node = query.node
      ordered = query.ordered

      # first do the node -- the link decision may depend on the
      # node decision (it is either the parent this node)
      if node is not Missing:
        if node.left not in (None, Missing):
          raise MQLInternalError(
              query,
              "Found node with non-empty left -- AccessControl can't handle that!"
          )

        # make sure this isn't a side door into has_permission
        # to cause trouble later.
        if node.typeguid == self.lookup_boot_guid('/boot/has_permission',
                                                  varenv):
          raise MQLInternalError(
              query,
              "Can't reference has_permission during a write regardless of how hard you try"
          )

        if node.state == 'create':
          if node.guid is not None:
            raise MQLInternalError(
                query,
                "Can't create a node which already has a guid",
                guid=node.guid)

          # now we need to find the has_permission contents
          # and change the guid...
          permissionguid = query['+has_permission'].node.guid
          if permissionguid != varenv.default_permission_guid:
            raise MQLInternalError(
                query,
                'Creating a node with something other than the default permission'
            )

          if scope.check_permission(
              self, varenv, permissionguid=permissionguid):
            # we'll need to create a link for this node.
            query['+has_permission'].link.scope = varenv.attribution_guid
            query['+has_permission'].link.access_control_ok = True

            node.scope = varenv.attribution_guid
            node.access_control_ok = True
          else:
            # *****************************************************************************************************************
            raise MQLAccessError(
                query,
                'User %(user)s cannot create with permission %(permission)s',
                user=varenv.get_user_id(),
                permission=permissionguid)
            # *****************************************************************************************************************

        elif node.state == 'remove':
          if node.guid in (None, Missing):
            raise MQLInternalError(
                query, 'Found node with guid=null but state=remove')

          if node.scope in (None, Missing):
            raise MQLInternalError(
                query,
                'Found node with guid %(guid)s but scope=null during check_write_access()',
                guid=node.guid)

          node_permission = query['+has_permission'].node.guid
          if scope.check_permission(
              self, varenv, permissionguid=node_permission):
            node.scope = varenv.attribution_guid
            node.access_control_ok = True

            # we'll need to remove the permission link for this node.
            query['+has_permission'].link.scope = varenv.attribution_guid
            query['+has_permission'].link.access_control_ok = True

          else:
            # *****************************************************************************************************************
            raise MQLAccessError(
                query,
                'User %(user)s does not have permission to destroy here',
                user=varenv.get_user_id())
            # *****************************************************************************************************************

        elif node.state == 'found':
          # set access_control_ok if it would be OK to write this node (even though we are not doing so)
          node_permission = query['+has_permission'].node.guid
          if scope.check_permission(
              self, varenv, permissionguid=node_permission):
            # node.scope is unchanged as we are not actually going to do the write...
            node.access_control_ok = True

        elif node.state == 'notpresent':
          # not ok to write a link pointing to this missing node
          pass
        else:
          raise MQLInternalError(
              query,
              'Found node with state %(state)s in check_write_access()',
              state=node.state)

      # now the link. link.left is either node, or link.parent (which we have already processed)
      if link is not Missing:
        if link.state in ('create', 'modify', 'remove'):
          if link.left in (None, Missing):
            raise MQLInternalError(
                query,
                "Found link with empty left -- AccessControl can't handle that!"
            )

          if link.state in ('create', 'modify') and link.guid:
            raise MQLInternalError(
                query,
                'Found link with guid which we are trying to create',
                guid=link.guid)
          elif link.state == 'remove' and not link.guid:
            raise MQLInternalError(
                query, 'Found link without guid which we are trying to remove')

          # link.left should have already been found, but may not have been processed
          # XXX (this is probably a bad order - we should always process nodes before the links that depend on
          # them for access control
          if link.left.state not in ('found', link.state):
            raise MQLInternalError(
                query,
                'Found link in state %(state)s with left in state %(leftstate)s',
                state=link.state,
                leftstate=link.left.state)
          # if we're changing the permission it's legal to talk about has_permission.
          # but we need to be extraordinarily careful in that case...
          if link.typeguid == self.lookup_boot_guid('/boot/has_permission',
                                                    varenv):
            self.check_change_permission(query, varenv)

          if link.unique and link.unique in ('left', 'both'):
            # reverse uniqueness requires the co-operation of the right guid
            if link.right in (None, Missing):
              raise MQLInternalError(
                  query, 'Found reverse unique link with empty right')
            if link.right.state not in ('found', link.state):
              raise MQLInternalError(
                  query,
                  'Found link in state %(state)s with right in state %(rightstate)s',
                  state=link.state,
                  leftstate=link.right.state)
            if not link.right.access_control_ok:
              # *****************************************************************************************************************
              raise MQLAccessError(
                  query,
                  'User %(user)s does not have permission to connect here',
                  user=varenv.get_user_id())
              # *****************************************************************************************************************

            # fall through to the usual checks...

          if link.left.access_control_ok:
            # this is a bit of a hack since we're in lojson which presumably doesn't know anything about the
            # schema, but this whole thing needs a rewrite anyway...
            if hasattr(link.query, 'high_query'
                      ) and link.query.high_query.property.property_permission:
              perm = link.query.high_query.property.property_permission
              if scope.check_permission(self, varenv, permissionguid=perm):
                link.scope = varenv.attribution_guid
                link.access_control_ok = True
              else:
                # *****************************************************************************************************************
                raise MQLAccessError(
                    query,
                    'User %(user)s does not have permission to connect property %(prop)s',
                    user=varenv.get_user_id(),
                    prop=link.query.high_query.property.id)
                # *****************************************************************************************************************
            else:
              link.scope = varenv.attribution_guid
              link.access_control_ok = True
          else:
            # *****************************************************************************************************************
            raise MQLAccessError(
                query,
                'User %(user)s does not have permission to connect here',
                user=varenv.get_user_id())
            # *****************************************************************************************************************

        elif link.state in ('found', 'notpresent'):
          pass
        else:
          raise MQLInternalError(
              query,
              'Found link with state %(state)s in check_write_access()',
              state=link.state)

      # it's possible to fail on a node if we have versioning.
      # but we don't have versioning at the moment.

      if ordered is not Missing:
        # if we passed on the link, we pass on the ordering
        if ordered.state in ('create', 'modify'):
          if ordered.left != link:
            raise MQLInternalError(
                query,
                "Found ordered which does not point to link -- AccessControl can't handle that!"
            )

          if ordered.state in ('create', 'modify') and ordered.guid:
            raise MQLInternalError(
                query,
                'Found ordered with guid which we are trying to create',
                guid=ordered.guid)
          elif ordered.state == 'remove' and not ordered.guid:
            raise MQLInternalError(
                query,
                'Found ordered without guid which we are trying to remove')

          # make sure this isn't a side door into has_permission to cause trouble later.
          if ordered.typeguid == self.lookup_boot_guid('/boot/has_permission',
                                                       varenv):
            raise MQLInternalError(
                query,
                "Can't reference has_permission during a write regardless of how hard you try"
            )

          # check we would be OK with the link
          # (we may not have checked the link if it is pre-existing)
          if ordered.left.left.access_control_ok:
            ordered.scope = varenv.attribution_guid
            ordered.access_control_ok = True
          else:
            # *****************************************************************************************************************
            raise MQLAccessError(
                query,
                'User %(user)s does not have permission to index here',
                user=varenv.get_user_id())
            # *****************************************************************************************************************

        elif ordered.state in ('found', 'notpresent'):
          pass
        else:
          # XXX we should handle 'remove' on ordered at some point.
          raise MQLInternalError(
              query,
              'Found order with state %(state)s in check_write_access()',
              state=ordered.state)

  def check_change_permission(self, query, varenv):

    if not query.link.update:
      raise MQLInternalError(
          query, 'You can only update permissions, not insert or delete them')

    # let's dance around this spot for a little bit - we're currently sitting on the permission.
    if not query.parent_clause:
      raise MQLInternalError(
          query, "Can't change permissions at the root of the query")

    if query.link.typeguid != self.lookup_boot_guid('/boot/has_permission',
                                                    varenv):
      raise MQLInternalError(
          query, "Can't call check_change_permission except on has_permission")

    old_permission_guid = query.parent_clause['+has_permission'].node.guid
    new_permission_guid = query.node.guid
    if not scope.check_change_permission_by_user(
        self,
        varenv,
        old_permission_guid=old_permission_guid,
        new_permission_guid=new_permission_guid):
      # *****************************************************************************************************************
      raise MQLAccessError(
          query,
          'User %(user)s cannot change the permission here',
          user=varenv.get_user_id())
      # *****************************************************************************************************************

  def check_circularity(self, head_query, varenv):
    circ_dict = {}

    for query in elements(head_query, dict):
      query.node.check_circularity(circ_dict)

    dumplog('CIRCULARITY_CHECK', circ_dict)

  def generate_check_responses(self, head_query, varenv):
    """
        Return a list of write queries necessary to make this entire query work.

        At this point the query is decorated with .prepare objects which
        contain the things found.
        """

    # for all nodes in the query
    # if they do not have a .prepare result
    # start generating a write
    # if we locate a prepare, stop the write (with an explicit left= or right=)
    # and recurse from that prepare looking for other writes

    has_written = False
    for query in dict_recurse(head_query):
      write_primitive = None

      if query.link is not Missing and query.link.state in ('create', 'remove',
                                                            'modify'):
        write_primitive = query.link
      elif query.node is not Missing and query.node.state in ('create',
                                                              'remove'):
        # if we do not write the (non Missing) link, we never have to write the node
        write_primitive = query.node
      elif query.ordered is not Missing and query.ordered.state in ('create',
                                                                    'modify'):
        # we may need to write just the order information to the (existing) link
        write_primitive = query.ordered

      # did we find something to do?
      if write_primitive:
        write_primitive.fake_check_result()

  def generate_write_queries(self, head_query, varenv):
    """
        Return a list of write queries necessary to make this entire query work.

        At this point the query is decorated with .prepare objects which
        contain the things found.
        """

    # for all nodes in the query
    # if they do not have a .prepare result
    # start generating a write
    # if we locate a prepare, stop the write (with an explicit left= or right=)
    # and recurse from that prepare looking for other writes

    has_written = False
    for query in dict_recurse(head_query):
      write_primitive = None

      if query.link is not Missing and query.link.state in ('create', 'remove',
                                                            'modify'):
        write_primitive = query.link
      elif query.node is not Missing and query.node.state in ('create',
                                                              'remove'):
        # if we do not write the (non Missing) link, we never have to write the node
        write_primitive = query.node
      elif query.ordered is not Missing and query.ordered.state in ('create',
                                                                    'modify'):
        # we may need to write just the order information to the (existing) link
        write_primitive = query.ordered

      # did we find something to do?
      if write_primitive:
        writeq = write_primitive.generate_write_query()

        dumplog('WRITE_QUERY', writeq)

        try:
          gresult = self.gc.write_varenv(writeq, varenv)
          has_written = True
        except MQLGraphError, e:
          subclass = e.get_kwd('subclass')
          if subclass == 'EXISTS' and write_primitive.unique == 'key':
            # must have gotten here due to a race condition in creating a
            # key in a namespace -- but that's ok, so silently succeed
            write_primitive.change_state('written')
            continue
          elif subclass in ['EXISTS', 'OUTDATED']:
            # we hit a uniqueness failure! Ick
            if not has_written:
              # thankfully this is the first such error; the write can be safely abandoned
              raise MQLResultError(
                  query,
                  'Uniqueness check failed (probably this write has already been done)',
                  subclass=subclass)
            else:
              # oh dear...
              LOG.fatal(
                  'mql.write.unique.fatal.error',
                  'Write uniqueness failure in half-written request',
                  query=head_query,
                  graph_query=writeq,
                  subclass=e.get_kwd('subclass'))
              raise MQLResultError(
                  query,
                  'Write partially complete (locking failure) -- please report this to developers@freebase.com',
                  subclass=subclass)
          else:
            raise

        dumplog('WRITE_GRAPH_RESULT', gresult)

        write_primitive.attach_write_results(gresult)

  def generate_write_result(self, query, varenv):
    # make the result look like the query.
    # uses the % query annotations in create_query_result
    # note that the result is already structurally isomorphic to the query; this just
    # converts guids and removes extra fields.
    if isinstance(query, list):
      write_result = []
      for elem in query:
        write_result.append(self.generate_write_result(elem, varenv))
    elif isinstance(query, dict):
      write_result = {}
      for key in query.original_query:
        if valid_relname(key):
          write_result[key] = self.generate_write_result(query[key], varenv)
        elif key[0] in '@:?':
          asked = query.original_query[key]

          if key[0] == ':':
            qp = query.link
          elif key[0] == '@':
            qp = query.node
          elif key[0] == '?':
            qp = query.ordered

          if key[1:] == 'update':
            write_result[key] = (qp.previous is not None)
          elif key[1:] in QueryPrimitive.writeinsns:
            # for each write instruction, we output whatever was actually done.
            write_result[key] = (qp.state == 'written')
          elif key[1:] == 'id':
            # handles asked == None correctly
            write_result[key] = asked
            if asked is None:
              varenv.lookup_manager.guid_list.append(qp.guid)
          elif key[1:] in QueryPrimitive.directives | QueryPrimitive.special:
            # should we output these?
            write_result[key] = asked
          elif key[1:] == 'guid':
            # handles asked == None correctly
            write_result[key] = qp.guid
          elif key[1:] == 'index':
            write_result[key] = qp.index
          elif key[1:] == 'value':
            write_result[key] = self.sanitize_value(
                getattr(qp, key[1:], None), getattr(qp, 'datatype', None),
                varenv)
          elif key[1:] in QueryPrimitive.values:
            # this better be what you said!!!
            write_result[key] = getattr(qp, key[1:], None)
          elif key[1:] in QueryPrimitive.pointers:
            # might be direct sub-query or constraint, or query
            if asked is None or isinstance(asked, str):
              # direct question, direct answer
              write_result[key] = getattr(qp, key[1:], None)
            elif isinstance(asked, dict):
              # sub-query, return it
              write_result[key] = self.generate_write_result(query[key], varenv)
            elif isinstance(asked, list) and len(asked) > 0 and isinstance(
                asked[0], str):
              # XXX more complicated -- is this the correct solution???
              write_result[key] = [getattr(qp, key[1:], None)]
            else:
              raise MQLInternalError(
                  query,
                  "Didn't understand the value of %(key)s",
                  value=asked,
                  key=key)

        else:
          raise MQLInternalError(
              query,
              "Didn't expect to see %(key)s in original query while filtering",
              key=key)

      if query.node is not Missing:
        query.node.change_state('done')
      if query.link is not Missing:
        query.link.change_state('done')

      query.write = write_result

    elif query is None:
      # there's no result here even though we expected one.
      raise MQLInternalError(query, "Didn't understand as a result")
    else:
      raise MQLInternalError(query, "Didn't understand as a result")

    return write_result

  def run_query(self, query, mode, varenv):
    # OK, now let's spit out some graph stuff
    graphq = element(query).node.generate_graph_query(mode)

    dumplog('GRAPHQ', graphq)

    if mode in (PrepareMode, ReadMode):
      try:
        result = self.gc.read_varenv(graphq, varenv)
      except EmptyResult:
        # debug ME-907
        LOG.exception(
            'mql.lojson.LowQuery.run_query()', graphq=graphq, varenv=varenv)

        # there's an implicit unescapable optional-ness at the root of every query
        result = []
    elif mode is WriteMode:
      try:
        result = self.gc.write_varenv(graphq, varenv)
      except EmptyResult:
        raise MQLInternalError(
            query, 'Write query returned EmptyResult!', graph_query=graphq)
    else:
      raise MQLInternalError(
          query, "Can't use %(mode)s mode to access the graph", mode=str(mode))

    dumplog('GRESULT', result)

    return result

  def read(self, orig_query, orig_varenv):
    # make sure we have a Varenv object, not just a dictionary
    if isinstance(orig_varenv, Varenv):
      varenv = orig_varenv
    else:
      varenv = Varenv(orig_varenv, self.lookup)

    transaction_id = varenv.get('tid')

    # stages:
    pprintlog(
        'LOW_READ',
        orig_query,
        transaction_id=transaction_id,
        level=log_util.DEBUG,
        push=True)

    # add "original_query" decorators.
    query = self.make_orig(orig_query)

    self.make_parents(query, True)

    # create QueryPrimitives
    self.add_query_primitives(query, varenv, ReadMode)

    # hook QueryPrimitives together
    self.link_query_primitives(query, varenv, ReadMode)

    dumplog('LOW_QUERY', query)
    dumplog('READ_PRIMITIVES', element(query).node)

    # turn QPs into write query.
    gresult = self.run_query(query, ReadMode, varenv)

    result = self.create_query_result(query, varenv, gresult, ReadMode)

    dumplog('LOW_RESULT', result)

    filter_result = self.filter_query_result(result, varenv)

    pprintlog(
        'LOW_FILTER_RESULT',
        filter_result,
        transaction_id=transaction_id,
        level=log_util.DEBUG,
        pop=True)

    return filter_result

  def check(self, orig_query, varenv=None):
    if varenv == None:
      varenv = {}

    # stages:
    dumplog('READ', orig_query)

    # add "original_query" decorators.
    query = self.make_orig(orig_query)

    self.make_parents(query, True)

    dumplog('QUERY', query)

  def write(self, query, varenv):
    raise MQLInternalError(
        query, 'lojson write() is no longer supported. Use MQL instead.')

  def internal_write(self, orig_query, orig_varenv):
    return self.internal_write_or_check(orig_query, orig_varenv, WriteMode)

  def internal_write_or_check(self, orig_query, orig_varenv, mode):
    """
        This is the lojson write function

        It should only be called in the following places:

        - from hijson write
        - from yasl during the bootstrap process
        - directly from the commandline (in case of emergency)
        """

    # make sure we have a Varenv object, not just a dictionary
    if isinstance(orig_varenv, Varenv):
      varenv = orig_varenv
    else:
      varenv = Varenv(orig_varenv, self.lookup)

    transaction_id = varenv.get('tid')

    # check that the user can write to $permission

    # stages:
    pprintlog(
        'LOW_%s' % str(mode),
        orig_query,
        transaction_id=transaction_id,
        level=log_util.DEBUG,
        push=True)

    # add "original_query" decorators.
    query = self.make_orig(orig_query)
    self.make_parents(query, True)

    # check that $user, $permission, $attribution all check out OK
    scope.check_write_defaults(self, varenv)

    # propagate @insert and @delete to links as :insert and :delete
    self.add_implied_writes(query, varenv)

    # create QueryPrimitives
    self.add_query_primitives(query, varenv, mode)

    # hook QueryPrimitives together
    self.link_query_primitives_root(query, varenv, mode)

    dumplog('LOW_%s_QUERY' % str(mode), query)

    dumplog('INITIAL_%s_PRIMITIVES' % str(mode),
            [x.node for x in elements(query, dict)])

    # run (recursively) all the preparations
    self.dispatch_prepares(query, varenv)

    dumplog('PREPARED_%s_PRIMITIVES' % str(mode),
            [x.node for x in elements(query, dict)])

    # check the query is not trying to do the same thing in two places.
    self.check_circularity(query, varenv)

    # this is the point where we check that the writes are legal
    # and set the scopes.
    self.check_write_access(query, varenv)

    if mode is WriteMode:
      # generate the writes
      self.generate_write_queries(query, varenv)
    else:
      self.generate_check_responses(query, varenv)

    dumplog('COMPLETED_%s_PRIMITIVES' % str(mode),
            [x.node for x in elements(query, dict)])

    # dump the filtered write tree
    write_result = self.generate_write_result(query, varenv)

    pprintlog(
        'LOW_%s_RESULT' % str(mode),
        write_result,
        transaction_id=transaction_id,
        level=log_util.DEBUG,
        pop=True)

    return write_result

    # figure out how far the write goes
    # book it as a potential write

    # figure out the query for the other side if we have a non-empty one
    # recursively prepare that query

    # get the list of writes
    # pipeline them to the graph
    # parse out the new guids
    # put it all back together
    # success!!

    # turn QPs into graph query.
    #self.run_query(query,WriteMode)


def cmdline_main():
  import cmdline
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

  # don't try to create a HighQuery context if run from the commandline
  op.low_only = True

  options, args = op.parse_args()

  queryfile = options.query_file
  if queryfile is not None:
    qf = open(queryfile, 'r')
    query = ''.join(qf.readlines())
    regex = re.compile('[\n\t]+')
    query = regex.sub(' ', query)
    qf.close()
  elif len(args) == 1:
    query = args[0]
  else:
    op.error('at most one query argument allowed')

  if options.raw:
    query = eval(query)
  else:
    # XXX should eventually use unicode, for now utf8
    query = json.loads(query, encoding='utf-8', result_encoding='utf-8')

  if hasattr(op, 'gc'):
    lowq = LowQuery(op.gc)
  else:
    lowq = LowQuery(None)

  if options.check:
    result = lowq.check(query, op.varenv)
  elif options.write:
    result = lowq.internal_write(query, op.varenv)
  else:
    result = lowq.read(query, op.varenv)

  if options.raw:
    pprint.pprint(result)
  else:
    print json.struct2json(result)


if __name__ == '__main__':
  cmdline_main()
