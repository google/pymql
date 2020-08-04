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

from error import MQLInternalError, MQLResultError, MQLTooManyValuesForUniqueQuery
from utils import Missing, find_key_in_query
from grquoting import quote
from env import DeferredIdLookup, DeferredMidOfGuidLookup, DeferredMidsOfGuidLookup, quote_value, unquote_value, Guid
from pymql.log import LOG
from collections import defaultdict

import mid


class ReadQP(object):
  """
    A more generic QP class used for reads.

    This is a more recent (read better) design which doesnt have all the lojson
    overhead.

    Otherwise, much the same idea as QueryPrimitive.

    """

  # 'guid' must be a single or set of literal guids (not a reference to another QP)
  guids = frozenset(['guid', 'left', 'right', 'scope', 'typeguid'])

  # not interested in 'name'
  # must be a literal string. see valid_values.
  # 'previous' may eventually be a pointer, but the graph doesn't
  # support it yet
  values = frozenset(
      ['datatype', 'value', 'timestamp', 'live', 'previous', 'next'])

  directives = frozenset([
      'optional', 'sort', 'pagesize', 'comparator', 'sort_comparator', 'cursor'
  ])

  # the user must not specify these directly.
  result_field = frozenset(['result'])

  graphfields = guids | values | directives
  results = guids | values

  result_pointers = (guids | frozenset(('previous', 'next')))

  def __init__(self, query, category):
    for field in self.graphfields:
      setattr(self, field, None)
    self.sort_comparator = []
    self.query = query
    self.contents = []
    self.implied = []
    self.parent = None
    self.linkage = None
    self.comparisons = []
    self.category = category
    self.result = []
    self.return_count = False
    self.return_estimate_count = False
    self.include_count = False
    self.include_estimate_count = False
    self.vars = {}
    # icky flag to note when we optimize away self.query
    self.final = None
    if category == 'node':
      self.make_node()
    elif category == 'link':
      self.make_link()
    elif category == 'value':
      self.make_value()
    elif category in ('index', '!index'):
      self.make_index()
    elif category == 'attached':
      self.make_node()
    elif category == 'directlink':
      # a direct link may be to a value link or a real link
      self.make_directlink()
    elif category == 'constraint':
      # this is usually done afterwards. Not all contraints
      # are values, some are links.
      # (what this really does is supresses returning a value)
      self.make_value()
    else:
      raise MQLInternalError(
          self.query, 'Unrecognized category %(category)s', category=category)

  def make_node(self):
    self.bang_indexes = defaultdict(list)
    self.left = Missing
    self.right = Missing
    self.value = None
    self.datatype = None
    self.guid = None

  def make_link(self):
    self.value = Missing
    # constrain on value=null because the graph is building an index on that, not on datatype=null
    self.datatype = None
    self.pagesize = 100

  def make_value(self):
    self.value = None
    self.datatype = None
    self.pagesize = 100
    # icky way to generate value!=null (see bug 6388 and bug 7133)
    self.comparisons.append(('value!=', Missing))

  def make_directlink(self):
    # could be a link or a value
    self.value = None
    self.datatype = None
    self.pagesize = 100

  def make_index(self):
    self.datatype = 'float'
    self.right = Missing
    self.value = None
    # order info is always optional
    self.optional = True

  def set_property(self, property, parent, child):
    """
        Inspect the schema property to discover what to do with me...

        """
    if property.enumeration:
      if child is not None:
        raise MQLInternalError(
            self.query, "Can't generate an enumeration with a valid right")

      # but we need to generate our own child...
      self.left = Guid(property.enumeration)
      parent.add_contents(self, 'right', False)

    elif property.reverse:
      if child is Missing:
        raise MQLInternalError(
            self.query, "Can't generate a reverse property with a missing left")
      elif child is None:
        # we may be a "key" clause who has a left, but isn't interested.
        pass
      else:
        self.add_contents(child, 'left', True)

      parent.add_contents(self, 'right', False)
    else:
      if child is Missing:
        self.right = Missing
      elif child is None:
        # we may have set self.right ourselves already.
        pass
      else:
        self.add_contents(child, 'right', True)

      parent.add_contents(self, 'left', False)

    if (property.has_id('/type/reflect/any_master') or
        property.has_id('/type/reflect/any_reverse') or
        property.has_id('/type/reflect/any_value')):
      # typeguid is unconstrained...
      self.typeguid = None
    else:
      self.typeguid = property

  def add_contents(self, child, field, contains=False):
    child.parent = self
    child.linkage = (field, contains)
    self.contents.append(child)

  def add_final(self, child_query, property, guid):
    self.final = FinalQP(child_query, property, guid)
    field = self.final.field

    if guid is None:
      self.add_result(field)
    else:
      setattr(self, field, guid)

  def generate_graph_query(self, qpush):
    if self.linkage:
      field, contains = self.linkage
      if contains:
        qpush('%s->(' % field)
      else:
        qpush('(<-%s ' % field)
    else:
      qpush('(')

    self.generate_fields(qpush)
    self.generate_comparisons(qpush)
    self.generate_vars(qpush)
    self.generate_result(qpush)

    for child in self.contents:
      child.generate_graph_query(qpush)

    qpush(') ')

  def generate_fields(self, qpush):
    for k in self.graphfields:
      v = getattr(self, k)
      if v is not None:
        self.generate_field(k, v, qpush)

  def generate_field(self, k, v, qpush):

    if k in self.guids:
      if v is Missing:
        qpush(k)
        qpush('=')
        qpush('null')
      else:
        guid = v.graph_guid()
        if guid is False:
          qpush('false')
        else:
          qpush(k)
          # graph_query() on DeferredXXX() is responsible for generation...
          qpush('=')
          qpush(v.graph_guid())
    elif k == 'value':

      qpush(k)
      qpush('=')
      qpush(quote_value(v))

    elif k in ['datatype', 'timestamp']:
      if v is Missing:
        v = 'null'

      qpush(k)
      qpush('=')
      qpush(v)
    elif k == 'optional':
      if v is True or v == 'optional':
        qpush(k)
      elif v == 'forbidden':
        qpush('count=0')
      elif v is False or v == 'required':
        # this is the default
        pass
      else:
        raise MQLInternalError(
            self.query,
            "Don't understand %(value)s as an argument to 'optional'",
            value=v)

    elif k == 'pagesize':
      qpush(k)
      qpush('=')
      qpush(str(v))

    elif k == 'sort':
      qpush(k)
      if isinstance(v, list) and len(v) > 0:
        qpush('=(')
        qpush(' '.join(v))
        qpush(')')
      elif isinstance(v, basestring):
        qpush('=')
        qpush(v)
    elif k == 'comparator':
      qpush('value-comparator=')
      qpush(quote(v))
    elif k == 'sort_comparator':
      if v:
        qpush('sort-comparator=(')
        qpush(' '.join(map(quote, v)))
        qpush(')')
    elif k == 'cursor':
      qpush(k)
      qpush('=')
      qpush(self.get_graph_cursor(v))

    else:
      raise MQLInternalError(self.query, "Can't generate", key=k, value=v)

    qpush(' ')

  def generate_comparisons(self, qpush):
    for comparison in self.comparisons:
      (key_op, v) = comparison
      qv = quote_value(v)
      # ugliness for bug 7256
      if qv is False:
        if key_op != 'guid!=':
          # don't know what went wrong, but we shouldn't be here.
          raise MQLInternalError(
              self.query,
              "Can't generate a comparison for %(key_op)s %(value)s",
              key_op=key_op,
              value=str(qv))
        else:
          # not equal to something that does not exist.
          pass
      else:
        qpush(key_op)
        qpush(qv)
        qpush(' ')

  def generate_vars(self, qpush):
    for var, key in self.vars.iteritems():
      qpush(var)
      qpush('=')
      qpush(key)
      qpush(' ')

  def generate_result(self, qpush):
    qpush('result=(')

    if self.return_count:
      # if we asked for the return:count, we only get the count
      return qpush('count) ')
    if self.return_estimate_count:
      # if we asked for the return:estimate-count, we only get the count
      return qpush('estimate-count) ')

    if self.include_count:
      # if we asked for the count:null, we get it along with other results
      qpush('count ')
    if self.include_estimate_count:
      # if we asked for the estimate-count:null, we get it along with other results
      qpush('estimate-count ')

    if self.cursor is not None:
      qpush('cursor (')
    else:
      qpush('(')

    for k in self.result:
      qpush(k)
      qpush(' ')

    if self.contents:
      qpush('contents')

    qpush(')) ')

  def add_result(self, field):
    if field not in self.result:
      self.result.append(field)

  def parse_result_root(self, result, varenv):
    if self.return_count:
      return self.parse_result_count(result, varenv)
    if self.return_estimate_count:
      return self.parse_result_count(result, varenv)

    cnt = None
    if self.include_count:
      cnt = self.parse_included_count(result, varenv)
      result = result[1:]
    est = None
    if self.include_estimate_count:
      est = self.parse_included_count(result, varenv)
      result = result[1:]

    if self.cursor is not None:
      if len(result):
        varenv['cursor'] = self.get_mql_cursor(result[0])
        result = result[1:]
      else:
        # no more results...
        varenv['cursor'] = False

    if self.category == 'directlink':
      outer_result = self.parse_result_direct_link(result, varenv)
    else:
      outer_result = []
      for sub_result in result:
        elt = {}
        if cnt is not None:
          elt['count'] = cnt
        if est is not None:
          elt['estimate-count'] = est
        outer_result.append(self.parse_result_node(sub_result, elt, varenv))
      self._finish_bang_indexes()
      outer_result = self.list_or_item(outer_result, varenv)

    return outer_result

  def _finish_bang_indexes(self):
    for indexes in self.bang_indexes.itervalues():
      for i, (index, high_result) in enumerate(sorted(indexes)):
        high_result['!index'] = i
    self.bang_indexes.clear()

  def parse_result_node(self, result, high_result, varenv):
    # nodes can only possibly have a single result, so we take the underlying clause directly.

    # this does not handle terminals !!!
    if self.implied:
      self.parse_result_implied_value(result, high_result, varenv)

    n = len(self.result)
    for i, qp in enumerate(self.contents):
      if qp.return_count:
        high_result[qp.query.key] = qp.parse_result_count(result[n + i], varenv)
        continue
      if qp.return_estimate_count:
        high_result[qp.query.key] = qp.parse_result_count(result[n + i], varenv)
        continue

      cnt = None
      est = None
      if qp.include_count:
        cnt = qp.parse_included_count(result[n + i], varenv)
        result[n + i] = result[n + i][1:]
      if qp.include_estimate_count:
        est = qp.parse_included_count(result[n + i], varenv)
        result[n + i] = result[n + i][1:]

      if qp.category == 'link':
        high_result[qp.query.key] = qp.parse_result_link(result[n + i], varenv)
      elif qp.category == 'value':
        high_result[qp.query.key] = qp.parse_result_value(result[n + i], varenv)
      elif qp.category == 'attached':
        high_result[qp.query.key] = qp.parse_result_right(result[n + i], varenv)
      elif qp.category == 'directlink':
        high_result[qp.query.key] = qp.parse_result_direct_link(
            result[n + i], varenv)
      elif qp.category == 'constraint':
        # constraints are supressed
        pass
      else:
        raise MQLInternalError(self.query,
                               "Can't parse_result two adjacent nodes")

      def insert_count(key, value):
        rv = high_result[qp.query.key]
        if isinstance(rv, list):
          for v in rv:
            v[key] = value
        elif isinstance(rv, dict):
          rv[key] = value
        else:
          raise MQLInternalError(self.query, '%s(key) error', key=key)

      if cnt is not None:
        insert_count('count', cnt)
      if est is not None:
        insert_count('estimate-count', est)

    return high_result

  def parse_result_implied_value(self, result, high_result, varenv):
    """
        Add the implied value results. These are things that dont directly
        generate
        contents; like "id": xxx or "timestamp": xxx
        """

    dz = dict(zip(self.result, result))

    for item in self.implied:
      # don't generate results if we implied this thing.
      if item.terminal == 'C':
        continue

      sprop = item.property
      dpn = item.stype.get_default_property_name()
      # never more than one result
      imp_result = {}
      value = None
      datatype = None
      typeguid = None
      if dpn in item:
        value = item[dpn].value

      if value is None:
        if (sprop.id == '/type/object/id'):
          if item[dpn].alternatives:
            # we need to look this up from the list we were given
            value = item[dpn].alternatives.lookup_id('#' + dz['guid'])
            if not value:
              raise MQLInternalError(
                  item,
                  "Can't locate returned guid from specified list of alternatives",
                  key=dpn)

          else:
            value = DeferredIdLookup('#' + dz['guid'], varenv.lookup_manager)
        elif (sprop.id == '/type/object/guid'):
          value = '#' + dz['guid']
        elif (sprop.id == '/type/object/timestamp') or (
            sprop.id == '/type/link/timestamp'):
          value = dz['timestamp']
        elif (sprop.id == '/type/link/target_value'):
          value = \
              item.stype.uncoerce_value(unquote_value(dz['datatype'],
                                                      dz['value'],
                                                      varenv.get('escape','html')),
                                                      dz['datatype'],
                                                      varenv.get('unicode_text'))
          datatype = dz['datatype']
          typeguid = '#' + dz['typeguid']
        elif (sprop.id == '/type/link/valid'):
          if dz['next'] == 'null' and dz['live'] == 'true':
            value = True
          elif dz['live'] == 'false':
            value = False
          elif dz['next'] != 'null':
            value = False
          else:
            raise MQLInternalError(item,
                                   "Invalid link; can't determine 'valid'",
                                   **dz)
        elif (sprop.id == '/type/link/operation'):
          if dz['previous'] == 'null' and dz['live'] == 'true':
            value = 'insert'
          elif dz['live'] == 'false':
            value = 'delete'
          elif dz['previous'] != 'null':
            value = 'update'
          else:
            raise MQLInternalError(item,
                                   "Invalid link; can't determine 'operation'",
                                   **dz)
        elif (sprop.id == '/type/link/reverse'):
          # nothing to ask about here
          if self.category == 'directlink':
            # direct links are always forward
            value = False
          else:
            # it depends on how you approached the link
            value = item.parent_clause.parent_clause.property.reverse
        elif (sprop.id == '/type/link/type'):
          # nothing to ask about here
          value = '/type/link'
        elif (sprop.id == '/type/object/mid'):
          # we have to go do a replaced_by lookup.
          if item.list:
            value = DeferredMidsOfGuidLookup('#' + dz['guid'],
                                             varenv.lookup_manager)
          else:
            # Don't go to the graph, just convert it.
            value = mid.of_guid(dz['guid'])
        else:
          raise MQLInternalError(item, "Can't get implied result here")

      if item.terminal and item.terminal in 'LVN':
        imp_result = value
      elif value is None:
        imp_result = None
      else:
        # a full dictionary result...
        if dpn in item:
          imp_result[dpn] = value
        if 'type' in item:
          type_res = item.stype.get_value_type(datatype, typeguid)
          if isinstance(item['type'], list):
            type_res = [type_res]
          imp_result['type'] = type_res

        # bug 7133; this code is incredibly unappetizing.
        expected_rpn = item.stype.get_right_property_name(self.query)
        data_rpn = item.stype.get_data_right_property_name(datatype, typeguid)
        rpn = None
        if expected_rpn and expected_rpn in item:
          # this is the case where the rpn in explicitly requested
          # so we give it to you regardless of whether or not it
          # actually exists.
          # we should have generated a finalqp here
          rpn = expected_rpn
        elif data_rpn and item.terminal == 'D':
          # so we didn't explicitly ask for an RPN
          # but we are using {} or [{}] so we should get
          # whatever RPN is available *if one exists*
          # we must have explicitly added the finalqp here too.
          rpn = data_rpn

        if rpn:
          if not self.final:
            raise MQLInternalError(item, 'Saw an rpn without a final QP')
          # note that rpn_res may be null (see bug 6388 comments)
          rpn_res = self.final.parse_result_final(dz, varenv)
          if self.final.query.list:
            rpn_res = [rpn_res]
          imp_result[rpn] = rpn_res

      if item.list:
        imp_result = [imp_result]

      high_result[item.key] = imp_result

  def parse_result_index(self, result, varenv):
    if len(result) > 1:
      #            raise MQLInternalError(self.query,"More than one piece of order information")
      try:
        error_query = self.parent.query.original_query
      except:
        error_query = self.query
      LOG.error(
          'multiple.indices',
          'More than one piece of order information (using first one)',
          query=repr(error_query),
          indices=repr(result))
      result = result[0]
    elif len(result) == 0:
      return None
    else:
      dz = dict(zip(self.result, result[0]))
      return unquote_value('float', dz['value'], False)

  def parse_result_count(self, result, varenv):
    if len(result) == 1:
      try:
        return self.list_or_item([int(result[0])], varenv)
      except ValueError:
        raise MQLInternalError(
            self.query,
            'Invalid return from count or estimate-count',
            value=result[0])
      except TypeError:
        raise MQLInternalError(
            self.query,
            'Invalid return from count or estimate-count',
            value=type(result[0]))
    elif len(result) == 0:
      # if you happened to manage to get optional: false with your count, you get an result of 0 count.
      # this is nicer than raising an internal exception.
      return self.list_or_item([0], varenv)
    else:
      raise MQLInternalError(self.query,
                             'More than one item for count or estimate-count')

  def parse_included_count(self, result, varenv):
    if len(result) == 0:
      # if you happened to manage to get optional: false with your count, you get an result of 0 count.
      # this is nicer than raising an internal exception.
      return 0
    else:
      try:
        return int(result[0])
      except ValueError:
        raise MQLInternalError(
            self.query,
            'Invalid return from count or estimate-count',
            value=result[0])
      except TypeError:
        raise MQLInternalError(
            self.query,
            'Invalid return from count or estimate-count',
            value=type(result[0]))

  def parse_result_direct_link(self, result, varenv):
    outer_result = []
    indexes = []

    for sub_result in result:
      high_result = {}

      n = len(self.result)
      for i, qp in enumerate(self.contents):
        if qp.category == 'attached':
          high_result[qp.query.key] = qp.parse_result_right(
              sub_result[n + i], varenv)
        elif qp.category == 'index':
          high_result['index'] = None
          index = qp.parse_result_index(sub_result[n + i], varenv)
          if index is not None:
            indexes.append((index, high_result))
        elif qp.category == 'constraint':
          pass
        else:
          raise MQLInternalError(
              self.query,
              "Can't parse_result_direct_link with category %(category)s",
              category=qp.category)

      if self.implied:
        self.parse_result_implied_value(sub_result, high_result, varenv)

      if self.query.terminal and self.query.terminal in 'LVN':
        high_result = high_result[self.query.stype.get_default_property_name()]

      outer_result.append(high_result)

    if indexes:
      for i, pair in enumerate(sorted(indexes)):
        pair[1]['index'] = i

    return self.list_or_item(outer_result, varenv)

  def parse_result_link(self, result, varenv):
    outer_result = []
    indexes = []

    for sub_result in result:
      high_result = {}
      link = {}

      n = len(self.result)
      for i, qp in enumerate(self.contents):
        if qp.category == 'node':
          # we are guaranteeing a single result here -- so no optional in this case.
          if len(sub_result[n + i]) != 1:
            raise MQLInternalError(self.query,
                                   'Expect only one node attached to a link')

          qp.parse_result_node(sub_result[n + i][0], high_result, varenv)
        elif qp.category == 'index':
          high_result[qp.category] = None
          index = qp.parse_result_index(sub_result[n + i], varenv)
          if index is not None:
            indexes.append((index, high_result))
        elif qp.category == '!index':
          high_result[qp.category] = None
          index = qp.parse_result_index(sub_result[n + i], varenv)
          if index is not None:
            self.parent.bang_indexes[self.query.property].append(
                (index, high_result))
        elif qp.category == 'attached':
          link[qp.query.key] = qp.parse_result_right(sub_result[n + i], varenv)
        else:
          raise MQLInternalError(
              self.query,
              "Can't parse_result link and %(category)s",
              category=qp.category)

      if self.final:
        dz = dict(zip(self.result, sub_result))
        high_result = self.final.parse_result_final(dz, varenv)
      elif self.query.terminal and self.query.terminal in 'LVN':
        # a terminal, but not final...
        high_result = high_result[self.query.stype.get_default_property_name()]
      elif self.implied:
        # a non terminal with a 'link' clause
        self.parse_result_implied_value(sub_result, link, varenv)

      # do we need link results?
      if 'link' in self.query:
        self.add_link_result(high_result, link, varenv)

      outer_result.append(high_result)
    if indexes:
      for i, (index, high_result) in enumerate(sorted(indexes)):
        high_result['index'] = i

    for node in (n for n in self.contents if n.category == 'node'):
      node._finish_bang_indexes()
    return self.list_or_item(outer_result, varenv)

  def parse_result_right(self, result, varenv):
    outer_result = []
    high_result = {}
    if self.category == 'attached':
      if self.include_count:
        high_result['count'] = self.parse_included_count(result, varenv)
        result = result[1:]
      if self.include_estimate_count:
        high_result['estimate-count'] = self.parse_included_count(
            result, varenv)
        result = result[1:]

    for sub_result in result:
      # this loop will execute 0 or 1 times only.
      self.parse_result_node(sub_result, high_result, varenv)

      if self.query.terminal and self.query.terminal in 'LVN':
        high_result = high_result[self.query.stype.get_default_property_name()]

    if high_result:
      outer_result.append(high_result)

    return self.list_or_item(outer_result, varenv)

  def parse_result_value(self, result, varenv):
    outer_result = []
    indexes = []
    stype = self.query.stype
    dpn = stype.get_default_property_name()

    for sub_result in result:
      high_result = {}
      link = {}

      dz = dict(zip(self.result, sub_result))
      # safety by default...
      value = stype.uncoerce_value(
          unquote_value(dz['datatype'], dz['value'],
                        varenv.get('escape', 'html')), dz['datatype'],
          varenv.get('unicode_text'))

      if self.query.terminal and self.query.terminal in 'LVN':
        # we can skip the rest of this clause; all we wanted was the value
        outer_result.append(value)
        continue

      k = find_key_in_query(dpn, self.query)
      if k is not None and self.query[k] is not None and self.query[
          k].terminal != 'C':
        high_result[k] = value
      else:
        full_dpn = stype.getprop(dpn, varenv).id
        if full_dpn in self.query and self.query[full_dpn].terminal != 'C':
          high_result[full_dpn] = value
      if 'type' in self.query:
        type_res = stype.get_value_type(dz['datatype'])
        if isinstance(self.query['type'], list):
          type_res = [type_res]
        high_result['type'] = type_res

      if self.final:
        # a final QP can't be a constraint right now, so we don't need to check.
        # this is bug 7133 rearing its ugly head...
        rpn_res = self.final.parse_result_final(dz, varenv)

        expected_rpn = stype.get_right_property_name(self.query)
        data_rpn = stype.get_data_right_property_name(dz['datatype'], None)
        rpn = None
        if expected_rpn and expected_rpn in self.query:
          rpn = expected_rpn
        elif data_rpn and self.query.terminal == 'D':
          rpn = data_rpn

        if rpn:
          if self.final.query.list:
            rpn_res = [rpn_res]
          high_result[rpn] = rpn_res

      if self.implied:
        # a non terminal with a 'link' clause
        self.parse_result_implied_value(sub_result, link, varenv)

      n = len(self.result)
      for i, qp in enumerate(self.contents):
        if qp.category == 'node':
          # we must have specified RPN to do this...
          rpn = stype.get_right_property_name(self.query)
          high_result[rpn] = qp.parse_result_right(sub_result[n + i], varenv)
        elif qp.category == 'index':
          high_result['index'] = None
          index = qp.parse_result_index(sub_result[n + i], varenv)
          if index is not None:
            indexes.append((index, high_result))
        elif qp.category == 'attached':
          link[qp.query.key] = qp.parse_result_right(sub_result[n + i], varenv)
        elif qp.category == 'constraint':
          # the RPN may be only a constraint.
          pass
        else:
          raise MQLInternalError(
              self.query,
              "Can't parse_result value and %(category)s",
              category=qp.category)

      # do we need link results?
      if 'link' in self.query:
        self.add_link_result(high_result, link, varenv)

      outer_result.append(high_result)

    if indexes:
      for i, pair in enumerate(sorted(indexes)):
        pair[1]['index'] = i

    return self.list_or_item(outer_result, varenv)

  def add_link_result(self, high_result, link, varenv):
    is_list = False
    if 'link' in self.query:
      link_q = self.query['link']
      if isinstance(link_q, list):
        is_list = True
        link_q = link_q[0]

      if link_q.terminal:
        if link_q.terminal == 'V':
          link = link_q.value
        elif link_q.terminal in 'LN':
          # this is always "master_property"
          link = link[link_q.stype.get_default_property_name()]
        elif link_q.terminal == 'C':
          link = Missing

      if link is Missing or not isinstance(high_result, dict):
        pass
      elif is_list:
        high_result['link'] = [link]
      else:
        high_result['link'] = link
    else:
      raise MQLInternalError(self.query,
                             "Didn't find link clause, but got link results")

  def list_or_item(self, result, varenv):
    # return a list if we want a list or the first item if we don't
    if self.query.list:
      return result
    elif len(result) == 1:
      return result[0]
    elif len(result) == 0:
      return None
    elif varenv.get('uniqueness_failure', None) == 'soft':
      # this supresses the exception in favor of a warning message
      LOG.warning(
          'soft.uniqueness.failure', repr(self.query), result=repr(result))
      return result[0]
    else:
      # this mutates the result, but we don't want it anyway...
      varenv.lookup_manager.do_id_lookups()
      result = varenv.lookup_manager.substitute_ids(result)
      raise MQLTooManyValuesForUniqueQuery(
          self.query, results=result, count=len(result))

  def get_graph_cursor(self, decoded_cursor):
    """
        This takes a decoded python cursor and returns a graph cursor
        """
    if decoded_cursor is True:
      # relying on quote_value() to do the right thing here...
      return 'null'
    elif decoded_cursor is False:
      return '"null:"'
    elif isinstance(decoded_cursor, str):
      return quote(decoded_cursor)
    else:
      raise MQLInternalError(
          self.query,
          'Cursor must be a string or boolean',
          cursor=repr(decoded_cursor))

  def get_mql_cursor(self, graph_cursor):
    """
        This takes a return value from the graph and produces a python cursor
        """
    graph_cursor = unquote_value('string', graph_cursor, False)

    if graph_cursor == 'null:' or graph_cursor is None:
      return False
    else:
      return graph_cursor


class FinalQP(object):
  """
    a hack to describe a terminal too complex to be shoo-horned into our
    basic QP tree
    """

  def __init__(self, query, property, guid):
    self.query = query
    if property.reverse:
      self.field = 'left'
    else:
      self.field = 'right'

    self.guid = guid

  def parse_result_final(self, dz, varenv):
    # XXX in theory we can return more than an id from a final qp. But we don't currently handle that case.
    if self.query.terminal in 'LN':
      # bug 6388 -- self.field may be "right" and in the case of an int in a text
      # slot, right may be null. Just give back "None" in that case...
      if dz[self.field] == 'null':
        return None
      else:
        return DeferredIdLookup('#' + dz[self.field], varenv.lookup_manager)
    elif self.query.terminal == 'V':
      return self.query.value
    else:
      raise MQLInternalError(
          self.query, "Can't handle a final that is not also a terminal")
