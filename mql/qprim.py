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
from grquoting import quote, unquote

from itertools import chain
from utils import (valid_value_op, valid_timestamp_op, valid_timestamp,
                   valid_history_op, valid_guid, ReadMode, WriteMode,
                   PrepareMode, CheckMode, Missing, ResultDict, incr_subseq)
from error import (MQLResultError, MQLInternalError, MQLInternalParseError,
                   MQLValueAlreadyInUseError, MQLTooManyValuesForUniqueQuery)

from pymql.log import LOG

_boolean_name = {True: 'true', False: 'false'}
_make_boolean = {'true': True, 'false': False}


class QueryPrimitive(object):
  """
    A single primitive in a query. May be a link or a node depending
    on left, right and typeguid.

    QueryPrimitives deal with unadorned graph names (e.g. scope, not
    @scope or :scope) although they do provide a convienence
    constructor that matches and strips the '@:' prefix

    They deal with # sign guids -- only the actual graph query
    generator removes # signs.

    A QueryPrimitive holds a reference (self.query) to the query node
    that created it but does not use it, and particularly does not use
    the attributes of the query

    In fact the only use QueryPrimitive makes of self.query is to
    create result.query for the benefit of the query processing in
    LowQuery
    """

  # these may be
  # - a single literal guid
  # - a list of literal guids
  # - an empty dict (aka a query)
  # - a reference to another QueryPrimitive
  pointers = set(['typeguid', 'scope'])
  connectors = set(['left', 'right'])

  # 'guid' must be a single or set of literal guids (not a reference to another QP)
  guid_field = set(['guid'])

  # not interested in 'name'
  # must be a literal string. see valid_values.
  # 'previous' may eventually be a pointer, but the graph doesn't
  # support it yet
  values = set(['datatype', 'value', 'timestamp', 'live', 'previous', 'next'])

  directives = set(['optional', 'sort', 'pagesize', 'newest', 'oldest'])

  # the user must not specify these directly.
  result_field = set(['result'])

  # used for ordering
  derived = set(['index'])
  cursor_field = set(['cursor'])
  comparator_field = set(['comparator'])

  # these are used to generate key and unique clauses
  writeinsns = set(
      ['insert', 'delete', 'update', 'link', 'unlink', 'ensure', 'ensurechild'])

  # mapping from python => graphd types
  # note we can't map from stuff like string->'url', so use this
  # structure wisely.
  make_datatypes = {
      int: 'integer',
      long: 'integer',
      str: 'string',
      float: 'float',
      bool: 'boolean',
      unicode: 'string'
  }

  # mapping from graphd => python types - for use with isinstance,
  # so tuples are allowed here.

  # note 'url' and 'bytestring' are in here, because graphd can
  # remember that
  check_datatypes = {
      'integer': (int, long),
      'float': float,
      'url': str,
      'string': basestring,
      'boolean': bool,
      'bytestring': str,
      'timestamp': str,
      'null': type(None)
  }

  check_comparators = set(['octet', 'number', 'datetime'])

  # parent is the pointer where I find the parent. Can be 'left' or 'right'
  # It is not possible to construct a query containing "... (<-scope ..."
  #
  # If I am the contents of my parent, parent is None, and container is a direct pointer to the parent, not an indirect reference
  #
  # type is what the parent calls me. I will use this to infer typeguid if it is needed and not specified.
  # id is a namespace name for me.
  #
  # reverse implies when someone calls add_parent() they get right and add_child() gets left.
  #
  # contents is the list of primitives that link to me and are not my parent or a pointer in me
  # (i.e. everything that I contain in the graph query without an -> on my side)
  #
  # children are the set of pointers I use to actually point to other things.
  #
  # child is the guid of my child. If I see :child, I will not generate a real subquery.
  # just left=:child or right=:child. This may abruptly curtail the query.
  # note that :child is always the same as @guid at the same level.
  #

  # these are the other meaningful slots - in QP that we don't understand
  # but may be passed. Note that parent, children, contents and vars
  # are never given to us directly.
  #
  # internal_use = set(['parent','children','contents','child','container','vars','valueops','timestampops'])

  # default allows a (unique) link to be arbitarily queried during a write, but a default value
  # to be substituted if a value is not found.

  special = set(['type', 'id', 'reverse', 'unique', 'default'])

  input_fields = guid_field | pointers | values | directives | derived | writeinsns | special | comparator_field
  all_fields = input_fields | result_field | connectors | cursor_field

  graphfields = guid_field | pointers | connectors | values | directives | result_field | cursor_field | comparator_field
  results = pointers | values

  result_pointers = (
      pointers | guid_field | connectors | set(('previous', 'next')))

  allowed_state_transitions = {
      None: ('insert', 'delete', 'ensurechild', 'ensure', 'link', 'unlink',
             'match', 'unique', 'default', 'order_read', 'order_info',
             'namespace_info'),
      'link': ('create', 'found'),
      'ensurechild': ('create', 'found'),
      'ensure': ('create', 'found'),
      'default': ('found',),
      'match': ('found',),
      'insert': ('create',),
      'delete': ('remove', 'notpresent'),
      'unlink': ('remove', 'notpresent'),
      'remove': ('written',),
      'create': ('written',),
      'unique': ('unique_check', 'update_check', 'replace_check'),
      'replace_check': ('update_check', 'checked'),
      'update_check': ('checked',),
      'unique_check': ('duplicate', 'checked'),
      'namespace_info': ('namespace_unique', 'namespace_regular'),
      'order_read': ('order_found', 'order_missing'),
      'order_found': ('create', 'found'),
      'order_missing': ('create',),
      'order_info': ('checked',),
      'namespace_unique': ('done',),
      'namespace_regular': ('done',),
      'duplicate': ('done',),
      'checked': ('done',),
      'written': ('done',),
      'notpresent': ('done',),
      'found': ('done',)
  }

  def __init__(self, prefix, qdict, mode):
    for field in self.all_fields:
      setattr(self, field, None)

    # we always ask for the guid of everything.
    self.result = ['guid']
    self.vars = {}
    self.contents = []
    self.children = []
    self.container = None
    self.parent = None
    self.ordered = None
    self.order_info = []
    self.child = None
    self.valueops = {}
    self.timestampops = {}
    self.history_ops = {}

    # where the unique checks are run
    self.unique_checks = []
    self.unique_namespace_info = None
    self.unique_namespace_checks = []

    # mode is ReadMode, WriteMode or CheckMode
    self.mode = mode
    self.state = None
    # these start out as Missing, not just None (aka unspecified)
    self.left = Missing
    self.right = Missing

    self.access_control_ok = False

    self.prefix = prefix

    self.query = qdict
    # this says - "There is only one slot for me in the query"
    # it only makes sense for links - nodes must always be unique
    # (as they are the direct slot of a link or another node)
    self.query_unique = (prefix == '@' or qdict.list is None)
    # keep the key this is referred to around as well.
    if qdict.key is not None:
      self.query_key = qdict.key

    self.constrain(prefix, qdict)

  # prefix is '@' (nodes) ':' (links),
  # '=' for unique components,
  # '?' for order attachments and '&' for order queries (icky icky)
  # '~' is for namespace unique checks,
  def constrain(self, prefix, qdict):
    if prefix not in '@:=?&~':
      raise MQLInternalError(
          self.query, 'invalid prefix %(prefix)s', prefix=prefix)

    for fullkey, v in qdict.iteritems():
      if fullkey[0] != prefix:
        continue

      k = fullkey[1:]
      if k[0] == '$':
        self.vars[k] = v
      elif valid_value_op(k):
        if k in self.valueops:
          raise MQLInternalParseError(
              self.query, 'Duplicate comparison operator %(key)s', key=fullkey)

        self.valueops[k] = self.check_comparison_op(k, v)

      elif valid_timestamp_op(k):
        if k in self.timestampops:
          raise MQLInternalParseError(
              self.query, 'Duplicate timestamp operator %(key)s', key=fullkey)

        self.timestampops[k] = self.check_timestamp_op(k, v)

      elif valid_history_op(k):
        if k in self.history_ops:
          raise MQLInternalParseError(
              self.query, 'Duplicate history operator %(key)s', key=fullkey)

        self.history_ops[k] = self.check_history_op(k, v)
      else:
        newv = self.transform_field(k, v)
        if newv is Missing:
          pass
        else:
          if getattr(self, k) is not None:
            raise MQLInternalParseError(
                self.query,
                'Duplicate attribute %(key)s',
                key=fullkey,
                value=v,
                duplicate=newv)

          if k in self.results:
            self.result.append(k)

          setattr(self, k, newv)

    self.check_or_make_datatype()

    if self.mode in (WriteMode, CheckMode):
      self.check_write_directives()

  def check_or_make_datatype(self):
    # the value and the datatype must agree. The value must agree
    # with the comparison operators which must agree with each
    # other and restrict the possible output.
    # first check the existing value, if requested
    if self.value is not None:
      if self.datatype is not None:
        if not isinstance(self.value, self.check_datatypes[self.datatype]):
          raise MQLInternalParseError(
              self.query,
              "Value of property is incompatible with specified or inferred datatype '%(datatype)s'",
              datatype=self.datatype,
              value=self.value)
      else:
        self.datatype = self.make_datatypes[type(self.value)]

    # now check any constraints
    for key, value in self.valueops.iteritems():

      # a special pass for ~="*". This is used in
      # hijson.py:build_low_json_value() to force value!=null, so we don't want it to force
      # datatype=string here. Yuck!
      if key == 'value~=' and value == '*':
        pass
      elif self.datatype is not None:
        # we've already constrained the datatype, so lets
        # verify that we're being consistent about the type
        # it needs to be this way around so that uri works.
        if not isinstance(value, self.check_datatypes[self.datatype]):
          raise MQLInternalParseError(
              self.query,
              "Constraint on %(key)s is incompatible with specified or inferred datatype '%(datatype)s'",
              key=key,
              value=value,
              datatype=self.datatype)

      else:
        # datatype is not yet constrained
        self.datatype = self.make_datatypes[type(value)]

    if 'value' in self.result and 'datatype' not in self.result:
      self.result.append('datatype')

  def check_write_directives(self):
    if ((self.insert or self.link or self.ensure or self.ensurechild or
         self.update or False) + (self.delete or self.unlink or False)) > 1:
      raise MQLInternalParseError(
          self.query, "Can't mix insert and delete directives on the same node")

    # the order here is very important -- this is the order of priority of the write directives
    # insert before link (we can't run prepares on a link that cannot be there)
    # delete before unlink (we must find everything on an @delete statement to proceed)
    # insert and ensure before update (we may be updating something with something we are ensuring or inserting)
    # ensure before ensurechild -- if we have touching directives it's OK to query this one
    if self.insert:
      self.change_state('insert')
    elif self.link:
      self.change_state('link')
    elif self.ensure:
      self.change_state('ensure')
    elif self.ensurechild:
      self.change_state('ensurechild')
    elif self.delete:
      self.change_state('delete')
    elif self.unlink:
      self.change_state('unlink')
    elif self.default:
      self.change_state('default')
    elif self.prefix == '=':
      self.change_state('unique')
    elif self.prefix == '?':
      self.change_state('order_read')
    elif self.prefix == '&':
      self.change_state('order_info')
    elif self.prefix == '~':
      self.change_state('namespace_info')
    else:
      self.change_state('match')

  def add_unique_query_primitives(self, mode):
    # figure out what sort of uniqueness we require and add the appropriate primitives
    if self.unique in ['right', 'both', 'key']:
      # regular uniqueness
      unique_qp = QueryPrimitive('=', self.query, mode)
      unique_qp.unique = 'right'
      unique_qp.orig = self
      unique_qp.parent = 'left'
      unique_qp.left = self.left

      self.left.unique_checks.append(unique_qp)

      unique_qp.link_unique_query_primitive()

    if self.unique in ['left', 'both']:
      # reverse uniqueness
      unique_qp = QueryPrimitive('=', self.query, mode)
      unique_qp.unique = 'left'
      unique_qp.orig = self
      unique_qp.parent = 'right'
      unique_qp.right = self.right

      self.right.unique_checks.append(unique_qp)

      unique_qp.link_unique_query_primitive()

    if self.unique == 'value':
      # value uniqueness
      unique_qp = QueryPrimitive('=', self.query, mode)
      unique_qp.unique = 'value'
      unique_qp.orig = self
      unique_qp.parent = 'left'
      unique_qp.left = self.left

      self.left.unique_checks.append(unique_qp)

      unique_qp.link_unique_query_primitive()

    if self.unique == 'key':
      # reverse value uniqueness for keys (youch!)
      unique_qp = QueryPrimitive('=', self.query, mode)
      unique_qp.unique = 'key'
      # this is really really really a hack...
      unique_qp.orig = self

      # a ns_unique_qp must not have a parent as this will prevent it from
      # generating a correct graph query.
      # unique_qp.parent = 'right'

      unique_qp.link_unique_query_primitive()

      self.left.unique_namespace_checks.append(unique_qp)

      # we have to defer the linkage of this primitive until we know the right.

  def run_namespace_prepare(self, reader):
    """
        This is an ugly hack to allow the checking of key uniqueness in unique
        namespaces

        The issue is that this is a circular dependency on the query, so it
        cannot always be done
        until the query itself has run. For example


        { "name": "Unique node namespace 12345",
          "key": { "value": "12345",
                   "namespace": { "name": "Unique namespace root 5678" },
                   "connect": "insert"
                 }
        }

        cannot be evaluated in a single pass.
        """

    # here's how this works:

    # if we don't have a left, we are a new namespace. We weren't found, so we never even made it to this function
    # check_circularity has to look out for us in this case.

    # if we have a left, we must be unique or again, we won't be here (we would be checked off in check_unique_namespace()

    # we may or may not have a right. The prepare phase has run to sufficient completion by the time we are called that
    # whether our right is our parent or our child we know its guid.

    # so all we need to do now is run
    # read (left=self.orig.left right=self.orig.right pagesize=2 typeguid=has_key result=((datatype value)) )
    # and find out what is there!

    if self.orig.left.state == 'found':
      self.left = self.orig.left.guid
    else:
      raise MQLInternalError(
          self.orig.query,
          "Didn't expect to be in run_namespace_prepare without a left!")

    if self.orig.right.state == 'found':
      self.right = self.orig.right.guid
    elif self.orig.right.state == 'create':
      # we're all good with a new node to the right; bail here

      self.change_state('checked')
      return

    graphq = self.generate_graph_query(PrepareMode)
    # however this works...
    result = reader(graphq)

    if len(result) == 1:
      self.attach_unique_result(reader, result[0])
    elif len(result) == 0:
      self.mark_unique_missing(reader)
    else:
      # for inserts and updates this bombs, for deletes it may just pass
      self.handle_multiple_results(reader, result)

  def link_unique_query_primitive(self):
    # this is backwards from add_unique_query_primitive(); it takes the unique primitive itself.

    orig_qp = self.orig

    # we need to figure out *which* of the unique check primitives need to be updated.
    # this is an issue in two sided uniqueness and namespace uniqueness
    if orig_qp.update == self.unique:
      # we are the uniqueness clause that can be modified
      self.change_state('update_check')
    elif orig_qp.update == 'keyreplace' and self.unique == 'key':
      # the ugly side of bug 6949
      self.change_state('replace_check')
    else:
      # we are a 'strict failure' uniqueness clause
      self.change_state('unique_check')

    if not isinstance(orig_qp.typeguid, str):
      raise MQLInternalParseError(
          self.query, "Can't use uniqueness with an underspecified typeguid")

    self.typeguid = orig_qp.typeguid
    self.result += ['scope', 'typeguid', 'value', 'datatype']
    self.comparator = orig_qp.comparator

    if self.unique == 'right':
      # these may both be null...
      self.value = orig_qp.value
      # this should be null or 'key'
      self.datatype = orig_qp.datatype

      self.right = None
      self.result.append('right')
    elif self.unique == 'left':
      # reverse uniqueness is always value=null
      if orig_qp.value is not None or orig_qp.datatype is not None:
        raise MQLInternalError(
            self.query,
            "Can't use reverse uniqueness with a value",
            value=orig_qp.value,
            datatype=orig_qp.datatype)

      self.value = None
      self.datatype = None

      self.left = None
      self.result.append('left')

    elif self.unique == 'value':
      # self.value is None so we find any value
      self.value = None
      self.datatype = None

      # may be null...
      # but must be a literal guid, not a subquery.
      # I'm not going to figure out what you were trying to say...
      if orig_qp.right is not Missing and not orig_qp.right.guid:
        raise MQLInternalParseError(
            self.query,
            "Can't write /type/text or /type/key  with an underspecified language/namespace"
        )

      elif orig_qp.right is not Missing:
        self.right = orig_qp.right.guid
      else:
        # XXX how do we feel about text objects and ints together? Not good I think...
        # this technically allows them by only checking uniqueness for right=null.
        self.right = Missing

    elif self.unique == 'key':
      # self.value is None so we find any value
      self.value = None
      self.datatype = None

      # these are undermined right now; we will do them in the second pass.
      self.left = Missing
      self.right = Missing

  def link_order_query_primitive(self, link):
    if self.prefix != '?' or not self.typeguid:
      raise MQLInternalError(
          self.query,
          'Called link_ordered_query_primitive on invalid primitive')

    self.parent = 'left'
    self.left = link

    link.ordered = self

  def add_unique_namespace_check(self, node):
    if self.prefix != '~':
      raise MQLInternalError(
          self.query,
          'Called add_unique_namespace_check on an invalid primitive')

    self.parent = 'left'
    self.left = node

    node.unique_namespace_info = self

  def add_index_query(self, parent, new_order):
    if self.prefix != '&':
      raise MQLInternalError(self.query,
                             'Called add_index_query on invalid primitive')

    # XXX note that we use None, not Missing for the other side of the primitive.
    # we don't know if there should be a right or not at this level
    if self.reverse:
      self.right = parent
      self.parent = 'right'
      self.left = None
    else:
      self.left = parent
      self.parent = 'left'
      self.right = None

    self.new_order = new_order
    parent.order_info.append(self)

  # linkage functions:
  # 1. parent (thing I am contained in (may be 'left', 'right' or a real QP)
  # 2. child (things I point to with left/right)
  # 3. pointers (things I point to with scope/typeguid)
  def add_parent(self, parent):
    if self.reverse:
      self.right = parent
      self.parent = 'right'
    else:
      self.left = parent
      self.parent = 'left'

    parent.contents.append(self)

  def add_child(self, child):
    if self.reverse:
      self.left = child
      self.child = 'left'
    else:
      self.right = child
      self.child = 'right'

    child.container = self

  def add_pointer(self, name, dest):
    if name not in self.pointers:
      raise MQLInternalError(
          self.query,
          'Only call add_pointer with a pointer, not %(key)s',
          key=name)

    setattr(self, name, dest)
    self.children.append(name)
    dest.container = self

  def generate_write_query(self):
    if self.mode is not WriteMode:
      raise MQLInternalError(
          self.query,
          "Can't write to the graph in mode %(mode)s",
          mode=str(self.mode))

    if self.state in ('create', 'remove'):
      # second to last line of defense.
      if not self.access_control_ok:
        raise MQLInternalError(self.query,
                               'Access control consistency check failure')

      graphq = '('
      if self.parent in self.connectors:
        parent = getattr(self, self.parent)
        if parent.state == 'found':
          graphq += self.parent + '=' + parent.guid[1:] + ' '
        elif parent.state == self.state:
          # we can hook together removals but not modifications.
          graphq += '<-' + self.parent + ' '
        else:
          raise MQLInternalError(
              self.query,
              'Invalid state %(state)s during write',
              state=self.state)

      for field in self.graphfields:
        graphq += self.generate_field(field, WriteMode)

      if self.state == 'remove':
        graphq += 'live=false '

      # and a unique=() clause if necessary
      graphq += self.generate_write_unique_clause()

      # XXX this code is clearly wrong (doesn't reference item in the loop)
      # not sure what the right code is at the moment.
      # attempted fix may or may not be correct
      for item in [x for x in [self.child] + self.children if x]:
        child = getattr(self, item)
        if child.state == 'found':
          graphq += self.child + '=' + child.guid[1:] + ' '
        elif child.state == self.state:
          graphq += self.child + '->' + child.generate_write_query()
        else:
          raise MQLInternalError(
              self.query,
              'Invalid state %(state)s during write',
              state=self.state)

      # XXX this seems wrong too...
      for item in self.contents:
        graphq += item.generate_write_query()

      if self.ordered:
        if self.ordered.state in 'found':
          pass
        elif self.ordered.state == 'create':
          graphq += self.ordered.generate_write_query()
        else:
          raise MQLInternalError(
              self.query,
              'Invalid state %(state)s during ordered write',
              state=self.state,
              ordered_state=self.ordered.state)

      # the trailing space is important to separate adjacent contents queries
      graphq += ') '

      return graphq

    else:
      raise MQLInternalError(
          self.query,
          'Node with no guid, in state %(state)s not written!',
          state=self.state)

  def generate_write_unique_clause(self):
    # Right now, don't try to enforce "create": "unless_exists" at the graph level.
    # Only enforce namespace and link uniqueness (which are the ugly cases if they fail)
    if self.state == 'remove':
      # removing things is always OK; nothing to validate in the graph
      return ''

    if self.previous is not None:
      # XXX the graph doesn't support the idea to check against "what will be there";
      # we just have to hope that what we prev out of existence is enough to make this work
      return ''

    if self.state != 'create':
      # what are we doing here?
      raise MQLInternalError(
          self.query,
          'Invalid state %(state)s cannot be written',
          state=self.state)

    if not self.left or self.left is Missing:
      # nothing to do for nodes
      return ''

    if self.left.state == 'create':
      # if this is a link where we are creating the left, there is no
      # useful enforcement (because we do not have left uniqueness at the moment)
      return ''

    if self.typeguid is None or self.typeguid is Missing:
      # everything must have a typeguid
      raise MQLInternalError(
          self.query,
          'Link with no typeguid in %(state)s cannot be written',
          state=self.state)

    if self.right is Missing and self.value is None:
      # all links (which we have here) must have either a right or a value
      raise MQLInternalError(
          self.query,
          'Link with no right or value in %(state)s cannot be written',
          state=self.state)

    # this is the basis of uniqueness checking (the most strict possible)
    clause = ['left', 'typeguid']

    # things that are not value unique can vary by value
    if self.value is not None:
      if self.unique != 'value':
        clause += ['value', 'datatype']
    elif self.unique == 'value':
      raise MQLInternalError(
          self.query,
          'Link with no value insists on value uniqueness',
          state=self.state)

    if self.right is not Missing:
      # things that are not right unique can vary by right
      if self.unique != 'right':
        if self.unique == 'key':
          # Keys should not be right-unique, otherwise we can end up with
          # duplicate names in the namespace. They should be unique in their
          # left, typeguid, datatype and values, otherwise we might end
          # up with more than one key with the same name pointing from a namespace.
          pass
        elif self.right.state == 'create':
          # this is a new thing; so it must be OK since we don't have left uniqueness
          return ''
        else:
          clause.append('right')

    elif self.unique == 'right':
      raise MQLInternalError(
          self.query,
          'Link with no right insists on right uniqueness',
          state=self.state)

    # give us the unique clause
    return 'unique=(%s) ' % (' '.join(clause))

  # this is recursive on the (now linked) set of QPs.
  def generate_graph_query(self, mode):

    if mode is PrepareMode and self.state == 'insert':
      raise MQLInternalError(
          self.query, "Shouldn't generate a prepare query on an 'insert'")

    graphq = '('
    if self.parent in self.connectors:
      graphq += '<-' + self.parent + ' '

    for field in self.graphfields:
      graphq += self.generate_field(field, mode)

    for var, val in self.vars.iteritems():
      graphq += var + '=' + val + ' '

    for op, val in chain(self.valueops.iteritems(),
                         self.timestampops.iteritems(),
                         self.history_ops.iteritems()):
      graphq += self.generate_comparison_op(op, val, mode)

    if mode is PrepareMode:
      graphq += self.generate_prepare_instructions()

    if self.child:
      if not getattr(self, self.child).is_unconditional_insert(mode):
        graphq += self.child + '->' + getattr(
            self, self.child).generate_graph_query(mode)

    # we do children separately from graphfields because order is critical.
    for item in self.children:
      if not getattr(self, item).is_unconditional_insert(mode):
        graphq += item + '->' + getattr(self, item).generate_graph_query(mode)

    for item in self.contents:
      if not item.is_unconditional_insert(mode):
        graphq += item.generate_graph_query(mode)

    if self.ordered:
      graphq += self.ordered.generate_graph_query(mode)

    if mode is PrepareMode:
      for item in self.unique_checks:
        graphq += item.generate_graph_query(mode)

      for order in self.order_info:
        graphq += order.generate_graph_query(mode)

      if self.unique_namespace_info:
        graphq += self.unique_namespace_info.generate_graph_query(mode)

    # the trailing space is important to separate adjacent contents queries
    graphq += ') '
    return graphq

  # XXX I hate the number of tests for unconditional inserts.
  # we need better code to do them all at the same time...
  # unconditional inserts make you a premature leaf
  def is_leaf(self, mode):
    if mode is ReadMode:
      return not self.child and not self.children and not self.contents and not self.ordered
    elif mode is PrepareMode:
      if self.child and not getattr(self,
                                    self.child).is_unconditional_insert(mode):
        return False
      for item in self.children:
        if not getattr(self, item).is_unconditional_insert(mode):
          return False

      for item in self.contents:
        if not item.is_unconditional_insert(mode):
          return False

      if len(self.unique_checks) > 0 or self.ordered or len(
          self.order_info) > 0:
        return False

      # all of the child, children and contents are unconditional inserts
      return True

  def guid_leaf(self, mode):
    return isinstance(self.guid, str) and self.is_leaf(mode)

  def is_unconditional_insert(self, mode):
    if mode is PrepareMode:
      if self.insert and not self.state == 'insert':
        raise MQLInternalError(self.query, 'Not inserting despite self.insert')

      return (self.state == 'insert')
    else:
      return False

  def generate_field(self, field, mode):
    """
        generates data going from lojson -> graph

        we assume (perhaps incorrectly) that transform_fields has
        validated the field values
        """
    # fields must generate their own trailing space.
    val = getattr(self, field, None)

    # shorthand for queries -- not generated.
    if val is None:
      # we will generate a pagesize by default
      if field == 'pagesize':
        if mode is PrepareMode:
          return 'pagesize=2 '
        elif mode is ReadMode:
          return 'pagesize=100 '
        else:
          return ''
      elif field == 'scope' and mode is WriteMode:
        # this is the final defense of the AccessControl system - if we don't
        # have a scope here, we can't do the write.
        raise MQLInternalError(self.query, 'write without scope')
      else:
        return ''

    elif field in self.pointers | self.guid_field | self.connectors:
      # don't try to generate our parent (infinite recursion)
      if field == self.parent:
        return ''
      elif isinstance(val, basestring):
        return field + '=' + val[1:] + ' '
      elif val is False:
        # doesn't matter where this is, it will fail the clause anyway
        return 'false '
      elif val is Missing:
        # we insist that things that don't look like they have a left or right really don't.
        return field + '=null '
      elif field != 'guid' and isinstance(val, QueryPrimitive):
        # we've already done this in the body of generate_graph_query()
        return ''
      elif mode is ReadMode and isinstance(val, list) and len(val) > 0:
        filtered_val = [guid[1:] for guid in val if guid != False]
        if filtered_val:
          return field + '=(' + ' '.join(filtered_val) + ') '
        else:
          # a list with no items cannot match
          return 'false '
      else:
        raise MQLInternalError(
            self.query,
            'Pointer field not guid, subclause or guid list',
            key=field,
            value=val)

    elif field == 'previous':
      if isinstance(val, basestring):
        return 'previous=' + val[1:] + ' '
      else:
        raise MQLInternalError(
            self.query, 'previous field not guid', key=field, value=val)

    elif field == 'value':
      rv = 'value='
      # booleans are also instances of integer, so bool has to go first.
      if isinstance(val, bool):
        rv += quote(_boolean_name[val])
      elif isinstance(val, (int, long, float)):
        rv += quote(str(val))
      elif isinstance(val, str):
        rv += quote(val)
      else:
        raise MQLInternalError(
            self.query,
            'Value field not integer, float, boolean or string',
            value=str(val))

      rv += ' '
      return rv

    elif field in ('datatype', 'timestamp'):
      return '%s=%s ' % (field, val)

    elif field == 'cursor':
      if val == True:
        return 'cursor=null '
      elif val == False:
        return "cursor=\"null:\" "
      else:
        return 'cursor=%s ' % (quote(val))

    elif field == 'comparator':
      return 'comparator=%s ' % (quote(val))

    # the directives are only available for reads
    if mode is ReadMode or (mode is PrepareMode and self.prefix in '&~'):
      if field == 'optional':
        if val == True:
          return '%s ' % field
        elif val == False:
          return ''
      elif field == 'pagesize':
        return '%s=%s ' % (field, val)
      elif field == 'sort':
        if isinstance(val, list) and len(val) > 0:
          return 'sort=(' + ' '.join(val) + ') '
        elif isinstance(val, basestring):
          return 'sort=' + val + ' '
      elif field == 'live':
        if isinstance(val, bool):
          return 'live=' + _boolean_name[val] + ' '
        elif val == 'dontcare':
          return 'live=dontcare '
      elif field in ('newest', 'oldest'):
        if isinstance(val, (int, long, float)):
          return field + '=' + str(val) + ' '
      elif field in ('previous', 'next'):
        if isinstance(val, basestring):
          return field + '=' + val[1:] + ' '
        else:
          return ''

    if mode in (ReadMode, PrepareMode):
      if field == 'result':
        rv = 'result=('

        # cursor results go in a strange place...
        if self.cursor is not None:
          rv += 'cursor '

        rv += '(' + ' '.join(val)
        if not self.is_leaf(mode):
          rv += ' contents'
        rv += ')) '
        return rv
    if mode is WriteMode:
      # no result= clauses in write queries
      if field == 'result':
        return ''

    # for example, optional=True during a write
    raise MQLInternalError(
        self.query,
        'illegal key %(key)s in mode %(mode)s',
        key=field,
        value=val,
        mode=str(mode))

  def generate_prepare_instructions(self):
    # XXX should do this on the basis of state, not flags.

    # we never need to generate optional on a node; only links can be optional.
    # at the root, the implicit optionality of the query is enough
    # (we don't distinguish between EmptyResult and () )

    if self.prefix == ':':
      # ensurechild(ren) are not optional - they must exist as a unit (or fail as a unit)
      # XXX are updates optional?

      # inserts are not optional - they are always written and never read!
      if self.ensure or self.link or self.unlink:
        return 'optional '
    elif self.prefix in '&@~':
      # & and ~ actually alway get optional, but get it from lojson.py...
      pass
    elif self.prefix == '=':
      # uniqueness checks are always optional
      return 'optional '
    elif self.prefix == '?':
      # ? attached to : is optional, ? attached to & is not...
      if self.left.prefix == ':':
        return 'optional '
    else:
      raise MQLInternalError(
          self.query, 'Unrecognized prefix %(prefix)s', prefix=self.prefix)

    return ''

  def check_comparison_op(self, k, v):
    if not valid_value_op(k):
      raise MQLInternalParseError(
          self.query, 'Invalid comparison operator', key=k, value=v)

    return v

  def check_timestamp_op(self, k, v):
    if not valid_timestamp_op(k):
      raise MQLInternalParseError(
          self.query, 'Invalid timestamp operator', key=k, value=v)

    if not valid_timestamp(v):
      raise MQLInternalParseError(
          self.query, 'Invalid timestamp format', key=k, value=v)

    return v

  def check_history_op(self, k, v):
    if not valid_history_op(k):
      raise MQLInternalParseError(
          self.query, 'Invalid history operator', key=k, value=v)

    if not isinstance(v, (int, long)) or v < 0:
      raise MQLInternalParseError(
          self.query, 'Invalid history operator', key=k, value=v)

    return v

  def generate_comparison_op(self, op, val, mode):
    rv = op
    if isinstance(val, bool):
      rv += quote(_boolean_name[val])
    elif isinstance(val, (int, long, float)):
      rv += quote(str(val))
    elif isinstance(val, str):
      rv += quote(val)
    else:
      raise MQLInternalError(
          self.query,
          'comparison not integer, float, boolean or string',
          key=op,
          value=str(val))

    rv += ' '
    return rv

  def transform_field(self, k, v):
    """
        does validation and id lookup (very low level for this, but
        that seems necessary) for data going from lojson -> graph
        XXX fixme!!!
        """
    if k not in self.input_fields:
      raise MQLInternalParseError(
          self.query, 'Invalid field %(key)s during constrain', key=k)

    # check we're not doing a write in a read or a read in a write
    if k in self.writeinsns and self.mode not in (WriteMode, CheckMode):
      raise MQLInternalParseError(
          self.query,
          'Invalid use of write directive in read',
          key=k,
          value=v,
          mode=str(self.mode))

    if k in self.directives and self.mode is not ReadMode:
      # one ugly (non-user visible exception -- we use "sort", "optional" and "pagesize" in '&' primitives
      if self.prefix not in '&~':
        raise MQLInternalParseError(
            self.query,
            'Invalid use of read directive %(key)s in write',
            key=k,
            value=v,
            mode=str(self.mode))

    if k in self.pointers:
      # should we allow a list? I think no right now.
      if isinstance(v, dict) and len(v) != 0:
        # descent
        # XXXcallback
        return Missing

      # deliberate fall through

    if k in self.guid_field | self.pointers | set(
        ('next', 'previous', 'default')):
      if v is None or v is False:
        return v
      elif isinstance(v, basestring) and valid_guid(v):
        return v
      elif (k in self.guid_field | self.pointers) and isinstance(
          v, list) and len(v) > 0:
        for val in v:
          if not ((isinstance(val, str) and valid_guid(val)) or val == False):
            raise MQLInternalParseError(
                self.query,
                "not a valid guid '%(value)s' in guid list",
                key=k,
                value=val)
        return v

      raise MQLInternalParseError(
          self.query,
          "value '%(value)s' not valid in guid slot '%(key)s",
          key=k,
          value=v)

    elif k == 'value':
      if v is None:
        return v
      elif type(v) in self.make_datatypes:
        return v

      raise MQLInternalParseError(
          self.query, 'value not valid in value slot', key=k, value=v)

    elif k in ('reverse', 'optional'):
      if isinstance(v, bool):
        return v

      raise MQLInternalParseError(
          self.query, 'unhandled flag value', key=k, value=v)
    elif k in ('pagesize', 'newest', 'oldest'):
      if isinstance(v, (int, long)):
        return v
      raise MQLInternalParseError(
          self.query, '%(key)s requires an int', key=k, value=v)
    elif k == 'index':
      if v is None or isinstance(v, (int, long)):
        return v
      raise MQLInternalParseError(
          self.query, '%(key)s requires an int or null', key=k, value=v)
    elif k == 'timestamp':
      if v is None or (isinstance(v, str) and valid_timestamp(v)):
        return v
      raise MQLInternalParseError(
          self.query, '%(key)s requires a valid timestamp', key=k, value=v)
    elif k == 'datatype':
      if v is None or v in self.check_datatypes:
        return v
      if v == 'guid':
        raise MQLInternalError(
            self.query, "You cannot alter the property '/type/object/mid'")
      raise MQLInternalParseError(
          self.query, '%(key)s requires a valid datatype', key=k, value=v)
    elif k == 'sort' and isinstance(v, (str, list)):
      return v
    elif k == 'type' and isinstance(v, str):
      return v
    elif k == 'id' and (isinstance(v, str) or v is None or isinstance(v, list)):
      return v
    elif k == 'live' and (isinstance(v, bool) or v == 'dontcare'):
      return v
    elif k == 'unique' and v in ('value', 'right', 'left', 'both', 'key'):
      return v
    elif k == 'update' and v in ('left', 'right', 'value', 'key', 'keyreplace'):
      return v
    elif k == 'comparator' and v in self.check_comparators:
      return v
    elif k in self.writeinsns:
      if isinstance(v, bool):
        return v
      elif isinstance(v, (int, long)):
        return not (v == 0)

      raise MQLInternalParseError(
          self.query, 'unhandled write instruction value', key=k, value=v)

    raise MQLInternalParseError(
        self.query, 'unhandled kv pair: %(key)s:%(value)s', key=k, value=v)

  def transform_result(self, field, result):
    # unquote values, add # signs to guids
    if field in self.result_pointers:
      if result == 'null':
        return None

      return '#' + result
    elif field == 'value':
      if result == 'null':
        return None
      return unquote(result)
    elif field in ('datatype', 'timestamp'):
      return result
    elif field == 'live':
      return _make_boolean[result]
    else:
      raise MQLInternalError(
          self.query,
          "Don't know how to transform %(key)s=%(result)s",
          key=field,
          result=result)

  def primitive_result(self, datatype, result):
    try:
      if result is None:
        return result
      elif datatype == 'boolean':
        return _make_boolean[result]
      elif datatype == 'integer':
        return int(result)
      elif datatype == 'float':
        return float(result)
      else:
        return result
    except ValueError:
      # how bad should this be???
      # return result #???
      raise MQLInternalError(
          self.query,
          'Found %(result)s in a slot for %(datatype)s',
          result=result,
          datatype=datatype)

  def change_state(self, newstate):
    if newstate not in self.allowed_state_transitions[self.state]:
      raise MQLInternalError(
          self.query,
          'Invalid state transition in write code %(state)s -> %(newstate)s',
          state=self.state,
          newstate=newstate)

    self.state = newstate

  def mark_missing(self, reader):
    # mark this node and its children and run their prepares

    if self.state in ('link', 'ensure', 'ensurechild', 'insert'):
      # this will bomb if the node is non-creatable.
      self.change_state('create')
    elif self.state == 'unlink':
      self.change_state('notpresent')
    elif self.state == 'delete':
      # we must find all parts of a delete to do the delete.
      raise MQLResultError(self.query, "Can't locate this item to delete it")
    else:
      # self.state == 'match' or something else that's weird...
      # we better be ready to create something before we decide that it is missing...
      if self.prefix == ':':
        raise MQLResultError(self.query,
                             "Can't get to this clause from its parent")
      elif self.prefix == '@':
        raise MQLResultError(self.query,
                             "Can't locate this clause - cannot continue")

      # not expecting this to happen, but can't afford to run off the end of this clause; it must raise.
      raise MQLInternalError(
          self.query,
          'State transition failure in missing node',
          state=self.state)

    if self.child:
      # this is a node attached to the link. We may want to run a query against it (or not)
      getattr(self, self.child).run_prepare(reader)

    for mychild in self.children:
      # these are my direct attachments. They may be there, ready to attach to me
      getattr(self, mychild).run_prepare(reader)

    for mycontent in self.contents:
      # these are my contents. If I'm not there, neither are they as they must point to me.
      mycontent.mark_missing(reader)

    if self.ordered:
      self.ordered.change_state('order_missing')

    for myuniquecheck in self.unique_checks:
      # if I don't exist, all my uniqueness checks automatically pass.
      myuniquecheck.change_state('checked')

    for order in self.order_info:
      # we will create this node, so it's OK to create the order records from scratch.
      if order.state != 'order_info':
        raise MQLInternalError(
            self.query, 'Not expecting a %(state) here', state=order.state)

      order.existing_order = []
      order.generate_new_order()

  def run_prepare(self, reader):
    if self.state in ('match', 'ensure', 'delete',
                      'default') and self.prefix == '@':
      if self.state == 'default':
        self.guid = self.default

      graphq = self.generate_graph_query(PrepareMode)
      # however this works...
      gresult = reader(graphq)

      if len(gresult) == 0:
        # we didn't find anything - move this primitive into the "will-create" state.
        # now move the children into the 'missing' state and run their nodes prepares....
        self.mark_missing(reader)
      elif len(gresult) == 1:
        # we at least found this node - attach_prepare_results will move it to the "found" state
        self.attach_prepare_results(reader, gresult[0])
      else:
        guids = [('#' + x[0]) for x in gresult]
        raise MQLResultError(
            self.query,
            'Need a unique result to attach here, not %(count)d',
            count=len(gresult),
            guids=guids)

    elif self.state in ('insert', 'ensurechild'):
      self.mark_missing(reader)

    else:
      raise MQLInternalError(
          self.query,
          "Don't know how to prepare with state %(state)s here",
          state=self.state)

  def attach_prepare_results(self, reader, result):
    """
        This directly attaches the results of prepare statements to the QPs.
        Originally I created a full result structure, but there are differences
        between reads and writes as to the handling of multiple results so it
        was
        easier to hook the QPs directly as here.
        """

    if self.state in ('link', 'ensure', 'ensurechild', 'match', 'default'):
      self.change_state('found')
    elif self.state in ('delete', 'unlink'):
      self.change_state('remove')
    elif self.state in ('order_read'):
      self.change_state('order_found')
    else:
      raise MQLInternalError(
          self.query, 'Not expecting a %(state) here', state=self.state)

    n = len(self.result)

    for i in xrange(n):
      # first make sure that we match what we expected to query here
      res = self.transform_result(self.result[i], result[i])

      # should we be using the datatype we got from the result instead?
      # we will throw an exception later (earlier?) in this code
      # if they do not match, so there should be no need.

      if self.result[i] == 'value':
        # do the conversion with the returned datatype. We know that we always ask for datatype when we ask for value
        # so this is OK.
        datatype = dict(zip(self.result, result))['datatype']
        res = self.primitive_result(self.datatype, res)

      if getattr(self, self.result[i], None) is not None:

        # MQL-455 when dealing with floats rounding errors can cause
        # the comparison on line 1249 to fail. So we basically check to
        # see if the difference berween the persisted and provided
        # values is less than 10-thousandth of the lower of the two. If
        # not we fall through and fail, if so, we ressign the res to be
        # the persisted value.
        if self.result[i] == 'value' and datatype == 'float':
          val = getattr(self, self.result[i], None)
          if abs(val - res) < 0.00001:
            res = val

        if getattr(self, self.result[i], None) != res:
          # we need to do a case-insensitive comparison on values...
          # XXX datatype == string also covers keys! we need to fix this...
          if self.result[i] == 'value' and datatype == 'string' and res.lower(
          ) == self.value.lower():
            # we are equal on a case insensitive basis...
            LOG.warning('case.insensitive.prepare',
                        [self.value, res] + result[0:n - 1])
            # we believe we're OK with this...
            # change the value we have to match what is really there...
            self.value = res
          else:
            raise MQLInternalError(
                self.query,
                "Values didn't match in prepare",
                key=self.result[i],
                value=getattr(self, self.result[i], None),
                newvalue=res)
      else:
        # we asked, so we set the value
        setattr(self, self.result[i], res)

    # now the direct child if any (fill in the '@' slots. Nodes never have a direct child.)
    if self.child:
      child = getattr(self, self.child)
      if child.state not in ('ensure', 'ensurechild', 'match', 'delete',
                             'default'):
        raise MQLInternalError(
            self.query, 'Not expecting a %(state)s here', state=child.state)

      if len(result[n]) == 1:
        child.attach_prepare_results(reader, result[n][0])
      else:
        guids = [('#' + x[0]) for x in result[n]]
        raise MQLInternalError(
            self.query,
            'Need a unique result to attach here, not %(count)d',
            key=self.child,
            count=len(result[n]),
            guids=guids)
      n += 1

    # the pointers, if any
    # note that this overrides the direct result set above. This is intentional.
    for mychild in self.children:
      if getattr(self, mychild).state not in ('match', 'ensure', 'delete',
                                              'ensurechild'):
        raise MQLInternalError(
            self.query,
            'Not expecting a %(state)s here',
            state=getattr(self, mychild).state)

      if len(result[n]) == 1:
        getattr(self, mychild).attach_prepare_results(reader, result[n][0])
      elif len(result[n]) == 0:
        # the child does not exist - it will need to be written
        getattr(self, mychild).mark_missing(reader)
      else:
        guids = [('#' + x[0]) for x in result[n]]
        raise MQLInternalError(
            self.query,
            'Direct children may have at most one link',
            guids=guids,
            count=len(result[n]))

      n += 1

    # and the contents - all links to other prims - links never have contents, only pointers and children
    for mycontent in self.contents:
      if mycontent.state == 'insert':
        # inserts are automatically missing - we MUST NOT increment n in this case as we didn't
        # ask for results.
        mycontent.mark_missing(reader)
      elif mycontent.state in ('match', 'ensure', 'delete', 'link', 'unlink',
                               'ensurechild'):
        if len(result[n]) == 1:
          mycontent.attach_prepare_results(reader, result[n][0])
        elif len(result[n]) == 0:
          # this link has not been found - it will need to be written
          mycontent.mark_missing(reader)
        else:
          guids = [('#' + x[0]) for x in result[n]]
          raise MQLTooManyValuesForUniqueQuery(
              self.query, results=guids, count=len(result[n]))
        n += 1
      else:
        raise MQLInternalError(
            self.query, 'Not expecting a %(state)s here', state=mycontent.state)

    if self.ordered:
      if self.ordered.state != 'order_read':
        raise MQLInternalError(
            self.query,
            'Not expecting a %(state)s here',
            state=self.ordered.state)

      if len(result[n]) == 1:
        self.ordered.attach_prepare_results(reader, result[n][0])
      elif len(result[n]) == 0:
        self.ordered.change_state('order_missing')
      else:
        guids = [('#' + x[0]) for x in result[n]]
        raise MQLInternalError(
            self.query,
            'Only one ordering record per link. Found %(count)d',
            guids=guids,
            count=len(result[n]))

      n += 1

    # and the uniqueness checks - these are a special case of the contents...
    for myuniquecheck in self.unique_checks:
      if myuniquecheck.state not in ('unique_check', 'update_check'):
        raise MQLInternalError(
            self.query,
            'Not expecting a %(state)s here',
            state=myuniquecheck.state)

      if len(result[n]) == 1:
        myuniquecheck.attach_unique_result(reader, result[n][0])
      elif len(result[n]) == 0:
        myuniquecheck.mark_unique_missing(reader)
      else:
        # for inserts and updates this bombs, for deletes it may just pass
        myuniquecheck.handle_multiple_results(reader, result[n])

      n += 1

    for order in self.order_info:
      if order.state != 'order_info':
        raise MQLInternalError(
            self.query, 'Not expecting a %(state)s here', state=order.state)

      # result[n] may have length 0
      order.add_order_results(result[n])
      order.generate_new_order()

      n += 1

    if self.unique_namespace_info:
      if len(result[n]) == 1:
        self.unique_namespace_info.check_unique_namespace(reader, result[n][0])
      elif len(result[n]) == 0:
        self.unique_namespace_info.check_unique_namespace(reader, None)
      else:
        raise MQLResultError(
            self.query,
            'Namespace has ambiguous uniqueness state',
            result=result[n])

      n += 1

    # check we processed everything...
    if n != len(result):
      raise MQLInternalError(
          self.query,
          'Got %(count)d results, expecting %(expected_count)d',
          count=len(result),
          expected_count=n)

  def add_order_results(self, results):
    self.existing_order = []

    for existing in results:
      # find out what the existing order looks like...
      n = len(self.result)
      guid = None
      order = None

      # ['guid', 'typeguid', 'contents']
      for i in xrange(n):
        if self.result[i] == 'guid':
          guid = self.transform_result(self.result[i], existing[i])

      # there's exactly one more thing -- the order info itself.
      # ['guid','typeguid','datatype','value']
      if len(existing[n]) != 1:
        raise MQLResultError(
            self.query,
            'More than one piece of order information at %(guid)s',
            guid=guid)

      m = len(self.ordered.result)
      for i in xrange(m):
        if self.ordered.result[i] == 'value':
          res = self.transform_result(self.ordered.result[i], existing[n][0][i])
          order = self.primitive_result(self.ordered.datatype, res)

      if guid is None or order is None:
        raise MQLInternalError(
            self.query, 'Found order information without guid and order value')

      # check we processed everything...
      if n + 1 != len(existing):
        raise MQLInternalError(
            self.query,
            'Got %(count)d results, expecting %(expected_count)d',
            count=len(self.result),
            expected_count=n)

      # add information about what we found. Note that the length of this array is also important.
      self.existing_order.append((order, guid))

  def generate_new_order(self):

    VERY_LARGE_NUMBER = 1000000.0
    VERY_SMALL_NUMBER = -VERY_LARGE_NUMBER
    # using self.new_order, and self.existing_order figure out what the new orders are for each primitive...
    # note that the formats are radically different. Oh well..

    # XXX This algorithm is sub-optimal in complicated ways - ask tsturge for more details.

    # what have we found?
    current_order = []
    seen_guids = set()
    for item in self.new_order:
      if item.state == 'found' and item.ordered.state == 'order_found':
        current_order.append(item)
        seen_guids.add(item.guid)

    # what is the highest order we cover?
    first_missing_order = VERY_LARGE_NUMBER
    for pair in self.existing_order:
      if pair[1] not in seen_guids:
        first_missing_order = min(pair[0], first_missing_order)

    best_preserved_order = []
    # what could we possibly preserve?
    for item in current_order:
      if item.ordered.value < first_missing_order:
        best_preserved_order.append([item.ordered.value, None, item])

    # find the best match between current_order and new_order
    best_preserved_guids = set(
        [x[2].guid for x in incr_subseq(best_preserved_order)])

    # so we need to change everything we are NOT preserving...
    # we need to fill in the list like [ (A,None), (B,-2), (C,None), (D,None), (E,3.1), (F,3.4), (G,None) ]
    # with good intermediate values for A,C,D and G (-3,-1,0,4.4)
    i = 0
    prev_order = VERY_SMALL_NUMBER
    next_order = None
    k = 0
    while i < len(self.new_order):
      item = self.new_order[i]
      if item.state == 'found' and item.ordered.state == 'order_found' and item.guid in best_preserved_guids:
        prev_order = item.ordered.value
        next_order = None
        k = i
        i += 1
        item.ordered.change_state('found')
        continue

      if next_order is None:
        j = i + 1
        while j < len(self.new_order):
          next_item = self.new_order[j]
          if next_item.state == 'found' and next_item.ordered.state == 'order_found' and next_item.guid in best_preserved_guids:
            next_order = next_item.ordered.value
            break
          j += 1
        if next_order is None:
          # we'll never go past the first missing order.
          next_order = first_missing_order

      # so what order will we give this item?
      if prev_order == VERY_SMALL_NUMBER and next_order == VERY_LARGE_NUMBER:
        assigned_order = i + 0.0
      elif prev_order == VERY_SMALL_NUMBER:
        assigned_order = next_order - (j - i)
      elif next_order == VERY_LARGE_NUMBER:
        assigned_order = prev_order + (i - k)
      else:
        assigned_order = prev_order + (next_order -
                                       prev_order) * (i - k + 0.0) / (
                                           j - k + 0.0)

      # and give the item the new order (possibly modifying the existing order)
      item.ordered.assign_order(assigned_order)

      i += 1

    self.change_state('checked')

  def assign_order(self, assigned_order):
    if self.state == 'order_found':
      self.previous = self.guid
      self.guid = None
    elif self.state == 'order_missing':
      pass
    else:
      raise MQLInternalError(
          self.query,
          "Can't assign an order to a primitive in state %(state)s",
          state=self.state)

    self.change_state('create')
    self.value = assigned_order

  def handle_multiple_results(self, reader, results):
    guids = [('#' + x[0]) for x in results]
    LOG.error(
        'multiple.unique.results',
        'got multiple results for %(state)s unique check',
        guids=guids,
        state=self.orig.state)
    if self.orig.state in ('remove', 'notpresent'):
      # we're trying to remove a duplicate.
      self.change_state('duplicate')
    else:
      # this is an outright failure.
      raise MQLResultError(
          self.query,
          'Unique check may have at most one result. Got %(count)d',
          guids=guids,
          count=len(results))

  def mark_unique_missing(self, reader):
    if self.orig.state in ('create', 'notpresent'):
      # nothing to do
      pass
    else:
      raise MQLInternalError(
          self.query,
          'Nothing in unique checks but not creating anything',
          state=self.orig.state)

    self.change_state('checked')

  def check_unique_namespace(self, reader, result):
    # default is non-unique
    is_unique = False
    if result is not None:
      # yes, this is what it takes to get True out of a graph result. I need a better architecture...
      is_unique = self.primitive_result(
          dict(zip(self.result, result))['datatype'],
          self.transform_result('value',
                                dict(zip(self.result, result))['value']))

    if not isinstance(is_unique, bool):
      raise MQLInternalError(
          self.query,
          'Expected a boolean result from the unique namespace check, not %(value)s',
          value=is_unique,
          result=result)

    # ideally, we would dispatch the second level checks now; we even have the reader avaialable.
    # But they have to wait until we have processed the first stage so we know the RHS
    # finally, the second stage namespace prepares -- these kick off their own queries...
    if is_unique:
      for namespace_check in self.left.unique_namespace_checks:
        if namespace_check.state == 'replace_check':
          namespace_check.change_state('update_check')

        if namespace_check.state in ('unique_check', 'update_check'):
          namespace_check.run_namespace_prepare(reader)
        else:
          raise MQLInternalError(
              self.query,
              'Not expecting a %(state)s here',
              state=namespace_check.state)

      self.change_state('namespace_unique')
    else:
      # make sure we aren't trying to use update on a non-unique namespace
      for namespace_check in self.left.unique_namespace_checks:
        if namespace_check.state == 'update_check':
          raise MQLResultError(
              self.left.query,
              "Can't use 'connect': 'update' on a namespace that is not unique")
        elif namespace_check.state in ('replace_check', 'unique_check'):
          namespace_check.change_state('checked')
        else:
          raise MQLInternalError(
              self.query,
              'Not expecting a %(state)s here',
              state=namespace_check.state)

      self.change_state('namespace_regular')

  def attach_unique_result(self, reader, result):
    n = len(self.result)

    for i in xrange(n):
      # first make sure that we match what we expected to query here
      res = self.transform_result(self.result[i], result[i])

      if self.result[i] == 'value':
        # we have a big problem with the datatype here.
        # we didn't know what the datatype was when we queried.
        # so self.datatype is None.
        # We can't use the datatype from orig in case we are (quite deliberately and properly)
        # attempting to change the datatype. So we need to inspect the returned datatype in
        # advance...
        datatype = dict(zip(self.result, result))['datatype']
        res = self.primitive_result(datatype, res)

      if getattr(self, self.result[i], None) is not None:
        # XXX we explicitly do not do the case insensitive check here as this codepath is called
        # on keys (things that have unique values) rather than on texts (things that have unique rights)
        if getattr(self, self.result[i], None) != res:
          if self.result[i] == 'value' and datatype == 'string' and res.lower(
          ) == self.value.lower():
            # discussion with Tristan -- we should give a nice error in case insensitive matches:
            LOG.warning('case.insensitive.unique.error',
                        [self.value, res] + result[0:n - 1])
            # still an error in this case
            raise MQLResultError(
                self.query,
                'Value exists that differs only in case',
                key=self.result[i],
                newvalue=getattr(self, self.result[i], None),
                value=res)
          else:
            raise MQLInternalError(
                self.query,
                "Values didn't match in prepare",
                key=self.result[i],
                newvalue=getattr(self, self.result[i], None),
                value=res)

      elif getattr(self.orig, self.result[i], None) is not None:
        # XXX there is weirdness around self.orig.right == Missing here.
        # this is why we don't ask for result=right in that case.

        # we only ask for result=left if we have left uniqueness
        if self.result[i] == 'right':
          newvalue = self.orig.right.guid
        elif self.result[i] == 'left':
          newvalue = self.orig.left.guid
        else:
          newvalue = getattr(self.orig, self.result[i])

        if newvalue != res:
          # we allow for case-insensitive matches to be changed.
          if self.state == 'update_check':
            # we must have already set self.guid by this point as we do it first.
            if not self.orig.update or self.orig.previous is not None:
              raise MQLInternalError(self.query,
                                     'Trying to update a non-updateable node')

            self.orig.previous = self.guid
          elif self.result[i] == 'value' and datatype == 'string' and res.lower(
          ) == newvalue.lower():
            # we are equal on a case insensitive basis...
            LOG.warning('case.insensitive.unique',
                        [self.value, res] + result[0:n - 1])
            # we believe we're OK with this...
            pass
          elif self.state == 'unique_check':
            # if this is a delete, finding a different value is not an error. You might
            # ask why we bother to do the unique check in the first place; mostly because
            # it is hard to detect a delete at the time the check is inserted.

            # we depend on the deletion already having been processed
            if self.orig.state == 'notpresent':
              pass
            else:
              # bug 6712; we want to provide a good error message here...
              # updates always occur on the child or the value.
              if self.unique in ('key', 'value', self.child):
                raise MQLValueAlreadyInUseError(
                    self.query,
                    key=self.result[i],
                    existing_value=res,
                    new_value=newvalue,
                    update=True)

              # if we don't think an update will help, just tell
              # the user to try a delete.
              raise MQLValueAlreadyInUseError(
                  self.query,
                  key=self.result[i],
                  existing_value=res,
                  new_value=newvalue,
                  update=False)

          else:
            raise MQLInternalError(
                self.query,
                'Invalid state %(state)s in attach_unique_result()',
                state=self.state)

      else:
        # we asked, so we set the value
        setattr(self, self.result[i], res)

    # check we processed everything...
    if n != len(result):
      raise MQLInternalError(
          self.query,
          'Got %(count)d results, expecting %(expected_count)d',
          count=len(result),
          expected_count=n)

    self.change_state('checked')

  def circularity_ref(self, field):
    # this function returns the guid that is stored in field (left, right, tg, scope).
    # if this field points to another QP, that QP is inspected for either a guid
    # or the notation that it will be created.
    if isinstance(getattr(self, field), basestring):
      return getattr(self, field)
    elif getattr(self, field) is Missing:
      # all missing rights are the same.
      return Missing
    elif isinstance(getattr(self, field), QueryPrimitive) and getattr(
        self, field).guid:
      return getattr(self, field).guid
    elif isinstance(getattr(self, field), QueryPrimitive) and getattr(
        self, field).state == 'create':
      return getattr(self, field)
    else:
      raise MQLInternalError(
          self.query, 'Unable to determine circularity reference', field=field)

  def check_circularity(self, circ_dict=None):
    """
        This function catches violations of uniqueness due to circularity. That
        is, even if we've checked that
        there is not a name for an object, or there is not a key of a certain
        type, that's no reason you
        can't try to create two of them at once! For example:
          { "@id": "/media_type",
            "+has_key": [ { ":value": "test",
                          ":unique": "right",
                          ":link": True,
                          "@id": "/media_type/this"
                          },
                          { ":value": "test",
                          ":unique": "right",
                          ":link": True,
                          "@id": "/media_type/that"
                          } ]
          }

        is a valiant attempt to give both /media_type/this and /media_type/that
        the alternate id /media_type/test

        Much more complicated variants exist by creating complex queries that
        just happen to find the same object
        at different points. Not all are malicious, but the malicious ones can
        be hard to figure out. '
        """

    if circ_dict is None:
      circ_dict = {}

    # check myself against the existing conditions

    if self.state in ('create', 'remove') and self.typeguid is not None:
      typeguid = self.circularity_ref('typeguid')

      if typeguid not in circ_dict:
        circ_dict[typeguid] = []
      circ_list = circ_dict[typeguid]
      for potential_clash in circ_list:
        self.check_collision(potential_clash)

      circ_list.append(self)

    # now recurse; only into contents and child (because we can't write to children)
    if self.child is not None:
      child = getattr(self, self.child)
      child.check_circularity(circ_dict)

    for item in self.contents:
      item.check_circularity(circ_dict)

  def check_collision(self, other):
    """
        Part of the circularity detection; makes sure we are not too much alike
        the other thing.

        Must raise MQLResultError in case of an error.
        """

    # this is written backwards to stop the if nesting getting too deep...
    if not (self.left and other.left):
      return False

    # must have the same typeguid to be an issue
    typeguid = self.circularity_ref('typeguid')
    if not (typeguid and typeguid == other.circularity_ref('typeguid')):
      return False

    # if you have a typeguid, you better also have a left...
    leftguid = self.circularity_ref('left')
    if not leftguid:
      raise MQLInternalError(
          self.query, 'Node with typeguid and no left!', typeguid=typeguid)

    if (leftguid != other.circularity_ref('left')):
      # check reverse uniqueness
      if self.unique in ('left', 'both') or other.unique in ('left', 'both'):
        if self.circularity_ref('right') == other.circularity_ref('right'):
          self.circularity_error(other, 'right')

      # other cases with different lefts are OK
      return False

    # these have the same left and same typeguid so they may pose a problem...

    # it's enough for either primitive to insist on uniqueness
    if self.unique == 'value' or other.unique == 'value':
      if self.circularity_ref('right') == other.circularity_ref('right'):
        self.circularity_error(other, 'value')

    # only check namespace uniqueness when the namespace claims to need it.
    if ((self.unique == 'key' and
         self.left.unique_namespace_info.state == 'namespace_unique') or
        (other.unique == 'key' and
         other.left.unique_namespace_info.state == 'namespace_unique')):
      if self.circularity_ref('right') == other.circularity_ref('right'):
        self.circularity_error(other, 'key')

    # or on right uniqueness -- one may insist on each (yeech!)
    if self.unique in ('right', 'both',
                       'key') or other.unique in ('right', 'both', 'key'):
      if self.circularity_are_values_equal(other):
        self.circularity_error(other, 'left')

    # also catch true duplicates (same right and same value)
    if (self.circularity_ref('right') == other.circularity_ref('right')
       ) and self.circularity_are_values_equal(other):
      self.circularity_error(other, 'right')

    # we passed.
    return False

  def circularity_are_values_equal(self, other):
    if isinstance(self.value, str) and isinstance(other.value, str):
      # string values are equal if
      # - they are identical
      # - they match case-insensitively and not both people assert octet comparison (if one does, they still match)
      if self.comparator == 'octet' and other.comparator == 'octet':
        return self.value == other.value
      else:
        return self.value.lower() == other.value.lower()
    else:
      # non-string values are equal if they are identical.
      return self.value == other.value

  def circularity_error(self, other, field):
    # hard to raise good exceptions here, so this attempts to explain better.
    kwds = {
        'query_1': self.query.get_orig(),
        'query_2': other.query.get_orig(),
        'property': self.typeguid,
        'key_1': self.query.key,
        'key_2': other.query.key,
        'value_1': self.value,
        'value_2': other.value,
    }
    messages = {
        'key':
            'Attempt to assign more than one key for an object in a unique '
            'namespace.',
        'value':
            'Attempt to assign the same key in a single namespace',
        'left':
            'Attempt to give a unique property more than one value',
        'right':
            'More than one attempt to write the same statement'
    }

    raise MQLResultError(self.query, messages[field], **kwds)

  def mark_everything_done(self):
    self.change_state('done')

    for item in [x for x in [self.child] + self.children if x]:
      child = getattr(self, item)
      child.mark_everything_done()

    for item in self.contents:
      item.mark_everything_done()

    if self.ordered:
      self.ordered.mark_everything_done()

    for item in self.unique_checks:
      item.mark_everything_done()

    for order in self.order_info:
      order.mark_everything_done()

    for item in self.unique_namespace_checks:
      item.mark_everything_done()

  def fake_check_result(self):
    """
        This runs in the same way that attach_write_results runs
        except that it always attaches the write or "False" for a
        guid that would have needed to be created.
        """
    self.guid = False

    if self.child:
      child = getattr(self, self.child)
      if child.state == self.state:
        child.fake_check_result()
      elif child.state == 'found':
        pass
      else:
        raise MQLInternalError(
            self.query,
            'Unexpected state %(state)s in fake_check_result',
            state=child.state)

    for pointer in self.children:
      child = getattr(self, self.pointer)
      if child.state == 'create':
        child.fake_check_result()
      elif child.state == 'found':
        pass
      else:
        raise MQLInternalError(
            self.query,
            'Unexpected state %(state)s in fake_check_result',
            state=child.state)

    for item in self.contents:
      item.fake_check_result()

    if self.ordered:
      if self.ordered.state == 'create':
        self.ordered.fake_check_result()
      elif self.ordered.state == 'found':
        pass
      else:
        raise MQLInternalError(
            self.query,
            'Unexpected state %(state)s in fake_check_result',
            state=self.ordered.state)

    self.change_state('written')

  def attach_write_results(self, result):
    """
        This runs to attach the results of a write (always result=(guid
        contents) )
        to the write directly. This is different from create_query_result
        in that the result is directly attached (in self.guid) rather than
        the result pointing back to the query in a many-to-one relationship.
        """

    self.guid = self.transform_result('guid', result[0])

    i = 1
    if self.child:
      child = getattr(self, self.child)
      if child.state == self.state:
        child.attach_write_results(result[i])
        i += 1
      elif child.state == 'found':
        pass
      else:
        raise MQLInternalError(
            self.query,
            'Unexpected state %(state)s in attach_write_results',
            state=child.state)

    for pointer in self.children:
      child = getattr(self, self.pointer)
      if child.state == 'create':
        child.attach_write_results(result[i])
        i += 1
      elif child.state == 'found':
        pass
      else:
        raise MQLInternalError(
            self.query,
            'Unexpected state %(state)s in attach_write_results',
            state=child.state)

    for item in self.contents:
      item.attach_write_results(result[i])
      i += 1

    if self.ordered:
      if self.ordered.state == 'create':
        self.ordered.attach_write_results(result[i])
        i += 1
      elif self.ordered.state == 'found':
        pass
      else:
        raise MQLInternalError(
            self.query,
            'Unexpected state %(state)s in attach_write_results',
            state=self.ordered.state)

    # check we processed everything
    if i != len(result):
      raise MQLInternalError(
          self.query,
          'Got %(count)d results, expecting %(expected_count)d',
          count=len(result),
          expected_count=i)

    self.change_state('written')

  def create_results(self, result, resultd, mode):
    """

        This translates a graph query result structure into a lojson
        result structure.  This is one of the most performance
        critical parts of lojson -- simple reads like

        [ { "@guid": null, "*": [ { ":value": null, ":guid": null, "@guid": null
        } ] } ]

        spend the single largest portion (around 30%) of their CPU time here.
        """

    if mode is not ReadMode:
      raise MQLInternalError(
          self.query, 'create_results() called on a %(mode)s', mode=str(mode))

    # here we are adding a list of items to the resultv itself
    resultd.query = self.query

    n = len(self.result)
    for i, this_result in enumerate(self.result):
      resultd[self.prefix + this_result] = \
          self.transform_result(this_result,result[i])

    # now transform integers and booleans if we got the
    # appropriate datatype.
    prefix_value = self.prefix + 'value'
    prefix_datatype = self.prefix + 'datatype'
    if (prefix_datatype in resultd and prefix_value in resultd):
      resultd[prefix_value] = \
          self.primitive_result(resultd[prefix_datatype],
                                resultd[prefix_value])

    elif prefix_value in resultd:
      raise MQLInternalError(
          self.query,
          'Found value in result without datatype',
          value=resultd[self.prefix + 'value'])

    # now the direct child if any (fill in the '@' slots. Nodes
    # never have a direct child.)
    if self.child:
      if len(result[n]) == 1:
        getattr(self, self.child).create_results(result[n][0], resultd, mode)
      elif len(result[n]) == 0 and getattr(self, self.child).optional:
        # this is problematic. It means that @guid is null,
        # @anything is null and all attributes are null but if
        # we don't say this then filter_result dies with a key
        # error
        pass
      else:
        guids = [('#' + x[0]) for x in result[n]]
        raise MQLInternalError(
            self.query,
            'Direct children must have exactly one result to match, got %(count)d',
            count=len(result[n]),
            guids=guids)
      n += 1

    # the pointers, if any
    # note that this overrides the direct result set above. This
    # is intentional.
    for mychild in self.children:
      if len(result[n]) == 1:
        resultd[self.prefix + mychild] = getattr(self, mychild).create_results(
            result[n][0], ResultDict(), mode)
      elif len(result[n]) == 0:
        resultd[self.prefix + mychild] = None
      else:
        guids = [('#' + x[0]) for x in result[n]]
        raise MQLInternalError(
            self.query,
            'Direct children must have at most one result, got %(count)d',
            count=len(result[n]),
            key=mychild,
            guids=guids)

      n += 1

    # and the contents - all links to other prims - links never have contents, only pointers and children
    for mycontent in self.contents:
      if mycontent.query_unique:
        if len(result[n]) == 1:
          resultd[mycontent.query_key] = mycontent.create_results(
              result[n][0], ResultDict(), mode)
        elif len(result[n]) == 0:
          resultd[mycontent.query_key] = None
        else:
          # nasty but quick way to get useful info - we know that the guid is always the first part element of the result.
          guids = [('#' + x[0]) for x in result[n]]
          raise MQLTooManyValuesForUniqueQuery(
              mycontent.query, results=guids, count=len(guids))

      else:
        resv = []
        for elem in result[n]:
          resv.append(mycontent.create_results(elem, ResultDict(), mode))
        resultd[mycontent.query_key] = resv

      n += 1

    if self.ordered:
      if len(result[n]) == 1:
        self.ordered.create_results(result[n][0], resultd, mode)
      elif len(result[n]) == 0:
        pass
      else:
        # nasty but quick way to get useful info - we know
        # that the guid is always the first part element of
        # the result.
        guids = [('#' + x[0]) for x in result[n]]
        raise MQLResultError(
            mycontent.query,
            'Order primitive may have at most one value. Got %(count)d',
            count=len(result[n]),
            guids=guids)

      n += 1

    # check we processed everything...
    if n != len(result):
      raise MQLInternalError(
          self.query,
          'Got %(count)d results, expecting %(expected_count)d',
          count=len(result),
          expected_count=n)

    return resultd
