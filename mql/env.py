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

from grquoting import quote, unquote
import cgi
from utils import Missing, valid_mid, valid_mql_key


class Varenv(dict):
  """
    A varenv is a container for per query MQL state
    It contains the following variables:

    $user - the user performing the query
    $permission - the default write permission
    $lang - the default language

    cursor - the cursor being applied to this query (if any)

    policy - the timeout policy being applied to this query
    tid - the transaction id of this query and any subqueries performed on its
    behalf

    It also contains the following attributes:

    sort_number - the current variable count in this query

    This is all subject to change as we get a better sense of what is really
    required...
    """

  def __init__(self, underlying_dict, lookup, keys=None):
    self.lookup_manager = LookupManager(lookup, self)

    if keys is None:
      keys = underlying_dict.iterkeys()

    for key in keys:
      if key in underlying_dict:
        val = underlying_dict[key]
        if isinstance(val, unicode):
          val = val.encode('utf-8')

        self[key] = val

    self.parent = underlying_dict

    self.sort_number = 0

    # we will resolve these on demand.
    self.lang_guid = None
    self.user_guid = None

    # we default lang and permission, but not user...
    if '$lang' not in self:
      self['$lang'] = '/lang/en'

    if '$permission' not in self:
      self['$permission'] = '/boot/all_permission'

    if 'vars_used' not in self:
      self['vars_used'] = set()

  def get_lang_id(self):
    return self['$lang']

  def get_lang_guid(self):
    if self.lang_guid is not None:
      return self.lang_guid
    else:
      result = self.lookup_manager.lookup.lookup_guid(self['$lang'], self)
      # this may fail so callers must handle "False" as a result.
      self.lang_guid = result
      return result

  def get_user_guid(self):
    if self.user_guid is not None:
      return self.user_guid
    elif self['$user']:
      result = self.lookup_manager.lookup.lookup_guid(self['$user'], self)
      # this may fail so callers must handle "False" as a result.
      self.user_guid = result
      return result
    else:
      return False

  def get_user_id(self):
    # ME-668 DO NOT change this to fix the username in error messages looks
    # bad problem. Causes a lot of grief in tons of other places - for now.
    # return self.lookup_manager.lookup_id(self['$user'], self)
    return self['$user']

  # get a human readable user id
  def get_hr_user_id(self):
    return self.lookup_manager.lookup_id(self['$user'], self)

  def var_used(self, var):
    self.setdefault('vars_used', set()).add(var)

    # hideous hack to propagate vars_used to the outer level
    if self.parent:
      self.parent.setdefault('vars_used', set()).add(var)

  def export(self, keys):
    # copy this list of keys back into the parent varenv
    if self.parent:
      for key in keys:
        if key in self:
          self.parent[key] = self[key]

  def child(self):
    """
        This produces a varenv suitable for use in an lojson or lookup subquery.

        We cant afford to have cursor passed into lookup or anything querier
        based...

        Currently we only need to preserve the tid and the timeout policy
        """
    cls = type(self)
    return cls(
        self,
        lookup=self.lookup,
        keys=['tid', 'policy', '$lang', '$permission', '$user'])

  def copy(self):
    """
        This produces a duplicate but independent varenv
        """
    cls = type(self)
    return cls(self, lookup=self.lookup)


class LookupManager(object):
  """
    A stateful container to hold DeferredIdLookup and DeferredGuidLookups

    The two main structures here are:
    guid_list - the resolvable guids found in this query
    guid_dict - the resolution of %guid_list
    """

  def __init__(self, lookup, varenv):
    self.varenv = varenv
    self.lookup = lookup

    # old style lookups
    self.guid_list = []
    self.guid_dict = {}

    # new style lookups
    self.guid_lookups = []
    self.id_lookups = []

    self.guid_to_mid_lookups = []
    self.mid_to_guid_lookups = []

  # they're just like ids.
  # but below, "do_id_lookups" means
  # "turn guids into ids" and "do_guid_lookups"
  # means "turn ids into guids.

  def do_mid_to_guid_lookups(self):
    mids = [defer.mid for defer in self.mid_to_guid_lookups]
    if mids:
      result = self.lookup.lookup_guids_of_mids(mids, self.varenv)
      for defer in self.mid_to_guid_lookups:
        if defer.mid in result:
          defer.guid = result[defer.mid]

  def do_guid_to_mid_lookups(self):
    guids = [defer.guid for defer in self.guid_to_mid_lookups]
    result = self.lookup.lookup_mids_of_guids(guids, self.varenv)
    for defer in self.guid_to_mid_lookups:
      if defer.guid in result:
        defer.mid = result[defer.guid]
      else:
        defer.mid = defer.guid

    self.guid_to_mid_lookups = []
    self.mid_to_guid_lookups = []

  # they're just like ids.
  # but below, "do_id_lookups" means
  # "turn guids into ids" and "do_guid_lookups"
  # means "turn ids into guids.

  def do_mid_to_guid_lookups(self):
    # these might be DeferredGuidOfMidOrGuidLookups, too.
    mids = [
        defer.mid
        for defer in self.mid_to_guid_lookups
        if isinstance(defer, DeferredGuidOfMidLookup)
    ]
    #ids  = [ defer.id for defer in self.guid_lookups if isinstance(defer, DeferredGuidLookup)]
    if mids:
      result = self.lookup.lookup_guids_of_mids(mids, self.varenv)
      for defer in self.mid_to_guid_lookups:
        if defer.mid in result:
          defer.guid = result[defer.mid]

  def do_id_lookups(self):
    guids = [defer.guid for defer in self.id_lookups]
    result = self.lookup.lookup_ids(guids, self.varenv)
    for defer in self.id_lookups:
      if defer.guid in result:
        defer.id = result[defer.guid]
      else:
        defer.id = defer.guid

  def do_guid_lookups(self):
    ids = [defer.id for defer in self.guid_lookups]
    result = self.lookup.lookup_guids(ids, self.varenv)
    for defer in self.guid_lookups:
      if defer.id in result:
        defer.guid = result[defer.id]

  def substitute_ids(self, result):
    if isinstance(result, DeferredIdLookup):
      return result.id
    elif isinstance(result, list):
      return map(self.substitute_ids, result)
    elif isinstance(result, dict):
      newdict = {}
      for key, item in result.iteritems():
        if isinstance(key, DeferredIdLookup):
          key = key.id
        newdict[key] = self.substitute_ids(item)
      return newdict
    return result

  def substitute_mids(self, result):
    if isinstance(result, DeferredMidsOfGuidLookup):
      return result.mid
    elif isinstance(result, DeferredMidOfGuidLookup):
      return result.mid[0]
    elif isinstance(result, list):
      if result and isinstance(result[0], DeferredMidsOfGuidLookup):
        return result[0].mid
      else:
        return map(self.substitute_mids, result)
    elif isinstance(result, dict):
      newdict = {}
      for key, item in result.iteritems():
        if isinstance(item, DeferredMidOfGuidLookup):
          item = item.mid[0]
        elif isinstance(item, DeferredMidsOfGuidLookup):
          item = item.mid
        newdict[key] = self.substitute_mids(item)
      return newdict
    return result

  def substitute_guids(self, result):
    if isinstance(result, DeferredIdLookup):
      return result.guid
    elif isinstance(result, list):
      return map(self.substitute_guids, result)
    elif isinstance(result, dict):
      newdict = {}
      for key, item in result.iteritems():
        if isinstance(key, DeferredIdLookup):
          key = key.guid
        newdict[key] = self.substitute_guids(item)
      return newdict
    return result


class Guid(object):

  def __init__(self, guid):
    self.guid = guid

  def graph_guid(self):
    if self.guid:
      return self.guid[1:]
    else:
      return self.guid

  def __str__(self):
    return self.guid

  def __repr__(self):
    return repr(self.__dict__)


def concat_guids(guidlist):
  # ugly... perhaps should be in readqp.py
  res = ['(']
  res.extend((guid.guid[1:] for guid in guidlist if guid.guid))
  if len(res) == 1:
    return False

  res.append(')')

  return ' '.join(res)


class FixedGuidList(Guid):

  def __init__(self, guidlist, varenv):
    self.children = []
    for guid in guidlist:
      child = Guid(guid)
      self.children.append(child)

  def graph_guid(self):
    return concat_guids(self.children)

  def __str__(self):
    return self.graph_guid()


class DeferredGuidLookup(Guid):

  def __init__(self, id, manager):
    self.id = id
    self.guid = None

    manager.guid_lookups.append(self)

  def graph_guid(self):
    if self.guid:
      return self.guid[1:]
    else:
      return self.guid

  def __str__(self):
    return self.id

  def __eq__(self, other):
    if isinstance(other, basestring):
      return self.id == other
    return self.id == other.id

  def __ne__(self, other):
    return not self.__eq__(other)


class DeferredGuidLookups(Guid):

  def __init__(self, idlist, manager):
    self.children = []
    for id in idlist:
      child = DeferredGuidLookup(id, manager)
      # this puts it into the varenv list
      self.children.append(child)

  def graph_guid(self):
    return concat_guids(self.children)

  def lookup_id(self, guid):
    # returns the id that resulted in this guid being placed in the list.
    for defer in self.children:
      if defer.guid == guid:
        return defer.id
    return False

  def __str__(self):
    return ' '.join([str(defer.id) for defer in self.children])


class DeferredGuidOfMidLookups(Guid):

  def __init__(self, midlist, manager):
    self.children = []
    for mid in midlist:
      child = DeferredGuidOfMidLookup(mid, manager)
      self.children.append(child)

  def graph_guid(self):
    return concat_guids(self.children)

  # alias
  def lookup_id(self, mid):
    return self.lookup_mid(mid)

  def lookup_mid(self, guid):
    for defer in self.children:
      if defer.guid == guid:
        return defer.mid
    return False

  def __str__(self):
    return ' '.join([str(defer.mid) for defer in self.children])


class DeferredGuidOfMidLookup(Guid):

  def __init__(self, mid, manager):
    self.mid = mid
    # alias
    self.id = mid
    self.guid = None
    manager.mid_to_guid_lookups.append(self)

  def graph_guid(self):
    if self.guid:
      return self.guid[1:]
    else:
      return self.guid

  def __str__(self):
    return self.mid

  def __eq__(self, other):
    if isinstance(other, basestring):
      return self.mid == other
    return self.mid == other.mid

  def __ne__(self, other):
    return not self.__eq__(other)


class DeferredMidOfGuidLookup(Guid):

  def __init__(self, guid, manager):
    self.guid = guid
    self.mid = None
    manager.guid_to_mid_lookups.append(self)

  def __str__(self):
    # it's possible we could have many mids - but we only want the first, the one
    # highest up the chain (lookup takes care of that) if we said mid: null
    return self.mid[0]


class DeferredMidsOfGuidLookup(Guid):

  def __init__(self, guid, manager):
    self.guid = guid
    self.mid = None
    manager.guid_to_mid_lookups.append(self)

  def __str__(self):
    # it's possible we could have many mids - but we only want the first, the one
    # highest up the chain (lookup takes care of that) if we said mid: null
    if isinstance(self.mid, list):
      if len(self.mid) > 0:
        return self.mid[0]

    # this shouldn't happen
    return self.guid


class DeferredGuidOfMidOrGuidLookups(Guid):

  def __init__(self, idlist, manager):

    self.children = []
    for id in idlist:
      if valid_mid(id):
        child = DeferredGuidOfMidLookup(id, manager)
      else:
        child = DeferredGuidLookup(id, manager)
      self.children.append(child)

  def graph_guid(self):
    return concat_guids(self.children)

  def lookup_id(self, guid):
    for defer in self.children:
      if defer.guid == guid:
        if isinstance(defer, DeferredGuidLookup):
          return defer.id
        elif isinstance(defer, DeferredGuidOfMidLookup):
          return defer.mid
    return False

  def lookup_mid(self, guid):
    for defer in self.children:
      if defer.guid == guid:
        if isinstance(defer, DeferredGuidLookup):
          return defer.id
        elif isinstance(defer, DeferredGuidOfMidLookup):
          return defer.guid
    return False

  def __str__(self):
    return ' '.join([str(defer.id) for defer in self.children])


class DeferredIdLookup(Guid):

  def __init__(self, guid, manager):
    self.guid = guid
    self.id = None

    manager.id_lookups.append(self)

  def __str__(self):
    return self.id

  def __eq__(self, other):
    try:
      if isinstance(other, str) or isinstance(other, unicode):
        rv = self.guid == other
        if not rv:
          return False
        return rv
      rv = self.guid == other.guid
      if not rv:
        return False
      return rv
    except:
      return False

  def __ne__(self, other):
    return not self.__eq__(other)


######################################################################

_boolean_name = {True: 'true', False: 'false'}
_make_boolean = {'true': True, 'false': False}


def quote_value(v):
  if isinstance(v, bool):
    return quote(_boolean_name[v])
  elif isinstance(v, (int, long, float)):
    return quote(str(v))
  elif isinstance(v, str):
    return quote(v)
  elif isinstance(v, Guid):
    return v.graph_guid()
  elif isinstance(v, list):
    # handle guid and value lists.
    return '(' + ' '.join((quote_value(x) for x in v)) + ')'
  elif v is Missing:
    return 'null'
  else:
    raise ValueError('Unknown type in quote_value %s' % type(v))


def unquote_value(datatype, value, safe):
  if value == 'null':
    return None
  else:
    value = unquote(value)

    if datatype == 'boolean':
      return _make_boolean[value]
    elif datatype == 'integer':
      return int(value)
    elif datatype == 'float':
      if value == 'nan':
        return value
      return float(value)
    elif not safe:
      # short circuit the unsafe case; everything below is safety escaping.
      return value
    elif datatype == 'url':
      if (value.find('javascript:') == 0):
        value = 'unsafe-' + value

      return value
    else:
      # this has no effect on timestamps and guids but I feel this is
      # better code than deliberately excluding them.
      return cgi.escape(value)
