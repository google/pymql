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

#
# Support for understanding user defined types, relationships, properties etc.
#

from utils import valid_idname, valid_guid, valid_key, reserved_word, find_key_in_query
from error import MQLParseError, MQLInternalError, MQLTypeError
from env import Guid

from pymql.util import keyquote
from pymql.log import LOG
from pymql.util.mwdatetime import coerce_datetime, uncoerce_datetime
import copy

_value_types = set(
    ('/type/value', '/type/int', '/type/text', '/type/float', '/type/boolean',
     '/type/rawstring', '/type/uri', '/type/key', '/type/datetime', '/type/id',
     '/type/enumeration'))

# these are the properties that are represented by graph intrinsics and not by direct linkage
# it might be nice to represent this in the graph itself, but this is where they are today.
_artificial_properties = set(
    ('/type/object/id', '/type/object/guid', '/type/object/mid',
     '/type/object/creator', '/type/object/attribution',
     '/type/object/timestamp', '/type/link/creator', '/type/link/attribution',
     '/type/link/master_property', '/type/link/valid', '/type/link/operation',
     '/type/link/reverse', '/type/link/type', '/type/link/source',
     '/type/link/target', '/type/link/target_value', '/type/value/type',
     '/type/value/value', '/type/text/lang', '/type/key/namespace',
     '/type/property/links', '/type/attribution/links',
     '/type/attribution/attributed'))

# these are real properties but their system use is too deep to allow delegation
_no_delegation_properties = _artificial_properties | \
    set(('/type/object/type',
         '/type/object/permission',
         '/type/object/key',
         '/type/namespace/keys',
         '/type/reflect/any_master',
         '/type/reflect/any_reverse',
         '/type/reflect/any_value'))

_no_delegation_property_guid_map = {}


class SchemaFactory(object):

  def __init__(self, querier, varenv):
    self.querier = querier
    self.init(varenv)

  def init(self, varenv):
    self.domains = {}
    self.types = {}
    self.guids = {}

    # don't want to die with enumeration errors loading /type. We assume it is all good...
    self.enumeration_ect_guid = self.querier.lookup.lookup_guid(
        '/type/enumeration', varenv)
    self.has_key_guid = self.querier.lookup.lookup_guid('/boot/has_key', varenv)

    self.add_domain('/type', varenv)
    try:
      #self.add_domain('/common', varenv)
      pass
    except MQLTypeError:
      # debug ME-907
      LOG.exception('mql.schema.SchemaFactory.init()', varenv=varenv)

      # a graph without /common!
      pass

  def add_schema_type(self, typepath, stype):
    self.types[typepath] = stype

  def add_domain(self, domainpath, varenv):
    if valid_idname(domainpath):
      ns_guid = self.querier.lookup.lookup_guid(domainpath, varenv)
      if not ns_guid:
        raise MQLTypeError(
            None, 'Domain %(domain)s could not be found', domain=domainpath)
      sdomain = SchemaDomain(self, ns_guid, varenv, domainpath)
      self.domains[domainpath] = sdomain
    else:
      raise MQLParseError(
          None, 'Domain id %(domain)s is invalid', domain=domainpath)

  def addtype(self, typepath, varenv):
    if valid_idname(typepath):
      stype = SchemaType(self, None, varenv, typepath, load=False)
      self.types[typepath] = stype
      return stype
    else:
      raise MQLParseError(
          None, 'Type id %(expected_type)s is invalid', expected_type=typepath)

  # flush everything - if we have possible cache consistency issues then we should do this...
  def flush(self, varenv):
    self.init(varenv)

  # the underlying guid may have changed, properties may have been added or deleted.
  # XXX when do we call this? Right now only when we find a legal property name
  # that we couldn't resolve to solve bug 889.
  # we need a much better technique to know when types change under the schema
  # cache, but today is not that day...
  def refresh_type(self, typepath, varenv):
    if valid_idname(typepath):
      # if we found it, remove it from the cache - note that the guid may have changed too...
      # (but we won't know that unless we flush the namespace)
      if typepath in self.types:
        typeguid = self.types[typepath].guid
        del self.types[typepath]
        del self.guids[typeguid]

      # and now reload it.
      self.addtype(typepath, varenv)
    else:
      raise MQLParseError(
          None, 'Type id %(expected_type)s is invalid', expected_type=typepath)

  def addtypebyguid(self, guid, varenv):
    if valid_guid(guid):
      # XXX should provide real support for this rubbish...
      stype = SchemaType(self, guid, varenv, None, load=False)
      self.types[stype.id] = stype
      return stype

    else:
      raise MQLInternalError(None, 'Type guid %(guid)s is invalid', guid=guid)

  def gettype(self, typepath):
    return self.types[typepath]

  def get_or_add_type(self, typepath, varenv):
    try:
      return self.gettype(typepath)
    except KeyError:
      return self.addtype(typepath, varenv)

  def gettypebyguid(self, guid):
    return self.guids.get(guid)

  def get_or_add_type_by_guid(self, guid, varenv):
    stype = self.gettypebyguid(guid)
    if not stype:
      stype = self.addtypebyguid(guid, varenv)

    return stype

  def lookup_id(self, guid, varenv):
    if guid in self.guids:
      return self.guids[guid].id
    else:
      return None


#
# A SchemaNode is the base type for every thing in a schema
#
class SchemaNode(Guid):

  def __init__(self, factory, guid):
    self.factory = factory
    self.set_guid(guid)

  def __repr__(self):
    return '<SchemaNode ' + str(self.guid) + '>'

  # unique name in the schema (v3.0 -- multiple names)
  def g(self):
    return self.guid

  def set_guid(self, guid):
    self.guid = guid
    # Property Aliasing -- it doesn't matter if we've already loaded this somewhere else already
    # just replace the version we have with this newer version.
    #if guid in self.factory.guids:
    #    raise MQLTypeError(None,"guid %(guid)s already loaded in schema.py as %(id)s",guid=guid,id=self.factory.guids[guid].id)
    if self.guid is not None:
      self.factory.guids[guid] = self


# this special subclass holds the 7 primitive value types:
# boolean, int, float, rawstring, text, uri
# XXX should we drive this off the schema for the type somewhere?
_datatype_map = {
    int: 'integer',
    long: 'integer',
    str: 'string',
    float: 'float',
    bool: 'boolean'
}
_valuetype_map = {
    '/type/int': 'integer',
    '/type/rawstring': 'bytestring',
    '/type/float': 'float',
    '/type/uri': 'url',
    '/type/boolean': 'boolean',
    '/type/text': 'string',
    '/type/key': 'key',
    '/type/datetime': 'timestamp',
    '/type/id': 'guid',
    '/type/enumeration': 'enumeration',
    '/type/value': None
}
_datatype_to_type_map = {
    'integer': '/type/int',
    'float': '/type/float',
    'url': '/type/uri',
    'boolean': '/type/boolean',
    'string': '/type/text',
    'timestamp': '/type/datetime',
    'bytestring': '/type/rawstring',
    'guid': '/type/id',
    'null': '/type/value',
    'key': '/type/key'
}
_case_sensitive = [
    '/type/key', '/type/uri', '/type/datetime', '/type/id', '/type/rawstring',
    '/type/enumeration'
]

# _default_property_map = { "/type/type": "id", "/type/value": "value", ...}


# this needs to be a function as we overwrite the result it on each call.
def get_schema_query(guid):
  delegated_prop_query = {
      ':optional': True,
      '@guid': None,
      'is_instance_of': {
          '@id': '/type/property',
          ':optional': True
      },
      '+is_instance_of': {
          '@id': '/type/extension',
          ':optional': True
      },
      'has_schema': {
          '@guid': None,
          ':optional': True
      },
      'has_expected_concept_type': {
          ':optional': True,
          '@guid': None
      },
      'requires_permission': {
          ':value': None,
          ':datatype': 'boolean',
          ':optional': True
      },
      '+has_permission': {
          '@guid': None,
          ':optional': True
      },
      'has_master_property': {
          ':optional': True,
          '@guid': None,
          'requires_permission': {
              ':value': None,
              ':datatype': 'boolean',
              ':optional': True
          },
          '+has_permission': {
              '@guid': None,
              ':optional': True
          },
          'is_unique_property': {
              ':value': None,
              ':datatype': 'boolean',
              ':optional': True
          }
      },
      # this needs reverse uniqueness to itself be unique...
      '-has_master_property': {
          ':optional': True,
          '@guid': None,
          'is_unique_property': {
              ':value': None,
              ':datatype': 'boolean',
              ':optional': True
          }
      },
      'is_unique_property': {
          ':value': None,
          ':datatype': 'boolean',
          ':optional': True
      },
      'is_enumeration_of': {
          '@guid': None,
          'is_unique_namespace': {
              ':value': None,
              ':datatype': 'boolean',
              ':optional': True
          },
          ':optional': True
      },
      'is_delegated_to': {
          '@guid': None,
          ':optional': True
      }
  }
  prop_query = copy.copy(delegated_prop_query)
  prop_query.update({
      'is_instance_of': {
          '@id': '/type/property'
      },  # not optional
      'has_schema': {
          '@guid': None
      },  # not optional
      'is_delegated_to': delegated_prop_query,
      # it's a property, it's a key, it's a dessert topic:
      ':value': None,
      ':comparator': 'octet'
  })
  return {
      '@guid': guid,
      # if this is /xxx/yyy, let's find out now
      # and save ourselves a call to lookup_id()
      'has_domain': {
          ':optional':
              True,
          '@guid':
              None,
          'is_instance_of': {
              '@id': '/type/domain'
          },
          'has_key': [{
              ':value': None,
              ':comparator': 'octet',
              '@guid': guid,
          }],
          '-has_key': [{
              ':optional': True,
              ':value': None,
              ':comparator': 'octet',
              '@id': '/'
          }]
      },
      'is_instance_of': {
          '@id': '/type/type'
      },
      'uses_properties_from': [{
          '@guid': None,
          ':optional': True
      }],
      'has_default_property_name': {
          ':value': None,
          ':optional': True
      },
      'has_key': [prop_query]
  }


def get_schema_query_by_id(id):
  schema_query = get_schema_query(None)
  (namespace, key) = id.rsplit('/', 1)
  # we might might get something directly in the root.
  if not namespace:
    namespace = '/'

  schema_query['-has_key'] = {':value': key, '@id': namespace}
  return schema_query


def get_domain_query(guid):
  ns_query = {
      '@guid': guid,
      'is_instance_of': {
          '@id': '/type/domain'
      },
      'has_key': [get_schema_query(None)]
  }

  ns_query['has_key'][0][':value'] = None
  ns_query['has_key'][0][':comparator'] = 'octet'

  # Property Aliasing; it doesn't matter if the types primary location is not in this domain.
  # this replaces the has_domain clause in get_schema_query
  #ns_query['has_key'][0]['has_domain'] = { '@guid': guid }

  return ns_query


def ugly_sort_key(json):
  name = json[':value']
  if name == 'value':
    name = '!!value'
  elif name == 'object':
    name = '!object'
  return name


class SchemaDomain(SchemaNode):
  """
    This class holds a domain of types (like /type or /common).

    It primarily allows the entire domain to be loaded in a single query
    """

  def __init__(self, factory, guid, varenv, id):
    super(SchemaDomain, self).__init__(factory, guid)

    self.id = id
    self.types = {}
    self.load_from_graph(varenv)

  def __repr__(self):
    name = self.id
    if name is None:
      name = self.guid
    return '<SchemaDomain ' + str(name) + '>'

  def get_id(self):
    return self.id

  def has_id(self, id):
    return (self.id == id)

  def load_from_graph(self, varenv):
    ns_query = get_domain_query(self.guid)

    LOG.debug('mql.schema.domain.query', 'loading domain', guid=self.guid)

    ns_result = self.factory.querier.read(ns_query, varenv)

    if ns_result is None:
      if self.id:
        domain = self.id
      else:
        domain = self.guid

      raise MQLTypeError(
          None,
          'Unable to load schema for domain %(domain)s %(guid)s',
          domain=domain,
          guid=self.guid)

    # XXX we need to order the types because uses_properties_from depends on this ordering during the parse sequence (ick)...
    results = ns_result['has_key']
    results.sort(key=ugly_sort_key)

    for result in results:
      typekey = result[':value']
      typename = self.id + '/' + typekey
      if valid_idname(typename):
        stype = SchemaType(self.factory, result['@guid'], varenv, typename,
                           self, False)

        stype.init_from_json(result, varenv)

        self.types[typekey] = stype
        self.factory.add_schema_type(typename, stype)
      else:
        raise MQLParseError(
            None, 'Invalid type name %(expected_type)', expected_type=typename)


#
# A SchemaType is a type for a type_type in a schema


# id and guid are considered public (directly accessible) properties
class SchemaType(SchemaNode):

  def __init__(self, factory, guid, varenv, id=None, domain=None, load=True):
    super(SchemaType, self).__init__(factory, guid)
    #if isreserved(id):
    # XXX Add custom excpetion when uncommenting
    #    raise Exception("Can't use reserved word %s as a typename" % id)
    self.id = id
    self.props = {}
    self.parent = None
    self.extends = []
    self.default_property_name = None
    self.domain = domain
    self.loaded = None

    if load:
      self.load_from_graph(varenv)
    elif not self.domain:
      # SchemaDomain does its own loading so we shouldn't call fake_load
      # this has ordering issues with loading /type before /type/object.
      self.fake_load(varenv)

  def __repr__(self):
    name = self.id
    if name is None:
      name = self.guid
    return '<SchemaType ' + str(name) + '>'

  def fake_load(self, varenv):
    # have to be careful that we aren't calling this on a special type, we must load all of them
    # fortunately, they are all in /type which we load explicitly.

    # oops, avoid recursion - we know that /type/object has no
    # parent anyway.
    if self.id == '/type/object':
      self.parent = None
    else:
      self.parent = self.factory.get_or_add_type('/type/object', varenv)

    # always 'object'
    self.set_category()

    self.loaded = False

  def load(self, varenv):
    if not self.loaded:
      self.load_from_graph(varenv)

  # the following methods are specific to the value types. I have thought about a ValueType
  # subclass to capture this behaviour...

  # there are 5 incompatible categories of types.
  # the enumeration pseudo-type (which cannot be combined with other values because of the magic resolution required.)
  # the value types (including the uninstantiable /type/value)
  # the object types (including the uninstantiable /type/object)
  # the link psuedo-type (only the uninstantiable /type/link)
  # the reflect psuedo-type (only the uninstantiable /type/reflect)
  def set_category(self):
    if self.id == '/type/link':
      self.category = 'link'
    elif self.id == '/type/reflect':
      self.category = 'reflect'
    elif (self.id == '/type/value' or
          (self.parent is not None and self.parent.id == '/type/value')):
      self.category = 'value'
    else:
      self.category = 'object'

  def get_category(self):
    # this function is called a lot, so we compute it once.x
    return self.category

  def get_comparator(self, varenv):
    self.load(
        varenv
    )  # why wouldn't this be loaded already by the time we need a comparator?
    try:
      dtype = _valuetype_map[self.id]
      # the graph doesn't care what your comparator is if you're
      # sorting by timestamps.
      if dtype == 'timestamp':
        return None
      elif dtype == 'float':
        return 'number'
      elif dtype == 'integer':
        return 'number'
      elif self.is_case_sensitive():
        return 'octet'
      else:
        return None
    except KeyError:
      return None

  # this returns a preferred (graph) datatype for this type. The datatype is the best possible that
  # is still compatible with the python type of value
  def get_datatype(self, value=None, comparison=None, explicit_type=None):
    dtype = _valuetype_map[self.id]
    if value is not None:
      vtype = _datatype_map[type(value)]
      if dtype is None:
        # we only want /type/value, so any subtype will do:
        return vtype
      try:
        if dtype == vtype:
          return dtype
        elif dtype in ('url', 'bytestring') and vtype == 'string':
          return dtype
        elif dtype == 'timestamp' and vtype == 'string' and coerce_datetime(
            value):
          return dtype
        elif dtype == 'float' and vtype == 'integer':
          # see bug #932 and #942. JSON (and javascript) don't really distinguish between
          # ints and floats, so we have to allow putting an int in a slot meant for a float.
          return dtype
        elif dtype == 'guid' and vtype == 'string' and (valid_idname(value) or
                                                        valid_guid(value)):
          return dtype
        elif dtype == 'key' and vtype == 'string' and (valid_key(value) or
                                                       comparison == '~='):
          # of course there isn't actually a graphd datatype=key despite my continued asking...
          return vtype
        elif dtype == 'enumeration' and vtype == 'string':
          return vtype
        else:
          raise MQLTypeError(
              None,
              '%(value)s is a JSON %(value_type)s, but the expected type is %(expected_type)s',
              value=value,
              value_type=vtype,
              expected_type=self.id,
              dtype=dtype)
      except KeyError:
        raise MQLTypeError(
            None,
            "Didn't understand %(value)s as a %(expected_type)s",
            value=value,
            expected_type=self.id)

    else:
      # XXX this is a fascinating question -- should we ask for the ect datatype?
      # or should we allow any possible return value?
      # right now I allow any possible return value so as not to bork
      # the instance inserter.

      # more notes (see bug 1902). If the user says { "type": "/type/text", "value": null }
      # then lets assume they know what they are doing, and only give them texts.
      # but if they say {} or { "type": null, "value": null }
      # give them everything. explicit_type is the value of "type".
      if explicit_type is None:
        return None
      elif explicit_type == self.id:
        # more hacks for /type/key (sigh...)
        if dtype in ('key', 'enumeration'):
          return 'string'

        return dtype
      else:
        # ick -- what have we done to get ourselves here?
        raise MQLInternalError(
            None,
            'Value type mismatch. Have %(expected_type)s, but user said %(type)s',
            expected_type=self.id,
            type=explicit_type)

  def coerce_value(self, value, comparison=None):
    """
        This coerces a value to fit this type.

        It also transforms it to a format suitable for storage in the graph.

        If the coercion is not possible, this function will throw an
        MQLTypeError
        """
    dtype = _valuetype_map[self.id]
    if value is not None:
      vtype = _datatype_map[type(value)]
      if dtype is None:
        # we only want /type/value, so any subtype will do:
        return value
      try:
        if dtype in ('string', 'url', 'bytestring') and vtype == 'string':
          return value
        elif dtype == vtype:
          return value
        elif dtype == 'timestamp' and vtype == 'string' and coerce_datetime(
            value):
          return coerce_datetime(value)
        elif dtype == 'float' and vtype == 'integer':
          # see bug #932 and #942. JSON (and javascript) don't really distinguish between
          # ints and floats, so we have to allow putting an int in a slot meant for a float.
          return float(value)
        elif dtype == 'key' and vtype == 'string' and (valid_key(value) or
                                                       comparison == '~='):
          # of course there isn't actually a graphd datatype=key despite my continued asking...
          return value
        elif dtype == 'guid' and vtype == 'string' and (valid_idname(value) or
                                                        valid_guid(value)):
          return value
        elif dtype == 'enumeration' and vtype == 'string':
          return keyquote.quotekey(value)
        else:
          raise MQLTypeError(
              None,
              '%(value)s is a JSON %(value_type)s, which cannot be coerced to %(expected_type)s',
              value=value,
              value_type=vtype,
              expected_type=self.id)
      except KeyError:
        raise MQLTypeError(
            None,
            "Didn't understand %(value)s as a %(expected_type)s",
            value=value,
            expected_type=self.id)

    else:
      # null/None is implicitly coercible to anything at all.
      return None

  def uncoerce_value(self, value, datatype, unicode_text):
    """
        This takes a value from the graph and 'uncoerces' it to be suitable for
        output.
        Currently only timestamps need to be 'uncoerced' as everything else is
        done in lojson itself.

        Note that this is really a static method (actually it is really a method
        on the schema object
        of the returned datatype.) In other words, this is a hack.

        unicode_text should be True or False, indicating if 8-bit
        'string' types should be decoded from UTF8 into Unicode.
        """
    dtype = _valuetype_map[self.id]

    if datatype == 'timestamp' and value is not None:
      return uncoerce_datetime(value)
    elif datatype == 'string':
      if dtype == 'enumeration':
        return keyquote.unquotekey(value)
      elif unicode_text:
        return unicode(value, 'utf-8')

    return value

  # XXX given a graph datatype and an expected concept type, returns the best match
  # type. This really is a nasty hack...
  def get_value_type(self, datatype=None, typeguid=None):
    # no datatype - must be me
    if not datatype:
      return self.id

    # compatible datatype - must be me
    if datatype == _valuetype_map[self.id]:
      return self.id
    elif datatype == 'string' and _valuetype_map[self.id] in ('key',
                                                              'enumeration'):
      # keys are represented in the graph as datatype=string
      return self.id
    elif datatype == 'string' and typeguid == self.factory.has_key_guid:
      # nasty nasty hack for /type/key
      return _datatype_to_type_map['key']

    # incompatible datatype - return best guess
    return _datatype_to_type_map[datatype]

  def is_case_sensitive(self):
    # /type/text is not case sensitive, but /type/rawstring, /type/key and /type/id are.
    # /type/datetime is (only because of the ISO standard requiring upper case)
    return (self.id in _case_sensitive)

  # much nastiness here.
  # this function returns what the RPN of the underlying type should be
  # and is used in query formulation (so you know that /type/text expects lang)
  # and /type/key expects namespace
  def get_right_property_name(self, query):
    if self.id == '/type/text':
      key = find_key_in_query('/type/text/lang', query)
      if key:
        return key
      key = find_key_in_query('lang', query)
      if key:
        return key
      return 'lang'
    elif self.id == '/type/key':
      key = find_key_in_query('/type/key/namespace', query)
      if key:
        return key
      key = find_key_in_query('namespace', query)
      if key:
        return key
      return 'namespace'
    else:
      return None

  # whereas this is used interpreting the returned data (hence the datatype and tg arguments)
  # here we know that /type/key wants namespace and /type/text wants lang.
  # bug 7133 is the reason to go to all this trouble in the first place.
  def get_data_right_property_name(self, datatype, typeguid):
    if datatype != 'string':
      return None
    elif typeguid == self.factory.has_key_guid:
      return 'namespace'
    else:
      return 'lang'

  # end of ValueType methods.

  # this returns the "collapsable" pd name.
  # ie if you refer to something of this type as a string directly,
  # what property of it were you specifying???
  # note that this could be stored in the schema, but it's important
  # that concepts, values and types only have one value (each) for this.
  def get_default_property_name(self):
    # should this load? it needs to load if the default property changes outside of /type

    if self.default_property_name:
      return self.default_property_name
    elif self.parent:
      return self.parent.get_default_property_name()
    else:
      raise MQLTypeError(
          None,
          "Can't determine the default property name for %(expected_type)s",
          expected_type=self.id)

  def getfactory(self):
    return self.factory

  def get_id(self):
    return self.id

  def has_id(self, id):
    return (self.id == id)

  def getshortname(self):
    return self.id.split('/')[-1]

  def getprop(self, name, varenv, recurse=True):

    # try /type/object or /type/value first if we are recursing
    if recurse and self.parent and name in self.parent.props:
      return self.parent.props[name]

    # we need to see everything to make further progress
    self.load(varenv)

    if name in self.props:
      return self.props[name]
    elif reserved_word(name, True):
      raise MQLTypeError(
          None,
          'Key %(property)s is a reserved word',
          expected_type=self.id,
          property=name)
    elif recurse:
      type_with_prop = None
      for extends in self.extends:
        # we need to load the extended type here
        extends.load(varenv)

        if name in extends.props:
          if type_with_prop:
            raise MQLTypeError(
                None,
                'Property %(property)s appears in both %(type1)s and %(type2)s which are extended by %(expected_type)s',
                expected_type=self.id,
                property=name,
                type1=type_with_prop.id,
                type2=extends.id)
          else:
            type_with_prop = extends

      if type_with_prop:
        return type_with_prop.getprop(name, varenv, False)

    raise MQLTypeError(
        None,
        'Type %(expected_type)s does not have property %(property)s',
        expected_type=self.id,
        property=name)

  def get_basic_props(self, varenv):
    if self.id == '/type/object':
      return self.prop_iterator(('id', 'name', 'type'), varenv)
    elif self.id == '/type/link':
      return self.prop_iterator(('master_property', 'reverse', 'type'), varenv)
    elif self.id == '/type/key':
      return self.prop_iterator(('value', 'type', 'namespace'), varenv)
    elif self.id == '/type/value':
      return self.prop_iterator(('value', 'type'), varenv)
    elif self.parent:
      return self.parent.get_basic_props(varenv)
    else:
      raise MQLTypeError(
          None, "Can't get basic props for %(type)s", type=self.id)

  def prop_iterator(self, proplist, varenv):
    for name in proplist:
      yield self.getprop(name, varenv)

  # iterate the available properties.
  def getprops(self, varenv, recurse=True):
    self.load(varenv)

    avoid_duplicates = set()

    for name in self.props:
      if name not in avoid_duplicates:
        avoid_duplicates.add(name)
        yield self.props[name]

    if recurse:
      for extends in self.extends:
        # recursion does not nest.
        for prop in extends.getprops(varenv, False):
          if prop.name not in avoid_duplicates:
            avoid_duplicates.add(prop.name)
            yield prop

  def addprop(self, name, propguid):
    if name in self.props or self.parent and name in self.parent.props:
      raise MQLTypeError(
          None,
          "type %(expected_type)s already has a property named '%(property)s'",
          expected_type=self.id,
          property=name)

    prop = SchemaProperty(self, propguid, name)
    self.props[name] = prop
    return prop

  def load_from_graph(self, varenv):
    # queries are in low-JSON
    if self.guid:
      base_query = get_schema_query(self.guid)
    elif self.id:
      base_query = get_schema_query_by_id(self.id)
    else:
      raise MQLInternalError(None, "Can't load a type without an id or guid!")

    LOG.debug(
        'mql.schema.type.query',
        'loading type',
        guid=self.guid,
        id=self.id,
        code=self.id or self.guid)

    result = self.factory.querier.read(base_query, varenv)

    if result is None:
      if self.id:
        expected_type = self.id
      else:
        expected_type = self.guid

      raise MQLTypeError(
          None,
          'Unable to load schema for %(expected_type)s',
          expected_type=expected_type)

    self.init_from_json(result, varenv)

  def init_from_json(self, result, varenv):
    # check we're loading the right thing...
    if '@guid' not in result:
      # weird case that I can't reproduce, but came up in production (MQL-520)
      raise MQLTypeError(
          None,
          'Unable to load schema for %(expected_type)s',
          expected_type=self.guid)
    if self.guid is None:
      self.set_guid(result['@guid'])
    elif result['@guid'] != self.guid:
      raise MQLInternalError(
          None,
          'Mismatched load of %(expected_type)s with guid %(type_guid)s != %(guid)s',
          expected_type=self.id,
          type_guid=self.guid,
          guid=result['@guid'])

    if self.id is None:
      # did we find an id?
      if result['has_domain'] is not None:
        if result['has_domain']['has_key'] and result['has_domain']['-has_key']:
          self.id = '/' + result['has_domain']['-has_key'][0][
              ':value'] + '/' + result['has_domain']['has_key'][0][':value']

      # not in a recognized root domain -- just grab any name we can find...
      if self.id is None:
        self.id = self.factory.querier.lookup.lookup_id(self.guid, varenv)

    # this requires that we've ordered the types in _basic_types
    # properly so that parents preceed their children
    if len(result['uses_properties_from']):
      for res in result['uses_properties_from']:
        self.extends.append(
            self.factory.get_or_add_type_by_guid(res['@guid'], varenv))

    if self.id in ('/type/object', '/type/value', '/type/link',
                   '/type/reflect'):
      # these types have no parent, nor should they extend anything...
      pass
    elif self.id in _value_types:
      # value types all have /type/value as their direct parent.
      self.parent = self.factory.gettype('/type/value')
    else:
      # all non basic types have a direct parent of /type/object, whether they say so or not.
      self.parent = self.factory.gettype('/type/object')

    # you automatically extend your parent.
    if self.parent and self.parent not in self.extends:
      self.extends.append(self.parent)

    if result.get('has_default_property_name'):
      # only core things in /type can use has_default_property_name for now.
      self.default_property_name = result['has_default_property_name'][':value']

    # we know the name and the parents, so we can set the category
    self.set_category()

    def set_property_permission(prop, thisprop):
      if thisprop.property_permission is not None:
        return
      if prop.get('requires_permission') and prop['requires_permission'][':value'] == True \
              and prop.get('+has_permission') and prop['+has_permission']['@guid']:
        thisprop.property_permission = prop['+has_permission']['@guid']
      else:
        thisprop.property_permission = None

    for prop in result['has_key']:
      propguid = prop['@guid']
      propname = prop[':value']

      # Property Aliasing; it doesn't matter if the property technically belongs in another type; load it here regardless.
      #if prop['has_schema']['@guid'] != self.guid:
      #    raise MQLTypeError(None,"Property %(property_guid)s has_key must have a schema of the underlying type %(type_guid)s != %(guid)s ",property_guid=propguid,type_guid=self.guid,guid=prop['has_schema']['@guid'],expected_type=self.id)

      # check reserved words outside /type
      if reserved_word(propname, (self.domain and self.domain.id == '/type')):
        raise MQLTypeError(
            None,
            'Property key %(key)s in type %(expected_type)s is a reserved word',
            property_guid=propguid,
            key=propname,
            type_guid=self.guid,
            expected_type=self.id)

      thisprop = self.addprop(propname, propguid)
      thisprop.is_extension = prop['+is_instance_of'] is not None

      if prop.get('has_expected_concept_type'):
        thisprop.ect_guid = prop['has_expected_concept_type']['@guid']

      # we do delegation first. If a property is delegated we
      # note the original typeguid and then replace it with
      # its delegatee. Only the ECT is not delegated.
      if prop.get('is_delegated_to'):
        if prop.get('has_master_property'):
          raise MQLTypeError(
              None,
              'Properties cannot be both delegated and reverse properties',
              property=propname,
              type=self.id)
        elif prop.get('is_enumeration_of'):
          raise MQLTypeError(
              None,
              'Properties cannot be both delegated and direct enumerations',
              property=propname,
              type=self.id)
        elif prop['is_delegated_to'].get('is_delegated_to'):
          raise MQLTypeError(
              None,
              'Properties cannot be recursively delegated',
              property=propname,
              type=self.id)
        elif not prop['is_delegated_to'].get(
            'has_schema') or not prop['is_delegated_to'].get('is_instance_of'):
          raise MQLTypeError(
              None,
              'You must delegate to a valid property',
              property=propname,
              invalid_guid=prop['is_delegated_to']['@guid'],
              type=self.id)
        elif prop['requires_permission'] and prop['requires_permission'].get(
            ':value'):
          raise MQLTypeError(
              None,
              "You cannot use 'requires_permission' with a delegated property",
              property=propname)

        # you must delegate a unique property to another unique property and vice versa.
        prop_unique = (
            prop.get('is_unique_property') and
            prop['is_unique_property'][':value'] == True)
        delegate_unique = (
            prop['is_delegated_to'].get('is_unique_property') and
            prop['is_delegated_to']['is_unique_property'][':value'] == True)
        if (not prop_unique and delegate_unique) or (not delegate_unique and
                                                     prop_unique):
          # You obviously don't know what you're doing, so i'm just going to pretend like you didn't
          # set your uniqueness to differ from the property you're delegating to.
          LOG.warning(
              'mql.delegated.unique.conflict',
              '',
              prop=prop['@guid'],
              delegated=prop['is_delegated_to']['@guid'])

        # check the ect s are compatible. This requires the information in /type so we check we have loaded it first.
        if thisprop.ect_guid and not (self.domain and
                                      self.domain.id == '/type'):
          thisprop_ect_category = 'object'
          if self.factory.gettypebyguid(thisprop.ect_guid):
            thisprop_ect_category = self.factory.gettypebyguid(
                thisprop.ect_guid).category

          delegate_prop_ect_guid = None
          delegate_prop_ect_category = 'object'
          if prop['is_delegated_to'].get('has_expected_concept_type'):
            delegate_prop_ect_guid = prop['is_delegated_to'][
                'has_expected_concept_type']['@guid']
            delegate_prop_ect = self.factory.gettypebyguid(
                delegate_prop_ect_guid)
            if delegate_prop_ect:
              delegate_prop_ect_category = delegate_prop_ect.category

          if thisprop_ect_category != delegate_prop_ect_category:
            raise MQLTypeError(
                None,
                'You must delegate to a property with a compatible expected type',
                property=propname,
                type=self.id)

          # enumerations can only be delegated to other enumerations.
          if (thisprop.ect_guid == self.factory.enumeration_ect_guid) or (
              delegate_prop_ect_guid == self.factory.enumeration_ect_guid) and (
                  thisprop.ect_guid != delegate_prop_ect_guid):
            raise MQLTypeError(
                None,
                'You must only delegate enumerations to enumerations',
                property=propname,
                type=self.id)

        # finally, you must not delegate an artificial property. This could in theory be made to work, but why?
        # bugs 7384, 7385, 7388 -- or a /type/key expecting or other funky property.
        if prop['is_delegated_to']['@guid'] in _no_delegation_property_guid_map:
          raise MQLTypeError(
              None,
              'You must not delegate %(property)s to system property %(delegated)s',
              property=(self.id + '/' + propname),
              type=self.id,
              delegated=_no_delegation_property_guid_map[prop['is_delegated_to']
                                                         ['@guid']])

        thisprop.set_delegation(prop['is_delegated_to']['@guid'])
        # replace prop by the delegatee...
        prop = prop['is_delegated_to']

      if prop.get('is_unique_property'
                 ) and prop['is_unique_property'][':value'] == True:
        # we refuse to allow reverse properties to be unique -- we just can't enforce it...
        thisprop.unique = True
      elif propname == self.get_default_property_name() or self.category in (
          'value', 'link'):
        # the default name is always unique. This should be a warning at least...
        raise MQLTypeError(
            None,
            'Saw a non-unique name %(property)s in type %(expected_type)s that must be unique',
            property=propname,
            expected_type=self.id)

      if prop.get('is_enumeration_of'):
        if prop.get('has_master_property'):
          raise MQLTypeError(
              None,
              'Properties cannot be reverse properties and enumerations',
              property=propname)
        if prop.get('-has_master_property'):
          raise MQLTypeError(
              None,
              'Enumerated properties cannot have reverse properties',
              property=propname)

        if thisprop.ect_guid != self.factory.enumeration_ect_guid:
          raise MQLTypeError(
              None,
              'Enumerated properties must have expected type /type/enumeration',
              property=propname)

        thisprop.set_enumeration(self.factory.has_key_guid,
                                 prop['is_enumeration_of']['@guid'])

        # bug 6797 -- force the uniqueness state of enumerated properties to match the state of their underlying namespaces
        if prop['is_enumeration_of'].get('is_unique_namespace') and prop[
            'is_enumeration_of']['is_unique_namespace'][':value'] == True:
          if not thisprop.unique:
            raise MQLTypeError(
                None,
                'Properties enumerating unique namespaces must themselves be unique',
                property=propname,
                enumeration=thisprop.enumeration)
        else:
          if thisprop.unique == True:
            raise MQLTypeError(
                None,
                'Unique enumerated properties must enumerate unique namespaces',
                property=propname,
                enumeration=thisprop.enumeration)

      elif thisprop.ect_guid == self.factory.enumeration_ect_guid:
        raise MQLTypeError(
            None,
            'A property with the type /type/enumeration, must specify an enumerating namespace',
            property=propname)

      elif prop.get('has_master_property') and prop.get('-has_master_property'):
        raise MQLTypeError(
            None,
            'Saw a property that is both a master and a reverse property',
            property=propname,
            expected_type=self.id)
      elif prop.get('has_master_property'):
        mprop = prop['has_master_property']
        thisprop.set_master_property(mprop['@guid'])
        set_property_permission(mprop, thisprop)

        if mprop.get('is_unique_property'
                    ) and mprop['is_unique_property'][':value'] == True:
          thisprop.master_unique = True
      elif prop.get('-has_master_property'):
        thisprop.set_reverse_property(prop['-has_master_property']['@guid'])

        if prop['-has_master_property'].get('is_unique_property') and prop[
            '-has_master_property']['is_unique_property'][':value'] == True:
          thisprop.reverse_unique = True

      set_property_permission(prop, thisprop)

    self.loaded = True


#
# A SchemaProperty represents a PD in the graph. It contains slots for
# the display names, infomation about the side, and about whether
# a relationship or attribute is described.
#


# name, guid and type are considered public (directly accessible) properties
class SchemaProperty(SchemaNode):

  def __init__(self, schematype, guid, name):
    super(SchemaProperty, self).__init__(schematype.factory, guid)
    self.type = schematype
    self.name = name
    # just to detect which is None..
    self.id = self.type.id + '/'
    self.id += self.name
    # self.id = self.type.id + "/" + self.name

    # probably the wrong way to set this up, but at least unambiguous.
    self.unique = False
    self.master_unique = False
    self.reverse_unique = False

    self.artificial = self.id in _artificial_properties

    if self.id in _no_delegation_properties:
      _no_delegation_property_guid_map[self.guid] = self.id

    self.reverse = False
    # forward properties have the same typeguid as themselves.
    # Reverse properties have the typeguid of the master property.
    # delegated properties have the typeguid they delegated to
    # (or the typeguid of the master of what they delegated to...)
    self.typeguid = guid
    self.ect_guid = None

    self.enumeration = None

    # what guid are we delegated to? This may not be the typeguid if we are delegated to a reverse property for example.
    self.delegated_to = None

    # place to stash the ReversedSchemaProperty if we needed one
    self.reversed = None
    # a place for the ReversedSchemaProperty to stash the original property
    self.orig_property = None

    self.property_permission = None
    self.is_extension = None

  def __repr__(self):
    name = self.id
    if name is None:
      name = self.guid
    return '<SchemaProperty ' + str(name) + '>'

  # this may cause the other type to be loaded...
  def getothertype(self, varenv):
    if self.ect_guid:
      return self.factory.get_or_add_type_by_guid(self.ect_guid, varenv)
    else:
      # the default expected concept type is /type/object
      return self.factory.gettype('/type/object')

  def gettypeguid(self):
    return self.typeguid

  def gettype(self):
    return self.type

  def set_master_property(self, typeguid):
    self.reverse = True
    self.typeguid = typeguid

  def set_reverse_property(self, typeguid):
    self.reverse = False

  def set_enumeration(self, typeguid, enumeration):
    self.enumeration = enumeration
    self.typeguid = typeguid
    self.reverse = True

  def set_delegation(self, guid):
    self.delegated_to = guid
    # for now we use the typeguid we delegate to, but this may change.
    self.typeguid = guid

  def has_id(self, id):
    # are we exactly this property???
    # this is mostly for special case hacks...
    return (self.id == id)

  def get_id(self):
    # are we exactly this property???
    # this is mostly for special case hacks...
    return self.id

  def is_unique(self):
    return self.unique

  def is_master_unique(self):
    if self.reverse:
      return self.master_unique
    else:
      return self.unique

  def is_reverse_unique(self):
    if self.reverse:
      return self.unique
    else:
      return self.reverse_unique

  def graph_guid(self):
    return self.typeguid[1:]

  def get_reversed(self, varenv):
    if not self.reversed:
      self.reversed = ReversedSchemaProperty(self, varenv)

    return self.reversed


class ReversedSchemaProperty(SchemaProperty):
  # this class supports bug 7253 (! property reversing support)
  def __init__(self, orig_property, varenv):
    super(ReversedSchemaProperty,
          self).__init__(orig_property.type, '!' + orig_property.guid,
                         orig_property.name)
    self.orig_property = orig_property
    self.id = '!' + self.orig_property.id

    # make sure this will work...
    self.check_orig_property(varenv)
    self.set_from_orig_property()

  def check_orig_property(self, varenv):
    # return a made-up SchemaProperty that is the schema property turned around.
    # raise an MQLTypeError if such a thing makes no sense (for instance if they expected type is not in the object category)
    if self.orig_property.artificial:
      raise MQLTypeError(
          None,
          "Can't reverse artificial property %(property)s",
          property=self.orig_property.id)

    if self.orig_property.orig_property:
      raise MQLInternalError(
          None,
          'Attempt to reverse an already reversed property %(property)s',
          property=self.orig_property.id)

    # can't reverse /type/reflect (even though /type/reflect/any_master should probably reverse to /type/reflect/any_reverse if thought
    # about for long enough...)
    if self.orig_property.type.get_category() != 'object':
      raise MQLTypeError(
          None,
          "Can't reverse property %(property)s on artificial type %(type)s",
          property=self.orig_property.id,
          type=self.orig_property.type.id)

    # can only reverse something if it pointed to an object itself
    if self.orig_property.getothertype(varenv).get_category() != 'object':
      raise MQLTypeError(
          None,
          "Can't reverse %(property)s as it expects %(expected_type)s, not an object",
          property=self.orig_property.id,
          expected_type=self.orig_property.getothertype(varenv).id)

    # this should be caught by the check above, but let's be 100% sure...
    if self.orig_property.enumeration:
      raise MQLTypeError(
          None,
          "Can't reverse an enumerated property %(property)s",
          property=self.id)

  def set_from_orig_property(self):
    self.reverse = not self.orig_property.reverse

    # this code is such crap, but it makes is_master_unique() and is_reverse_unique() work...
    if self.reverse:
      # the orig_property was forward, we are reverse
      self.master_unique = self.orig_property.unique
      self.unique = self.orig_property.reverse_unique
    else:
      # the orig_property was reverse, so we are forward again.
      self.reverse_unique = self.orig_property.unique
      self.unique = self.orig_property.master_unique

    # even if we are delegated the typeguid is unchanged.
    self.typeguid = self.orig_property.typeguid

    # the expected type is the schema, so it's guid is the guid of the schema
    self.ect_guid = self.orig_property.type.guid

    # if the original was delegated, so are we
    self.delegated_to = self.orig_property.delegated_to


# special case object for /type/link
class LinkProperty(object):
  # totally bogus, but shouldn't clash with any real property
  id = '/boot/link_property'
  reverse = False
  unique = True
  typeguid = None
  type = None  # XXX should be a pointer to /type/link
  artificial = False
  name = 'link'
