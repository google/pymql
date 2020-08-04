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


import os, sys, time

from traceback import format_exc
from itertools import izip
from urlparse import urlparse
from httplib import HTTPConnection
from threading import Thread, Event, currentThread
from Queue import Queue, Empty
from docutils.core import publish_string

from mw import json
from mw.mql.error import MQLError, MQLInternalError, MQLInternalParseError
from mw.mql.utils import encode_cursor, decode_cursor
from mw.util.pattern import Pattern
from paste.deploy.converters import asbool
import apikeys

from adapter import HTTPAdapter, \
    AdapterError, AdapterException, AdapterUserError, ResultTypeError

PYTHON_ADAPTERS = {}

RESERVED_KEYS = set(('optional', 'value', '*', 'index', 'link', 'connect'))
EMQL_NAMESPACE = "/freebase/emql"
EMQL_PREFIX = "@"
EMQL_ADAPTER_TIMEOUT=60.0

DOCS_BASE = os.path.join(os.path.dirname(__file__), 'docs')
RST_SETTINGS = {'stylesheet_path': os.path.join(DOCS_BASE, 'documentation.css'),
                'input_encoding': 'utf-8'}
CRAWL_LIMIT = 100
MQL_LIMIT = 100
MQL_LITERAL_TYPES = (basestring, int, long, float, bool, type(None))
NIL = object()

USE_AUGMENTS = False
P_EXPECTED_TYPE = "/type/property/expected_type"
P_URI = "/type/extension/uri"
P_API_KEY_NAMES = "/type/extension/api_key_names"
P_PRE = "/type/extension/pre"
P_FETCH = "/type/extension/fetch"
P_REDUCE = "/type/extension/reduce"
P_HELP = "/type/extension/help"
P_APPLICATION = "/freebase/foreign_api_extension/application"
P_FOREIGN_API = "/freebase/foreign_api_extension/uses"

def get_python_adapter(tid, graph, mql, me, cache, path, *args):

    adapter = PYTHON_ADAPTERS.get(path)
    if adapter is None:
        module, classname = path.rsplit('.', 1)
        if module.startswith('mw.'):
            try:
                cls = reduce(getattr, module.split('.')[1:] + [classname],
                             __import__(module))
                adapter = cls(tid, graph, mql, me, cache, *args)
                PYTHON_ADAPTERS[path] = adapter
            except Exception:
                print format_exc()
                me.get_session().log('error', 'emql.adapter.import',
                                     format_exc(), cls=path)
        else:
            me.get_session().log('error', 'emql.adapter.import',
                                 "only local implementations allowed",
                                 cls=path)

    return adapter


def load_emql_properties(tid, graph, mql, me, cache,
                         properties, declaring_type, namespace,
                         names, mqlres):

    adapters = {}
    for property in mqlres:
        name = reduce(dict.get, names, property)
        if name in properties: # overriding existing property
            continue
        if name is not None:
            id = '/'.join((namespace, name))
        else:
            id = property['id']
            name = id.rsplit('/', 1)[1]
        guid = property['guid'][1:]
        uri = property[P_URI]
        try:
            scheme, x, path, x, x, x = urlparse(uri)
        except Exception:
            me.get_session().log('error', 'emql.schema.load',
                                 'while parsing uri: %s' % format_exc(),
                                 uri=uri, property=id)
            continue
            
        expected_type = property[P_EXPECTED_TYPE]
        if expected_type is not None:
            is_literal = (expected_type['id'] in ('/type/value',
                                                  '/type/link') or
                          '/type/value' in expected_type['extends'])
            expected_type = expected_type['guid'][1:]
        else:
            is_literal = None

        proxy = None
        if scheme not in ('http', 'https', 'python'):
            config = me.get_session().get_config()
            pattern = config.get('emql.%s' %(scheme))
            if pattern is None:
                me.get_session().log('error', 'emql.scheme.load',
                                     'no emql config for scheme',
                                     scheme=scheme, uri=uri, property=id)
                continue
            proxy = config.get('emql.%s.proxy' %(scheme))
            if proxy is not None:
                proxy = asbool(proxy)
            uri = str(Pattern(pattern).set_uri(uri))

            try:
                scheme, x, path, x, x, x = urlparse(uri)
            except Exception:
                me.get_session().log('error', 'emql.schema.load',
                                     'while parsing uri: %s' % format_exc(),
                                     uri=uri, property=id)
                continue

        

        if scheme in ('http', 'https'):
            # we only actually need the application when calling over
            # http, because we're signing the http request
            application = property.get(P_APPLICATION)
            if application:
                application = application["guid"]
            adapter = HTTPAdapter(tid, graph, mql, me, cache,
                                  uri, property[P_API_KEY_NAMES],
                                  property.get(P_FOREIGN_API),
                                  proxy,
                                  property[P_PRE], property[P_FETCH],
                                  property[P_REDUCE], property[P_HELP],
                                  application)
        elif scheme == 'python':
            # note: foreign_apis and the owning application are not
            # passed to python adapters, these are considered legacy
            adapter = get_python_adapter(tid, graph, mql, me, cache, path,
                                         uri, property[P_API_KEY_NAMES],
                                         property[P_PRE], property[P_FETCH],
                                         property[P_REDUCE], property[P_HELP])
        else:
            continue

        if adapter is not None:
            adapters[id] = adapter

        properties[name] = \
            (id, guid, declaring_type, expected_type, is_literal)
        cache.PROPERTIES[guid] = \
            (declaring_type, expected_type, is_literal)

    return adapters, properties


def load_emql_namespace(tid, graph, mql, me, cache):

    query = [{"key": {"namespace": EMQL_NAMESPACE, "value": None},
              "type": "/type/extension",
              "guid": None,
              P_URI: None,
              P_API_KEY_NAMES: [],
              P_PRE: None,
              P_FETCH: None,
              P_REDUCE: None,
              P_HELP: None,
              P_EXPECTED_TYPE: {"guid": None, "id": None, "extends": [],
                                "optional": True}}]

    mqlres, x = mql.mqlread(tid, query, schema=True)

    adapters, properties = \
        load_emql_properties(tid, graph, mql, me, cache, {}, None,
                             EMQL_NAMESPACE, ('key', 'value'), mqlres)
    cache.TYPES[EMQL_NAMESPACE] = properties
    cache.ADAPTERS.update(adapters)


def load_type(tid, graph, mql, me, cache, type):

    augments = "!/type/extension/augments"
    query = {"guid": '#' + type,
             "id": None,
             "type": "/type/type",
             "properties": [{
                "id": None,
                "guid": None,
                "optional": True,
                "type": [],
                P_EXPECTED_TYPE: {"guid": None, "id": None, "extends": [],
                                  "optional": True},
                P_URI: None,
                P_API_KEY_NAMES: [],
                P_PRE: None,
                P_FETCH: None,
                P_REDUCE: None,
                P_HELP: None,
                P_APPLICATION: {"guid": None, "optional": True, "limit": 1},
                P_FOREIGN_API: apikeys.get_extension_api_query(optional=True),
             }],
             augments: [{
                "guid": None,
                "name": None,
                P_EXPECTED_TYPE: {"guid": None, "id": None, "extends": [],
                                  "optional": True},
                P_URI: None,
                P_API_KEY_NAMES: [],
                P_PRE: None,
                P_FETCH: None,
                P_REDUCE: None,
                P_HELP: None,
                P_APPLICATION: {"guid": None, "optional": True, "limit": 1},
                P_FOREIGN_API: apikeys.get_extension_api_query(optional=True),
                "optional": True}]}

    mqlres, x = mql.mqlread(tid, query, schema=True)
    if mqlres is not None:
        properties = {}

        if not USE_AUGMENTS:
            del mqlres[augments][:]
    
        for property in mqlres['properties']:
            if '/type/extension' in property['type']:
                mqlres[augments].append(property)
            else:
                id = property['id']
                guid = property['guid'][1:]
                expected_type = property[P_EXPECTED_TYPE]
                if expected_type is not None:
                    is_literal = (expected_type['id'] in ('/type/value',
                                                          '/type/link') or
                                  '/type/value' in expected_type['extends'])
                    expected_type = expected_type['guid'][1:]
                else:
                    is_literal = None

                properties[id.rsplit('/', 1)[1]] = \
                    (id, guid, type, expected_type, is_literal)
                cache.PROPERTIES[guid] = \
                    (type, expected_type, is_literal)

        adapters, properties = \
            load_emql_properties(tid, graph, mql, me, cache,
                                 properties, type, mqlres['id'], ('name',),
                                 mqlres[augments])
        cache.TYPES[type] = properties
        cache.ADAPTERS.update(adapters)


def find_property_by_name(tid, graph, mql, me, cache,
                          type, tobj, name, allow_default=True):

    if name.startswith(EMQL_PREFIX):
        if EMQL_NAMESPACE not in cache.TYPES:
            load_emql_namespace(tid, graph, mql, me, cache)
        try:
            return cache.TYPES[EMQL_NAMESPACE][name[len(EMQL_PREFIX):]]
        except KeyError:
            raise NoSuchPropertyError(tid, mql, type, name)

    if type not in cache.TYPES:
        load_type(tid, graph, mql, me, cache, type)

    quint = cache.TYPES.get(type, {}).get(name)
    if quint is None:
        if type != tobj:
            quint = find_property_by_name(tid, graph, mql, me, cache,
                                          tobj, tobj, name, False)

        if quint is None and allow_default:
            quint = (mql.lookup_id(tid, type) + '/' + name, None,
                     type, None, False)

    return quint


def find_property_by_id(tid, graph, mql, me, cache, id):

    guid = graph.find(tid, id, cache.CACHE)
    if guid is not None:
        triple = cache.PROPERTIES.get(guid)
        if triple is not None:
            return (id, guid) + triple

    declaring_type_id, name = id.rsplit('/', 1)
    if declaring_type_id == EMQL_NAMESPACE:
        if EMQL_NAMESPACE not in cache.TYPES:
            load_emql_namespace(tid, graph, mql, me, cache)
        declaring_type = EMQL_NAMESPACE
    else:
        declaring_type = graph.find(tid, declaring_type_id, cache.CACHE)
        if declaring_type is not None:
            if declaring_type not in cache.TYPES:
                load_type(tid, graph, mql, me, cache, declaring_type)
        else:
            raise NoSuchTypeError(tid, mql, declaring_type_id)
                
    if guid is None:
        quint = cache.TYPES[declaring_type].get(name)
        if quint is not None:
            return quint
    else:
        triple = cache.PROPERTIES.get(guid)
        if triple is not None:
            return (id, guid) + triple

    return (id, None, declaring_type, None, False)


def _walk(tid, graph, mql, me, cache,
          parent, queryfn, listfn, mergefn, *args):

    if parent:
        if isinstance(parent, list):
            reductions = []
            for query in parent:
                reduction = _walk(tid, graph, mql, me, cache, query,
                                  queryfn, listfn, mergefn, *args)
                if reduction:
                    reductions.append(reduction)

            if listfn is not None:
                listfn(tid, graph, mql, me, cache, parent, *args)

            return reductions

        elif isinstance(parent, dict):
            if mergefn is not None:
                reductions = mergefn(tid, graph, mql, me, cache, parent,
                                     queryfn, listfn, *args)
            else:
                reductions = {}
                for property, query in parent.items():
                    if property in RESERVED_KEYS:
                        continue
                
                    stop, _args = queryfn(tid, graph, mql, me, cache,
                                          parent, query, property,
                                          reductions, *args)
                    if stop:
                        continue

                    if isinstance(query, (list, dict)):
                        reduction = _walk(tid, graph, mql, me, cache, query,
                                          queryfn, listfn, None, *_args)
                        if reduction:
                            reductions[property] = reduction

            return reductions


def _emql_merge(tid, graph, mql, me, cache, parent, queryfn, listfn, *args):

    reductions = {}
    queries = {}
    sorts = None
    returns = None
    is_mql = True
    with_count = False

    for property, query in parent.items():

        if property == 'sort':
            if isinstance(query, list):
                sorts = query
            else:
                sorts = query.split(',')
            continue

        if property in ('count', 'estimate-count'):
            with_count = True
            continue

        if property in ('result-count', 'value'):
            reductions['insert'] = [(property, query)]
            is_mql = False
            del parent[property]
            continue

        if property == 'return':
            returns = []
            if isinstance(query, dict):
                for pair in query.iteritems():
                    return_, value = pair
                    if return_ in ('result-count', 'value'):
                        returns.append(pair)
                    elif return_ == 'count':
                        with_count = True
                    else:
                        queries.setdefault(return_, []).append(['return', pair])
                is_mql = False
                del parent[property]
            elif isinstance(query, basestring):
                for return_ in query.split(','):
                    if return_ == 'result-count':
                        returns.append(('result-count', None))
                        is_mql = False
                    elif return_.startswith('value:'):
                        returns.append(return_.split(':', 1))
                        is_mql = False
                    elif return_ not in ('count', 'estimate-count'):
                        returns.append((return_, None))
                        queries.setdefault(return_, []).append(['return', (return_, None)])
                        is_mql = False
                    else:
                        returns.append((return_, None))
            else:
                returns.append(('value', query))
                is_mql = False
            continue

        if property == 'limit':
            reductions['limit'] = query
            continue

        if property == 'optional':
            reductions['optional'] = query
            continue

        if property == 'cursor':
            if isinstance(query, basestring):
                query = decode_cursor(query)
            elif isinstance(query, dict) and 'cursor' in query:
                query['cursor'] = decode_cursor(query['cursor'])
            reductions['cursor'] = query
            del parent['cursor']
            continue

        for i in xrange(len(property)):
            if not property[-(i+1)] in "~|<>!=":
                break
        if i > 0:
            operator = property[-i:]
            property = property[:-i]
        else:
            operator = None

        if property in RESERVED_KEYS:
            continue

        queries.setdefault(property, []).append([operator, query])

    for property, query in queries.iteritems():
        stop, _args = queryfn(tid, graph, mql, me, cache,
                              parent, query, property, reductions, *args)
        is_emql, is_literal = stop
        
        if is_emql:
            is_mql = False
            continue

        if is_literal:
            continue

        for operator, _query in query:
            if operator is None:
                if isinstance(_query, list):
                    reduction = _walk(tid, graph, mql, me, cache, _query,
                                      queryfn, listfn, _emql_merge, *_args)
                    if reduction:
                        reductions[property] = reduction
                        is_mql = reduction[0]['mql']

                elif isinstance(_query, dict):
                    reduction = _walk(tid, graph, mql, me, cache, _query,
                                      queryfn, listfn, _emql_merge, *_args)
                    if reduction:
                        reductions[property] = reduction
                        is_mql = reduction['mql']
                        if not is_mql and 'return' in reduction:
                            reduction['cardinality'] = property
                            parent[property] = [parent[property]]
                            reductions[property] = [reduction]
                break

    if sorts is not None:
        mqlsorts = []
        for sort in sorts:
            if sort[0] in "+-":
                props = sort[1:].split('.')
            else:
                props = sort.split('.')
            try:
                reduce(lambda q, p: q[p] if isinstance(q, dict) else q[0][p],
                       props, parent)
                mqlsorts.append(sort)
            except KeyError:
                break
        if not mqlsorts:
            is_mql = False
            del parent['sort']
            reductions['sort'] = ','.join(sorts)
        elif len(mqlsorts) < len(sorts):
            is_mql = False
            parent['sort'] = ','.join(mqlsorts)
            reductions['sort'] = ','.join(sorts)

    if returns:
        for pair in returns:
            return_, value = pair
            if return_ in ('result-count', 'value'):
                reductions.setdefault('return', []).append(pair)
            elif return_ in ('count', 'estimate-count'):
                if not is_mql or len(returns) > 1:
                    raise NotImplementedError("emql " + return_)
        if not is_mql:
            parent.pop('return', None)

    if not is_mql and with_count:
        raise NotImplementedError("emql count, use result-count instead")

    reductions['mql'] = is_mql
    if is_mql:
        reductions['cleanup'] = []
    else:
        parent['emql:guid'] = None
        reductions['cleanup'] = ['emql:guid']

    return reductions


def _emql_preprocess(tid, graph, mql, me, cache,
                     parent, query, property, reductions,
                     type, tobj, calls):

    if property.startswith('!'):
        inverted = True
        property = property[1:]
    else:
        inverted = False

    if ':' in property:
        prefix, name = property.split(':', 1)
        id, guid, declaring_type, expected_type, is_literal = \
            find_property_by_name(tid, graph, mql, me, cache,
                                  type, tobj, prefix)

        if id not in cache.ADAPTERS:
            if name.startswith('/'):
                id, guid, declaring_type, expected_type, is_literal = \
                    find_property_by_id(tid, graph, mql, me, cache, name)
                name = name.rsplit('/', 1)[1]
            else:
                id, guid, declaring_type, expected_type, is_literal = \
                    find_property_by_name(tid, graph, mql, me, cache,
                                          type, tobj, name)
            if id in cache.ADAPTERS:
                raise NotImplementedError("prefixed eMQL properties")

            if guid is None:
                raise NoSuchPropertyError(tid, mql, type, property)

        else:
            if inverted:
                raise NotImplementedError("inverted eMQL properties")

            for pair in query:
                if pair[0] is None:
                    pair[0] = 'insert'
                    pair[1] = (property, pair[1])
                    break
            if calls.add(tid, graph, mql, me, cache, id, property, name,
                         parent, query, reductions):
                return (True, False), (expected_type, tobj, calls)
            else:
                raise NoSuchPropertyError(tid, mql, type, property)

        if inverted:
            expected_type = declaring_type

        return (False, is_literal), (expected_type, tobj, calls)

    if property.startswith('/'):
        id, guid, declaring_type, expected_type, is_literal = \
            find_property_by_id(tid, graph, mql, me, cache, property)
        name = property.rsplit('/', 1)[1]
    else:
        id, guid, declaring_type, expected_type, is_literal = \
            find_property_by_name(tid, graph, mql, me, cache,
                                  type, tobj, property)
        name = property

    if id in cache.ADAPTERS:
        if inverted:
            raise NotImplementedError("inverted eMQL properties")
                
        if calls.add(tid, graph, mql, me, cache, id, property, name,
                     parent, query, reductions):
            return (True, False), (expected_type, tobj, calls)
        else:
            raise NoSuchPropertyError(tid, mql, type, property)
    elif guid is None:
        raise NoSuchPropertyError(tid, mql, type, property)

    if inverted:
        expected_type = declaring_type

    return (False, is_literal), (expected_type, tobj, calls)


def _emql_collect(tid, graph, mql, me, cache,
                  parent, query, property, reductions, calls):

    if calls.collect(property, parent):
        return True, (calls,)

    return False, (calls,)


def _emql_insert(tid, graph, mql, me, cache,
                 parent, query, property, x, calls, reductions, mqlcursor):

    if calls.insert(property, parent, query):
        return True, (calls, reductions, mqlcursor)

    if isinstance(reductions, list):
        reductions = reductions[0].get(property, {})
    else:
        reductions = reductions.get(property, {})

    return False, (calls, reductions, None)


def _emql_delete(tid, graph, mql, me, cache, parent,
                 calls, reductions, mqlcursor):

    if parent and isinstance(parent[0], dict):
        i = 0
        while i < len(parent):
            if parent[i].get(':delete', False):
                del parent[i]
            elif [':delete'] in parent[i].itervalues():
                del parent[i]
            else:
                i += 1

        if reductions:
            cursor = reductions[0].get('cursor')
            if isinstance(cursor, dict):
                if cursor.get(':cursor', True) is None:
                    mqlcursor = False

        if not parent and mqlcursor is False:
            parent.append(':delete')


def _emql_sub(tid, graph, mql, me, cache, reductions, reduction, property, x, 
              mqlquery, mqlres, calls, morefn):

    if property not in ('cursor', 'sort', 'limit', 'cleanup',
                        'optional', 'return', 'insert', 'mql', 'cardinality'):
        if (isinstance(mqlres, list) and mqlres and
            isinstance(mqlres[0], dict) and 'emql:guid' in mqlres[0]):
            _mqlquery = mqlquery[0][property]
            constraint = {'guid': None}
            if property.startswith('!'):
                key = 'emql:' + property[1:]
            else:
                key = '!emql:' + property

            if isinstance(_mqlquery, list):
                _mqlquery = [_mqlquery[0].copy()]
                _mqlquery[0][key] = constraint
                reductions[property][0]['cleanup'].append(key)
            elif isinstance(_mqlquery, dict):
                _mqlquery = _mqlquery.copy()
                _mqlquery[key] = constraint
                reductions[property]['cleanup'].append(key)

            results = []
            for _mqlres in mqlres:
                constraint['guid'] = _mqlres['emql:guid']
                _walk(tid, graph, mql, me, cache, reduction,
                      _emql_sub, morefn, None,
                      _mqlquery, _mqlres[property], calls, morefn)
                _val = _mqlres[property]
                if isinstance(_val, list):
                    if _val != [':delete']:
                        results.append(_mqlres)
                elif isinstance(_val, dict):
                    if not _val.get(':delete', False):
                        results.append(_mqlres)
                else:
                    results.append(_mqlres)

            mqlres[:] = results
                        
        elif isinstance(mqlres, dict) and 'emql:guid' in mqlres:
            _mqlquery = mqlquery[property]
            constraint = { 'guid': mqlres['emql:guid'] }
            if property.startswith('!'):
                key = 'emql:' + property[1:]
            else:
                key = '!emql:' + property

            if isinstance(_mqlquery, list):
                _mqlquery[0][key] = constraint
                reductions[property][0]['cleanup'].append(key)
            elif isinstance(_mqlquery, dict):
                _mqlquery[key] = constraint
                reductions[property]['cleanup'].append(key)
                    
            _walk(tid, graph, mql, me, cache, reduction,
                  _emql_sub, morefn, None,
                  _mqlquery, mqlres[property], calls, morefn)
            val = mqlres[property]
            if isinstance(val, list) and val == [':delete']:
                mqlres[':delete'] = True
            elif isinstance(val, dict) and val.get(':delete', False):
                mqlres[':delete'] = True

    return True, (mqlquery, mqlres, calls)


def _emql_more(tid, graph, mql, me, cache, reductions,
               mqlquery, mqlres, calls, morefn):

    if reductions:
        x, cursor = _get_cursor(mqlquery, reductions)

        mqlcursor = cursor[':cursor']
        if mqlcursor and cursor['crawl']:
            if mqlcursor is True:
                del mqlres[:]

            reduction = reductions[0]
            limit = reduction.get('limit', 100)
            while mqlcursor and len(mqlres) < limit:
                _mqlres, mqlcursor = _emql_query_(tid, graph, mql, me, cache,
                                                  mqlquery, mqlcursor,
                                                  calls, reductions, None)
                _walk(tid, graph, mql, me, cache,
                      reductions, _emql_sub, None, None,
                      mqlquery, _mqlres, calls, None)

                if _mqlres != [':delete']:
                    mqlres.extend(_mqlres)

                cursor['crawl'] -= 1
                if not cursor['crawl']:
                    break

            if not mqlcursor:
                if len(mqlres) < cursor.get('at-least', 1):
                    mqlres[:] = [':delete']

        elif not mqlres:

            if isinstance(mqlquery, list):
                if mqlquery and mqlquery[0]:
                    optional = mqlquery[0].get('optional', False)
                else:
                    optional = True
            elif isinstance(mqlquery, dict):
                if mqlquery:
                    optional = mqlquery.get('optional', False)
                else:
                    optional = True

            if not optional:
                mqlres[:] = [':delete']

  
def _emql_reduce(tid, graph, mql, me, cache, parent, reduction, property, x, 
                 mqlres, calls):

    if property in ('cursor', 'optional', 'mql', 'cardinality'):
        pass

    elif property == 'cleanup':
        if isinstance(mqlres, list) and mqlres and isinstance(mqlres[0], dict):
            for _mqlres in mqlres:
                for key in reduction:
                    _mqlres.pop(key, None)            
        elif isinstance(mqlres, dict):
            for key in reduction:
                mqlres.pop(key, None)

    elif isinstance(mqlres, list) and mqlres and isinstance(mqlres[0], dict):
        if property == 'sort':
            if (len(mqlres) > 1 and
                not ('return' in parent or 'insert' in parent)):
                _do_sort(reduction, mqlres)
                if 'limit' in parent:
                    del mqlres[parent['limit']:]

        elif property in ('return', 'insert'):
            if 'sort' in parent:
                _do_sort(parent['sort'], mqlres)
            if 'limit' in parent:
                del mqlres[parent['limit']:]

            returns = {}
            for pair in reduction:
                prop, value = pair
                if prop == 'result-count':
                    value = len(mqlres)
                elif prop != 'value':
                    prop, value = calls.reduce(tid, graph, mql, me, cache,
                                               prop, mqlres)
                returns[prop] = value

            if property == 'return':
                mqlres[:] = [returns]
            else:
                for key, value in returns.iteritems():
                    if ':' in key:
                        op, props = key.split(':', 1)
                        props = props.split('.')
                        prop = ':'.join((op, props.pop()))
                    else:
                        props = key.split('.')
                        prop = props.pop()
                    for _mqlres in mqlres:
                        _mqlres_ = reduce(_emql_get, props, _mqlres)
                        _mqlres_[prop] = value

        elif property == 'limit':
            if not ('sort' in parent or
                    'return' in parent or
                    'insert' in parent):
                del mqlres[reduction:]

        elif property in mqlres[0]:
            for _mqlres in mqlres:
                _walk(tid, graph, mql, me, cache, reduction,
                      _emql_reduce, None, None, _mqlres[property], calls)
                if isinstance(reduction, list) and _mqlres[property]:
                    if reduction[0].get('cardinality') == property:
                        if len(_mqlres[property]) > 1:
                            raise CardinalityError(_mqlres[property])
                        _mqlres[property] = _mqlres[property][0]

    elif isinstance(mqlres, dict):
        if property in mqlres:
            _walk(tid, graph, mql, me, cache, reduction,
                  _emql_reduce, None, None, mqlres[property], calls)

    return True, (mqlres, calls)


def _emql_get(mqlres, prop):

    if isinstance(mqlres, dict):
        return mqlres[prop]
    else:
        value = mqlres[0]
        if isinstance(value, dict):
            value = value[prop]
        return value


def _do_sort(reduction, mqlres):

    sorts = []
    for sort in reduction.split(','):
        if sort.startswith('-'):
            sort = sort[1:]
            reverse = True
        elif sort.startswith('+'):
            sort = sort[1:]
            reverse = False
        else:
            reverse = False
        sorts.append((sort.split('.'), reverse))

    def compare(r0, r1):
        for sort, reverse in sorts:
            v0 = reduce(_emql_get, sort, r0)
            v1 = reduce(_emql_get, sort, r1)
            if reverse:
                eq = cmp(v1, v0)
            else:
                eq = cmp(v0, v1)
            if eq != 0:
                return eq
        return 0
                
    mqlres.sort(compare)


def _get_cursor(query, reductions, default=None, parameter=None):

    if isinstance(query, list):
        if reductions:
            reduction = reductions[0]
            return_ = reduction.get('return')
            if return_ is not None:
                is_count = 'count' in return_ or 'estimate-count' in return_
            else:
                is_count = False
            if 'cursor' in reduction:
                cursor = reduction['cursor']
            else:
                cursor = { ':cursor': default }
                reduction['cursor'] = cursor
        else:
            is_count = False
            cursor = { ':cursor': default }
            reductions = [{'cursor': cursor}]

        limit = query[0].get('limit', 100)
            
        if cursor is True:
            cursor = { ':cursor': True }
            with_cursor = True
            reductions[0]['cursor'] = cursor

        elif cursor is False:
            cursor = { ':cursor': None }
            with_cursor = False
            reductions[0]['cursor'] = cursor

        elif isinstance(cursor, dict):
            if ':cursor' not in cursor:
                pagesize = cursor.get('pagesize', limit)
                if pagesize > 0:
                    cursor[':cursor'] = cursor.get('cursor', True)
                    with_cursor = not not cursor[':cursor']
                    if pagesize != limit:
                        query[0]['limit'] = pagesize
                else:
                    cursor[':cursor'] = None
                    with_cursor = False
            else:
                with_cursor = None

        elif isinstance(cursor, (int, long)):
            pagesize = cursor
            if pagesize > 0:
                cursor = { ':cursor': True, 'pagesize': pagesize }
                with_cursor = True
                if pagesize != limit:
                    query[0]['limit'] = pagesize
            else:
                cursor = { ':cursor': None }
                with_cursor = False
            reductions[0]['cursor'] = cursor

        elif isinstance(cursor, (str, unicode)):
            cursor = { ':cursor': cursor }
            with_cursor = True
            reductions[0]['cursor'] = cursor

        elif parameter is not None:
            raise ParameterError(parameter, "invalid cursor: '%s'" %(cursor))

        else:
            raise ValueError(cursor)

        if is_count:
            cursor[':cursor'] = None
            with_cursor = False

    else:
        cursor = { ':cursor': None }
        with_cursor = False

    if cursor[':cursor']:
        crawl = cursor.get('crawl', True)
        if crawl is True:
            cursor['crawl'] = CRAWL_LIMIT
    else:
        cursor['crawl'] = False

    return with_cursor, cursor


def _emql_query_(tid, graph, mql, me, cache,
                 query, mqlcursor, calls, reductions, parameter):

    try:
        if calls.control['debug']:
            mqlres, mqlcursor, cost = mql.mqlread(tid, query, cursor=mqlcursor,
                                                  cost=True)
            calls.costs.append([query, cost])
        else:
            mqlres, mqlcursor = mql.mqlread(tid, query, cursor=mqlcursor)
    except (MQLInternalError, MQLInternalParseError):
        raise
    except MQLError, e:
        if parameter is not None:
            raise ParameterError(parameter, "invalid MQL query or adapter 'pre' return clauses. %s: %s" %(str(e), json.dumps(query, indent=2, sort_keys=True).replace('\n', '\\n')))
        raise

    # mqlcursor should be a cursor or False after mqlread
    # a count query doesn't satisfy this
    if mqlcursor is True:
        mqlcursor = False

    _walk(tid, graph, mql, me, cache, mqlres, _emql_collect, None, None, calls)

    try:
        calls.fetch(tid, graph, mql, me, cache)
    except AdapterError, e:
        if parameter is not None:
            raise ParameterError(parameter, "adapter 'fetch' phase failed: %s" %(str(e)))
        raise

    _walk(tid, graph, mql, me, cache, mqlres, _emql_insert, _emql_delete, None,
          calls, reductions, mqlcursor)

    return mqlres, mqlcursor


def _emql_query(tid, graph, mql, me, cache,
                query, control, api_keys, parameter=None):

    calls = AdapterCalls(control, api_keys)
    tobj = graph.find(tid, '/type/object', cache.CACHE)
    debug = control['debug']

    if debug not in (True, False):
        stop_after_pre = debug == 'pre'
        stop_after_fetch = debug == 'fetch'
        control['debug'] = debug != "false"
    else:
        stop_after_pre = False
        stop_after_fetch = False

    try:
        if isinstance(query, list):
            if not (query and isinstance(query[0], dict)):
                raise OuterQueryShapeError(query)
            type_id = query[0].get('type')
        elif isinstance(query, dict):
            type_id = query.get('type')
        else:
            raise OuterQueryShapeError(query)

        if type_id is None or isinstance(type_id, (list, dict)):
            type = tobj
        elif isinstance(type_id, basestring) and type_id.startswith('/'):
            type = graph.find(tid, type_id, cache.CACHE)
            if type is None:
                raise NoSuchTypeError(tid, mql, type_id)
        else:
            raise TypeError("Invalid type id: %s" %(type_id))

        if type_id == '/type/link':
            mqlres, x = mql.mqlread(tid, query)
            return None, None, mqlres

        phase = None
        with_return = False
        if isinstance(query, dict):
            with_return = 'return' in query

        reductions = _walk(tid, graph, mql, me, cache,
                           query, _emql_preprocess, None,
                           _emql_merge, type, tobj, calls)

        phase = 'pre'
        calls.pre(tid, graph, mql, me, cache)

        if stop_after_pre:
            return calls, None, query

        was_single = False
        if with_return and reductions.get('mql') is False:
            query = [query]
            reductions = [reductions]
            was_single = True

        cursor = mql.get_cursor()
        if cursor is None:
            with_cursor, cursor = _get_cursor(query, reductions, True,
                                              parameter)
        else:
            with_cursor = cursor is not None
            cursor = {':cursor': cursor}
            
        phase = 'fetch'
        mqlres, cursor[':cursor'] = \
            _emql_query_(tid, graph, mql, me, cache, query, 
                         cursor[':cursor'], calls, reductions, parameter)

        _walk(tid, graph, mql, me, cache,
              reductions, _emql_sub, _emql_more, None,
              query, mqlres, calls, _emql_more)

        if stop_after_fetch:
            return calls, cursor[':cursor'], mqlres

        phase = 'reduce'
        if isinstance(mqlres, list) and mqlres == [':delete']:
            del mqlres[:]
        elif isinstance(mqlres, dict) and ':delete' in mqlres:
            mqlres = None
        else:
            _walk(tid, graph, mql, me, cache,
                  reductions, _emql_reduce, None, None, mqlres, calls)

        if was_single and mqlres:
            if len(mqlres) > 1:
                raise CardinalityError(mqlres)
            mqlres = mqlres[0]

    except AdapterError, e:
        if parameter is not None:
            raise ParameterError(parameter, "adapter '%s' phase failed: %s" %(phase, str(e)))
        raise

    except (PropertyError, ShapeError, TypeError), e:
        if parameter is not None:
            raise ParameterError(parameter, str(e))
        raise

    except NotImplementedError, e:
        if parameter is not None:
            raise ParameterError(parameter, "%s, not implemented" %(str(e)))
        raise

    return calls, cursor[':cursor'] if with_cursor else None, mqlres


def _emql_help(tid, graph, mql, me, cache, property, control, format,
               api_name, parameter=None):

    if 'timeout' not in control:
        control['timeout'] = EMQL_ADAPTER_TIMEOUT

    if property in ('extended', None):
        input = file(os.path.join(DOCS_BASE, 'emql.txt'))
        body = input.read()
        input.close()
        if format == 'rst':
            return 'text/x-rst', body
        elif format in (None, 'html'):
            body = publish_string(source=body, writer_name='html',
                                  settings_overrides=RST_SETTINGS)
            return "text/html", body
        else:
            raise NotImplementedError(format)

    elif property in ('gallery', '*'):
        query = [{
            "type": "/type/extension",
            "name": None,
            "augments": None,
            "key": [{
                "namespace": None,
                "value": None,
                "optional": True
            }],
            "help": True
        }]

        mqlres, x = mql.mqlread(tid, query, schema=True)

        props = []
        for prop in mqlres:
            augments = prop['augments']
            name = prop['name']
            keys = prop['key']
            if augments is not None and name is not None:
                if USE_AUGMENTS:
                    props.append('/'.join((augments, name)))
            elif keys:
                for key in keys:
                    name = key['value']
                    if name is not None:
                        namespace = key['namespace']
                        if namespace == EMQL_NAMESPACE:
                            props.append(EMQL_PREFIX + name)
                        else:
                            props.append('/'.join((namespace, name)))
        props.sort()

        if format in (None, 'html', 'rst'):
            rst = []
            title = "eMQL Property Gallery"
            rst.append('=' * len(title))
            rst.append(title)
            rst.append(rst[0])
            rst.append('')
            for prop in props:
                rst.append("| `%s <http:%s?help=%s>`_" %(prop, api_name, prop))
            rst.append('')

            if format == 'rst':
                return 'text/x-rst', '\n'.join(rst)

            body = publish_string(source='\n'.join(rst), writer_name='html',
                                  settings_overrides=RST_SETTINGS)
        
            return "text/html", body

        elif format == 'json':
            contents = [{"name": prop,
                         "key": prop.replace('/', '.'),
                         "content": "%s?help=%s" %(api_name, prop)}
                        for prop in props]

            return "text/plain; subtype=json; charset=UTF-8", contents

    else:
        tobj = graph.find(tid, '/type/object', cache.CACHE)

        if property.startswith(EMQL_PREFIX):
            property = '/'.join((EMQL_NAMESPACE, property[len(EMQL_PREFIX):]))

        if property.startswith('/'):
            quint = find_property_by_id(tid, graph, mql, me, cache, property)
        else:
            quint = find_property_by_name(tid, graph, mql, me, cache,
                                          tobj, tobj, property, True)
        id = quint[0]
        if id in cache.ADAPTERS:
            adapter = cache.ADAPTERS[id]
            if adapter.phases['help']:
                params = dict(property=id)
                try:
                    me.get_session().log('notice', 'emql.help.start', '',
                                         adapter=adapter.uri, params=params)
                    mimetype, body = adapter.help(tid, graph, mql, me, control,
                                                  params)
                finally:
                    me.get_session().log('notice', 'emql.help.end', '',
                                         adapter=adapter.uri)

                if (format in (None, 'html') and
                    mimetype.startswith('text/x-rst')):
                    mimetype = "text/html"
                    body = publish_string(source=body, writer_name='html',
                                          settings_overrides=RST_SETTINGS)

                return mimetype, body

            if parameter is not None:
                raise ParameterError(parameter, "no help for: %s" %(id))

            raise ValueError("no help for: %s" %(property))

        if parameter is not None:
            raise ParameterError(parameter, "not an eMQL property: %s" %(id))

        raise ValueError("no such property: %s" %(property))


def read(tid, mss, cache, query, control, api_keys, help=False):

    if control is None:
        control = {'cache': True, 'debug': False}

    if 'deadline' not in control:
        deadline = mss.varenv.get('deadline', None)
        if deadline:
            control['deadline'] = deadline

    me = None
    try:
        graph = mss.ctx.graphdb
        mql = mql_interface(mss, None)
        me = me_interface(mss, None)

        def deepcopy(q):
            if type(q) is list:
                return [deepcopy(_q) for _q in q]
            elif type(q) is dict:
                return dict((_k, deepcopy(_v)) for _k, _v in q.iteritems())
            else:
                return q
        
        query = deepcopy(query)
        me.get_session().log('notice', 'emql.request.start', '',
                             query=query, help=help)

        if help:
            # non False help represents format
            mimetype, content = \
                _emql_help(tid, graph, mql, me, cache, query, control, help,
                           '/api/service/mqlread')
            return mimetype, content

        calls, cursor, mqlres = \
            _emql_query(tid, graph, mql, me, cache,
                        query, control, api_keys, None)

    except MQLError:
        raise
    except Exception, e:
        if control['debug']:
            raise
        raise EMQLError(e, query)
    finally:
        if me is not None:
            me.get_session().log('notice', 'emql.request.end', '')

    if control['debug'] and calls is not None:
        debug = calls.get_debug()
    else:
        debug = None

    return debug, cursor, mqlres

#
# Install a property and its adapter programmatically bypassing graphd
# registration, a development feature.
#
#  mss: an ME session object
#
#  cache: an instance of emql_cache to hold the eMQL schema cache
#
#  property_id: the id of the fake extension property to install or the real
#               property to override
#
#  adapter_class: the python class implementing the adapter (optional)
#
#  uri: the URL to the adapter implementation (optional unless adapter_class
#       is unspecified)
#
#  api_key_names: the names of api keys to pass to the adapters (optional,
#                 unless the adapter calls external APIs that require API
#                 keys) 
#
#  pre, fetch, reduce, help:
#    True or False for the phases the adapter implements. If the adapter
#    is implemented by a local Python class, that is, uri is unspecified,
#    these values are deduced from the adapter_class.
#

def install_property(tid, mss, cache, property_id,
                     adapter_class=HTTPAdapter, uri=None, api_key_names=(),
                     foreign_apis=(),
                     pre=True, fetch=True, reduce=False, help=False):

    graph = mss.ctx.graphdb
    mql = mql_interface(mss, None)
    me = me_interface(mss, None)

    id, guid, declaring_type, expected_type, is_literal = \
        find_property_by_id(tid, graph, mql, me, cache, property_id)

    if id in cache.ADAPTERS:
        me.get_session().log('warning', 'emql.install.property',
                             'overriding adapter', property=id)

    if uri is None:
        uri = 'python:' + '.'.join((adapter_class.__module__,
                                    adapter_class.__name__))

    if adapter_class is not HTTPAdapter:
        names = set(dir(adapter_class))
        pre = 'pre' in names
        fetch = 'fetch' in names
        reduce = 'reduce' in names
        help = 'help' in names

    cache.ADAPTERS[id] = adapter_class(tid, graph, mql, me, cache,
                                       uri, api_key_names,
                                       foreign_apis,
                                       pre, fetch, reduce, help)


class Runner(Thread):

    def __init__(self):
        super(Runner, self).__init__(name="emql_runner")

        self._event = Event()
        self.event = Event()
        self.task = None

        self.live = True
        self.setDaemon(True)
        self.start()

    def run(self):

        while self.live:
            self._event.wait()
            self.exception = None
            call, method, tid, graph, mql, me, adapter = self.task
            try:
                # graphdb handle cannot be used by more than one thread
                method(tid, None, mql, me, adapter)
            except AdapterError, e:
                self.exception = e
            except Exception, e:
                self.exception = AdapterError(str(e), format_exc())
            finally:
                self._event.clear()
                self.event.set()

    def assign(self, task):

        self.task = task
        self.event.clear()
        self._event.set()


class Runners(Queue):

    def __init__(self, runners_max):
        Queue.__init__(self, 0)

        self.runners_max = runners_max
        self.runners_count = 0

    def get(self, timeout=None):

        wait = False
        while True:
            try:
                return Queue.get(self, wait, timeout)
            except Empty:
                if wait:
                    raise TimeoutError
                if self.runners_count < self.runners_max:
                    self.runners_count += 1
                    return Runner()
                wait = True
                continue

RUNNERS = Runners(32)


class AdapterCall(object):

    def __init__(self, key, params, property, id, phases, parent,
                 api_keys, control):

        self.key = key
        self.mqlres = []
        self.params = params
        self.property = property
        self.id = id
        self.phases = phases
        self.parent = parent
        self.api_keys = api_keys
        self.control = control.copy()
        self.shape = 'nil'

        self.log = {}
        self.headers = {}
        self.costs = {}

        parent[':'.join((key, 'guid'))] = None

        def _control(query):
            _query = query
            if isinstance(_query, list) and _query:
                _query = query[0]
            if isinstance(_query, dict):
                for _key, _value in _query.items():
                    if _key == 'emql:query':
                        query = _value
                    elif _key.startswith('emql:'):
                        self.control[_key[5:]] = _query.pop(_key)
            return query

        if 'query' in params:
            query = params['query'] = _control(params['query'])

            if query is None:
                self.shape = 'none'
                self.optional = 'constraints' not in params
            elif isinstance(query, list):
                if query:
                    if isinstance(query[0], dict):
                        self.shape = '[{}]'
                        self.optional = query[0].get('optional', False)
                    else:
                        raise QueryShapeError(query)
                else:
                    self.shape = '[]'
                    self.optional = True
            elif isinstance(query, dict):
                self.shape = '{}'
                self.optional = query.get('optional', not query)
            elif isinstance(query, MQL_LITERAL_TYPES):
                self.shape = 'literal'
                self.optional = False
            else:
                raise QueryShapeError(query)
        else:
            self.optional = 'constraints' not in params

        for pair in params.get('constraints', ()):
            pair[1] = _control(pair[1])

        if 'timeout' not in self.control:
            self.control['timeout'] = EMQL_ADAPTER_TIMEOUT

    def get_timeout(self):

        if 'deadline' in self.control:
            timeout = self.control['deadline'] - time.time()
        else:
            timeout = EMQL_ADAPTER_TIMEOUT

        if self.control['timeout'] > timeout:
            self.control['timeout'] = timeout

        return timeout

    def check_shape(self, result):

        if result is NIL:
            if self.shape == '{}':
                return None
            if self.shape in ('[]', '[{}]'):
                return []
            return None
        elif self.shape in ('none', 'literal'):
            if isinstance(result, MQL_LITERAL_TYPES):
                return result
        elif self.shape == '[]':
            if isinstance(result, list):
                if not result or isinstance(result[0], MQL_LITERAL_TYPES):
                    return result
        elif self.shape == '{}':
            if isinstance(result, dict):
                return result
        elif self.shape == '[{}]':
            if isinstance(result, list):
                if not result or isinstance(result[0], dict):
                    return result

        raise ResultShapeError(result, self.params['query'])

    def pre(self, tid, graph, mql, me, adapter):

        uri = self.control.get('url', adapter.uri)
        try:
            me.get_session().log('notice', 'emql.pre.start', '',
                                 adapter=uri, params=self.params)
            result = adapter.pre(tid, graph, mql, me, self.control,
                                 self.parent, self.params, self.api_keys)
            if not isinstance(result, dict):
                raise ResultTypeError('pre', property, uri, type(result), dict)
        except AdapterError:
            raise
        except Exception:
            if self.control['debug']:
                raise
            raise AdapterException(uri, format_exc())
        finally:
            me.get_session().log('notice', 'emql.pre.end', '', adapter=uri)

        for name, value in result.iteritems():
            if name.startswith(':'):
                if name in (':log', ':headers', ':costs'):
                    getattr(self, name[1:])['pre'] = value
                elif name == ':extras':
                    self.extras = value
            elif name == 'limit':
                limit = self.parent.get('limit', MQL_LIMIT)
                if limit > value:
                    raise ParameterError("query", "limit %d is too large for property %s (max %d)'" %(limit, property, value))
            elif name == 'optional':
                pass
            elif name != 'guid':
                self.parent[':'.join((self.key, name))] = value

    def fetch(self, tid, graph, mql, me, adapter):

        if hasattr(self, 'extras'):
            self.params[':extras'] = self.extras

        uri = self.control.get('url', adapter.uri)
        result = None
        try:
            me.get_session().log('notice', 'emql.fetch.start', '',
                                 adapter=uri, params=self.params)
            result = adapter.fetch(tid, graph, mql, me, self.control,
                                   self.mqlres, self.params, self.api_keys)

            if not isinstance(result, dict):
                raise ResultTypeError('fetch', self.property,
                                      uri, type(result), dict)

            if ':error' in result:
                raise AdapterUserError('fetch', self.property,
                                       uri, result[':error'])
        except AdapterUserError, e:
            value = self.control.get('error', NIL)
            if value is NIL:
                raise
            if value == ':error':
                value = e.error
            result = dict((res['guid'], value) for res in self.mqlres)
        except AdapterError:
            value = self.control.get('error', NIL)
            if value is NIL:
                raise
            result = dict((res['guid'], value) for res in self.mqlres)
        except Exception:
            if self.control['debug']:
                raise
            raise AdapterException(uri, format_exc())
        finally:
            me.get_session().log('notice', 'emql.fetch.end', '', adapter=uri)

        for name in result.keys():
            if name.startswith(':'):
                if name in (':log', ':headers', ':costs'):
                    getattr(self, name[1:])['fetch'] = result.pop(name)
                elif name == ':extras':
                    self.extras = result.pop(name)

        self.results = result
        del self.mqlres[:]

        return result

    def reduce(self, tid, graph, mql, me, adapter, mqlres):

        if hasattr(self, 'extras'):
            self.params[':extras'] = self.extras

        uri = self.control.get('url', adapter.uri)
        try:
            me.get_session().log('notice', 'emql.reduce.start', '',
                                 adapter=uri, params=self.params)
            result = adapter.reduce(tid, graph, mql, me, self.control,
                                    mqlres, self.params, self.api_keys)
            
            if not isinstance(result, dict):
                raise ResultTypeError('reduce', self.property,
                                      uri, type(result), dict)
            if ':error' in result:
                raise AdapterUserError('reduce', self.property,
                                       uri, result[':error'])
        except AdapterUserError, e:
            value = self.control.get('error', NIL)
            if value is NIL:
                raise
            if value == ':error':
                value = e.error
            result = {'value': value}
        except AdapterError:
            value = self.control.get('error', NIL)
            if value is NIL:
                raise
            result = {'value': value}
        except Exception:
            if self.control['debug']:
                raise
            raise AdapterException(uri, format_exc())
        finally:
            me.get_session().log('notice', 'emql.reduce.end', '', adapter=uri)

        for name in result.keys():
            if name.startswith(':'):
                getattr(self, name[1:])['reduce'] = result.pop(name)

        return result


class AdapterCalls(object):

    def __init__(self, control, envelope_api_keys):

        self.count = 0
        self.calls = {}
        self.costs = []
        self.control = control

        # the apis that come in from the mql envelope
        self.envelope_api_keys = envelope_api_keys or {}

    def add(self, tid, graph, mql, me, cache, id, property, name,
            parent, query, reductions):

        if id in cache.ADAPTERS:
            params = {'property': property}
            reduction = 'insert'
            for operator, _query in query:
                if operator is None:
                    params['query'] = _query
                    del parent[property]
                elif operator in ('return', 'insert'):
                    x, _query = _query
                    params['query'] = _query
                    reduction = operator
                    parent.pop(property, None)
                else:
                    params.setdefault('constraints', []).append([operator,
                                                                 _query])
                    del parent[property + operator]

            adapter = cache.ADAPTERS[id]
            phases = adapter.phases

            # XXX to resolve: get_api_keys() returns a list of keys, keyed
            # by their foreign_api_id, but envelope_api_keys is a dict
            if hasattr(adapter, 'foreign_apis'):
                api_keys = me.get_session().get_api_keys(id, adapter.foreign_apis)
            else:
                api_keys = dict((key_name, self.envelope_api_keys.get(key_name))
                                for key_name in adapter.api_key_names)

            if ':' in property:
                name, path = property.split(':', 1)
                params['property'] = name
                _query = params['query']
                if isinstance(_query, dict):
                    _query['value'] = path
                else:
                    params['query'] = { 'value': path }

            self.count += 1
            key = "emql_%d_%s" %(self.count, name.replace(EMQL_PREFIX, ''))

            self.calls[key] = AdapterCall(key, params, property, id, phases,
                                          parent, api_keys, self.control)

            if phases['reduce']:
                if reduction in ('return', 'insert'):
                    reductions.setdefault(reduction, []).append((key, None))
                else:
                    reductions.setdefault(reduction, []).append(key)

            return True

        return False

    def collect(self, property, parent):

        if ':' in property:
            key, property = property.split(':', 1)
            if property == 'guid' and key in self.calls:
                call = self.calls[key]
                if call.phases['fetch']:
                    prefix = key + ':'
                    mqlres = {}
                    for property, query in parent.iteritems():
                        if property.startswith(prefix):
                            x, property = property.split(':', 1)
                            mqlres[property] = query
                    call.mqlres.append(mqlres)
                return True

        return False

    def insert(self, property, parent, guid):

        if ':' in property:
            key, name = property.split(':', 1)
            call = self.calls.get(key)
            if call is not None:
                del parent[property]
                if name == 'guid' and call.phases['fetch']:
                    results = call.results
                    if 'query' in call.params:

                        if call.optional:
                            parent[call.property] = call.check_shape(results.get(guid, NIL))
                        elif guid in results:
                            parent[call.property] = call.check_shape(results[guid])
                        else:
                            parent[':delete'] = True

                    elif guid not in results:
                        parent[':delete'] = True

                return True

        return False

    def get_timeout(self):

        if 'deadline' in self.control:
            timeout = self.control['deadline'] - time.time()
        else:
            timeout = EMQL_ADAPTER_TIMEOUT

        return min(self.control.get('timeout', EMQL_ADAPTER_TIMEOUT), timeout)

    def run_tasks(self, sync, async, timeout):

        def _run(_task):
            runner = RUNNERS.get(timeout)
            runner.assign(_task)
            return _task[0], runner

        runners = [_run(_task) for _task in async]
        try:
            for task in sync:
                task[1](*task[2:])
        finally:
            for call, runner in runners:
                try:
                    runner.event.wait(call.get_timeout())
                    if not runner.event.isSet():
                        raise TimeoutError
                    if runner.exception is not None:
                        raise runner.exception
                finally:
                    RUNNERS.put(runner)

    def pre(self, tid, graph, mql, me, cache):

        sync = []
        async = []
        for call in self.calls.itervalues():
            if call.phases['pre']:
                no_async = not call.control.get('async', True)
                _task = (call, call.pre, tid, graph, mql, me,
                         cache.ADAPTERS[call.id])
                if (not sync) or no_async:
                    sync.append(_task)
                else:
                    async.append(_task)

        if sync:
            self.run_tasks(sync, async, self.get_timeout())

    def fetch(self, tid, graph, mql, me, cache):

        sync = []
        async = []
        for call in self.calls.itervalues():
            if call.phases['fetch']:
                if call.mqlres:
                    no_async = not call.control.get('async', True)
                    _task = (call, call.fetch, tid, graph, mql, me,
                             cache.ADAPTERS[call.id])
                    if (not sync) or no_async:
                        sync.append(_task)
                    else:
                        async.append(_task)
                else:
                    call.results = {}

        if sync:
            self.run_tasks(sync, async, self.get_timeout())

    def reduce(self, tid, graph, mql, me, cache, key, mqlres):

        call = self.calls[key]
        if call.phases['reduce']:
            result = call.reduce(tid, graph, mql, me, cache.ADAPTERS[call.id],
                                 mqlres)
        else:
            raise AdapterError('no reduce phase for %s' %(call.property))

        return call.property, result['value']

    def get_debug(self):

        def get_debug(name):
            return [{ 'property': call.property,
                      'pre': getattr(call, name).get('pre', []),
                      'fetch': getattr(call, name).get('fetch', []),
                      'reduce': getattr(call, name).get('reduce', []) }
                    for call in self.calls.itervalues()]

        debug = { 'logs': get_debug('log'),
                  'costs': get_debug('costs'),
                  'headers': get_debug('headers'),
                  'mql_costs': self.costs }

        return debug


class ParameterError(ValueError):

    def __init__(self, parameter, *args):

        super(ParameterError, self).__init__(*args)
        self.parameter = parameter

    def __str__(self):

        return "Parameter %s: %s" %(self.parameter,
                                    super(ParameterError, self).__str__())


class CardinalityError(ValueError):
    def __str__(self):
        return "Unique query may have at most one result. Got %d" %(len(self.args[0]))

class PropertyError(ValueError):
    def __init__(self, tid, mql, *args):
        super(PropertyError, self).__init__(*args)
        self.tid = tid
        self.mql = mql

class NoSuchTypeError(PropertyError):
    def __str__(self):
        return "Type %s not found" %(self.args[0])

class NoSuchPropertyError(PropertyError):
    def __str__(self):
        type, name = self.args
        if name.startswith('/'):
            type, name = name.rsplit('/', 1)
        elif name.startswith(EMQL_PREFIX):
            type, name = EMQL_NAMESPACE, name[len(EMQL_PREFIX):]
        else:
            type = self.mql.lookup_id(self.tid, type)
        return "Type %s does not have property %s" %(type, name)

class ShapeError(TypeError):
    pass

class QueryShapeError(ShapeError):
    def __init__(self, query, *args):
        super(QueryShapeError, self).__init__(*args)
        self.query = query
    def __str__(self):
        return "Query shape be a literal, None, {} or [{}]: %s" %(self.query)

class OuterQueryShapeError(QueryShapeError):
    def __str__(self):
        return "Query shape must be {} or [{}]: %s" %(self.query)

class ResultShapeError(ShapeError):
    def __init__(self, query, result, *args):
        super(ResultShapeError, self).__init__(*args)
        self.query = query
        self.result = result
    def __str__(self):
        return "Result shape '%s' doesn't match query shape '%s'" %(self.result, self.query)


class TimeoutError(ValueError):
    pass


class EMQLError(MQLError):

    def __init__(self, exception, query):

        if isinstance(exception, basestring):
            message = exception
            exception = None
            args = ()
        else:
            message = "%s: %s" %(type(exception).__name__, str(exception))
            args = exception.args

        super(EMQLError, self).__init__('EMQL', None, message, *args)
        self.exception = exception
        self.error['query'] = query



class mss_interface(object):
    __slots__ = ('mss', 'parameter')

    def __init__(self, mss, parameter):

        self.mss = mss
        self.parameter = parameter


class mql_interface(mss_interface):

    def mqlread(self, tid, query, cursor=None, cost=False, lang=None,
                schema=False):

        if schema:
            prev_cursor = self.mss.varenv.pop('cursor', NIL)
            prev_lang = self.mss.varenv.pop('$lang', NIL)
            prev_asof = self.mss.varenv.pop('asof', NIL)
        else:
            varenv = {}
            if cursor:
                varenv['cursor'] = cursor
            if lang:
                varenv['$lang'] = lang
            if varenv:
                self.mss.push_varenv(**varenv)

        try:
            mqlres = self.mss.mqlread(query)
        except (MQLInternalError, MQLInternalParseError):
            raise
        except MQLError, e:
            if self.parameter is not None:
                raise ParameterError(self.parameter, str(e))
            raise
        finally:
            if schema:
                if prev_cursor is not NIL:
                    self.mss.varenv['cursor'] = prev_cursor
                if prev_lang is not NIL:
                    self.mss.varenv['$lang'] = prev_lang
                if prev_asof is not NIL:
                    self.mss.varenv['asof'] = prev_asof
            elif varenv:
                cursor = self.mss.varenv.get('cursor')
                self.mss.pop_varenv()

        if cost:
            return mqlres, cursor, self.mss.get_cost()

        return mqlres, cursor

    def lookup_id(self, tid, guid):

        try:
            id = self.mss.lookup_id('#' + guid)
            return id or None
        except (MQLInternalError, MQLInternalParseError):
            raise
        except MQLError, e:
            if self.parameter is not None:
                raise ParameterError(self.parameter, str(e))
            raise

    def lookup_id_guids(self, tid, id_guids):

        try:
            guids = [ig.guid for ig in id_guids.itervalues()]
            ids = self.mss.lookup_ids(guids)
            for ig in id_guids.itervalues():
                ig.id = ids[ig.guid] or None
        except (MQLInternalError, MQLInternalParseError):
            raise
        except MQLError, e:
            if self.parameter is not None:
                raise ParameterError(self.parameter, str(e))
            raise

    def get_cursor(self):

        return self.mss.varenv.get('cursor')


class me_interface(mss_interface):

    def __init__(self, mss, parameter):

        super(me_interface, self).__init__(mss, parameter)

        proxy_addr = mss.config.get("me.external_proxy", None)
        if proxy_addr:
            x, self.proxy_host, x, x, x, x = urlparse(proxy_addr)
        else:
            self.proxy_host = None

        graphd_addr = mss.config.get('graphd.address', '').split(' ')[0]
        pieces = graphd_addr[0].split('.')

        if len(pieces) > 2:
            pod = pieces[1]
        else:
            pod = mss.config.get('graphd.pod') or 'emql'

        self.pod = pod

    def get_session(self):
        return self

    def get_config(self):
        return self.mss.config

    def get_graph(self):
        return self.mss.ctx.graph

    def relevance_query(self, tid, **query):
        return self.mss.relevance_query(**query)

    def geo_query(self, tid, **query):
        return self.mss.geo_query(**query)

    def fetch_blob(self, tid, blobid):
        return self.mss.ctx.blobd.get_blob(blobid)

    def http_connect(self, host, path, timeout=None, proxy=None):

        if self.proxy_host and proxy is not False:
            connection = HTTPConnection(self.proxy_host, timeout=timeout)
            connection.connect()
            return 'http://%s%s' %(host, path), connection

        return path, HTTPConnection(host, timeout=timeout)

    def bake_cookies(self, tid, domain):
        return self.mss.bake_cookies(tid, self.pod, domain)

    def mqlread(self, query, **kwds):
        return self.mss.mqlread(query, **kwds)

    def authenticate(self):
        return self.mss.authenticate()

    def get_user_id(self):
        return self.mss.get_user_id()

    def get_app_id(self):
        return self.mss.authorized_app_id

    def get_app_api_key(self, application_guid):
        return self.mss.get_app_api_key(application_guid)

    def get_api_keys(self, extension_id, *args, **kwds):
        return apikeys.get_api_keys(self.mss, extension_id, *args, **kwds)

    def log(self, level, event, message, **kwds):
        from mw.log import LOG
        getattr(LOG, level)(event, message, **kwds)


class id_guid(object):
    __slots__ = ('guid', 'id')

    def __init__(self, guid):
        self.guid = '#' + guid
        self.id = guid

    def __hash__(self):
        return hash(self.guid)

    def __str__(self):
        return self.id

    def __cmp__(self, other):
        return cmp(self.id, str(other))


class formatted_id_guid(object):
    __slots__ = ('format', 'ig')

    def __init__(self, format, ig):
        self.format = format
        self.ig = ig

    def __str__(self):
        return self.format % self.ig.id


class emql_cache(object):
    __slots__ = ('TYPES', 'PROPERTIES', 'CACHE', 'ADAPTERS')

    def __init__(self):

        for slot in self.__slots__:
            setattr(self, slot, {})

    def set_state(self, tid, mss, state):

        graph = mss.ctx.graphdb
        mql = mql_interface(mss, None)
        me = me_interface(mss, None)

        for slot, arg in izip(self.__slots__, state):
            setattr(self, slot, arg)

        for id, state in self.ADAPTERS.iteritems():
            scheme, x, path, x, x, x = urlparse(state[0])
            if scheme == 'http':
                adapter = HTTPAdapter(tid, graph, mql, me, self, *state)
            elif scheme == 'python':
                adapter = get_python_adapter(tid, graph, mql, me, self,
                                             path, *state)
            else:
                continue

            if adapter is not None:
                self.ADAPTERS[id] = adapter

    def get_state(self):

        return (self.TYPES, self.PROPERTIES, self.CACHE,
                dict((id, adapter._get_state())
                     for id, adapter in self.ADAPTERS.iteritems()))

    def clear(self):

        for slot in self.__slots__:
            getattr(self, slot).clear()

    def refresh(self, tid, graph, mql, me, type=None):

        if type is not None:
            if type == EMQL_NAMESPACE:
                load_emql_namespace(tid, graph, mql, me, self)
            else:
                load_type(tid, graph, mql, me, self, type)
        else:
            self.clear()

        return self.ADAPTERS.keys()

    def status(self):

        return [[id, adapter.uri] for id, adapter in self.ADAPTERS.iteritems()]
