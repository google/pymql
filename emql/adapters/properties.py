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


from itertools import izip, chain

from mw.util import parsedt

from mw.emql.adapter import Adapter
from mw.emql.emql import id_guid, formatted_id_guid, MQL_LIMIT

API_TRANS = "http://api.freebase.com/api/trans"


class properties_adapter(Adapter):

    def __init__(self, tid, graph, mql, me, cache, *args):
        super(properties_adapter, self).__init__(tid, graph, mql, me, cache,
                                                 *args)

        #self.freebase = "http://%s/view%%s" %(me.freebase_server_name)
        self.freebase = "http://www.freebase.com/view%s"
        self.thumbnail = "http://api.freebase.com/api/trans/image_thumb%s"

        cache = cache.CACHE
        self.params = {
            'permission': graph.find(tid, '/type/object/permission', cache),
            'key': graph.find(tid, '/type/object/key', cache),
            'name': graph.find(tid, '/type/object/name', cache),
            'alias': graph.find(tid, '/common/topic/alias', cache),
            'master': graph.find(tid, '/type/property/master_property', cache),
            'property': graph.find(tid, '/type/property', cache),
            'schema': graph.find(tid, '/type/property/schema', cache),
            'prop_type': graph.find(tid, '/type/property/expected_type', cache),
            'prop_unique': graph.find(tid, '/type/property/unique', cache),
            'prop_unit': graph.find(tid, '/type/property/unit', cache),
            'iio': graph.find(tid, '/boot/is_instance_of', cache),
            'keys': graph.find(tid, '/type/namespace/keys', cache),
            'cvt': graph.find(tid, '/freebase/type_hints/mediator', cache),
            'left_order': graph.find(tid, '/boot/has_left_order', cache),
            'right_order': graph.find(tid, '/boot/has_right_order', cache),
            'en': graph.find(tid, '/lang/en', cache),
            'unit': graph.find(tid, '/freebase/unit_profile/abbreviation', cache),
            'float': graph.find(tid, '/type/float', cache),
            'int': graph.find(tid, '/type/int', cache),
            'boolean': graph.find(tid, '/type/boolean', cache),
            'text': graph.find(tid, '/type/text', cache),
            'rawstring': graph.find(tid, '/type/rawstring', cache),
            'uri': graph.find(tid, '/type/uri', cache),
            'datetime': graph.find(tid, '/type/datetime', cache),
            'document': graph.find(tid, '/common/document', cache),
            'image': graph.find(tid, '/common/image', cache),
        }

    def fetch(self, tid, graph, mql, me, control, args, params, api_keys):

        if graph is None:
            graph = me.get_graph()

        query = params.get('query')
        if isinstance(query, list):
            is_list = True
            if query:
                query = query[0]
        else:
            is_list = False

        q_ordered = False
        q_limit = MQL_LIMIT
        q_text = q_thumbnail = True
        q_url = False
        q_type = q_unit = q_unique = False
        if isinstance(query, dict):
            q_values = query.get('values')
            q_type = 'type' in query
            q_unit = 'unit' in query
            q_unique = 'unique' in query
            if q_values:
                if isinstance(q_values, list):
                    q_values = q_values[0]
                if isinstance(q_values, dict):
                    q_ordered = q_values.get('ordered', q_ordered)
                    q_limit = q_values.get('limit', q_limit)
                    q_text = 'text' in q_values
                    q_thumbnail = q_text or ('thumbnail' in q_values)
                    q_url = 'url' in q_values

        results = {}
        costs = { "schema": 0, "values": 0 }
        ids = {}
        def make_id(guid, format=None):
            if guid is None:
                return None
            ig = ids.get(guid)
            if ig is None:
                ids[guid] = ig = id_guid(guid)
            if format:
                return formatted_id_guid(format, ig)
            return ig

        class _schema(object):
            __slots__ = ('id', 'name', 'type', 'cvt', 'unique', 'unit',
                         'cvt_props')
            def __init__(_self, *args):
                for arg, slot in izip(args, _self.__slots__):
                    setattr(_self, slot, arg)

        class _value(object):
            def __init__(_self, schema, target, name, value):
                type = schema.type
                if target and not value:
                    _self.id = make_id(target)
                    if q_text:
                        _self.text = name
                    if q_url:
                        _self.url = make_id(target, self.freebase)
                    if q_thumbnail and type == self.params['image']:
                        _self.thumbnail = make_id(target, self.thumbnail)
                    elif q_text and type == self.params['document']:
                        _self.text = self.get_blurb(me, target)
                elif q_text:
                    if schema.unit:
                        _self.text = ' '.join((value, schema.unit))
                    elif type == self.params['datetime']:
                        _self.text = parsedt.format_isodate(value)
                    else:
                        _self.text = value
                try:
                    if type == self.params['float']:
                        _self.value = float(value)
                    elif type == self.params['int']:
                        _self.value = int(value)
                    elif type == self.params['boolean']:
                        _self.value = value == "true"
                    elif type == self.params['datetime']:
                        _self.value = value
                    elif not q_text and value is not None:
                        _self.value = value
                except:
                    pass

        class _cvt_value(object):
            __slots__ = ('props', 'values')
            def __init__(_self, props):
                _self.props = props
                _self.values = {}
            def add(_self, prop, value):
                _self.values.setdefault(prop, []).append(value)
            def value(_self):
                if q_text or q_type or q_unit or q_unique:
                    result = {}
                    text = []
                    for prop in _self.props:
                        values = _self.values.get(prop.id)
                        if values:
                            result[make_id(prop.id)] = value = {
                                'values': values
                            }
                            if q_text:
                                value['text'] = prop.name
                            if q_type:
                                value['type'] = make_id(prop.type)
                            if q_unique:
                                value = not not prop.unique
                            if q_unit and prop.unit:
                                value = prop.unit
                            if q_text:
                                def _text():
                                    for value in values:
                                        _value = value.get('text')
                                        if _value is not None:
                                            yield _value
                                text.append(', '.join(_text()))
                    if q_text:
                        result['text'] = ' - '.join(text)
                    return result
                else:
                    return dict((make_id(prop), values)
                                for prop, values in _self.values.iteritems())

        cvts = {}
        master_schema = {}
        cvt_schema = {}
        objects = []

        for mqlres in args:
            guid = mqlres['guid']
            results[guid] = {}
            guid = guid[1:]
            objects.append(guid)

        for prop, master, name, type, unique, cvt, cvt_props, unit in self.find_properties(tid, graph, objects, q_text, costs, control):

            schema = _schema(prop, name, type, cvt, unique, unit)
            master = master or prop
            master_schema[master] = schema

            if cvt:
                schema.cvt_props = _cvt_props = []
                for prop, master, name, type, unique, unit in cvt_props:
                    master = master or prop
                    if master not in cvt_schema:
                        cvt_schema[master] = _schema(prop, name, type, False,
                                                     unique, unit)
                    _cvt_props.append(cvt_schema[master])

        for topic, master, target, name, value in self.find_values(tid, graph, objects, master_schema, q_ordered, q_limit, costs, control):
            topic = '#' + topic
            schema = master_schema[master]

            result = results[topic].get(master)
            if result is None:
                results[topic][master] = result = {'id': make_id(schema.id)}
                if q_text:
                    result['text'] = schema.name
                if q_type:
                    result['type'] = make_id(schema.type)
                if q_unique:
                    result['unique'] = not not schema.unique
                if schema.cvt:
                    result['mediator'] = True
                if q_unit and schema.unit:
                    result['unit'] = schema.unit

            if schema.cvt:
                value = target
                cvts[target] = _cvt_value(schema.cvt_props)
            else:
                value = _value(schema, target, name, value).__dict__

            if 'values' in result:
                result['values'].append(value)
            else:
                result['values'] = [value]

        if cvt_schema:
            for cvt, master, target, name, value in self.find_values(tid, graph, cvts, cvt_schema, q_ordered, q_limit, costs, control):
                schema = cvt_schema[master]
                value = _value(schema, target, name, value).__dict__
                cvts[cvt].add(schema.id, value)

        for topic, properties in results.iteritems():
            if is_list:
                results[topic] = properties = properties.values()
            else:
                results[topic] = result = {}
                properties = properties.itervalues()

            for property in properties:
                if not is_list:
                    result[property.pop('id')] = property
                if property.get('mediator'):
                    property['values'] = [cvts[cvt].value()
                                          for cvt in property['values']]

        results[':costs'] = costs

        mql.lookup_id_guids(tid, ids)

        return results

    def find_properties(self, tid, graph, sources, order_cvts,
                        costs, control):

        params = { 'sources': ' '.join(sources) }
        params.update(self.params)

        if order_cvts:
            params['order_cvts'] = '''
                sort=+$order sort-comparator="number"
                (<-left optional typeguid=%(right_order)s $order=value)
            ''' % params
        else:
            params['order_cvts'] = ''

        outbound = graph.query(tid, '''
            read id=%%q cost="" (optional
              result=((guid literal="null" $name $type $unique $cvt $cvt_props $unit))
              guid!=(%(permission)s %(iio)s %(name)s %(alias)s)
              (<-typeguid left=(%(sources)s))
              (<-left optional typeguid=%(name)s right=%(en)s $name=value)
              (<-left typeguid=%(prop_type)s $type=right
                 right->(optional
                   (<-left typeguid=%(cvt)s $cvt=value value="true")
                   (<-right typeguid=%(schema)s
                      $cvt_props=((left $cvt_master $cvt_name $cvt_type $cvt_unique $cvt_unit))
                      %(order_cvts)s
                      left->((<-left optional typeguid=%(master)s
                                $cvt_master=right)
                             (<-left optional typeguid=%(name)s
                                $cvt_name=value right=%(en)s)
                             (<-left optional typeguid=%(prop_type)s
                                $cvt_type=right)
                             (<-left optional typeguid=%(prop_unique)s
                                $cvt_unique=value)
                             (<-left optional typeguid=%(prop_unit)s
                                right->((<-left typeguid=%(unit)s
                                           $cvt_unit=value)))))))
              (<-left optional typeguid=%(prop_unique)s $unique=value)
              (<-left optional typeguid=%(prop_unit)s
                 right->((<-left typeguid=%(unit)s
                            $unit=value)))
              (<-left right=%(property)s typeguid=%(iio)s))
        ''' % params, (tid,))

        inbound = graph.query(tid, '''
            read id=%%q cost="" (optional 
              result=(($prop guid $name $type $unique $cvt $cvt_props $unit))
              guid!=(%(keys)s)
              (<-typeguid right=(%(sources)s))
              (<-right typeguid=%(master)s left!=%(key)s
                 left->($prop=guid           
                   (<-left optional typeguid=%(name)s value!=null
                      $name=value right=%(en)s)
                   (<-left typeguid=%(prop_type)s $type=right
                      right->(optional
                        (<-left typeguid=%(cvt)s $cvt=value value="true")
                        (<-right typeguid=%(schema)s 
                           $cvt_props=((left $cvt_master $cvt_name $cvt_type $cvt_unique $cvt_unit))
                           %(order_cvts)s
                           left->((<-left optional typeguid=%(master)s
                                     $cvt_master=right)
                                  (<-left optional typeguid=%(name)s
                                     $cvt_name=value right=%(en)s)
                                  (<-left optional typeguid=%(prop_type)s
                                     $cvt_type=right)
                                  (<-left optional typeguid=%(prop_unique)s
                                     $cvt_unique=value)
                                  (<-left optional typeguid=%(prop_unit)s
                                     right->((<-left typeguid=%(unit)s
                                                $cvt_unit=value)))))))
                   (<-left optional typeguid=%(prop_unique)s $unique=value)
                   (<-left optional typeguid=%(prop_unit)s
                      right->((<-left typeguid=%(unit)s
                                 $unit=value)))))
              (<-left right=%(property)s typeguid=%(iio)s))
        ''' %(params), (tid,))

        for results in (outbound, inbound):
            for id, cost, records in results.iterate('ok id=%s cost=%s (%...)\n', debug=control['debug']):
                costs['schema'] += int(cost.split(' ', 1)[0].split('=')[1])
                for prop, master, name, type, unique, cvt, cvt_props, unit in records.iterate('(%g %g %s %g %b %b %n %s)'):
                    if cvt_props is not None:
                        cvt_props = list(cvt_props.iterate('(%...)').next()[0].iterate('(%g %g %s %g %b %s)'))
                    else:
                        cvt_props = []

                    yield (prop, master, name, type, unique, cvt, cvt_props, unit)

    def find_values(self, tid, graph, sources, props, sort, limit,
                    costs, control):

        props = ' '.join(props)

        if limit:
            pagesize = "pagesize=%d" %(limit)
        else:
            pagesize=''

        if sort:
            l_sort = '''
                sort=+$order sort-comparator="number"
                (<-left optional typeguid=%(left_order)s $order=value)
            ''' %(self.params)
            r_sort = '''
                sort=+$order sort-comparator="number"
                (<-left optional typeguid=%(right_order)s $order=value)
            ''' %(self.params)
        else:
            l_sort = r_sort = ''

        def find_values(sources):

            cursors = [(None, None)]
            
            name = self.params['name']
            en = self.params['en']

            for cursor in cursors:
                query = '''
                    read id=%%q cost="" (optional
                      result=(($l_values $r_values))
                      guid=(%s)
                      (<-typeguid optional %s %s cursor=%%q left=(%s)
                         $l_values=(cursor (left typeguid right $name value))
                         right->(optional 
                           (<-left typeguid=%%g $name=value right=%%g)))
                      (<-typeguid optional %s %s cursor=%%q right=(%s)
                         $r_values=(cursor (right typeguid left $name value))
                         left->(optional 
                           (<-left typeguid=%%g $name=value right=%%g))))
                ''' %(props,
                      pagesize, l_sort, sources,
                      pagesize, r_sort, sources)

                result = graph.query(tid, query, (tid, cursor[0], name, en,
                                                       cursor[1], name, en))
                    
                for id, cost, records in result.iterate('ok id=%s cost=%s (%...)\n', debug=control['debug']):
                    costs['values'] += int(cost.split(' ', 1)[0].split('=')[1])
                    for l_cursor, l_values, r_cursor, r_values in records.iterate('((%s %...) (%s %...))'):
                        for record in chain(l_values.iterate('(%g %g %g %s %s)'),
                                            r_values.iterate('(%g %g %g %s %s)')):
                            yield record

                        if not limit:
                            if l_cursor != 'null:' or r_cursor != 'null:':
                                cursors.append((l_cursor, r_cursor))
    
        if limit:
            for source in sources:
                for record in find_values(source):
                    yield record
        else:
            for record in find_values(' '.join(sources)):
                yield record

    def get_blurb(self, me, guid):

        path = '/api/trans/blurb/guid/%s' %(guid)
        url, connection = me.get_session().http_connect('api.freebase.com', path)
        connection.request('GET', url)
        response = connection.getresponse()

        return response.read()


    def help(self, tid, graph, mql, me, control, params):
        from docs import properties_adapter_help

        return 'text/x-rst;', properties_adapter_help
