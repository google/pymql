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

from mw.emql.adapter import Adapter
from mw.emql.emql import id_guid, formatted_id_guid, MQL_LIMIT


class geosearch_adapter(Adapter):

    def pre(self, tid, graph, mql, me, control, parent, params, api_keys):

        constraints = params.get('constraints')
        property = params.get('property').rsplit('/', 1)[-1]
        params = params.get('query')

        if params is None:
            if constraints is not None:
                for operator, _params in constraints:
                    if operator == '~=':
                        params = _params
                        break
        
        if isinstance(params, dict) and params.get('location') is None:
            if constraints is not None:
                for operator, _params in constraints:
                    if operator == '~=':
                        params['location'] = _params
                        break

        if isinstance(params, list):
            if params:
                params = params[0]
            else:
                params = None

        if isinstance(params, (str, unicode)):
            params = { 'location': params }
        elif params is None or params.get('location') is None:
            raise ValueError, 'no location'

        args = {}
        result = {}

        for arg, value in params.iteritems():
            if arg.endswith('|='):
                name = str(arg[:-2])
            else:
                name = str(arg)
            if name in ('location', 'limit',  'type', 'location_type',
                        'within', 'mql_filter', 'geometry_type',
                        'negate', 'outer_bounds', 'as_of_time'):
                args[name] = value
            elif name not in ('distance', 'distance<=', 'unit', 'score',
                              'inside', 'contains'):
                result[name] = value

        if property == 'near':
            args['inside'] = params.get('inside', False)
            if 'within' not in args:
                args['within'] = params.pop('distance<=', 1)
            if params.get('unit', 'kms') == 'miles':
                args['within'] *= 1.609
            if 'distance' in params:
                args['order_by'] = 'distance'
            elif 'score' in params:
                args['order_by'] = 'relevance'
        elif property == 'inside':
            args['inside'] = params.get('inside', True)
            if 'score' in params:
                args['order_by'] = 'relevance'
        elif property == 'contains':  # extension property not yet created
            args['contains'] = params.get('contains', True)
            if 'score' in params:
                args['order_by'] = 'relevance'

        if 'geometry_type' not in args:
            args['geometry_type'] = 'point'

        for arg, value in parent.iteritems():
            if arg.endswith('|='):
                name = str(arg[:-2])
            else:
                name = str(arg)
            if name not in args:
                if name == 'limit':
                    args[name] = value
                elif name == 'type' and isinstance(value, basestring):
                    args['type_strict'] = 'any'
                    args[name] = value

        if 'limit' not in args:
            args['limit'] = MQL_LIMIT # plug-in default MQL limit

        if 'order_by' in args:
            matches = me.get_session().geo_query(tid, format='ac',
                                                 timeout=control['timeout'],
                                                 **args)
            guids = ['#' + match['guid'] for match in matches]
        else:
            matches = me.get_session().geo_query(tid, format='guids',
                                                 timeout=control['timeout'],
                                                 **args)
            guids = ['#' + guid for guid in matches]

        if guids:
            result['guid|='] = guids
        else:
            result['guid|='] = ['#00000000000000000000000000000000']

        if 'order_by' in args:
            order_by = args['order_by']
            result[':extras'] = {
                'fetch-data': { 'order_by': order_by,
                                'order': dict((match['guid'], match[order_by])
                                              for match in matches) }
            }

        return result

    def fetch(self, tid, graph, mql, me, control, args, params, api_keys):

        constraints = params.get('constraints')
        order = params.get(':extras', {}).get('fetch-data')
        params = params.get('query')

        was_list = False
        if isinstance(params, list):
            if params:
                params = params[0]
                was_list = True
            else:
                params = None

        if params is None:
            if constraints is not None:
                for operator, _params in constraints:
                    if operator == '~=':
                        params = _params
                        break

        if isinstance(params, (str, unicode)):
            results = dict((mqlres['guid'], params) for mqlres in args)
        else:
            if order is not None:
                order_by = order['order_by']
                order = order['order']
                if order_by == 'distance':
                    for mqlres in args:
                        mqlres['distance'] = order[mqlres['guid'][1:]]
                    if params.get('unit', 'kms') == 'miles':
                        for mqlres in args:
                            mqlres['distance'] /= 1.609
                            mqlres['unit'] = 'miles'
                elif order_by == 'relevance':
                    for mqlres in args:
                        mqlres['score'] = order[mqlres['guid'][1:]]

            if 'guid' in params:
                fn = dict.get
            else:
                fn = dict.pop

            results = {}
            for mqlres in args:
                mqlres['location'] = params['location']
                results[fn(mqlres, 'guid')] = [mqlres] if was_list else mqlres

        return results

    def help(self, tid, graph, mql, me, control, params):
        from docs import geosearch_adapter_help

        return 'text/x-rst;', geosearch_adapter_help


class point_adapter(Adapter):

    geolocation = "/location/location/geolocation"
    longitude = "/location/geocode/longitude"
    latitude = "/location/geocode/latitude"

    def pre(self, tid, graph, mql, me, control, parent, params, api_keys):

        params = params.get('query')
        if params and isinstance(params, list):
            limit = params[0].get('limit', MQL_LIMIT)
        else:
            limit = 1

        return {
            self.geolocation: [{
                self.longitude: None,
                self.latitude: None,
                "guid": None,
                "limit": limit,
                "optional": True
            }]
        }

    def fetch(self, tid, graph, mql, me, control, args, params, api_keys):

        params = params.get('query')
        results = {}

        def geojson(geolocation):
            return {
                "geometry": {
                    "coordinates": [
                        geolocation[self.longitude],
                        geolocation[self.latitude]
                    ],
                    "id": geolocation['guid'],
                    "type": "Point"
                }
            }

        for mqlres in args:
            if isinstance(params, list):
                results[mqlres['guid']] = \
                    [geojson(geolocation)
                     for geolocation in mqlres[self.geolocation]]
            elif mqlres[self.geolocation]:
                results[mqlres['guid']] = geojson(mqlres[self.geolocation][0])
            
        return results

    def help(self, tid, graph, mql, me, control, params):
        from docs import point_adapter_help

        return 'text/x-rst;', point_adapter_help


class shape_adapter(Adapter):

    def fetch(self, tid, graph, mql, me, control, args, params, api_keys):

        params = params.get('query')
        if params and isinstance(params, list):
            params = params[0]
            was_list = True
            limit = params.get('limit', MQL_LIMIT)
        else:
            was_list = False
            limit = 1

        kwds = {
            "mql_output": "null",
            "geometry_type": "polygon,multipolygon",
            "format": "json"
        }

        if isinstance(params, dict):
            for arg, value in params.iteritems():
                name = str(arg)
                if name in ('accessor', 'puffer', 'simplify', 'collect'):
                    kwds[name] = value

        mss = me.get_session()
        results = {}
        for mqlres in args:
            guid = mqlres['guid']
            result = mss.geo_query(tid, location=guid,
                                   timeout=control['timeout'], **kwds)
            if was_list:
                results[guid] = [feature['geometry']
                                 for feature in result['features']]
            elif result['features']:
                results[guid] = result['features'][0]['geometry']

        return results

    def help(self, tid, graph, mql, me, control, params):
        from docs import shape_adapter_help

        return 'text/x-rst;', shape_adapter_help
