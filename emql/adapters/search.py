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


class search_adapter(Adapter):

    def pre(self, tid, graph, mql, me, control, parent, params, api_keys):

        constraints = params.get('constraints')
        params = params.get('query')

        if params is None:
            if constraints is not None:
                for operator, _params in constraints:
                    if operator == '~=':
                        params = _params
                        break
        
        if isinstance(params, dict) and params.get('query') is None:
            if constraints is not None:
                for operator, _params in constraints:
                    if operator == '~=':
                        params['query'] = _params
                        break

        if isinstance(params, list):
            if params:
                params = params[0]
            else:
                params = None

        if isinstance(params, (str, unicode)):
            params = { 'query': params }
        elif params is None or params.get('query') is None:
            raise ValueError, 'no query'

        args = {}
        result = {}

        for arg, value in params.iteritems():
            if arg.endswith('|='):
                name = str(arg[:-2])
            else:
                name = str(arg)
            if name in ('query', 'prefix', 'prefixed',
                        'type', 'type_strict', 'domain', 'domain_strict',
                        'type_exclude', 'type_exclude_strict',
                        'domain_exclude', 'domain_exclude_strict',
                        'limit', 'denylist', 'related', 'property',
                        'mql_filter', 'geo_filter', 'as_of_time', 'timeout'):
                args[name] = value
            elif name != 'score':
                result[name] = value

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

        if 'score' in params:
            matches = me.get_session().relevance_query(tid, format='ac', **args)
            guids = ['#' + match['guid'] for match in matches]
        else:
            matches = me.get_session().relevance_query(tid, format='guids', **args)
            guids = ['#' + guid for guid in matches]

        if guids:
            result['guid|='] = guids
        else:
            result['guid|='] = ['#00000000000000000000000000000000']

        if 'score' in params:
            result[':extras'] = {
                "fetch-data": dict((match['guid'], match['score'])
                                   for match in matches)
            }

        return result

    def fetch(self, tid, graph, mql, me, control, args, params, api_keys):

        constraints = params.get('constraints')
        scores = params.get(':extras', {}).get('fetch-data')
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
            if scores is not None:
                for mqlres in args:
                    mqlres['score'] = scores[mqlres['guid'][1:]]

            if 'guid' in params:
                fn = dict.get
            else:
                fn = dict.pop

            results = {}
            for mqlres in args:
                mqlres['query'] = params['query']
                results[fn(mqlres, 'guid')] = [mqlres] if was_list else mqlres

        return results

    def help(self, tid, graph, mql, me, control, params):
        from docs import search_adapter_help

        return 'text/x-rst;', search_adapter_help


