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


import math
from itertools import izip, chain

from mw.emql.adapter import Adapter, AdapterUserError
from mw.emql.emql import id_guid, formatted_id_guid


class stats_adapter(Adapter):

    def reduce(self, tid, graph, mql, me, control, mqlres, params, api_keys):

        constraints = params.get('constraints')
        op = params.get('property')
        params = params.get('query')

        args = None
        if isinstance(params, dict):
            args = params.get('value', '').split('.')

        if not args:
            raise ValueError, "%s: missing 'value' argument" %(op)

        def get(res, prop):
            if isinstance(res, dict):
                return res[prop]
            else:
                value = res[0]
                if isinstance(value, dict):
                    value = value[prop]
                return value

        values = []
        for _mqlres in mqlres:
            value = reduce(get, args, _mqlres)
            if value is not None:
                values.append(value)

        if values:
            if op.startswith('@'):
                op = op[1:]

            try:
                if op == 'average':
                    return dict(value=float(sum(values)) / len(values))

                if op == 'median':
                    values.sort()
                    return dict(value=values[len(values) / 2])

                if op == 'min':
                    return dict(value=min(values))

                if op == 'max':
                    return dict(value=max(values))

                if op == 'total':
                    return dict(value=sum(values))

                if op == 'sigma':
                    average = float(sum(values)) / len(values)
                    squares = sum((value - average) * (value - average)
                                  for value in values)
                    return dict(value=math.sqrt(squares / len(values)))

            except TypeError, e:
                raise AdapterUserError('reduce', op, self.uri, str(e))

            raise NotImplementedError, op
        
        return dict(value=None)

    def help(self, tid, graph, mql, me, control, params):
        from docs import stats_adapter_help

        return 'text/x-rst;', stats_adapter_help
