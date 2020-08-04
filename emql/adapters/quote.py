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
from urllib import urlencode

from mw.emql.adapter import Adapter, REQUEST_HEADERS
from mw.emql.emql import id_guid, formatted_id_guid


class quote_adapter(Adapter):

    ticker = "/business/stock_ticker_symbol/ticker_symbol"

    def pre(self, tid, graph, mql, me, control, parent, params, api_keys):

        return {self.ticker: None}

    def fetch(self, tid, graph, mql, me, control, args, params, api_keys):

        query = params.get('query')
        results = {}

        format = ''
        keys = []

        # format documented at http://alexle.net/archives/196
        if isinstance(query, dict):
            for key in query.iterkeys():
                if key == 'volume':
                    format += 'v'
                    keys.append(key)
                elif key == 'price':
                    format += 'l1'
                    keys.append(key)
                elif key == 'ticker':
                    pass
                elif key == 'high':
                    keys.append(key)
                    format += 'h'
                elif key == 'low':
                    keys.append(key)
                    format += 'g'
                else:
                    raise ValueError, key
        else:
            format = 'l1'
            keys = ['price']

        url, connection = me.get_session().http_connect('download.finance.yahoo.com', '/d/quotes.csv')
        connection.request('POST', url,
                           urlencode({'s': ','.join(mqlres[self.ticker]
                                                    for mqlres in args),
                                      'f': format }),
                           REQUEST_HEADERS)
        response = connection.getresponse()
        response = response.read()

        results = {}
        for mqlres, values in izip(args, response.rstrip().split('\r\n')):
            if query is None:
                results[mqlres['guid']] = values
            else:
                result = {}
                for key, value in izip(keys, values.split(',')):
                    if value == "N/A":
                        value = None
                    elif key in ('high', 'low', 'price'):
                        value = float(value)
                    elif key == 'volume':
                        value = long(value)
                    result[key] = value
                if 'ticker' in query:
                    result['ticker'] = mqlres[self.ticker]
                results[mqlres['guid']] = result

        return results

    def help(self, tid, graph, mql, me, control, params):
        from docs import quote_adapter_help

        return 'text/x-rst;', quote_adapter_help
