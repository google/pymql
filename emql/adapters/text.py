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
from mw.emql.emql import id_guid, formatted_id_guid


class text_adapter(Adapter):

    def pre(self, tid, graph, mql, me, control, parent, params, api_keys):

        return {"/common/document/content":
                  {"optional": True, "blob_id": None, "media_type": None},
                "/common/document/source_uri": None,
                "guid": None}

    def fetch(self, tid, graph, mql, me, control, args, params, api_keys):

        params = params.get('query')
        results = {}

        for mqlres in args:
            guid = mqlres['guid']
            content = mqlres["/common/document/content"]
            if content is not None:
                mediatype = content["media_type"]
                if mediatype and mediatype.startswith("/media_type/text"):
                    blob_id = content["blob_id"]
                    if blob_id:
                        chars = me.get_session().fetch_blob(tid, blob_id)
                        try:
                            chars = unicode(chars, 'utf-8')
                        except:
                            pass

                        if params is None:
                            results[guid] = chars
                        else:
                            results[guid] = result = params.copy()
                            if 'maxlength' in result:
                                chars = chars[:result['maxlength']]
                            if 'chars' in result:
                                result['chars'] = chars
                            if 'length' in result:
                                result['length'] = len(chars)

            elif mqlres["/common/document/source_uri"] is not None:
                if params is None:
                    maxlength = None
                    mode = 'blurb'
                else:
                    maxlength = params.get('maxlength')
                    mode = params.get('mode', 'blurb')
                    if mode not in ('blurb', 'raw'):
                        raise ValueError, "invalid mode: '%s'" %(mode)

                query = '/guid/%s' %(guid[1:])
                if maxlength:
                    query += '?maxlength=%d' %(maxlength)

                url, connection = me.get_session().http_connect('api.freebase.com', '/api/trans/%s' %(mode) + query)
                connection.request('GET', url)
                response = connection.getresponse()
                chars = response.read()

                if params is None:
                    results[guid] = chars
                else:
                    results[guid] = result = params.copy()
                    if 'chars' in result:
                        result['chars'] = chars
                    if 'length' in result:
                        result['length'] = len(chars)

        return results

    def help(self, tid, graph, mql, me, control, params):
        from docs import text_adapter_help

        return 'text/x-rst;', text_adapter_help
