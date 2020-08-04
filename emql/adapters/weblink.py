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


from traceback import format_exc

from mw.emql.adapter import Adapter
from mw.emql.emql import MQL_LIMIT
from mw.util.pattern import Pattern

P_URI_TEMPLATE_NS = "!/common/uri_template/ns"
P_ANNOTATION = "/common/topic/annotation"
NIL = object()


class weblink_adapter(Adapter):

    def pre(self, tid, graph, mql, me, control, parent, params, api_keys):

        params = params.get('query')
        was_list = False
        if isinstance(params, list):
            was_list = True
            if params:
                params = params[0]
            else:
                params = None

        result = {}

        if isinstance(params, (str, unicode)):
            params = { 'url': params }

        elif params is None or (params.get('url') is None and
                                params.get('host') is None and
                                params.get('path') is None):

            if was_list:
                if params is None:
                    limit = MQL_LIMIT
                else:
                    limit = params.get('limit', MQL_LIMIT)
            else:
                limit = 1

            if params is None:
                with_description = False
                with_category = False
                with_template = False
                with_annotation = False
            else:
                with_description = 'description' in params
                with_category = 'category' in params
                with_template = 'template' in params
                with_annotation = 'annotation' in params

            template = {
                "weblink:template": { "value": None },
                "weblink:guid": None
            }
            if with_template:
                _template = params['template']
                if isinstance(_template, dict):
                    template.update(_template)
                elif isinstance(_template, list):
                    raise TypeError, 'template subquery must be {} or literal'
                else:
                    template['template'] = _template
                template['optional'] = False

            if with_description:
                template["weblink:name"] = params["description"]

            # category is extracted from:
            #  - uri template when off of topic directly
            #  - annotation when off of annotation or its resource(s)

            if with_category:
                _template = template.copy()
                _template["weblink:category"] = params["category"]
            else:
                _template = template

            result["key"] = [{
                "optional": True,                   
                "limit": limit,
                "namespace": {P_URI_TEMPLATE_NS: _template},
                "value": None
            }]

            result[P_ANNOTATION] = [{
                "optional": True,                   
                "limit": limit,
                "guid": None,
                "resource": [{
                    "optional": False,
                    "guid": None,
                    "key": [{
                        "optional": False,
                        "limit": limit,
                        "namespace": {P_URI_TEMPLATE_NS: template},
                        "value": None
                    }]
                }]
            }]
            if with_category:
                result[P_ANNOTATION][0]["weblink:category"] = params["category"]

            return result

        args = {}
        for arg, value in params.iteritems():
            name = str(arg)
            if name in ('url', 'host', 'path', 'description', 'category'):
                if value is not None:
                    args[name] = value
            elif name not in ('score', 'limit',
                              'optional'): # pass-thru extra MQL
                result[name] = value

        with_scores = 'score' in params
        limit = parent.get('limit', MQL_LIMIT)
        start = args.pop('start', 0)
        mss = me.get_session()

        urls = {}
        done = False
        ratio = 1.0
        while not done:
            _limit = int(limit + limit*ratio)
            matches = mss.relevance_query(tid, api='urlsearch',
                                          timeout=control['timeout'],
                                          format='urls', start=start,
                                          limit=_limit, **args)

            if len(matches) < _limit:
                done = True

            count = 0
            for match in matches:
                start += 1

                if not with_scores:
                    match.pop('score')
                if 'topic' in match:
                    holder = match.pop('topic')
                elif 'annotation' in match:
                    holder = match.pop('annotation')
                else:
                    holder = match.pop('resource')
                    match.pop('topics', None)

                if holder in urls:
                    urls[holder].append(match)
                else:
                    urls[holder] = [match]
                    count += 1
                    limit -= 1
                    if limit == 0:
                        done = True
                        break

            if not done:
                ratio = len(matches) / (count + 1.0)

        if urls:
            result['guid|='] = urls.keys()
            result[':extras'] = {
                "fetch-data": urls
            }
        else:
            result['guid|='] = ['#00000000000000000000000000000000']

        return result

    def fetch(self, tid, graph, mql, me, control, args, params, api_keys):

        if ':extras' in params:
            urls = params.get(':extras', {}).get('fetch-data')
        else:
            urls = None

        params = params.get('query')

        was_list = False
        if isinstance(params, list):
            was_list = True
            if params:
                params = params[0]
            else:
                params = None

        if isinstance(params, (str, unicode)):
            results = dict((mqlres['guid'], urls[mqlres['guid']][0]['url'])
                           for mqlres in args)
        elif urls is not None:
            if 'guid' in params:
                fn = dict.get
            else:
                fn = dict.pop
            limit = params.get('limit', MQL_LIMIT)
            with_description = 'description' in params
            with_category = 'category' in params

            results = {}
            for mqlres in args:
                holder = fn(mqlres, 'guid')
                if was_list:
                    result = []
                    for url in urls[holder]:
                        if not with_description:
                            url.pop('description', None)
                        url.pop('annotation_categories', None)
                        category = url.pop('template_category', None)
                        if with_category:
                           url['category'] = category 
                        if len(result) == limit:
                            break
                        res = mqlres.copy()
                        res.update(url)
                        result.append(res)
                    results[holder] = result
                else:
                    mqlres.update(urls[holder][0])
                    results[holder] = mqlres
        else:
            if params is None:
                is_string = True
                with_template = False
                with_category = False
                with_key = False
            else:
                is_string = False
                with_template = 'template' in params
                with_category = 'category' in params
                with_key = 'key' in params

            results = {}
            patterns = {}

            for mqlres in args:
                if was_list:
                    result = []
                    if is_string:
                        limit = MQL_LIMIT
                    else:
                        limit = params.get('limit', MQL_LIMIT)
                else:
                    result = None

                def template_uris(mqlres, result, category=NIL):

                    for key in mqlres['key']:
                        _result = {}
                        template = key['namespace'][P_URI_TEMPLATE_NS]
                        if 'weblink:name' in template:
                            pattern = template.pop('weblink:name')
                            if pattern is None:
                                description = None
                            else:
                                description = Pattern(pattern, mqlres['guid'], key['value'])
                            _result['description'] = description
                        else:
                            description = None
                        if category is NIL:
                            if 'weblink:category' in template:
                                _result['category'] = template.pop('weblink:category')
                        elif with_category:
                            _result['category'] = category

                        pattern = template.pop('weblink:template')['value']
                        if with_template:
                            if isinstance(params['template'], dict):
                                _result['template'] = template
                            else:
                                _result['template'] = pattern
                        if with_key:
                            _result['key'] = key['value']
                        url = Pattern(pattern, mqlres['guid'], key['value'])
                        if was_list:
                            if len(result) == limit:
                                break
                            if is_string:
                                result.append(url)
                            else:
                                _result['url'] = url
                                result.append(_result)
                        else:
                            if is_string:
                                result = url
                            else:
                                _result['url'] = url
                                result = _result
                            break
                        guid = template.pop('weblink:guid')
                        patterns.setdefault(guid, []).append((url, description))

                    return result

                result = template_uris(mqlres, result)
                for annotation in mqlres[P_ANNOTATION]:
                    category = annotation.pop("weblink:category", None)
                    for resource in annotation["resource"]:
                        result = template_uris(resource, result, category)

                if result:
                    results[mqlres['guid']] = result

            for pairs in patterns.itervalues():
                url, description = pairs[0]
                query = url.mql_query("url")
                if description is not None:
                    query.update(description.mql_query("description"))
                if query:
                    try:
                        pairs = dict((pair[0].guid, pair) for pair in pairs)
                        query['guid|='] = pairs.keys()
                        query['guid'] = None
                        res, cursor = mql.mqlread(tid, [query])
                        for res in res:
                            url, description = pairs[res['guid']]
                            url.set_mqlres(res, "url")
                            if description is not None:
                                description.set_mqlres(res, "description")
                    except Exception, e:
                        # if any error occurs, assumption not satisfied
                        # string will left be (partially) unexpanded during str
                        pass

            if was_list:
                if is_string:
                    for guid, result in results.iteritems():
                        results[guid] = [str(url) for url in result]
                else:
                    for result in results.itervalues():
                        for url in result:
                            url['url'] = str(url['url'])
                            description = url.get('description')
                            if description is not None:
                                url['description'] = str(description)
            else:
                if is_string:
                    for guid, url in results.iteritems():
                        results[guid] = str(url)
                else:
                    for url in results.itervalues():
                        url['url'] = str(url['url'])
                        description = url.get('description')
                        if description is not None:
                            url['description'] = str(description)

        return results

    def help(self, tid, graph, mql, me, control, params):
        from docs import weblink_adapter_help

        return 'text/x-rst;', weblink_adapter_help
