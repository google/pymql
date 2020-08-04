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

import mw, urllib, urlparse
from datetime import datetime
from collections import defaultdict
from mw.emql.adapter import Adapter
from lxml import etree

class nytimes_articles_adapter(Adapter):

    def pre(self, tid, graph, mql, me, control, parent, params, api_keys):
        return {
            "key": [{ 
                "optional": True, 
                "value": None, 
                "namespace" : "/user/jamie/nytdataid",
                "limit": 10
             }]
        }
    
    def get_articles(self, me, nytd_key, api_keys):
        url, connection = me.get_session().http_connect('data.nytimes.com', "/%s.rdf" % nytd_key)
        connection.request('GET', url)
        response = connection.getresponse()
        rdf = response.read()
        rdf = etree.fromstring(rdf)
        
        # Grab the search api call
        search_url = rdf.xpath("//nyt:search_api_query", namespaces=rdf.nsmap)
        if not search_url:
            return []
        
        search_url = urlparse.urlparse(search_url[0].text)
        params = urlparse.parse_qs(search_url.query)
        params['api-key'] = api_keys['nytimes_articles']
        params['fields'] = ','.join([
            'date', 
            'url', 
            'nytd_lead_paragraph',
            'nytd_title', 
            'byline',
            'nytd_byline',
            'small_image_url',
            'small_image_height',
            'small_image_width',
            'source_facet'
        ])
        
        # build the actual query
        url, connection = me.get_session().http_connect(search_url.hostname, search_url.path)
        qs = urllib.urlencode(params, doseq=True)
        connection.request('GET', "%s?%s" % (url, qs))
    
        response = connection.getresponse()
        json = mw.json.loads(response.read())
    
        json = [{
            'headline': j['nytd_title'],
            'text': j['nytd_lead_paragraph'],
            'byline': j.get('nytd_byline', j.get('byline', None)),
            'source': j.get('source_facet', None),
            'date': datetime.strptime(j['date'], '%Y%m%d').isoformat(),
            'img': ({'url': j['small_image_url'],
                     'height': j.get('small_image_height') or None,
                     'width': j.get('small_image_widget') or None}
                    if j.get('small_image_url')
                    else None),
            'url': j['url']
        } for j in json['results']]
        
        return json
    
    def fetch(self, tid, graph, mql, me, control, args, params, api_keys):
        result = defaultdict(list)
        query = params['query'] or {}
        
        if isinstance(query, list):
            query = query[0]
        
        limit = query.get('limit', 5)
        
        if not (api_keys and api_keys.get('nytimes_articles')):
            raise Exception('This property requires a New York Times API key. '
                            'Get one here: http://developer.nytimes.com/apps/register')
        
        for mqlres in args:
            if not mqlres['key']:
                continue
            
            for key in mqlres['key']:
                articles = self.get_articles(me, key['value'], api_keys)
                result[mqlres['guid']].extend(articles)
            
        return dict((k, v[:limit]) for k,v in result.iteritems())

    def help(self, tid, graph, mql, me, control, params):
        from docs import nytimes_adapter_help

        return 'text/x-rst;', nytimes_adapter_help


