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

import mw, urllib, rfc822, time, datetime
from collections import defaultdict
from mw.emql.adapter import Adapter

def rfc822_to_iso(d):
    r = rfc822.parsedate(d)
    r = time.mktime(r)
    r = datetime.datetime.fromtimestamp(r)
    return r.isoformat()

class tweets_from_adapter(Adapter):

    def pre(self, tid, graph, mql, me, control, parent, params, api_keys):
        return {
            '/internet/social_network_user/twitter_id': { 
                'value': None, 'limit': 1, 'optional': True 
             },
            ':extras': {'foo': 'bar'}
        }

    def fetch(self, tid, graph, mql, me, control, args, params, api_keys):
        result = defaultdict(list)
        query = params['query'] or {}

        if isinstance(query, list):
            query = query[0]

        limit = query.get('limit', 5)
        raw = query.get('raw', None)

        for mqlres in args:
            if not mqlres['/internet/social_network_user/twitter_id']:
                continue

            url, connection = me.get_session().http_connect('twitter.com',
                                                            "/statuses/user_timeline.json")
            qs = urllib.urlencode({
                    'count': limit,
                    'screen_name': mqlres['/internet/social_network_user/twitter_id']['value']
            })
            connection.request('GET', "%s?%s" % (url, qs))
            response = connection.getresponse()
            json = mw.json.loads(response.read())
            tweets = []
            if 'error' in json:
                me.log('error', 'emql.adapters.twitter', json['error'], response=json)
                raise Exception(json['error'])

            for j in json:
                tweet = {
                    'timestamp': rfc822_to_iso(j['created_at']),
                    'key': j['id'],
                    'text': j['text'],
                    'user': {'name': j['user']['name'],
                             'profile_image_url': j['user']['profile_image_url'],
                             'screen_name': j['user']['screen_name'],
                             'url': 'http://twitter.com/%s' % j['user']['screen_name']},
                    'url': 'http://twitter.com/%s/status/%s' % (j['user']['screen_name'], j['id'])
                }
                if raw:
                    tweet['raw'] = j
                tweets.append(tweet)

            result[mqlres['guid']].extend(tweets)

        return dict((k, v[:limit]) for k,v in result.iteritems())

    def help(self, tid, graph, mql, me, control, params):
        from docs import twitter_adapter_help

        return 'text/x-rst;', twitter_adapter_help
