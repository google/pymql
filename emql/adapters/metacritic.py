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

import mw
from lib import bdb_lookup

#TODO: python docs
#TODO: log exceptions?

class metacritic_adapter(mw.emql.adapter.Adapter):
    
    SECRET='random_rodent'
    
    def make_result(self,key,scores):
        return {
                'key'       : key,
                'url'       : 'http://www.metacritic.com/video/titles/%s' % key,
                'score'     : scores['metascore'],
                'userscore' : scores['userscore'],
                'attribution_html' : '<span>TODO</span>'
        }

    def check_secret(self,params,guid,result):
        if params.get('query') and params.get('query').get('secret') == self.SECRET:
            return True
        else:
            result[guid] = { 'error':'Invalid auth' }
            return False
    
    def get_key(self, me, guid):
        result = bdb_lookup(me,guid,'source-metacritic-movie')
        if result:
            return result[0]
        else:
            return None
    
    def get_scores(self,me,guid):
        result = bdb_lookup(me,guid,'metacritic-scores')
        return result
    
    def fetch(self, tid, graph, mql, me, control, args, params, api_keys):
        result = {}
        for mqlres in args:
            guid = mqlres['guid']
            if not self.check_secret(params,guid,result):
                continue
            key  = self.get_key(me,guid)
            if not key:
                continue
            scores = self.get_scores(me,guid)
            if not scores:
                #TODO: log
                continue
            result[guid]=self.make_result(key,scores)
        return result
    
