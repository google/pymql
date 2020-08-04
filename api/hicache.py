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


from locache import LojsonCachePolicy
from mw.log import LOG

class LWTCachePolicy(LojsonCachePolicy):
    """
    Long run, we can probably factor 'mss' out of here completely, right?
    """
    cost_prefix = 'c'

    def __init__(self, mss, tag='mql'):
        # give fake ctx/varenv because we'll be overriding all uses
        # and want to make sure that any time LojsonCachePolicy tries
        # to access ctx/varenv, that it explodes loudly, rather than
        # silently using a bad value
        super(LWTCachePolicy, self).__init__(None, None, tag,
                                             start_time=mss.time_start)
        self.mss = mss

    def _set_varenv(self, varenv):
        # this is a no-op because we're forwarding to self.mss.varenv
        pass

    def _get_varenv(self):
        return self.mss.varenv

    # wrap the existing varenv 
    varenv = property(_get_varenv, _set_varenv)

    def annotate_key_object(self, key_obj):
        return self.get_varenv_envelope(key_obj, ("cursor", "macro", "escape",
                                                  "uniqueness_failure", "$lang",
                                                  "asof", "normalize_only", "unicode_text"))
    
    def annotate_result(self, result):
        full_result = super(LWTCachePolicy, self).annotate_result(result)
        
        full_result["tid"] = self.mss.transaction_id

        if 'cursor' in self.mss.varenv:
            full_result['cursor'] = self.mss.varenv['cursor']

        return full_result
    
    def extract_result(self, full_result):
        # all of this should maybe be done in the mqlread itself?
        
        # set the age header to at least this old
        
        # this is the other place where the use of
        # mss.time_start is important
        self.mss.cache_age = max(self.mss.cache_age,
                                 self.start_time - full_result['time'])

        if 'cursor' in full_result:
            self.mss.varenv['cursor'] = full_result['cursor']

        return super(LWTCachePolicy, self).extract_result(full_result)
        
    
    def add_cost(self, costkey, value=1):
        self.mss.add_cost(self.cost_prefix + costkey, value)

    def should_read_cache(self):
        return self.varenv.get("cache",True)

    def should_write_cache(self):
        # allow certain reads to not write-through to the cache (for
        # instance, crawlers and results with cursors
        cache_writes = not self.varenv.get('no_store_cache', False)

        # we don't cache past the first page in a cursor'ed query
        has_working_cursor = 'cursor' in self.varenv and self.varenv['cursor'] != True

        return cache_writes and not has_working_cursor
