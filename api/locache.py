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

"""
Stuff for caching lojson
"""

import time
from datetime import datetime

from mw.mql.utils import valid_guid, valid_timestamp
from cache import BasicCachePolicy, CacheEntry, log_kwds
from mw.log import LOG

class LojsonCachePolicy(BasicCachePolicy):
    """
    Generic last_write_time-based cache policy. This makes sure that
    any entries stored with this policy will only be valid relative to
    the current last_write_time stored in varenv
    """
    cost_prefix = 'l'
    
    def __init__(self, ctx, varenv, tag='lojson',
                 start_time=None,
                 flush_time=60*60):
        super(LojsonCachePolicy, self).__init__(tag)
        if start_time is None:
            start_time = time.time()
        self.start_time = start_time
        self.flush_time = flush_time
        self.varenv = varenv
        self.ctx = ctx

    def _log_kwds(self, **kwds):
        return log_kwds(cachegroup=self.varenv.get('cachegroup'), **kwds)

    def annotate_key_object(self, key_obj):
        # since we're using asof to determine expiry, we need to
        # include asof in the query as well, so queries with 'asof'
        # don't infect real expiring entries

        return self.get_varenv_envelope(key_obj, ("asof",)) 

    def get_varenv_envelope(self, key_obj, keys=None):
        """
        Makes sure MQL keys are dependent on other MQL-specific
        properties like the current language, uniqueness failures, etc
        """
        envelope = { "query": key_obj,
                     "type": self.tag }

        # we have to be very carefuly here; "cursor": false is
        # different from "cursor": null.  so we must include
        # everything that has a non-null value.  see bug 6951 (and bug
        # 7206) for the problems that this piece of code can cause.
        for varenv_param in keys:
            if self.varenv.get(varenv_param, None) is not None:
                envelope[varenv_param] = self.varenv[varenv_param]

        return envelope

    def annotate_result(self, result):

        expires, long_lived = self.get_expires()
        if long_lived:
            # we'll interpret long_lived as 'lives longer than
            # the dateline'
            write_dateline = -1
        else:
            write_dateline = self.varenv.get("write_dateline", None)
        
        full_result = { "result": result,
                        "time": self.start_time,
                        "expires": expires,
                        "dateline": write_dateline }

        # lets us trace origination of the query
        tid = self.varenv.get('tid')
        if tid is not None:
            full_result["tid"] = tid
            
        return full_result

    def get_expires(self):
        # make sure 'asof' caches well: The basic idea here is that
        # anytime you're requesting 'asof', the data will never
        # change, so it can outlive the mwLastWriteTime. However, if
        # asof is actually in the future, or at least ahead of the
        # current dateline, new data may come in between 'now' and
        # 'asof'
        write_dateline = self.varenv.get("write_dateline", None)
        last_write_time = int(self.varenv.get("last_write_time", 0))
        expires = int(self.start_time + self.flush_time)
        long_lived = False
        
        if 'asof' in self.varenv:
            asof = self.varenv['asof']
            
            # make sure asof is in the past
            if valid_guid(asof):
                asof_dateline = asof[1:]
                # datelines are strings that will simply sort correctly
                if asof_dateline < write_dateline:
                    expires += 60*60    # add one hour to expiry
                    long_lived = True
                    LOG.notice("%s.cache.never_expires" % self.tag,
                               "Request has valid dateline asof in the past, never expiring", **self._log_kwds())
                else:
                    LOG.warn("%s.cache.asof.future_dateline" % self.tag,
                             "Request using dateline asof=%s in the future - replica needs to catch up?" % asof,
                             asof_dateline=asof_dateline,
                             write_dateline=write_dateline, **self._log_kwds())
                    
                    
            elif valid_timestamp(asof):
                # if no last write time, calculate "now" as "as fresh
                # as last_write_time" - using flush_time as a
                # reasonable safety zone, because we don't want to
                # accidentally cache an as_of_now that was queried 5
                # minutes ago as never expiring.
                if last_write_time == 0:
                    lwt_iso = time.time() - self.flush_time
                    lwt_iso = datetime.fromtimestamp(lwt_iso)
                else:
                    lwt_iso = datetime.fromtimestamp(last_write_time)
                    
                # format the current time as an iso string, so that we
                # can do direct string comparison with the ISO string
                # specified in asof
                lwt_iso = lwt_iso.strftime("%Y-%m-%dT%H:%M:%SZ")
                if asof < lwt_iso:
                    expires += 60*60    # add one hour to expiry
                    long_lived = True
                    LOG.notice("%s.cache.never_expires" % self.tag,
                               "Request has valid asof in the past, never expiring", **self._log_kwds())
                else:
                    LOG.warn("%s.cache.asof.future_date" % self.tag,
                             "Request using asof=%s in the future" % asof,
                             asof=asof,
                             last_write_time=last_write_time,
                             lwt_iso=lwt_iso, **self._log_kwds())
            else:
                LOG.warn("%s.cache.asof.bad" % self.tag,
                         "Request using unknown asof format: %s" % asof, asof=asof, **self._log_kwds())
        return (expires, long_lived)
    
    def extract_result(self, full_result):
        
        return full_result["result"]

    def is_expired(self, key, full_result):
        """
        Fully implement last_write_time/ datetline support
        """
        
        # get these at the moment that we're testing expiration, in
        # case there have been reads since this CacheEntry was created
        write_dateline = self.varenv.get("write_dateline", None)
        last_write_time = int(self.varenv.get("last_write_time", 0))
        
        entry_expires = full_result['expires']
        entry_timestamp = full_result['time']
        entry_dateline = full_result['dateline']
        
        # to be logged
        lparams = { 
            "now": self.start_time,
            "timestamp": entry_timestamp,
            "expires": entry_expires,
            "dateline": entry_dateline,
            "lwt": last_write_time,
            "lwd": write_dateline,
            "key": key,
            }

        if 'tid' in full_result:
            lparams['cached_tid'] = full_result['tid']
            
        if 'asof' in self.varenv:
            lparams['requested_asof'] = self.varenv['asof']

        if 'asof' in full_result:
            lparams['asof'] = full_result['asof']

        # entries with '-1' dateline never expire
        if entry_dateline != -1:
            expired = False
            # make sure to log all of these scenarios

            if entry_dateline < write_dateline:
                LOG.notice("%s.cache.result" % self.tag, "",
                           **self._log_kwds(code="stale.dateline", **lparams))
                expired = True

            # the one second granularity of last_write_time means we
            # must treat equality as a miss, hence the "<="
            if int(entry_timestamp) <= last_write_time:
                LOG.notice("%s.cache.result" % self.tag, "",
                           **self._log_kwds(code="stale.lwt.timestamp",
                                           **lparams))
                expired = True

            if entry_expires < self.start_time:
                LOG.notice("%s.cache.result" % self.tag, "",
                           **self._log_kwds(code="stale.expired", **lparams))
                expired = True

            if expired:
                return expired

        LOG.notice("%s.cache.result" % self.tag, "",
                   **self._log_kwds(code="hit", **lparams))
        
        return False

    def add_cost(self, costkey, value=1):
        key = self.cost_prefix + costkey
        self.ctx.gc.totalcost.setdefault(key, 0)
        self.ctx.gc.totalcost[key] += value
                    
        
class LojsonCache(object):
    def __init__(self, memcache, timeout=0,flush_time=60*60):
        self.memcache = memcache

        self.timeout = timeout
        self.flush_time = int(flush_time)
                         
    def lowread_wrapper(self,ctx,query,varenv):

        policy = LojsonCachePolicy(ctx, varenv,
                                   flush_time=self.flush_time)

        entry = CacheEntry(query, policy, self.memcache)

        (cacheresult, result) = entry.get()

        if cacheresult == 'hit':
            return result

        result = ctx.low_querier.read(query, varenv)

        entry.set(result)

        return result

# this really needs to be merged in with LojsonCache
class CachedLowQuery(object):

    def __init__(self,ctx):
        self.ctx = ctx

    def __getattr__(self, attr):
        """
        lazily forward all access (usually just low_querier and
        lookup) to the ctx, because some attribute accesses on ctx
        will result in a graph connection
        """
        return getattr(self.ctx, attr)

    # the only supported method.
    def read(self,query,varenv):
        if self.ctx.locache:
            result = self.ctx.locache.lowread_wrapper(self.ctx, query, varenv)
        else:
            result = self.ctx.low_querier.read(query, varenv)

        return result

    
