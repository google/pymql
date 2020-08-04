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


#
#
#  this is intended to be the basic middle tier api,
#   expressed without http dependencies.
#
#

import os, sys, copy, time, resource, socket, datetime, rfc822, re

from urllib import urlencode
from httplib import HTTPConnection, BadStatusLine
from itertools import chain

from Queue import Queue, Empty, Full
from Cookie import SimpleCookie
from mw.tid import generate_transaction_id

from mw.log import LOG
from mw.error import SessionError, RelevanceError, UserLookupError
from mw.user.cache import get_user_by_name
from mw.user.cookie import cookie_headers_from_user, make_lwt_morsel
from mw import siteconfig

from collections import defaultdict

from mw.mql import graphctx, grparse
from mw.mql.lojson import LowQuery
from mw.mql.hijson import HighQuery
from mw.mql.utils import valid_idname, mql_diff
# Part of workaround for bug GD-257:
from mw.mql.error import MQLTimeoutError, MQLConnectionError

from mw.mql.pathexpr import wrap_query

from mw.api.envelope import MQLEnvelope

from mw.api.cache import CacheEntry, memcache_client, MockMemcache
from mw.api.locache import LojsonCache, CachedLowQuery
from mw.api.hicache import LWTCachePolicy

from mw import json

from difflib import unified_diff

# The future.
from mw.mql.dime import Dime

# importing merely for backwards compatibility
from mw.api.op import OP

tid_seqno = 0

RELEVANCE_REQUEST_HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"
}

# class Quota(object):
#     """
#     a Quota object accumulates the total cost of a series of requests.
#
#     this is for resource usage measurements only right now.
#     later it could allow an approximate limit on the cost - graphd allows
#     request limiting using the cost= argument but we have no quota policy
#     or storage yet.
#     """
#     def __init__(self):
#         pass
#

class varenv_property(object):
    """
    This makes any slot a property-like object, except that setting it
    means pushing the new value into varenv
    """
    def __init__(self, varenv_slot):
        self.varenv_slot = varenv_slot
        
    def __set__(self, mss, value):
        return mss.push_varenv(**{self.varenv_slot: value})

    def __get__(self, mss, cls):
        if not mss:
            raise AttributeError("Not for class objects")
        return mss.varenv[self.varenv_slot]

class Session(object):
    """
    this is a series of api requests using the same transaction_id.

    this class also provides the main entrypoints for mql.

    this is not related to an http "session" in any way.  a better
    name would be welcomed.  "transaction" is misleading because
    it implies semantics (atomicity) that is in no way present.
    "request" is accurate from point of view of the http api, but
    is misleading since the session may require multiple underlying
    requests to a variety of internal services.

    a Session may be constructed from some combination of:
      - wsgi environ dictionary (http envelope)
      - json request envelope
      - command-line arguments if invoked from a script

    a session should have roughly the lifespan of a single http request
    or shell command.

    a session should eventually include a "quota"

    at some point it will probably be desirable to "fork" sessions,
    to allow better log tracking of subrequests.

    currently a Session uses a single ServiceContext.  this should
    change later since a Session might well want to make parallel
    subrequests under the same transaction_id.
    """

    # default pool for no config (config loaded via a file) -
    # eventually this should be mapped into ctx_config_pools
    ctx_pool = Queue(40)

    # maps a config to a Queue of connections
    ctx_config_pools = defaultdict(lambda: Queue(40))

    # cheesy, just for rusage rss calculation - lets hope we don't
    # fork!
    #_current_pid = os.getpid()


    lang = varenv_property('$lang')
    transaction_id = varenv_property('tid')
    permission = varenv_property('$permission')
    deadline = varenv_property('deadline')

    def __init__(self, ctx=None, transaction_id=None,
                 config=None,
                 deadline=None):

        # set this before any varenv_property are loaded
        
        # stack for push_*/pop_*
        # tuple of ((new_keys, old_varenv))
        self.varenv_stack = []
        self.user_stack = []

        # the varenv itself
        self.varenv = {'unicode_text': True,
                       'grwlog': []}

        # find the right pool of graph connections - a different pool
        # for each configuration
        if config is not None:

            # hashing the value of the dict, so even if a different
            # dict with the same values come in, map to the same
            # ServiceContext pool
            config_key = tuple(sorted((k,v) for (k,v) in config.iteritems()
                                      if not isinstance(v, (dict, list))))
            # override the class-level queue, so we can put() later
            self.ctx_pool = self.ctx_config_pools[config_key]
            
        if ctx is None:
            try:
                ctx = self.ctx_pool.get(block=False)
            except Empty:
                LOG.notice("new.process", "MQL cache is going to be empty, this is going to be a slow request.")
                ctx = ServiceContext(config=config)

                # this needs to be refactored - load_config should be
                # private to ServiceContext
                if config is None:
                    ctx.load_config()
                    
                ctx.connect()

        self.ctx = ctx

        self.lang = '/lang/en'
        self.request_id = None
        self.transaction_id = transaction_id
        # initialize to -1 so the first sequence id is 0
        self._current_sequence_id = -1
        self._signed_user = None

        # this is a bit of a hack - we have to keep track of the fact
        # that the user existed, but that the session expired. Then
        # wss can clear the cookie
        self._user_session_expired = False

        self.permission = '/boot/all_permission'

        # The application that is running this code
        self.authorized_app_id = None

        # accumulated cost of this session
        self.cost = None

        # XXX
        # This should get moved out when we move relevance out of here.
        # Relevance cost for this session
        self.other_costs = {}

        # this state is accumulated by checking counters in
        #  the graphctx.  it would be cleaner to make
        #  graphctx stateless - then they could be more
        #  freely reused.
        self.varies_by_user = False
        self.write_occurred = False

        # assume a fresh user who has never written anything
        # this comes from the mwLastWriteTime cookie.
        self.last_write_time = 0
        # this is the dateline of the last write
        self.write_dateline = None

        # this is the age of the oldest response returned by memcache -- it goes into the HTTP Age: header. (bug 4904)
        self.cache_age = 0

        self.ctx.flush_if_necessary({ "tid": self.transaction_id })

        self.deadline = deadline

        self.reset_cost()


        self.hints = set()

        self.mql_cache_policy = LWTCachePolicy(self, 'mql')

        self.auth_context = None

        # the auth_context last consumed
        self.auth_context_consumed = None


    def _get_config(self):
        """
        Generic interface to the current configuration - should be a
        read-only dictionary, but we don't have that just yet.
        """
        return self.ctx.get_config()

    config = property(_get_config)

    def _get_full_config(self):
        return self.ctx.get_config(full=True)
    
    full_config = property(_get_full_config)

    # lightweight wrappers that forward to ctx
    # (to shield consumers from knowing about ctx)
    def _set_sql(self, connection):
        # assert not self.ctx._sql_connection, "Still connected to sql"
        self.ctx._sql_connection = connection

    def _get_sql(self):
        return self.ctx._sql_connection

    sql = property(_get_sql, _set_sql)

    def is_really_production(self):
        """
        Checks both a configuration flag and the hostname to ensure that we're on
        the production pod.
        """
        if not hasattr(self, "_is_really_production"):
            # this should probably be cached more globally than on this instance...
            self._is_really_production = False
            if self.config.get("me.is_production", False):
                import socket
                # get the fully qualified hostname
                hostname = socket.getfqdn()
                assert hostname, hostname
                self._is_really_production = hostname and hostname.endswith("p01.sjc1.metaweb.com")
                if not self._is_really_production:
                    LOG.warn("service.is_really_production",
                             "me.is_production does line up with hostname %s" % hostname,
                             hostname=hostname)
        return self._is_really_production
        
    def close(self):
        """
        please call this to release valuable resources!

        XXX should  use __del__(self) but there may be
        issues with GC cycles and finalization?
        """
        if self.ctx is not None:
            # recycle the SQL connection immediately
            if self.sql:
                self.sql.close()
                self.sql = None
                pass
            try:
                self.ctx_pool.put(self.ctx, block=False)
            except Full:
                # just let it be deleted
                pass
            self.ctx = None

    def get_grwlog(self):
        try:
            return self.varenv.get('grwlog', None)
        except AttributeError:
            # finish_init() was never called
            # - see ME028/01.xml, bug ME-931
            return None

    def get_nreqs(self):
        # nreqs is the number of graph requests it took to run this request
        if not self.ctx.connected():
            return 0
        
        return self.ctx.gc.nrequests - self.graph_requests_start

    def add_cost(self, key, value):
        """
        Add a new cost, assuming that the cost stored in `key` starts at zero.

        This will also do its best to store integers, rather than
        floats, for nicer display if possible
        """
        if key not in self.other_costs:
            self.other_costs[key] = 0

        # handle 1.0, "1.0", "1.1" or 1
        f_value = float(value)
        # integers are best, if possible
        if value == int(f_value):
            value = int(f_value)
        else:
            value = f_value

        # this will upconvert ints to floats if value is a float
        self.other_costs[key] += value

    def get_max_rss(self, rusage_data):
        if rusage_data.ru_maxrss:
            return rusage_data.ru_maxrss

        # fall back to /proc
        # this is super-cheezy and probably expensive, but linux
        # doesn't have a working ru_maxrss
        #meminfo_path = '/proc/%d/status' % self._current_pid
        meminfo_path = '/proc/self/status'
        if not os.path.exists(meminfo_path):
            return rusage_data.ru_maxrss
        
        with file(meminfo_path) as f:
            for line in f:
                m =  re.match("VmRSS:\s+(\d+)", line)
                if m:
                    rss_str = m.group(1)
                    return int(rss_str)

        return 0

    def reset_cost(self):
        """
        Reset all costs returned from get_cost
        """
        # system-level costs
        self.time_start = float(time.time())
        self.rusage_start = resource.getrusage(resource.RUSAGE_SELF)
        self.rusage_start_maxrss = self.get_max_rss(self.rusage_start)

        # Reset graph cost
        if self.ctx.connected():
            self.ctx.gc.reset_cost()
            self.graph_requests_start = self.ctx.gc.nrequests
            self.ctx.gc.write_occurred = False
            self.ctx.high_querier.reset_cost()
        else:
            self.graph_requests_start = 0

        # Reset CDB cost ('bctn', 'cdbtn')
        if hasattr(self.ctx, 'blobd'):
            self.ctx.blobd.cost = 0
            self.ctx.blobd.cdb_cost = 0

        # reset DIME costs
        if self.ctx.dime:
            self.ctx.dime.reset_cost()

        self.other_costs = {}

    def get_cost_values(self):
        """
        Get the cost from the current request, and from the gc -
        returns a dictionary mapping cost -> value
        """
        # now pull in other costs from the gc (these are fresh as of
        # the above call to reset_cost())
        if self.ctx.connected():
            totalcost = self.ctx.gc.totalcost
            graph_items = [(k, totalcost[k])
                           for k,name,desc in grparse.cost_parameters
                           if k in totalcost]

            # get mql cpu times
            if self.ctx.dime:
                mql_items = self.ctx.dime.get_cost()
            else:
                mql_items = self.ctx.high_querier.get_cost()
        else:
            graph_items = {}
            mql_items = {}

        # We don't have to reset the cost since this object lives for exactly
        # one request.

        # dt is total time spent in the session
        dt = float(time.time()) - self.time_start

        # detailed resource usage info from the os
        rusage_end = resource.getrusage(resource.RUSAGE_SELF)
        rusage_end_maxrss = self.get_max_rss(rusage_end)

        cu = rusage_end.ru_utime - self.rusage_start.ru_utime
        cs = rusage_end.ru_stime - self.rusage_start.ru_stime

        cc = cu + cs
        cc = int(cc*1000.0)/1000.0

        utime = int(cu*1000.0)/1000.0
        stime = int(cs*1000.0)/1000.0
        maxrss = rusage_end_maxrss - self.rusage_start_maxrss
        ixrss = rusage_end.ru_ixrss - self.rusage_start.ru_ixrss
        idrss = rusage_end.ru_idrss - self.rusage_start.ru_idrss
        isrss = rusage_end.ru_isrss - self.rusage_start.ru_isrss
        minflt = rusage_end.ru_minflt - self.rusage_start.ru_minflt
        majflt = rusage_end.ru_majflt - self.rusage_start.ru_majflt
        nswap = rusage_end.ru_nswap - self.rusage_start.ru_nswap
        inblock = rusage_end.ru_inblock - self.rusage_start.ru_inblock
        oublock = rusage_end.ru_oublock - self.rusage_start.ru_oublock
        msgsnd = rusage_end.ru_msgsnd - self.rusage_start.ru_msgsnd
        msgrcv = rusage_end.ru_msgrcv - self.rusage_start.ru_msgrcv
        nsignals = rusage_end.ru_nsignals - self.rusage_start.ru_nsignals
        nvcsw = rusage_end.ru_nvcsw - self.rusage_start.ru_nvcsw
        nivcsw = rusage_end.ru_nivcsw - self.rusage_start.ru_nivcsw

        # nreqs is the number of graph requests it took to run this request
        dnreqs = self.get_nreqs()

        me_items = [('dt', int(dt*1000.0)/1000.0),
                    ('cc', cc),
                    ('utime', utime),
                    ('stime', stime),
                    ('maxrss', maxrss),
                    ('ixrss', ixrss),
                    ('idrss', idrss),
                    ('isrss', isrss),
                    ('minflt', minflt),
                    ('majflt', majflt),
                    ('nswap', nswap),
                    ('inblock', inblock),
                    ('oublock', oublock),
                    ('msgsnd', msgsnd),
                    ('msgrcv', msgrcv),
                    ('nsignals', nsignals),
                    ('nvcsw', nvcsw),
                    ('nivcsw', nivcsw),
                    ('nreqs', dnreqs)]

        me_items = [(k,v) for (k,v) in me_items if v]
        
        if hasattr(self.ctx, 'blobd'):
            if self.ctx.blobd.cost:
                me_items.append(('bctn', self.ctx.blobd.cost))
            if self.ctx.blobd.cdb_cost:
                me_items.append(('cdbtn', self.ctx.blobd.cdb_cost))
        all_costs = {}

        all_costs.update(me_items)
        all_costs.update(graph_items)

        # other costs -
        # including MQL costs, 
        # can end up adding to mql and graph costs - add them up
        for k,v in chain(mql_items.iteritems(),self.other_costs.iteritems()):
            if k in all_costs:
                v = all_costs[k] + v
            all_costs[k] = round(v, 3)

        return all_costs
            
    def get_cost(self):
        """
        Output the cost into a nicely formatted string, suitable for
        X-Metaweb-Cost

        see parse_cost for parsing
        """
        all_costs = self.get_cost_values()
        return ', '.join(['%s=%s' % item for item in
                          sorted(all_costs.iteritems())])

    def finish_init(self):
        """
        build the varenv and other preliminaries to using mql.
        this must be called before mqlread, mqlwrite, blobwrite, ...
        """
        if self.transaction_id is None:
            self.transaction_id = generate_transaction_id("get_user")

        self.varenv.update({ '$user': self.get_user_id(),
                        'last_write_time': self.last_write_time,
                        'write_dateline': self.write_dateline,
                        })

        # to allow mss = Session().finish_init()
        return self

    #XXX Why wasn't this here before ? what is wrong with getting the _user from the session?
    def get_user(self, validate=False, authenticate=False):

        # first see if we can pull in any credentials
        if authenticate:
            self.authenticate()
            
        if self._signed_user and validate:
            self._signed_user.validate(self)
        return self._signed_user

    def get_user_id(self, guid=True, validate=False, authenticate=False):
        # don't set vary by user as the callers of this don't actually
        # vary by user.
        
        # we'll need to validate if we need the guid - sadly validate
        # means checking both SQL and graph, we only need to hit the
        # graph
        user = self.get_user(validate=validate or guid,
                             authenticate=authenticate)
        if user:
            if guid:
                return user.guid
            else:
                return user.id

    def get_app_id(self):
        return self.authorized_app_id

    def get_app_api_key(self, application_guid):
        """
        Get an application's key and secret

        return an object that has 'key' and 'secret' attributes
        """

        # this is a bad dependency
        from mw.user import sqlmodel
        conn = sqlmodel.get_sql_connection(self)
        
        if not conn:
            LOG.notice("api.appkey.missing", "SQL not configured, cannot retrieve application key")
            return None

        consumer_tokens = sqlmodel.mwOAuthConsumer.selectBy(guid=application_guid,
                                                            connection=conn)
        if not consumer_tokens:
            return None

        return consumer_tokens[0]

    def authenticate(self):
        """
        If there is an auth_context, use it to authenticate. This
        allows external authentication mechanisms to encapsulate their
        state in the auth_context object.
        """
        self.varies_by_user = True
        if self.auth_context:
            if self.auth_context is not self.auth_context_consumed:
                self.auth_context.authenticate(self)

            # record that we've used this auth context already
            self.auth_context_consumed = self.auth_context

    def next_sequence_id(self):
        """
        Returns the next sequence id for building subtids.
        """
        self._current_sequence_id += 1
        return self._current_sequence_id

    def push_varenv(self, **kwds):
        """
        Push one or more variables into varenv. Each call to
        `push_varenv` should be balanced with a single call to
        `pop_varenv`.

        Sample usage::

            mss.push_varenv(cursor=my_cursor, unicode_text=True)

            mss.mqlread(my_query)

            mss.pop_varenv()

        For specialized variables with dollar signs, use push_variables()
        """
        if '$user' in kwds:
            LOG.notice('varenv.switch.user', 'switching from user %s to %s' % (self.varenv['$user'],
                                                  kwds['$user']))

        # keep track of which keys were pushed. Other variables in
        # varenv might be modified, and we'll need to reflect that
        # down into the varenv
        self.varenv_stack.append((kwds.keys(), self.varenv))
        # XXX how deep?
        self.varenv = copy.copy(self.varenv)
        self.varenv.update(kwds)

        return EndingContext(self.pop_varenv)

    def pop_varenv(self):
        """
        Balance a call to `push_varenv` / `push_variables` - it
        doesn't matter how many variables are in the push_varenv
        call. See `push_varenv` for sample usage.
        """

        # NOTE: all copies of varenv refer to the same
        # grwlog, so back propogation is not necessary

        if len(self.varenv_stack) > 0:
            # this is tricky. The goal here is to let any variables
            # that were touched in the current varenv, survive the
            # pop... BUT if the user pushed any variables, they should
            # not be pushed.

            # the important keys here are last_write_time and
            # write_dateline, which need to survive across multiple
            # reads/writes in the same request
            current_varenv = self.varenv
            pushed_keys, new_varenv = self.varenv_stack.pop(-1)

            migrated_keys = []
            for key, value in current_varenv.iteritems():
                if key not in pushed_keys:
                    # this is just for logging
                    if key in new_varenv and new_varenv[key] != value:
                        migrated_keys.append(key)

                    # it's ok to copy every value, since most will be
                    # the same, and we want the changed keys to
                    # survive
                    new_varenv[key] = current_varenv[key]

            if pushed_keys:
                LOG.info("pop_varenv", "Popping varenv keys",
                           popped_keys=pushed_keys,
                           migrated_keys=migrated_keys)

            self.varenv = new_varenv
            
            return pushed_keys
        else:
            LOG.warn("pop_varenv", "Over-popping the varenv!")

    def push_variables(self, **kwds):
        """
        Purely a convenience method to push "$" style variables like
        $user just say push_variables(user="/user/my_id") and pop with
        pop_variables.

        For instance, this::

           mss.push_variables(user="/user/alecf")

        is equivalent to::

           mss.push_varenv(**{"$user": "/user/alecf"})

        """
        new_kwds = {}
        for key, value in kwds.iteritems():
            new_kwds["$" + key] = value

        return self.push_varenv(**new_kwds)

    pop_variables = pop_varenv

    
    def login(self, user_id, attribution_id=None, validate=False):
        """
        Login as the given user. Can be used as a context manager:

        with mss.login("/user/alecf"):
            mss.mqlwrite(...)
        """
        # note that if attribution_id is not specified, this will
        # *clear* the attribution
        assert user_id.startswith("/user/") and \
            valid_idname(user_id), "%s is not a valid user id, should be /user/username" % user_id
        user_name = user_id[len("/user/"):]
        user = get_user_by_name(user_name)
        if not user:
            # TODO get_user_by_name should actually propogate it's error so 
            # the caller has some idea why the login failed
            raise UserLookupError("Unable to login as user %s" % user_name,
                                  app_code="/service/login/error", 
                                  username=user_name)
        elif validate:
            user.validate(self)
        
        self.user_stack.append(self._signed_user)
        self._signed_user = user
        self.push_variables(user=user_id, attribution=attribution_id)
        
        return EndingContext(self.logout)

    def logout(self):
        old_keys, new_varenv = self.varenv_stack[-1]
        assert sorted(old_keys) == ['$attribution', '$user'], "Can't log out, wrong varenv keys on the stack: %s" % (old_keys,)

        # this is a little cheezy
        if self._signed_user:
            self._signed_user.save_prefs(self)
        self._signed_user = self.user_stack.pop()

        self.pop_varenv()

    def begin_privileged(self):
        """
        Start using scope.Privileged to do unprotected mql writes

        can be used as a context manager:

        with mss.begin_privileged():
            mss.mqlwrite(...)
            
        """
        from mw.mql import scope
        LOG.warn("privileged_mode.start", "Entering privileged mode - current user can write to the graph without any security restrictions")
        self.push_variables(privileged=scope.Privileged)

        return EndingContext(self.end_privileged)

    def end_privileged(self):
        LOG.warn("privileged_mode.end", "Exiting privileged mode")
        old_keys, new_varenv = self.varenv_stack[-1]
        assert old_keys and old_keys == ['$privileged'], "Can't exit privileged mode - wrong varenv keys on the stack: %s" % (old_keys,)

        self.pop_varenv()

    def begin_permission(self, permission_id):
        """
        Start using the given permission_id for all mqlwrites

        can be used as a context manager:

        with mss.begin_permission(permission_id):
            mss.mqlwrite(...)
        """
        LOG.notice("permission.start", "Using permission %s" % permission_id, code=permission_id)
        self.push_variables(permission=permission_id)
        return EndingContext(self.end_permission)

    def end_permission(self):
        old_keys, new_varenv = self.varenv_stack[-1]
        assert old_keys and old_keys == ['$permission'], "Can't drop permission - wrong varenv keys on the stack: %s" % (old_keys,)
        LOG.notice("permission.end", "Stopping using permission",
                   code=self.varenv['$permission'])
        self.pop_varenv()
    
    def update_varenv_user(self):
        """
        This is a little odd - we're making sure that the user in
        varenv['$user'] is in sync with get_user_id() - really these
        things should be unified somehow, perhaps when self._signed_user is set?
        """
        self.varenv['$user'] = self.get_user_id(guid=False)

    def set_last_write_time(self, lwt, dateline):
        #assert(self.last_write_time is None, "self.last_write_time: %s" % (str(self.last_write_time)))
        self.last_write_time = lwt
        self.write_dateline = dateline

    def touch(self, since=None):
        """
        Experimental method that may be useful in scripts to avert
        cache hits.  Updates the last write time.
        """
        # TODO: Not sure if the dateline needs to be updated here as well
        if not since:
            since = time.time()
        if isinstance(since, datetime.datetime):
            since = time.mktime(since.timetuple())
        if since > self.varenv['last_write_time']:
            self.varenv['last_write_time'] = since

    def mqlread(self, sq, **kwds):
        # make python happy it got an object, but not one that is
        # None, because that's a possible MQL result. If this value
        # ever escapes this function, we know there's a bug.
        initial_result = result = object()

        assert self.ctx.high_querier, "No graph servers configured"
        with self.push_varenv(**kwds):
            pagecnt = 1
            if 'page' in self.varenv:
                pagecnt = self.varenv['page'] + 1
    
            if pagecnt > 1 and 'cursor' not in self.varenv:
                self.varenv['cursor'] = True
    
            for i in range(0, pagecnt):
    
                self.varenv['page'] = i # return the last page tried
    
                entry = CacheEntry(sq, self.mql_cache_policy,
                                   self.ctx.memcache,
                                   cachegroup=self.varenv.get('cachegroup'))
    
                (cacheresult, result) = entry.get()
    
                if cacheresult != 'hit':

                    # Two MQL's makes a DIME.
                    if self.ctx.dime:
                        env = self.ctx.dime.env_from_varenv(self.varenv)
                        dateline = self.varenv.get('write_dateline', None)
                        tid = self.varenv.get('tid',None)
                        result_env = self.ctx.dime.mqlread(sq, tid=tid, dateline=dateline, env=env)
                        if 'cursor' in result_env:
                            self.varenv['cursor'] = result_env['cursor']
                        result = result_env['result']

                        # If we're using a DIME-like service and we want to
                        # report MQL compatability in the log stream, run
                        # through pymql, diff the output, log it.
                        if self.config.get("me.mql_compatability_mode", True):
                            mql_diff(sq,(result,"dime"), self)

                    else:
                        self.add_cost('mr', 1)
                        result = self.ctx.high_querier.read(sq, self.varenv)
                    entry.set(result)
    
                # stop if we're at the end of a cursor'ed stream.
                # careful because varenv['cursor'] = None is not
                # really a valid cursor at all, but we don't want that
                # to imply cursoring.
                if 'cursor' in self.varenv and self.varenv['cursor'] == False:
                    break

        self.add_hint('read')

        assert result is not initial_result, "Strange exit from mqlread, should NEVER happen"

        return result

    def mqlwrite(self, sq, **kwds):
        LOG.notice('mqlwrite', '%s' %
                 json.dumps(sq, indent=2),
                 varenv="%r" % (self.varenv))

        if self.config.get("me.mqlwrite_is_mqlcheck"):
            LOG.notice('mqlcheck', 'Switching to mqlcheck for performance pod')
            return self.mqlcheck(sq, **kwds)

        # this doesn't play well with the user stored in varenv
        # self._signed_user.validate(self)
        try:
            if kwds:
                self.push_varenv(**kwds)
            result = None
            self.varenv['gr_log_code'] = "write"
            result = self.ctx.high_querier.write(sq, self.varenv)
            self.varenv.pop('gr_log_code')

        finally:
            self._copy_lwt_from_varenv(self.varenv)
            if kwds:
                self.pop_varenv()
            LOG.notice('mqlwrite', '%s' % json.dumps(result, indent=2))

        self.add_hint('write')

        return result

    def mqlcheck(self, sq, **kwds):
        varenv = self.varenv


        LOG.notice('mqlcheck', '%s' % json.dumps(sq))
        try:
            result = None
            result = self.ctx.high_querier.check(sq, varenv)

        finally:
            LOG.notice('mqlcheck', '%s' % json.dumps(result))

        return result

    def mql_to_gql(self, tid, mql):
        """
        Return the GQL constraints for a MQL query
        """
        with self.push_varenv(tid=tid):
            if self.ctx.dime:
                return self.ctx.dime.to_gql(mql, tid=tid)
            else:
                return self.ctx.high_querier.to_gql(tid, mql, self.varenv)

    def bake_cookies(self, tid, pod, domain):
        """
        Bake cookies for a given domain.
        'pod' is used for baking the mwLastWriteTimeCookie.
        """
        cookies = {}
        domain = domain.lower()

        root_domain = domain.count('.')
        if root_domain > 1:
            root_domain = domain.split('.', root_domain - 1)[-1]
        else:
            root_domain = domain
            
        is_metaweb = root_domain in ('freebase.com', 'sandbox-frebase.com', 'metaweb.com',
                                     'freebaseapps.com', 'localhost')

        if not is_metaweb:
            return cookies

        lwt = make_lwt_morsel(self, pod)
        if lwt:
            cookies[lwt.key] = lwt.OutputString()

        # cookie_headers_from_user needs a valid user with a guid
        user = self.get_user()
        headers = cookie_headers_from_user(user) if user else {}
        for header, value in headers:
            if header.lower() != 'set-cookie':
                continue
            cookie = SimpleCookie(value)
            for key, morsel in cookie.iteritems():
                # check that metaweb cookies are passed to metaweb sites
                # or that the cookie is not domain-constrained
                # or that domain is the domain of the cookie
                # or that domain is a sub-domain of the cookie
                if (key in ('metaweb-user', 'metaweb-user-info') 
                    or not morsel['domain'] 
                    or domain == morsel['domain'].lower() 
                    or domain.endswith('.' + morsel['domain'].lower())):
                    cookies[key] = morsel.OutputString()
        return cookies

    def emqlread(self, tid, query, control,
                 api_keys=None, cache=None, help=False):

        if tid:
            _tid = tid
            self.push_varenv(tid=tid)
        elif 'tid' in self.varenv:
            _tid = self.varenv['tid']
        else:
            raise ValueError, 'no tid'

        if self.varenv.get('normalize_only', False):
            raise NotImplementedError, "normalize_only with extended is not implemented"

        if cache is None:
            from mw.emql.emql import emql_cache
            cache = emql_cache()
            # increment the version whenever you change the state
            entry = CacheEntry('emql_cache:version=1', self.mql_cache_policy,
                               self.ctx.memcache,
                               cachegroup='emql')
    
            status, state = entry.get()
            if status == 'hit':
                cache.set_state(_tid, self, state)
        else:
            entry = None

        try:
            from mw.emql.emql import read
            return read(_tid, self, cache, query, control, api_keys, help)
        finally:
            if tid:
                self.pop_varenv()
            if entry is not None:
                entry.set(cache.get_state())

    def env_read(self, envelope):
        mql_env = MQLEnvelope(self)
        return mql_env.read(envelope)

    def env_reads(self, envelope):
        mql_env = MQLEnvelope(self)
        return mql_env.reads(envelope)

    def env_write(self, envelope):
        response = MQLEnvelope(self).write(envelope)
        self._copy_lwt_from_varenv(self.varenv)
        return response

    def env_writes(self, envelope):
        response = MQLEnvelope(self).writes(envelope)
        self._copy_lwt_from_varenv(self.varenv)
        return response

    def env_check(self, envelope):
        return MQLEnvelope(self).check(envelope)

    def env_checks(self, envelope):
        return MQLEnvelope(self).checks(envelope)

    def lookup_guid(self, id):
        """
        id -> guid mapping
        """
        if self.ctx.dime:
            return self.ctx.dime.lookup_guid(id, tid=self.varenv.get('tid'))

        return self.ctx.lookup.lookup_guid(id, self.varenv)

    def lookup_guids(self, ids):
        """
        Lookup multiple guids in one shot
        """
        if self.ctx.dime:
            return self.ctx.dime.lookup_guids(ids, tid=self.varenv.get('tid'))
        return self.ctx.lookup.lookup_guids(ids, self.varenv)

    def lookup_id(self, guid):
        """
        guid -> id mapping
        """
        if self.ctx.dime:
            return self.ctx.dime.lookup_ids(guid, tid=self.varenv.get('tid'))
        return self.ctx.lookup.lookup_id(guid, self.varenv)

    def lookup_ids(self, guids,**kws):
        """
        Lookup multiple ids in one shot
        """
        if 'always_succeed' in kws:
            LOG.info('deprecated.argument', 'someone called lookup_ids with always_succeed, they should take that out.')
        if self.ctx.dime:
            return self.ctx.dime.lookup_ids(guids, tid=self.varenv.get('tid'))

        # We will always succeed...lookup_ids will return you mids.
        return self.ctx.lookup.lookup_ids(guids, self.varenv)
        

    def fetch_fresh_dateline(self, ignore_current_dateline=False):
        latest_dateline = None

        # Usually a fresh graphd dateline is available in
        # GraphContext.dateline as a result of a previous
        # read or write, but sometimes it isn't and this
        # explicit lookup is necessary.

        # Recommended method for finding dateline 'now' as of
        # me/dev/98:
        #
        # Find the timestamp of the most recent *link*
        # (which will always be newer than the most recent node.)
        #
        # There is a margin of error here as what we need is the
        # graph master dateline 'now', but what we get is a random
        # graph replica dateline 'now'.

        query = {
            "sort": "-timestamp",
            "timestamp": None,
            "type": "/type/link",
            "limit": 1
        }

        # bypass memcache, and don't worry about our current dateline
        if ignore_current_dateline:
            self.push_varenv(cache=False, write_dateline=None)
        else:
            self.push_varenv(cache=False)
        result = self.mqlread(query)
        if result:
            # We want the dateline, not the result.
            latest_dateline = self.varenv["dateline"]
        else:
            LOG.warning("_fetch_dateline", "graph dateline query failed")

        self.pop_varenv()

        LOG.notice("dateline.lookup", "", dateline=latest_dateline)
        return latest_dateline

    def _copy_lwt_from_varenv(self, varenv):
        # Track timestamp and dateline changes in such a way
        # that they are non-decreasing.  In other words, don't copy
        # timestamp and dateline changes that go backward in time.
        # At least one case of this problem is due to abandonment
        # of a temporary varenv in MQLEnvelope.write().  Not being
        # sure what impact changing that code will have - just filter
        # the bad side effect here.

        if 'last_write_time' in varenv:
            if (varenv['last_write_time'] > self.last_write_time):
                self.last_write_time = varenv['last_write_time']

        if 'write_dateline' in varenv:
            # see bug 5666 -- sometimes the "write" does nothing so does not set a new dateline.
            #result = graphctx.dateline_compare(varenv['write_dateline'], self.write_dateline)
            if self.write_dateline != varenv['write_dateline']:
                # expected case for writes
                self.write_dateline = varenv['write_dateline']
          
    def add_hint(self, hint):
        """
        add a new hint - wrapper around self.hints.add because we may
        eventually do some checking to make sure that we aren't
        combining incompatible hints
        """
        self.hints.add(hint)


    def _relevance_connect(self, server, connection=None, timeout=None):

        if connection is not None:
            try:
                connection.close()
            except:
                pass

        host = getattr(self.ctx, server + '_host')
        LOG.info("relevance.connect", '', host=host,server=server)

        if timeout is not None:
            connection = HTTPConnection(host, timeout=timeout)
        else:
            # a timeout must be used here at all times to avoid
            # server deadlocks (even when no_timeouts is requested).
            timeout = self.ctx.timeout_policy['timeout']
            connection = HTTPConnection(host, timeout=timeout)
            setattr(self.ctx, server + '_http_conn', connection)

        return connection

    def _create_relevance_query(self, query):

        for name, arg in query.iteritems():
            if isinstance(arg, (list, dict)):
                query[name] = json.dumps(arg)
            elif isinstance(arg, unicode):
                query[name] = arg.encode('utf-8')

        if 'tid' not in query:
            query['tid'] = self.transaction_id

        return urlencode(query)

    def _relevance_cost_accounting(self, headers, start_time):

        self.add_cost("rt", time.time() - start_time)
        if 'x-metaweb-cost' in headers:
            response_cost = dict([x.split('=') for x in headers.get('x-metaweb-cost', '').split(', ')])

            for k,v in response_cost.iteritems():
                self.add_cost('rel:' + k, v)

    def _handle_relevance_result(self, result, status, value):
        """
        Deals with a raw HTTP result - a string body, numeric status, etc
        Then, deals with an actual python object representation of a
        response, i.e. decoded from json
        """
        if status in (200, 400, 408, 500):
            try:
                result = json.loads(result)
            except:
                LOG.error('relevance.parse.error', status)
                raise RelevanceError(result, http_code=status)
        else:
            LOG.error('relevance.error', status)
            raise RelevanceError(result, http_code=status)

        if 'code' in result and 'error' in result['code']:
            LOG.error('relevance.error', result)
            raise RelevanceError(result['messages'][0]['message'],
                                 http_code=status)

        LOG.info('relevance.request.end', '')

        # If they don't specify what to return, return everything.
        if value:
            return result[value]

        return result

    def _relevance_query(self, server, api, value=None, timeout=None, **query):
        """
        Run an http query on the relevance server.
        """
        start_time = time.time()

        LOG.info('relevance.request.start', '',
                 query=query, server=server, api=api)

        query = self._create_relevance_query(query)

        if timeout is not None:
            connection = self._relevance_connect(server, None, timeout)
        else:
            if self.ctx.no_timeouts:
                socket.setdefaulttimeout(None)
            connection = getattr(self.ctx, server + '_http_conn', None)
            if connection is None:
                connection = self._relevance_connect(server)

        attempts = 2

        if self.deadline:
            headers = RELEVANCE_REQUEST_HEADERS.copy()
            headers["x-metaweb-deadline"] = rfc822.formatdate(self.deadline)
        else:
            headers = RELEVANCE_REQUEST_HEADERS

        while True:
            try:
                LOG.notice('relevance.query', '',
                           request_url="http://%s:%s%s?%s" %
                           (connection.host, connection.port,api,query),
                           attempts=attempts, api=api, headers=headers)
                connection.request('POST', api, query, headers)
                response = connection.getresponse()
                break
            except BadStatusLine, e:
                LOG.warning("relevance.http.connection.error",
                            "bad status line: '%s'" %(str(e)))
                connection = self._relevance_connect(server, connection,
                                                     timeout)
                attempts -= 1
                if attempts == 0:
                    self.add_cost('rt', (time.time() - start_time))
                    raise
            except socket.error, e:
                LOG.warning("relevance.http.connection",
                            "socket error: '%s'" %(str(e)))
                connection = self._relevance_connect(server, connection,
                                                     timeout)
                attempts -= 1
                if attempts == 0:
                    self.add_cost('rt', (time.time() - start_time))
                    raise

        self.add_cost('rt', (time.time() - start_time))
        result = response.read()

        headers = dict(response.getheaders())

        self._relevance_cost_accounting(headers, start_time)

        LOG.info('relevance.result', '', resultlen=len(result),
                   headers=headers, http_status=response.status)

        return self._handle_relevance_result(result, response.status, value)

    def _relevance_status(self, server, command):
        """
        Run a status command on the relevance server.
        """

        LOG.notice('relevance.status.request', '', server=server)

        try:
            result = self._relevance_query(server, '/api/service/manager',
                                           command=command)
        except Exception, e:
            LOG.error('relevance.status.error', str(e))
            raise SessionError(str(e),
                               app_code='/service/relevance/not_configured')

        return result

    def _relevance_version(self, server, command):
        """
        Run a (status) command to get the version.
        """
        LOG.notice('relevance.version.request', '', server=server)
        try:
            return self._relevance_query(server, '/api/service/manager',
                                         'version', command=command)
        except Exception, e:
            LOG.error('relevance.version.error', str(e))
            raise SessionError(str(e),
                               app_code='/service/relevance/not_configured')

    def relevance_query(self, full_result=False, timeout=None,
                        api='search', **query):
        LOG.info('relevance.rel.request.start', '', query=query)

        api = '/api/service/' + api
        if full_result:
            result = self._relevance_query('rel', api, None, timeout, **query)
        else:
            result = self._relevance_query('rel', api, 'result', timeout, **query)
        LOG.info('relevance.rel.request.end', '')
        return result

    def relevance_version(self):
        return self._relevance_version('rel', "status")

    def relevance_status(self):
        return self._relevance_status('rel', "status")

    def geo_query(self, timeout=None, **query):
        LOG.info('relevance.geo.request.start', '', query=query)
        result = self._relevance_query('geo', '/api/service/geosearch',
                                       'result', timeout, **query)
        LOG.info('relevance.geo.request.end', '', result=result)
        return result

    def geo_status(self, command="status"):
        return self._relevance_status('geo', command)

    # other entry points
    #  /type/content and blob handling is currently in mw.api,
    #    but should move to mw.api as a supported python client api.
    #
    #  simplesearch
    #  autocomplete
    #  login
    #    these go directly from wsgi to the relevant python code.
    #    so there's no supported good non-http python api yet.
    #

    
# Stolen from clipy/clipy/querier/graph.py
def env_to_err(self, query_result):
    """
    Turn the MQL envelope back into MQL errors, possibly even
    exceptions

    the `envelope` parameter is merely here for debugging
    """ 
    if query_result["code"] != "/api/status/ok":
        # turn this back into a MQLError
        # first make sure it's in the shape we expect (one error in
        # messages
        assert len(query_result["messages"]) == 1
        message = query_result["messages"][0]
        info = dict((str(k), v) for k, v in message['info'].iteritems())
        # now identify the MQLError classfirst identify the class
        error_cls = message['code'][len('/api/status/error/mql/'):]
        error_cls = [cls for cls in dir(mw.mql.error)
                     if cls.lower() == "MQL%sError".lower() % error_cls]
        exc = None
        tid = query_result['transaction_id']

        if not error_cls:
            if message['code'] == '/api/status/error/envelope/parse':
                error_cls = ["MQLParseError"]
            elif message['code'] == '/api/status/error/mql/emql':
                exc = mw.mql.error.MQLError("EMQL", None, 
                                            message['message'], tid=tid, **info)
            else:
                raise Exception(query_result)
        if not exc:
            assert len(error_cls) == 1, error_cls
            error_cls = getattr(mw.mql.error, error_cls[0])

            # now raise the exception with the appropriate parameters
            exc = error_cls(None, message['message'], 
                            # info -> kwds
                            tid=tid, **info)
        if 'query' in message:
            exc.error['query'] = message['query']
        if 'path' in message:
            exc.error['path'] = message['path']
        raise exc


def parse_cost(cost):
    # costs in the form 'a=0, la=123, ' etc...
    costs = (c.lower().strip().split('=') for c in cost.split(','))
    return dict((k, float(v)) for k,v in costs)

def open(environ=None, config=None, *args, **kwds):
    """
    open a metaweb service session
    """
    mss = Session(*args, **kwds)
    return mss

class ServiceContext(object):
    """
    this holds metaweb service state that can be cached and
    reused across multiple requests.

    for now this is compatible with MiniCtx!

    the motivation for MiniCtx was basically "isolate the stuff that
    is expensive to create".  ServiceContext builds on MiniCtx, and
    other code.  it encapsulates several pieces of state:

    - configuration for reaching graphd, blobd, and other services.

    - an open connection to an instance of graphd

    - expensive-to-rebuild cache state, mostly schemas at this point.

    there will almost certainly be reasons to separate out these pieces of
    state later:

    - we may have code that spreads requests across a pool of graph
      connections

    - cached graph state could be shared across multiple
      ServiceContexts in the same process, at least if they are
      connected to the same graph replica (or up to the same
      dateline?)

    - writes might take place through a different graph connection
      than reads.

    basically we don't know at this point.  later decisions about
    cache consistency, graph replica consistency, and appserver
    threading model will clarify what needs to be done here.
    """

    def __init__(self, ctx=None, config=None, **kwds):

        if config is not None:
            self.load_config(config)

        # copy from another ctx (is this ever used?)
        elif ctx is not None:
            self.graphd_addr = ctx.graphd_addr
            self.clobd_read_addrs = ctx.clobd_read_addrs
            self.relevance_addr = ctx.relevance_addr
            self.geo_addr = ctx.geo_addr
            self.mql_addr = ctx.mql_addr
            self.debug = ctx.debug
            self.last_flush_time = ctx.last_flush_time

            # Cannot find connect_graph method??
            #if ctx.gc is not None:
            #    self.connect_graph()
        else:
            # for now these match up with load_config
            self.graphd_addr = None
            self.clobd_read_addrs = None
            self.memcache_addr = None
            # where is max_writes?
            self.relevance_addr = None
            self.mql_addr = None
            self.geo_addr = None
            self.uniqueness_failure = 'hard'
            self.flush_time_interval = 60*60
            self.timeout_policy = None
            self.no_timeouts = False

        # allow attributes to be specified in constructor - pretty
        # sure the only consumer is the OptionParser, which should
        # probably be passing in a config object
        self.__dict__.update(kwds)

        self.last_flush_time = float(time.time())

        # XXX does anybody use this?
        self.low_only = False

        self.debug = False
        self.reset()

    def reset(self):

        # these members are the same as in mw.mql.MiniCtx
        self._gc = None
        self._low_querier = None
        self._high_querier = None
        
        self.locache = None
        self._graphdb = None
        self._sql_connection = None

    def flush_if_necessary(self,varenv):
        if (time.time() - self.last_flush_time > self.flush_time_interval):
            self.flush(varenv)

    def flush(self, varenv, flush_schema=True, flush_ids=True):
        LOG.notice("ctx.flush", "Flushing namespace cache")

        if flush_schema or flush_ids:
            self.last_flush_time = float(time.time())
            if flush_ids:
                self.lookup.flush()
                
            if flush_schema:
                self.high_querier.schema_factory.flush(varenv)

    def load_config(self, config=None):
        """
        initialize a ServiceContext from a mw.siteconfig config.
        """

        # loads up a config via paste
        if config is None:
            config = siteconfig.get_config2()

        self.config = config
        
        self.clobd_read_addrs = \
            siteconfig.get_addr_list2(config, 'clobd.address', quiet=True)
        
        self.geo_addr = \
            siteconfig.get_addr_list2(config, 'geo.address',
                                        quiet=True)
        self.graphd_addr = \
            siteconfig.get_addr_list2(config, 'graphd.address', quiet=True)
            
        self.max_writes = \
            int(config.get('mql.max_writes', 10000000))

        self.memcache_addr = \
            siteconfig.get_addr_list2(config, 'memcache.address', quiet=True)

        self.mql_addr = \
            siteconfig.get_addr_list2(config, 'dime.address', quiet=True)

        self.relevance_addr = \
            siteconfig.get_addr_list2(config, 'relevance.address',
                                        quiet=True)

        self.uniqueness_failure = \
            config.get('mql.uniqueness_failure', 'hard')
        
        self.flush_time_interval = \
            int(config.get('mql.flush_time_interval', 60*60))

        # The timeout policy defined here, based on mwbuild config,
        # supersedes the static 'default' policy defined in graphctx.py.
        # You can override individual policy elements, but any syntax
        # error results in rejection of the entire set of overrides.

        defaults = graphctx.GraphContext.builtin_timeout_policies['default']

        def lookup(sitecfg_key, graphctx_key, coerce_from_string):
            s = config.get(sitecfg_key)
            if s:
                try:
                    v = coerce_from_string(s)
                except ValueError, e:
                    LOG.error("mql.timeout.config.error", str(e),
                              sitecfg_key=sitecfg_key,
                              graphctx_key=graphctx_key, s=s)
                    raise
            else:
                v = defaults[graphctx_key]
            return v

        try:
            self.timeout_policy = {
                'connect': lookup('mql.gd_connect_timeout_in_seconds',
                    'connect', float),
                'timeout': lookup('mql.gd_read_timeout_in_seconds',
                    'timeout', float),
                'down_interval': lookup('mql.gd_down_timeout_in_seconds',
                    'down_interval', float),
                'retry': lookup('mql.gd_retry_timeouts_in_seconds',
                    'retry', lambda s: [float(t) for t in s.split()]),
                'dateline_retry': lookup('mql.gd_dateline_retry_in_seconds',
                    'dateline_retry', float)
            }
        except ValueError, e:
            self.timeout_policy = defaults

        #self.debug = config.get('debug', 'whatever')
        self.debug = False

        no_timeouts = config.get('me.no_timeouts', '')
        self.no_timeouts = no_timeouts.lower() == 'true'


    def get_config(self, full=False):
        if full:
            return self.config
        supported_keys = ('graphd.address',
                          'api.address',
                          'clobd.address',
                          'memcache.address',
                          'relevance.address',
                          'geo.address',
                          'me.is_production',
                          'smtp.address')
        supported_prefixes = ('mql.', 'me.', 'emql.', 'login.')
        # extract supported keys
        return dict((k,v) for (k,v) in self.config.iteritems()
                    if k in supported_keys or k.startswith(supported_prefixes))

    def connected(self):
        """
        Check for an active graph connection without actually
        connecting to the graph
        """
        return self._gc is not None

    def connect_graph(self):
        """
        Actually connect to the graph
        """
        if self._gc is not None:
            return
        
        if self.graphd_addr:
            failure = False
            try:
                if self.no_timeouts:
                    LOG.warning("mql.timeout", "timeouts are turned off")

                self._gc = graphctx.GraphContext(addr_list=self.graphd_addr,
                                                 readonly=False,
                                                 debug=self.debug,
                                                 custom_policy=self.timeout_policy,
                                                 no_timeouts=self.no_timeouts)

                tid = generate_transaction_id("service_boot")

                self._low_querier = LowQuery(self._gc)
            
                self._lookup = self._low_querier.lookup

                if not self.low_only:
                    cached_low_querier = CachedLowQuery(self)
                    
                    self._high_querier = HighQuery(self.low_querier, tid, cached_low_querier)
            except:
                # any exceptions, we need to completely clear our state
                failure = True
                raise

            finally:
                if failure:
                    self._gc = None

    # connect to the graph using graphd's libgraphdb library
    def connect_graphdb(self):

        # lazily import this only on demand
        from graphdb import GraphDB
        
        _graphdb = self._graphdb

        if _graphdb is None:
            if self.no_timeouts:
                LOG.warning("graphdb.timeout", "timeouts are turned off")
                connect = None
                timeout = None
            else:
                # graphdb timeouts are expressed in milliseconds
                connect = int(self.timeout_policy['connect'] * 1000.0)
                timeout = int(self.timeout_policy['timeout'] * 1000.0)

            _graphdb = GraphDB(timeout=timeout,
                               logger=MEGraphDBLogger(),
                               loglevel=GraphDB.CL_LEVEL_OVERVIEW)

            try:
                hosts = [':'.join((host, str(port)))
                         for host, port in self.graphd_addr]
                _graphdb.connect(LOG.request_state.tid, hosts, connect)
            except:
                LOG.error("graphdb.connect.failed", self.graphd_addr)
                raise

            self._graphdb = _graphdb

        return _graphdb

    def disconnect_graphdb(self):

        self._graphdb = None

    # get the graphdb property to get a connected graphdb handle
    # del the graphdb property to drop it (once all refs to it are gone)
    graphdb = property(connect_graphdb, None, disconnect_graphdb)


    # the following properties are just wrappers around their
    # corresponding "_" functions, but with a lazy connection to the
    # graph. This allows mss to be used without actually connecting to
    # the graph.
    @property
    def gc(self):
        self.connect_graph()
        return self._gc

    @property
    def high_querier(self):
        self.connect_graph()
        return self._high_querier

    @property
    def lookup(self):
        self.connect_graph()
        return self._lookup

    @property
    def low_querier(self):
        self.connect_graph()
        return self._low_querier

    def connect(self):
        # you must call load_config() before you call connect()

        #self.blobd = None
        if self.clobd_read_addrs:
            from mw.blob.blobclient import BLOBClient
            self.blobd = BLOBClient(self.clobd_read_addrs)

        memcache = None

        # config can now specify a mock memcache, which means it is
        # only in-memory and lives for the life of the process. This
        # is primarily used for unit testing
        if hasattr(self, 'config') and self.config.get('memcache.mock'):
            memcache = MockMemcache()
            
        elif self.memcache_addr:
            memcache = memcache_client(self.memcache_addr,
                                       debug=self.debug)

        if memcache:
            self.memcache = memcache
            self.locache = LojsonCache(self.memcache,
                                       flush_time=self.flush_time_interval)
        else:
            self.locache = None
            self.memcache = None


        if self.relevance_addr:
            self.rel_host = '%s:%d' %(self.relevance_addr[0])
        else:
            self.rel_host = None

        if self.geo_addr:
            self.geo_host = '%s:%d' %(self.geo_addr[0])
        else:
            self.geo_host = None
           
        if self.mql_addr:
            mql_hosts = ['%s:%d' % a for a in self.mql_addr]
            self.dime = Dime(mql_hosts)
        else:
            self.mql_host = None
            self.dime = None
            

class EndingContext(object):
    """
    Totally generic context manager that simply runs a callback on
    exit. Turns this::

        try:
            foo()
        finally:
            bar()

    into this:

        with EndingContext(bar):
            foo()

    But it is more useful when a function wants to masquerade as
    context manager, like this::

        class StatefulObject(object):

            def my_function(self, state):
                self.some_state = state
                return EndingContext(self.do_cleanup)

            def do_cleanup(self):
                del self.some_state

    Which allows you to say::

        with obj.my_function(some_state):
            obj.do_some_work()
    
    """
    def __init__(self, callback, *args, **kwds):
        self.exit_callback = callback
        self.exit_args = args
        self.exit_kwds = kwds

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        self.exit_callback(*self.exit_args, **self.exit_kwds)

class MqlCursor(object):
    """
    Generic cursor container, which allows you do do this::

        with mss.MqlCursor() as cursor:
            while cursor.current_value:
                result = mss.mqlread(...)

            
    """
    def __init__(self, mss, initial_value=True):
        self.mss = mss
        mss.push_varenv(cursor=initial_value)

    @property
    def current_value(self):
        return self.mss.varenv.get('cursor')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        old_keys, new_varenv = self.mss.varenv_stack[-1]
        assert old_keys and old_keys == ['cursor'], \
            "Can't drop permission - wrong varenv keys on the stack: %s" % (old_keys,)
        self.mss.pop_varenv()


# a bridge to forward libgraphdb logs to ME's logger
class MEGraphDBLogger(object):

    def __init__(self):
        
        from graphdb import GraphDB

        self.methods = {
            GraphDB.CL_LEVEL_ULTRA: LOG.debug,
            GraphDB.CL_LEVEL_VERBOSE: LOG.debug,
            GraphDB.CL_LEVEL_DEBUG: LOG.debug,
            GraphDB.CL_LEVEL_DETAIL: LOG.debug,
            GraphDB.CL_LEVEL_INFO: LOG.info,
            GraphDB.CL_LEVEL_FAIL: LOG.notice,
            GraphDB.CL_LEVEL_OVERVIEW: LOG.notice,
            GraphDB.CL_LEVEL_OPERATOR_ERROR: LOG.error,
            GraphDB.CL_LEVEL_ERROR: LOG.critical,
            GraphDB.CL_LEVEL_FATAL: LOG.fatal,
        }

    def log(self, tid, loglevel, text):

        try:
            self.methods[loglevel]("libgraphdb", text, TID=tid)
        except Exception, e:
            self.methods[GraphDB.CL_LEVEL_ERROR]("libgraphdb", "logging error",
                                                 err=str(e), args=e.args,
                                                 TID=tid)
