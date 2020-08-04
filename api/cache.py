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


from hashlib import md5
from itertools import izip
import time, re, sys

from mw.log import LOG
# from mw.mql.graphctx import dateline_compare

from mw import json
import cPickle as pickle

import pylibmc
from decorator import decorator

class PickledFile(object):
    """
    This is a wrapper around a normal file-like object that honors the
    protocol described in pickle.Pickler and pickle.Unpickler. The
    idea is simply that it records the length and hash of data as it is read
    from an actual file, by intercepting the read/readline/write methods.

    At any point, the `length` attribute will contain the total number
    of bytes read from the stream. And the method get_key() will
    return a key that can be used to hash the data

    Note that while this wrapper is used by both read AND write
    consumers, this is NOT meant to be used in a combined read/write
    context. A single instance should only be used for reading or
    writing, but not both.

    If used in a write context, the `file` argument to __init__() can
    be None, which allows accumulation of length and hash that would
    be written, without actually storing any data.
    """
    def __init__(self, file=None):
        self.file = file
        self.file_read = file.read
        self.length = 0

    def read(self, n):
        # this is called way too much
        data = self.file_read(n)
        self.length += len(data)
        return data

    def readline(self):
        data = self.file.readline()
        self.length += len(data)
        return data

    def write(self, data):
        self.length += len(data)
        if self.file is not None:
            return self.file.write(data)

class LoggingPickler(object):
    """
    This is a wrapper/subclass of pickle.Pickler. Since
    cPickle.Pickler cannot be subclassed, this uses automatic
    attribute delegation to emulate subclassing, only overriding the
    `dump` method to log the total number of bytes dumped.
    """
    def __init__(self, file, protocol=None):
        self.pfile = PickledFile(file)
        self.pickler = pickle.Pickler(self.pfile, protocol=protocol)
        
    def __setattr__(self, attr, val):
        if attr not in ('pfile', 'pickler'):
            return setattr(object.__getattr__(self, 'pickler'), attr, val)
        object.__setattr__(self, attr, val)

    def __getattr__(self, attr):
        return getattr(object.__getattr__(self, 'pickler'), attr)
    
    def dump(self, obj):
        """
        Dump to the given file, and then close it. This object is
        rendered inoperable after that, meaning further calls to
        read/write will fail.
        """
        res = self.pickler.dump(obj)
#         LOG.notice("memcache.pickle.length",
#                    "Length is %d" % self.pfile.length,
#                    code=self.pfile.length)
        del self.pfile
        return res

has_whitespace = re.compile(r"\s").search

def keymeta_serialize(v):
    """
    For generating memcache keys: make something short and sweet out
    of userdata - i.e. boolean 'False' becomes '0' etc, and all
    non-primitives just get md5'd.

    The goal here is simply to generate a unique string to use for
    this value in memcache. This means to shorten long strings, we
    need to use a hash, rather than just truncation.
    """
    
    
    # sort out guys first, keep the short version
    if v is True:
        return '1'
    
    if v is False:
        return '0'
    
    if isinstance(v, (int, long, float)):
        return str(v)

    # all incoming values should be ascii strings, hence str() should
    # be safe. These are things like asof values, /lang/en, etc.
    if isinstance(v, basestring):
        v = str(v.replace(":", "#"))

    else:
        # some complex structure, use json format instead turns out
        # jsonlib2 is actually faster than cPickle!  since this is a
        # one-way hash, it's ok not to escape characters here
        v = json.dumps(v)
    
    # Strings shouldn't be longer than an md5 hash. don't let strings
    # get bigger than 32 characters. Also, memcache doesn't like
    # whitespace in keys. Either of these cases forces a hash instead
    # of the raw value
    if len(v) > 32 or has_whitespace(v):
        v = md5(v).hexdigest()
        
    return v
    
class LoggingUnpickler(object):
    """
    This is a wrapper/subclass of pickle.Unpickler. Since
    cPickle.Unpickler cannot be subclassed, this uses automatic
    attribute delegation to emulate subclassing, only overriding the
    `load` method to log the total number of bytes dumped.
    """
    def __init__(self, file):
        self.pfile = PickledFile(file)
        self.pickler = pickle.Unpickler(self.pfile)

    def __setattr__(self, attr, val):
        if attr not in ('pfile', 'pickler'):
            return setattr(object.__getattr__(self, 'pickler'), attr, val)
        object.__setattr__(self, attr, val)

    def __getattr__(self, attr, val):
        return getattr(object.__getattr__(self, 'pickler'), attr, val)
    
    def load(self):
        res = self.pickler.load()
#         LOG.notice("memcache.unpickle.length",
#                    "Length is %d" % self.pfile.length,
#                    code=self.pfile.length)
        
        # make sure not to leak
        del self.pfile
        return res

class BasicCachePolicy(object):
    """
    A cache policy is an object which gets called to do a few things:
    
    - annotate the key object before the object is stored in memcache
    
    - annotate the data stored in memcache
    
    - extract the original data from the annotated data

    - determine if data stored in memcache is valid or not

    A cache policy instance exists for the life of an mss, so that it
    may have access to things like transaction_id, time_start, and the
    like.

    This default policy is basically a no-op policy with infinite
    lifetime. A 'tag' is required to uniquely identify this class of
    entry in the cache, so that two policy's keys don't collide.

    Usage::

        >>> policy = BasicCachePolicy('foo')
        >>> entry = CacheEntry("my key", policy, memcache)
        >>> entry.set("some data for 'my key'")
        >>> entry.get()
        "some data for 'my key'"
    

    Usage:

    >>> policy = BasicCachePolicy('foo')

    >>> entry = CacheEntry("my key", policy, memcache)

    >>> entry.set("some data for 'my key'")
    >>> entry.get()
    "some data for 'my key'"
    
    """

    def __init__(self, tag):
        self.tag = tag
    
    def annotate_key_object(self, key_obj):
        """
        Override this to add metainformation to the cache
        key. Typically you would wrap the result like::

          return {"real_data": key_obj, "meta1": "metadata1", ...}
        """
        return {"key": key_obj,
                "tag": self.tag}

    def get_expires(self):
        """
        return a memcache-compatible expiration, which could be either
        an absolute time, or a relative time, as well as a True or
        False indicating if the data is 'long lived' meaning . Per the
        memcache docs:

        ...the actual value sent may either be Unix time (number of
           seconds since January 1, 1970, as a 32-bit value), or a
           number of seconds starting from current time. In the latter
           case, this number of seconds may not exceed 60*60*24*30
           (number of seconds in 30 days); if the number sent by a
           client is larger than that, the server will consider it to
           be real Unix time value rather than an offset from current
           time.

        typically we'll use absolute time
        """


        return (0, False)

    def is_expired(self, key, full_result):
        """
        Return True or False indicating if the given result from
        memcache is expired or not
        
        Open question is, do we want to distinguish between these?
        
         - hard expired in memcache (maybe we wouldn't get here..)
        
         - expired by metadata
        
         - expired by lwt or other context-sensitive policy
        """
        return False
    
    def annotate_result(self, result):
        """
        Build the actual result object that will be stored in the
        cache. Usually this involves wrapping `result` in a dictionary
        of some sort. The return value from this function is what is
        actually stored in the cache.
        """
        return result

    def extract_result(self, full_result):
        """
        Once we have a raw object out of the cache, extract the real
        object. This is kind of the opposite of annotate_result, but
        also allows state (like the age the of the query) to be
        extracted and stored
        """
        return full_result

    def add_cost(self, costkey, value=1):
        """
        Add cost accounting - costkey will be something like:
        
          - 'r' for a cache read
          - 'w' for a cache write
          - 'h' for a cache hit
          - 'm' for a cache miss
          - 'm+h' for a cache miss or hit

        (not sure if this belongs here or elsewhere...)
        """
        pass
    
    def should_read_cache(self):
        """
        Should we allow reads from the cache?
        """
        return True

    def should_write_cache(self):
        """
        Should we write all successful reads through to the cache?
        """
        return True

    def allow_stale(self):
        """
        Should we return the stale result on a miss?
        """
        return False

class MemcacheChecker(object):
    """
    Context manager which logs errors in memcache. Usage:

    with MemcacheChecker(memcache):
        memcache.get(...)

    memcache is an instance of memcache.Client
    """
    def __init__(self, memcache):
        self.memcache = memcache

    def __enter__(self):
        """
        record the 'state' of memcache opaquely - save the value from
        this call for a call to check_memcache_state, which will
        log an error if the state changes in a bad way

        """
        if hasattr(self.memcache, 'servers'):
            live_servers = set(server for server in self.memcache.servers
                               if server.deaduntil == 0)
            self.old_state = live_servers

    def __exit__(self, type, value, traceback):
        """
        check the memcache state as compared to a previous state - if
        the state has changed in a bad way, log it.

        python-memcached doesn't provide a good way of catching
        server errors, so the best thing we can do is compare the
        list of "up" servers before and after access
        """
        if hasattr(self.memcache, 'servers'):
            now_live_servers = set(server for server in self.memcache.servers
                                   if server.deaduntil == 0)

            down_servers = self.old_state - now_live_servers
            for server in down_servers:
                LOG.warn("memcache.dead", "Server just went down",
                         code=str(server))

        
class CacheEntry(object):
    """
    A CacheEntry is an opaque object which is used to actually get/set
    things in a cache. It calculates the actual cache key for a
    query object, as well as any additional state that is necessary
    for getting and setting data from the cache, dealing with
    staleness, etc.

    This object exists for the use of a single query/result.

    """

    # this is the memcache data format version - if data in memcache
    # ever changes format, we increment this version, which means new
    # requests for new data will be cache misses

    # keep this a monotonically increasing integer, don't be fancy
    # with subversion numbers/etc!

    # version history:
    # 1 -> 2: switch from cPickle to json using jsonlib
    # 2 -> 3: generational name/value in key
    version = 3
    version_str = "v=%s" % version
    
    def __init__(self, key_obj, policy=None, cache=None, cachegroup=None):
        """
        Create a CacheEntry.

        * `key_obj`
        
        * `cache` - any object with a get / set API, including
          python-memcached

        * `cachegroup` - an optional label for logging

        """
        if policy is None:
            policy = BasicCachePolicy('default')
        self.cache = cache
        self._policy = policy
        self.key_obj = key_obj
        self.cachegroup = cachegroup

        # generate the key immediately, essentially 'snapshotting' the
        # key_obj
        self.key = self.get_key()

        # make sure these keys at least show up, because we have tools
        # that expect them
        self._policy.add_cost('r', 0)
        self._policy.add_cost('h', 0)
        self._policy.add_cost('m+h', 0)
        self._policy.add_cost('m', 0)
        self._policy.add_cost('w', 0)
    
    def _make_key(self, key_obj):
        """
        Create the raw key by hashing the pickled value. This is
        potentially expensive, and should not change. So if you do
        call this, store it somewhere.
        """

        # XXX this should be refactored at some point so we don't
        # stick stuff into a dictionary in annotate_key_object, and then pull
        # it all back out again here. Can we avoid the intermediate format?
        keystr = [self.version_str]
        
        keystr.extend(('%s=%s' % (k,keymeta_serialize(v)))
                       for k,v in sorted(key_obj.iteritems()))

        keystr = ':'.join(keystr)
        return keystr

    def _log_kwds(self, **kwds):
        return log_kwds(cachegroup=self.cachegroup, **kwds)

    def get_key(self):
        """
        Generate a key for self.key_obj:

        * Use self._policy to annotate the key
        * Pickle the annotated key and use it's hash
        """
        if hasattr(self, '_key'):
            return self._key

        # let the policy annotate the key
        key_obj_with_policy = self._policy.annotate_key_object(self.key_obj)
        self._key = self._make_key(key_obj_with_policy)

        return self._key

    def check_result(self, key, full_result):
        """
        Returns True or false to let you know if the result is valid
        """
        cache_miss = full_result is None or self._policy.is_expired(key, full_result)
        if cache_miss:
            self._policy.add_cost('m')
        else:
            self._policy.add_cost('h')
        self._policy.add_cost('m+h')
        return not cache_miss

    def raw_get(self):
        """
        Gets the raw value from the cache
        """
        key = self.get_key()
        self._policy.add_cost('r')
        
        # no memcache hooked up?
        if not self.cache:
            return None

        with MemcacheChecker(self.cache):
            try:
                return self.cache.get(key)
            except pylibmc.Error as e:
                LOG.error("memcache.error.get", "memcache get failure", error=e,
                          **self._log_kwds())

    def get(self):
        """
        A full, synchronous get-with-expiration check.

        The whole get-check-extract process is in three easily
        accessible stages so that async cache APIs can call them
        separately.
        """
        if not self._policy.should_read_cache():
            # this is now pretty normal for client. We should only log
            # this when the client isn't requesting it.
            # LOG.warn("memcache.skip.read", "Skipping cache", **self._log_kwds())
            self._policy.add_cost('s')
            return ('miss', None)

        full_result = self.raw_get()

        key = self.get_key()
        if not self.check_result(key, full_result):
            # would rather log this in the policy object, because this
            # is the only use of tag?
            LOG.notice("%s.cache.result" % self._policy.tag, "",
                       key=key, keyobj=self.key_obj,
                       **self._log_kwds(code="miss"))
            if self._policy.allow_stale() and full_result:
                return ('miss', self._policy.extract_result(full_result))
            else:
                return ('miss', None)
                

        return ('hit', self._policy.extract_result(full_result))

    def raw_set(self, full_result):
        """
        Sets the raw value directly in the cache
        """
        key = self.get_key()
        
        if not self.cache:
            return

        self._policy.add_cost('w')
        expires, long_lived = self._policy.get_expires()

        with MemcacheChecker(self.cache):
            try:
                return self.cache.set(key, full_result, time=expires)
            except (pylibmc.WriteError, pylibmc._pylibmc.MemcachedError) as e:
                LOG.error("memcache.error.set", "memcache set failure", error=e,
                          **self._log_kwds())


    def set(self, result):
        """
        A full, synchronous set-with-policy operation.
        """
        if not self._policy.should_write_cache():
            LOG.warn("memcache.skip.write", "Per policy, not writing result to the cache",
                     **self._log_kwds())
            return
        
        full_result = self._policy.annotate_result(result)

        success = self.raw_set(full_result)
        if not success:
            LOG.error("memcache.set.write", "Failed to write %s" % self.get_key(),
                      key=self.get_key(),
                      **self._log_kwds(code=success))

        # acts as an identity function so it can be used with Twisted
        # deferreds and such
        return result

    def __hash__(self):
        return hash(self.get_key())

class CacheEntryList(object):
    """
    Allows getting and setting of multiple cache entries at once.

    When created, CacheEntry objects will be created for each key_obj
    passed to the constructor. These are stored in the `cache_entries`
    attribute.

    The advantage to using this class over maintaining your own list
    of CacheEntries is that `get` and `set` use the `get_multi` and
    `set_multi` memcache APIs, which means fewer roundtrips, etc.

    
    """

    def __init__(self, key_objs, policy, cache, cachegroup=None):
        """
        Create a CacheEntryList

        * `key_objs` - an iterable of keys
        * `cache` - an object with a get_multi / set_multi API, including
           python-memcached
           """
        self.cache = cache
        self._policy = policy
        self.cachegroup = cachegroup
        
        # generate the set of cache entries
        self.cache_entries = [CacheEntry(key_obj, self._policy, self.cache) 
                              for key_obj in key_objs]

    def _log_kwds(self, **kwds):
        return log_kwds(cachegroup=self.cachegroup, **kwds)

    def get(self):
        """
        Gets all the cache entries - will return a triple of::
        
            ('hit', 'miss' or 'skip', value, CacheEntry)
            
        for each cache entry passed in to the CacheEntryList constructor
        """

        # no memcache hooked up?
        if not self._policy.should_read_cache() or not self.cache:
            LOG.warn("memcache.skip.read", "Skipping cache", **self._log_kwds())
            self._policy.add_cost('s')
            return [('skip', None, ce) for ce in self.cache_entries]

        self._policy.add_cost('r')

        with MemcacheChecker(self.cache):
            try:
                memcache_result = self.cache.get_multi([ce.get_key() for ce in self.cache_entries])
            except pylibmc.Error as e:
                memcache_result = {}
                LOG.error("memcache.error.get_multi", "memcache get_multi failure", error=e,
                          **self._log_kwds())
        
        assert isinstance(memcache_result, dict)
        result = []

        # create an entry in the result for each cache entry
        for ce in self.cache_entries:
            key = ce.get_key()
            mr = memcache_result.get(key)
            if not ce.check_result(key, mr):
                result.append(('miss', None, ce))
            else:
                result.append(('hit', ce._policy.extract_result(mr), ce))

        misses = [miss for miss in result if miss[0] == 'miss']
        hits = [hit for hit in result if hit[0] == 'hit']
        
        miss_keys = [ce.key_obj for (status, value, ce) in misses]
        miss_hashes = [ce.get_key() for (status, value, ce) in misses]

        hit_keys = [ce.key_obj for (status, value, ce) in hits]
        hit_hashes = [ce.get_key() for (status, value, ce) in hits]

        if miss_keys:
            code = "hits+misses" if hit_keys else "all miss"
        else:
            code = "all hit" if hit_keys else "empty"
        LOG.notice("%s.cache.multiresult" % self._policy.tag, "",
                   miss_hashes=miss_hashes,
                   hit_hashes=hit_hashes,
                   miss_count=len(misses),
                   hit_count=len(hits),
                   **self._log_kwds(code=code))
        return result

    def set(self, values):
        """
        Sets a value for each key passed into the constructor.
        """
        if not values:
            return values
        if not self._policy.should_write_cache() or not self.cache:
            LOG.warn("memcache.skip.write", "Per policy, not writing result to the cache",
                     **self._log_kwds())
            return

        self._policy.add_cost('w')
        cache_mapping = izip(self.cache_entries, values)
        with MemcacheChecker(self.cache):
            try:
                result = self.cache.set_multi(dict((ce.get_key(), ce._policy.annotate_result(result))
                                              for ce, result in cache_mapping))
                if result:
                    # this only gets logged by python-memcached
                    # implementation
                    LOG.error("memcache.set_multi.write", 
                              "Failed to write %s results" % result,
                              keys=result,
                              **self._log_kwds())
            except pylibmc.WriteError as e:
                LOG.error("memcache.error.set_multi", "memcache set_multi failure", error=e, **self._log_kwds())
                result = {}
                
        return values

def log_kwds(code=None, cachegroup=None, **kwds):
    """
    Return a set of keywords to be used with every log message.
    Allows `code` and `cachegroup` to be None (and won't log `None` as
    a result), but also logs cachegroup in `code` if `code` is not
    already specified.
    """
    if cachegroup:
        kwds["cachegroup"] = kwds["code"] = cachegroup

    if code:
        kwds["code"] = code

    return kwds
    
def memcache_client(addr_list, debug=False):
    """
    Factory for creating connection to memcache - handles all the
    various pickling/unpickling stuff, so this can be used elsewhere
    """
    addrs = ['%s:%s' % addr for addr in addr_list]
    import pylibmc
    mc = pylibmc.Client(addrs, binary=False)
    mc.behaviors = {"tcp_nodelay": True,
                    "ketama": True,
                    "cache_lookups": True,
                    }
    return mc

class MockMemcache(object):
    """
    A global emulation of the python-memcache client, providing
    in-memory storage. this is only to be used for testing. Unless
    otherwise specified, this is a singleton which shares states
    across all instances.

    Usage::

        # by default everything is shared
        >>> m = MockMemcache()
        >>> m.set("abc", "def")
        True
        >>> m = MockMemcache()
        >>> m.get("abc")
        'def'

        # try with singletons
        >>> m = cache.MockMemcache(singleton=False)
        >>> m.get("abc")
        >>>
        
    """

    # globally shared data
    data = {}
    
    def __init__(self, singleton=True):
        if not singleton:
            self.data = {}
    def get(self, key):
        if key not in self.data:
            return None
        
        return pickle.loads(self.data[key])

    def set(self, key, value, time=0, min_compress_len=0):
        self.data[key] = pickle.dumps(value)
        return True

    def set_multi(self, keyvalues):
        for k,v in keyvalues.iteritems():
            self.set(k,v)
        return []

    def get_multi(self, keys):

        results = ((k, self.get(k)) for k in keys)

        return dict(results)


    def get_stats(self):
        """
        Make this look like an memcache client with no hosts. For now
        this makes this look like a broken/disconnected memcache. But
        in the long term we'll want to return something non-False, so
        that is looks like a functioning memcache.
        """
        return {}               # empty for now



@decorator
def _safe_entrypoint(f, *args, **kwds):
    """
    Decorator that just makes the function avoid throwing exception
    """
    try:
        return f(*args, **kwds)
    except pylibmc.Error as e:
        LOG.error("memcache.%s.error" % f.__name__, "Error in memcache call", error=e)
    # return None

class SafeClient(pylibmc.Client):

    @_safe_entrypoint
    def get(self, *args, **kwds):
        return super(SafeClient, self).get(*args, **kwds)

    @_safe_entrypoint
    def set(self, *args, **kwds):
        return super(SafeClient, self).set(*args, **kwds)

    @_safe_entrypoint
    def delete(self, *args, **kwds):
        return super(SafeClient, self).delete(*args, **kwds)

    @_safe_entrypoint
    def flush_all(self, *args, **kwds):
        return super(SafeClient, self).flush_all(*args, **kwds)

Client = SafeClient                     # make us look like memache/pylibmc

def SafeMemcachedNamespaceManager(*args, **kwds):
    """
    wrapper around the beaker memcache client, that gives a "safe"
    memcache that doesn't throw exceptions
    """

    from beaker.ext import memcached

    # temporarily monkey-patch beaker during the instantiation of the
    # namespace manager
    old_memcache = memcached.memcache
    memcached.memcache = sys.modules[__name__]
    result = memcached.MemcachedNamespaceManager(*args, **kwds)
    memcached.memcache = old_memcache

    return result
