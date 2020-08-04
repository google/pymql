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


import re, zlib
from urlparse import urlparse

RE_KEY = re.compile('\$([0-9A-F][0-9A-F][0-9A-F][0-9A-F])')
RE_VARS = re.compile('{([^}]+)}')
RE_NS = re.compile('([^]]+)\[([^]]+)\]')


class Pattern(object):

    def __init__(self, pattern, guid=None, key=None, error=None):

        self.pattern = pattern
        self.guid = guid
        self.vars = dict((var, None) for var in RE_VARS.findall(pattern))
        self.error = error
        if 'key' in self.vars:
            self.vars['key'] = self.decode_key(key)

    # returns a utf-8 encoded string of the pattern with variables
    # whose value is not None expanded.
    # if error is not None, the entire pattern is replaced with error 
    # when a None variable value is encountered
    def __str__(self):

        string = self.pattern
        error = self.error

        if isinstance(string, unicode):
            for var, value in self.vars.iteritems():
                if value is not None:
                    if isinstance(value, str):
                        value = unicode(value, 'utf-8')
                    elif not isinstance(value, unicode):
                        value = unicode(value)
                    string = string.replace(u'{%s}' %(var), value)
                elif error is not None:
                    if isinstance(error, str):
                        string = unicode(error, 'utf-8')
                    elif not isinstance(error, unicode):
                        string = unicode(error)
                    else:
                        string = error
                    break
            string = string.encode('utf-8')
        else:
            for var, value in self.vars.iteritems():
                if value is not None:
                    if isinstance(value, unicode):
                        value = value.encode('utf-8')
                    elif not isinstance(value, str):
                        value = str(value)
                    string = string.replace('{%s}' %(var), value)
                elif error is not None:
                    if isinstance(error, unicode):
                        string = error.encode('utf-8')
                    elif not isinstance(error, str):
                        string = str(error)
                    else:
                        string = error
                    break

        return string

    # returns a unicode string of the pattern with variables
    # whose value is not None expanded.
    # if error is not None, the entire pattern is replaced with error 
    # when a None variable value is encountered
    def __unicode__(self):

        string = self.pattern
        error = self.error

        if isinstance(string, unicode):
            for var, value in self.vars.iteritems():
                if value is not None:
                    if isinstance(value, str):
                        value = unicode(value, 'utf-8')
                    elif not isinstance(value, unicode):
                        value = unicode(value)
                    string = string.replace(u'{%s}' %(var), value)
                elif error is not None:
                    if isinstance(error, str):
                        string = unicode(error, 'utf-8')
                    elif not isinstance(error, unicode):
                        string = unicode(error)
                    else:
                        string = error
                    break
        else:
            for var, value in self.vars.iteritems():
                if value is not None:
                    if isinstance(value, unicode):
                        value = value.encode('utf-8')
                    elif not isinstance(value, str):
                        value = str(value)
                    string = string.replace('{%s}' %(var), value)
                elif error is not None:
                    if isinstance(error, unicode):
                        string = error.encode('utf-8')
                    elif not isinstance(error, str):
                        string = str(error)
                    else:
                        string = error
                    break
            string = unicode(string, 'utf-8')

        return string

    def decode_key(self, key):

        value = key
        if value is not None:
            value = RE_KEY.sub('\\u\\1', value)
            if value is not key:
                value = value.decode('unicode-escape').encode('utf-8')

        return value

    def _prop_name(self, prefix, var, prop):

        # use adler32 as it's shorter than hash on 64-bit and just as fast
        return "%s_%x:%s" %(prefix or "p", zlib.adler32(var) & 0xffffffff, prop)

    def mql_query(self, prefix=None):

        query = {}
        for var, value in self.vars.iteritems():
            if var != 'key' and value is None:
                _query = prev = query
                for prop in var.split('.'):
                    nsprop = RE_NS.search(prop)
                    if nsprop is not None:
                        prop, ns = nsprop.groups()
                        prop = self._prop_name(prefix, var, prop)
                        _query[prop] = {
                            "key": [{
                                "limit": 1, "namespace": ns, "value": None
                            }]
                        }
                        break
                    else:
                        prop = self._prop_name(prefix, var, prop)
                        _query[prop] = [{"limit": 1}]
                        prev = _query
                        _query = _query[prop][0]
                else:
                    # last prop is assumed to be prop: null compatible
                    # so that name or literal queries require no hacks
                    prev[prop] = None

        if query:
            query["guid"] = self.guid

        return query

    def set_key(self, key):

        if 'key' in self.vars:
            self.vars['key'] = self.decode_key(key)

        return self

    def set_mqlres(self, mqlres, prefix=None, clear=False):

        if clear:
            for var in self.vars.iterkeys():
                if var != 'key': 
                    self.vars[var] = None

        for var, value in self.vars.iteritems():
            if var != 'key' and value is None:
                value = mqlres
                for prop in var.split('.'):
                    nsprop = RE_NS.search(prop)
                    try:
                        if nsprop is not None:
                            prop, ns = nsprop.groups()
                            prop = self._prop_name(prefix, var, prop)
                            value = value[prop]['key'][0]['value']
                            break
                        else:
                            prop = self._prop_name(prefix, var, prop)
                            value = value[prop]
                            if isinstance(value, list):
                                value = value[0]
                    except:
                        value = None
                        break
    
                self.vars[var] = value

        return self

    def set_uri(self, uri):

        vars = self.vars
        (vars['scheme'], vars['host'], vars['path'], x,
         vars['query'], vars['fragment']) = urlparse(uri)

        return self
