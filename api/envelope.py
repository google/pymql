#!/usr/bin/env python
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


from mw.mql.utils import valid_key, valid_idname, encode_cursor, decode_cursor, valid_timestamp
from mw.mql.error import MQLError
from mw.log import LOG
import cgi

def asbool(obj):
    if isinstance(obj, (str, unicode)):
        obj = obj.strip().lower()
        if obj in ['true', 'yes', 'on', 'y', 't', '1']:
            return True
        elif obj in ['false', 'no', 'off', 'n', 'f', '0']:
            return False
        else:
            raise ValueError(
                "String is not true/false: %r" % obj)
    return bool(obj)

class MQLEnvelope(object):
    '''
    This class abstracts mql reads and mql writes through JSON envelopes

    It does not throw exceptions; instead all errors are handled by
    returning envelope errors or other JSON error structures.
    
    '''
    
    def __init__(self,mss):
        self.mss = mss

    def envelope_error(self,message,**kwds):
        # this should match the MQLError definition inside mw/mql/error.py
        error_detail = { 'code': '/api/status/error/envelope/parse',
                         'info': kwds,
                         'message': message
                         }
        LOG.warning("envelope.error", repr(error_detail))

        self.mss.add_hint('mql-error')
        return { 'code': '/api/status/error',
                 'messages': [ error_detail ]
                 }

    def record_error(self, e, response):
        response['code'] = '/api/status/error'
        self.mss.add_hint('mql-error')
        e.add_error_inside()

        # Escape the error dictionary - this resolves an XSS vulnerability
        def escape_error(error_dict):
            for key, value in error_dict.iteritems():
                if isinstance(value, dict):
                    error_dict[key] = escape_error(value)
                elif isinstance(value, str):
                    error_dict[key] = cgi.escape(value)
            return error_dict

        error_dict = escape_error(e.get_error())
        response['messages'] = [ error_dict ]

    def get_permission_of(self,id):
        # be careful -- this code is security critical...
        if not isinstance(id, basestring) or not valid_idname(id):
            return (None,self.envelope_error("Invalid use_permission_of -- must be a valid id",id=id))            
        
        # now try and validate it...
        try:
            response = self.mss.mqlread({ "id": id, "permission": None })
            if response is None:
                return (None,self.envelope_error("Invalid use_permission_of -- id not found",id=id))

            # success!
            return (response['permission'],None)
            
        except MQLError, e:
            # we'll log the MQLError as it is created as well.
            return (None,self.envelope_error("Cannot determine permission",id=id))

    def validate_uniqueness_failure(self,value):
        if value != 'hard' and value != 'soft':
            return (None,self.envelope_error("Invalid uniqueness_failure value -- must be 'hard' or 'soft'",value=value))
        else:
            return (value,None)

    def validate_page(self,page):
        if not (isinstance(page, (int, long)) and page >= 0):
            return (None,self.envelope_error("Invalid page -- must be a non-negative integer",page=page,cl=page.__class__))
        else:
            return (int(page),None)

    def validate_cursor(self,cursor):
        # XXX when we want to encrypt the cursor, we should do it here...
        if not isinstance(cursor, (basestring, bool)):
            return (None,self.envelope_error("Invalid cursor -- must be a boolean or a string",cursor=cursor))
        else:
            return (decode_cursor(cursor),None)

    def validate_macro(self,macro):
        # minimal validation ; MQL is responsible for the validity of the internals.
        if not isinstance(macro,dict):
            return (None,self.envelope_error("Invalid macro -- must be a dictionary of substitutions",macro=macro))
        else:
            return (macro,None)

    def validate_lang(self,lang):
        if not isinstance(lang, basestring) or not valid_idname(lang):
            return (None,self.envelope_error("Invalid lang -- must be a valid id",lang=lang))
        else:
            return (lang,None)
                
    def validate_escape(self,escape):
        if escape != "html" and escape is not False:
            return (None,self.envelope_error("Invalid escape -- must be false or \"html\". \"html\" is the default.",escape=escape))
        else:
            return (escape,None)

    def validate_attribution(self,attribution):
        if not isinstance(attribution, basestring) or not valid_idname(attribution):
            return (None,self.envelope_error("Invalid attribution -- must be a valid id",attribution=attribution))
        else:
            return (attribution,None)

    def validate_as_of_time(self,as_of_time):
        if not isinstance(as_of_time, basestring) or not valid_timestamp(as_of_time):
            return (None,self.envelope_error("Invalid as_of_time -- must be a valid timestamp",as_of_time=as_of_time))
        else:
            return (as_of_time,None)

    def common_validation(self,query,varenv):
        # we can get basically any JSON object in as query from the user. Let's be very
        # defensive
        if not isinstance(query,dict):
            return self.envelope_error("Query envelope must be a dictionary")
        
        if 'query' not in query:
            return self.envelope_error("Missing 'query' parameter",key='query')
            
        if 'escape' in query:
            (escape,error) = self.validate_escape(query['escape'])
            
            if error:
                return error
            else:
                varenv['escape'] = escape

        if 'lang' in query:
            (lang,error) = self.validate_lang(query['lang'])

            if error:
                return error
            else:
                varenv['$lang'] = lang

        return None

    def read(self,query):
        response = {}

        varenv = {}
        
        error = self.common_validation(query,varenv)

        if error:
            return error        

        if 'uniqueness_failure' in query:
            (value,error) = self.validate_uniqueness_failure(query['uniqueness_failure'])
            if error:
                return error
            else:
                varenv['uniqueness_failure'] = value
        else:
            varenv['uniqueness_failure'] = self.mss.ctx.uniqueness_failure

        if 'page' in query:
            (page,error) = self.validate_page(query['page'])
            if error:
                return error
            else:
                varenv['page'] = page

        if 'cursor' in query:
            (cursor,error) = self.validate_cursor(query['cursor'])
            
            if error:
                return error
            else:
                varenv['cursor'] = cursor

        if 'macro' in query:
            (macro,error) = self.validate_macro(query['macro'])
            
            if error:
                return error
            else:
                varenv['macro'] = macro

        if 'as_of_time' in query:
            (as_of_time,error) = self.validate_as_of_time(query['as_of_time'])

            if error:
                return error
            else:
                varenv['asof'] = as_of_time

        if 'normalize_only' in query:
            varenv['normalize_only'] = query['normalize_only']

        self.mss.push_varenv(**varenv)
        try:
            if query.get('extended', False):

                if not asbool(self.mss.config.get('mql.extended')):
                    from mw.emql.emql import EMQLError
                    raise EMQLError("extended MQL service not enabled",
                                    query['query'])

                control = {'cache': query.get('cache', True),
                           'debug': query.get('debug', False)}

                debug, cursor, result = \
                    self.mss.emqlread(None, query['query'], control,
                                      query.get('api_keys'))

                response['result'] = result
                response['code'] = '/api/status/ok'
            
                if cursor is not None:
                    response['cursor'] = encode_cursor(cursor)
                if debug:
                    response['debug'] = debug

            else:
                response['result'] = self.mss.mqlread(query['query'])
                response['code'] = '/api/status/ok'

                if 'cursor' in self.mss.varenv:
                    response['cursor'] = encode_cursor(self.mss.varenv['cursor'])
                if 'page' in self.mss.varenv and self.mss.varenv['page'] > 0:
                    response['page'] = self.mss.varenv['page']

        except MQLError, e:
            self.record_error(e, response)
        finally:
            self.mss.pop_varenv()

        return response

    def write(self,query):
        response = {}

        varenv = {}
        
        error = self.common_validation(query,varenv)

        if error:
            return error        

        if 'cursor' in query:
            return self.envelope_error("Invalid 'cursor' parameter in an MQL write",key='cursor')

        # ugly code to avoid exceptions
        if 'use_permission_of' in query:
            (permission,error) = self.get_permission_of(query['use_permission_of'])
            if error:
                return error
            else:
                varenv['$permission'] = permission
                
        if 'attribution' in query:
            (attribution,error) = self.validate_attribution(query['attribution'])
            if error:
                return error
            else:
                varenv['$attribution'] = attribution

        varenv['uniqueness_failure'] = self.mss.ctx.uniqueness_failure
        
        self.mss.push_varenv(**varenv)
        try:
            try:
                response['result'] = self.mss.mqlwrite(query['query'])
                response['code'] = '/api/status/ok'                    
            except MQLError, e:
                self.record_error(e, response)

        finally:
            self.mss.pop_varenv()

        return response

    def check(self,query):
        response = {}

        varenv = {}
        
        error = self.common_validation(query,varenv)

        if error:
            return error        

        if 'cursor' in query:
            return self.envelope_error("Invalid 'cursor' parameter in an MQL check",key='cursor')

        # ugly code to avoid exceptions
        if 'use_permission_of' in query:
            (permission,error) = self.get_permission_of(query['use_permission_of'])
            if error:
                return error
            else:
                varenv['$permission'] = permission

        if 'attribution' in query:
            (attribution,error) = self.validate_attribution(query['attribution'])
            if error:
                return error
            else:
                varenv['$attribution'] = attribution
                
        varenv['uniqueness_failure'] = self.mss.ctx.uniqueness_failure
        
        self.mss.push_varenv(**varenv)

        try:
            try:
                response['result'] = self.mss.mqlcheck(query['query'])
                response['code'] = '/api/status/ok'                    
            except MQLError, e:
                self.record_error(e, response)
        finally:
            self.mss.pop_varenv()

        return response

    def reads(self,queries):
        response = {}
        if not isinstance(queries,dict):
            # best guess at a completely invalid envelope
            return { "envelope:error": self.envelope_error('Invalid MQL envelope -- must be a dictionary') }
        
        for k,v in queries.iteritems():
            if not valid_key(k):                
                response[k] = self.envelope_error('Invalid envelope key -- must be a valid MQL key',key=k)
            else:
                response[k] = self.read(v)
            
        return response

    def writes(self,queries):
        response = {}
        if not isinstance(queries,dict):
            # best guess at a completely invalid envelope
            return { "envelope:error": self.envelope_error('Invalid MQL envelope -- must be a dictionary') }

        if not len(queries) == 1:
            # best guess at a completely invalid envelope
            return { "envelope:error": self.envelope_error('Invalid MQL envelope -- must have only one write') }
        
        for k,v in queries.iteritems():
            if not valid_key(k):                
                response[k] = self.envelope_error('Invalid envelope key -- must be a valid MQL key',key=k)
            else:
                response[k] = self.write(v)
            
        return response

            
    def checks(self,queries):
        response = {}
        if not isinstance(queries,dict):
            # best guess at a completely invalid envelope
            return { "envelope:error": self.envelope_error('Invalid MQL envelope -- must be a dictionary') }

        if not len(queries) == 1:
            # best guess at a completely invalid envelope
            return { "envelope:error": self.envelope_error('Invalid MQL envelope -- must have only one check') }
        
        for k,v in queries.iteritems():
            if not valid_key(k):                
                response[k] = self.envelope_error('Invalid envelope key -- must be a valid MQL key',key=k)
            else:
                response[k] = self.check(v)
            
        return response
