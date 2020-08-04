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


import re, socket, rfc822, cgi

from errno import EPIPE
from urllib import urlencode
from urlparse import urlsplit, urlunsplit
from HTMLParser import HTMLParser

from mw import json
from oauth import oauth

from apikeys import get_context

import hmac, hashlib

REQUEST_HEADERS = {
    "user-agent": "eMQL",
    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
}

HMAC_SHA1 = oauth.OAuthSignatureMethod_HMAC_SHA1()

class Adapter(object):

    def __init__(self, tid, graph, mql, me, cache,
                 uri, api_key_names, pre, fetch, reduce, help):

        self.uri = uri
        self.api_key_names = api_key_names

        if pre is None:
            pre = True
        if fetch is None:
            fetch = True
        if reduce is None:
            reduce = False
        if help is None:
            help = False

        self.phases = { 'pre': pre, 'fetch': fetch, 'reduce': reduce,
                        'help': help }

    def __getattr__(self, name):

        if name in ('pre', 'fetch', 'reduce', 'help'):
            raise NotImplementedError, name

        raise AttributeError, name

    def _get_state(self):

        return (self.uri, self.api_key_names,
                self.phases['pre'], self.phases['fetch'],
                self.phases['reduce'], self.phases['help'])


class AdapterError(RuntimeError):
    pass

class AdapterException(AdapterError):
    def __init__(self, uri, traceback):
        super(AdapterException, self).__init__(uri, traceback)
        self.uri = uri
        self.traceback = traceback
    def __str__(self):
        return "%s: %s" %(self.uri, self.traceback)

class AdapterUserError(AdapterError):
    def __init__(self, phase, property, uri, error):
        super(AdapterUserError, self).__init__(phase, property, uri, error)
        self.error = error
    def __str__(self):
        return "During %s, the adapter for %s at %s returned an error: %s" %(self.args)

class ResultTypeError(AdapterError):
    def __init__(self, phase, property, uri, result_type, desired_type):
        super(ResultTypeError, self).__init__(phase, property, uri,
                                              result_type, desired_type)
    def __str__(self):
        return "During %s, the adapter for %s at %s returned a %s but the expected type is %s" %(self.args)

class AdapterTypeError(AdapterError, TypeError):
    pass


class HTTPAdapter(Adapter):

    def __init__(self, tid, graph, mql, me, cache,
                 url, api_key_names, foreign_apis,
                 proxy, pre, fetch, reduce, help,
                 application_guid):
        super(HTTPAdapter, self).__init__(tid, graph, mql, me, cache,
                                          url, api_key_names,
                                          pre, fetch, reduce, help)

        (self.scheme, self.host, self.path,
         self.query, self.fragment) = urlsplit(url)

        self.domain = self._get_domain(self.host)
        self.proxy = proxy
        self.application_guid = application_guid
        self.foreign_apis = foreign_apis

    def _get_state(self):

        return (self.uri, self.api_key_names,
                self.foreign_apis,
                self.proxy,
                self.phases['pre'], self.phases['fetch'],
                self.phases['reduce'], self.phases['help'],
                self.application_guid)

    def _get_domain(self, host):

        return host.split('.', 1)[1] if host.count('.') > 1 else host

    def _request(self, connection, method, url, body, headers, cookies):

        retried = False

        while True:
            try:
                connection.putrequest(method, url)
                connection.putheader("content-length", len(body))
                for header, value in headers.iteritems():
                    connection.putheader(header, value)
                for cookie in cookies.itervalues():
                    connection.putheader("set-cookie", cookie)
                connection.endheaders()
                connection.send(body)

                return
            except socket.error, e:
                if not retried and e[0] == EPIPE and connection.auto_open:
                    retried = True
                    continue  # try one more time as is done in HTTPConnection
                raise
 
    def get_oauth_service(self, me):
        """
        Figure out the oauth service to use to sign the request to the
        adapter. If the adapter is associated with an application,
        we'll just sign it with that consumer key
        """
        if not self.application_guid:
            return None

        consumer_token = me.get_app_api_key(self.application_guid)
  
        if not consumer_token:
            # this means the app is not oauth-enabled
            return None

        service = {
            "consumer_key": consumer_token.key,
            "consumer_secret": consumer_token.secret
            }

        return service


    def get_call_context(self, me, url):
        """
        A unique string that gets passed to the adapter to indicate
        the authentication context - a combination of the current user
        and application
        """
        me.authenticate()
        return get_context(me.get_session())

    def get_auth_headers(self, me, full_url, post_params):
        # reconstruct the base url
        url_data = urlsplit(full_url)

        # oauth needs the url without the parameters
        base_url = urlunsplit((url_data.scheme, url_data.netloc, url_data.path, None, None))
        if url_data.query:
            oauth_params = post_params.copy()
            # XXX this drops duplicates like foo=bar&foo=baz, but it
            # prevents foo=bar from becoming foo=['bar']
            oauth_params.update(dict(cgi.parse_qsl(url_data.query, keep_blank_values=True)))
        else:
            oauth_params = post_params

        oauth_service = self.get_oauth_service(me)
        if not oauth_service:
            return

        consumer = oauth.OAuthConsumer(oauth_service['consumer_key'],
                                       oauth_service['consumer_secret'])
        
        if oauth_service.get('access_token_key'):
            accessToken = oauth.OAuthToken(oauth_service['access_token_key'],
                                           oauth_service['access_token_secret'])
        else:
            # just sign with consumer key
            accessToken = None
            
        oauthRequest = \
                     oauth.OAuthRequest.from_consumer_and_token(consumer,
                                                                token=accessToken,
                                                                parameters=oauth_params,
                                                                http_method='POST',
                                                                http_url=base_url)
        oauthRequest.sign_request(HMAC_SHA1, consumer, accessToken)

        return oauthRequest.to_header()
        
        
    def _call(self, tid, me, control, **post_params):

        headers = REQUEST_HEADERS.copy()
        headers["x-metaweb-tid"] = tid

        deadline = control.get('deadline')
        if deadline:
            headers["x-metaweb-deadline"] = str(deadline)
        
        cache = control['cache']
        debug = control['debug']
        url = control.get('url')
        # Acre times out after 30s.
        # Other services' behaviour is not known so a minute is given.
        timeout = control['timeout']

        if cache is False:
            headers["cache-control"] = "no-cache"
        elif cache not in (True, None):
            headers["cache-control"] = cache

        if debug:
            headers["x-acre-enable-log"] = "true"
            post_params['debug'] = "true"

        if url is not None:
            scheme, host, path, query, fragment = urlsplit(url)
            domain = self._get_domain(host)
        else:
            host = self.host; path = self.path; query = self.query
            scheme = self.scheme
            domain = self.domain
            fragment = self.fragment

        url, connection = me.get_session().http_connect(host, path, timeout,
                                                        self.proxy)
        
        # pass the context on to the adapter
        context = self.get_call_context(me, url)
        if context:
            post_params['context'] = context

        
        body = urlencode(post_params, doseq=True)
        cookies = me.get_session().bake_cookies(tid, domain)

        response = result = None

        full_url = urlunsplit((scheme, host, path, query, fragment))
        auth_headers = self.get_auth_headers(me, full_url, post_params)
        if auth_headers:
            new_headers = headers.copy()
            new_headers.update(auth_headers)
            headers = new_headers
        
        try:
            self._request(connection, 'POST', full_url, body, headers, cookies)
            response = connection.getresponse()
            result = response.read()
        except Exception, e:
            raise HTTPAdapterError(post_params['call'], 'http://%s%s' %(host, path),
                                   response, "request failed: %s" %(str(e)),
                                   body)

        messages = None
        if debug:
            headers = dict(response.getheaders())
            if headers.get('server', '').startswith('Acre-'):
                result = json.loads(result)
                messages = result['logs']
                result = result['body']

        if response.status == 200:
            if post_params['call'] == 'help':
                return response.getheader("Content-Type"), result

            try:
                result = json.loads(result)
            except Exception, e:
                result = "While parsing '%s': %s" %(result, str(e))
            else:
                if isinstance(result, dict):
                    if messages:
                        result[':log'] = messages
                    if debug:
                        result[':headers'] = \
                            [header for header in response.getheaders()
                             if not header[0].startswith('x-wf-')]
                return result

        raise HTTPAdapterError(post_params['call'], 'http://%s%s' %(host, path),
                               response, result, body, messages)

    def pre(self, tid, graph, mql, me, control, parent, params, api_keys):

        return self._call(tid, me, control,
                          call='pre', parent=json.dumps(parent),
                          params=json.dumps(params),
                          api_keys=json.dumps(api_keys))

    def fetch(self, tid, graph, mql, me, control, args, params, api_keys):

        return self._call(tid, me, control,
                          call='fetch', args=json.dumps(args),
                          params=json.dumps(params),
                          api_keys=json.dumps(api_keys))

    def reduce(self, tid, graph, mql, me, control, args, params, api_keys):

        return self._call(tid, me, control,
                          call='reduce', args=json.dumps(args),
                          params=json.dumps(params),
                          api_keys=json.dumps(api_keys))

    def help(self, tid, graph, mql, me, control, params):

        return self._call(tid, me, control, call='help',
                          params=json.dumps(params))


class HTTPAdapterError(AdapterError):

    def __init__(self, phase, url, response, error, body, messages=None):

        self.phase = phase
        self.url = url
        self.response = response
        self.error = error
        self.body = body
        self.messages = messages

    def __str__(self):

        try:
            status = self.response.status
            content_type = self.response.getheader('content-type')
            if (content_type is not None and
                content_type.startswith('text/html')):
                data = []
                class parser(HTMLParser):
                    def handle_data(self, _data):
                        _data = _data.strip()
                        if _data:
                            data.append(_data)
                parser().feed(self.error)
                error = '\\n'.join(data)
            else:
                error = self.error
        except:
            status = 'an'
            error = self.error

        msg = ["'%s' phase on '%s' resulted in %s error:" %(self.phase, self.url, status), error, "Body:", self.body]
        if self.messages is not None:
            msg.append("Log:")
            msg.append(json.dumps(self.messages))

        return '\\n'.join(msg).replace('\n', '\\n').replace('\t', '    ')
