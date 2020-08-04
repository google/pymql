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
#  utilities for dealing with http
#
#  url escaping
#  content-type parsing and graph lookup
#
#  originally from mw/client/escaping.py
#  duplicated in mw/mql/grquoting.py
#


import urllib

# Table mapping response codes to messages; entries have the
# form {code: (shortmessage, longmessage)}.
# See http://www.w3.org/hypertext/WWW/Protocols/HTTP/HTRESP.html
from BaseHTTPServer import BaseHTTPRequestHandler
http_status_codes = BaseHTTPRequestHandler.responses


# some useful uri splitting code in the "urischemes" thirdparty module.
#
# later i found that the most complete uri manipulation module 
# seems to be in 4Suite:
#
# from Ft.Lib import Uri, Iri


#
#
# ALLOW:
#
# '~' is in the unreserved set, so they should be available like "_.-"
# ':' is in pchar
# '@' is in pchar (though naive text parsers may think it's an email address)
#
# "$" is a valid sub-delim
# "!" is a valid sub-delim
# "*" is a valid sub-delim
# "," is a valid sub-delim
# ";" is a valid sub-delim
#
# GENERALLY DISALLOW:
#
# "&" is in sub-delims but has special meaning to form parsers
# "=" is in sub-delims but excluded due to avoid any possible confusion
# "+" is in sub-delims but excluded due to avoid any possible confusion
#     with form-encoded queries

# ALWAYS DISALLOW
#
# "'" is in sub-delims but likely to confuse
# "(" is in sub-delims but definitely confuses email text parsers
# ")" is in sub-delims but definitely confuses email text parsers

# [A-Za-z0-9] and "_.-" are always safe in urllib.quote
# additionally, we allow:
our_safe = "~:@$!*,;"

# this handles unicode
def base_urlencode(data, safe):
    if isinstance(data, unicode):
        data = data.encode('utf_8')
    return urllib.quote(data, safe)


def urlencode(data):
    '''
    default url-encoder - please shift to one of the more
    specific versions, depending on whether you're quoting
    a path segment or a query arg.
    '''
    # "_.-" are always untouched
    return base_urlencode(data, ',')



# within path segments (between slashes) we don't need
#  to follow the same rules as for forms parsing.
#
# "=" is only special to form parsers
# "&" is only special to form parsers
# "+" is only special to form parsers
def urlencode_pathseg(data):
    '''
    urlencode for placement between slashes in an url.
    '''
    return base_urlencode(data, our_safe + "=&+")


# "/" is allowed in query but reserved in path segments
# "?" is allowed in query but reserved in path segments
def urlencode_querykey(data):
    '''
    encode for placement before '=' in a query argument

    this allows '/?'
    '''
    return base_urlencode(data, our_safe + '/?')


# "/" is allowed in query but reserved in path segments
# "?" is allowed in query but reserved in path segments
# "=" should be allowed by form parsers after the key=
def urlencode_queryvalue(data):
    '''
    encode for placement after '=' in a query argument

    this allows '/?='
    '''
    return base_urlencode(data, our_safe + '/?')


# "/" is allowed in query but reserved in path segments
# "?" is allowed in query but reserved in path segments
# "=" is only special to form parsers
# "&" is only special to form parsers
# "+" is only special to form parsers
def urlencode_fragment(data):
    '''
    encode for placement after '=' in a query argument

    this allows '/?='
    '''
    return base_urlencode(data, our_safe + '/?=&+')

#
# who knows what browsers do?  it ain't rfc3986 that's for sure.
#
def urlencode_formtext(data):
    '''
    encode a form key or value, pretending to be a browser.

    this version encodes space as '+' rather than as '%20',
    which is used when you are pretending to be a browser form
    submit.
    '''
    if isinstance(data, unicode):
        data = data.encode('utf_8')
    return urllib.quote_plus(data, our_safe)


def urldecode(data):
    '''
    replace "%xx" with character equivalent
    '''
    return urllib.unquote(data)
