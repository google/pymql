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

# Table mapping response codes to messages; entries have the
# form {code: (shortmessage, longmessage)}.
# See http://www.w3.org/hypertext/WWW/Protocols/HTTP/HTRESP.html
from BaseHTTPServer import BaseHTTPRequestHandler
import traceback
from pymql.log import LOG


# HTTP error code messages
# XXX: We really really need to move to py2.5
def is_valid_HTTP_code(code):
  return code in BaseHTTPRequestHandler.responses.keys()


def get_HTTP_err(code):
  return '%d %s' % (code, BaseHTTPRequestHandler.responses[code][0])


class ParameterizedError(Exception):
  """
    This is a special Exception class that is used to format messages
    where the contents of the message itself are important. Use it
    exactly how you would use the python % format operator:

    class MyException(ParameterizedError):
        pass

    raise MyException('Got an error in query %(query)s', query=q)

    This will format the string appropriately, but allow exception
    handlers to unpack the relevant data and optionall reinsert it
    into the result string
    """
  DEF_PFX = '/api/status/error'
  DEF_ME_CODE = '/unknown/unknown'

  def __init__(self,
               msg,
               http_code=400,
               app_code=DEF_ME_CODE,
               inner_exc=None,
               **kwds):
    self.msg = msg
    Exception.__init__(self, msg)

    if not is_valid_HTTP_code(http_code):
      http_code = 500
    self.http_status = get_HTTP_err(http_code)
    self.http_code = http_code

    # app_code and and api code setup
    codes = app_code.split('/')
    if len(codes) < 3:
      codes = self.DEF_ME_CODE.split('/')
    self.comp_code = '%s/%s' % (self.DEF_PFX, codes[1])
    self.app_code = '%s' % '/'.join(codes[2:])
    self.messages = [self.gen_msgs(**kwds)]

    if not kwds.has_key('error'):
      # don't extract the current frame (__init__)
      stack = traceback.extract_stack()[:-1]
      kwds['traceback'] = '\r\n'.join(traceback.format_list(stack))

    # log inner exception or self
    exc = self
    if inner_exc:
      exc = inner_exc
    comp = app_code[1:].replace('/', '.')
    if exc == self:
      LOG.debug(comp, msg, **kwds)
    else:
      LOG.exception(msg, **kwds)
    self.kwds = kwds

  def gen_msgs(self, **kwds):
    return {
        'code': '%s/%s' % (self.DEF_PFX, self.app_code),
        'message': self.msg,
        'info': kwds.copy()
    }

  def get_err_dict(self):
    return {
        'status': self.http_status,
        'code': self.comp_code,
        'messages': self.messages
    }

  def __str__(self):
    return str(self.get_err_dict())


class NetworkAddressError(ParameterizedError):
  pass


class ContentLoadError(ParameterizedError):
  pass


class TypeVerifyError(ParameterizedError):
  pass


class EmailError(ParameterizedError):
  pass


class SubscriptionError(ParameterizedError):
  pass


class MSSError(ParameterizedError):
  pass


class UserLookupError(ParameterizedError):
  pass


class UserAuthError(ParameterizedError):
  pass


class BlobError(ParameterizedError):
  pass


class BLOBClientError(ParameterizedError):
  pass


class RelevanceError(ParameterizedError):
  pass


class TextSearchError(ParameterizedError):
  pass


class AutocompleteError(ParameterizedError):
  pass


class EmptyResult(ParameterizedError):
  pass


class GraphConnectionError(ParameterizedError):
  pass


class FormattingError(ParameterizedError):
  pass


class SessionError(ParameterizedError):
  pass


class ConfigError(ParameterizedError):
  pass


class SanitizationError(ParameterizedError):
  pass


class BlurbError(ParameterizedError):
  pass


class DomainOperationError(ParameterizedError):
  pass


class GenericRuntimeError(ParameterizedError):
  pass


class OAuthDisabledError(ParameterizedError):
  pass


class RecaptchaError(ParameterizedError):

  def __init__(self,
               msg,
               http_code=500,
               app_code=ParameterizedError.DEF_ME_CODE,
               inner_exc=None,
               **kwds):
    self.message = msg
    ParameterizedError.__init__(
        self,
        msg,
        http_code=http_code,
        app_code=app_code,
        inner_exc=inner_exc,
        **kwds)


class ReadOnlyDatabaseError(ParameterizedError):

  def __init__(self, msg=None, *args, **kwds):
    msg = msg or 'You cannot save right now. Please try again later'
    ParameterizedError.__init__(self, msg, *args, **kwds)
