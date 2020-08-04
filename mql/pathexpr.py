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
#  code for dealing with metaweb path expressions
#
#  there will be a lot of variants of this language based
#    on embedding, it would be nice to have them all abstracted
#    out at some point but for now we're still figuring out
#    what the differences are.
#

import sys, os, re

if __name__ == '__main__':
  sys.path.append(os.path.abspath('../..'))

from pymql.log import LOG
from error import MQLInternalError

from pymql import json
from pymql.error import EmptyResult, ParameterizedError


class JSONResponse(object):

  def __init__(self, **kws):
    self.response = {
        'status': '500 Internal Server Error',
        'code': '/api/status/error/server',
        'messages': []
    }
    self.extend(**kws)

  def extend(self, **kws):
    for k, v in kws.iteritems():
      if k == 'messages':
        self.response[k] += v
      else:
        self.response[k] = v

  def log(self, text, **kws):
    kws['message'] = text
    self.response['messages'].append(kws)


#
#
#  snipped from mod_python 3.1.3 apache.py
#
#   - modified to format result as a json-like structure.
#
import traceback


def json_traceback(response=None, exception=None, **kws):
  """
    This function is only used when debugging is on.
    It sends the output similar to what you'd see
    when using Python interactively to the browser
    """

  debug = 1
  etype, evalue, etb = sys.exc_info()

  try:  # try/finally
    try:  # try/except

      if debug and etype is IOError and str(evalue)[:5] == 'Write':
        # if this is an IOError while writing to client,
        # it is probably better not to try to write to the cleint
        # even if debug is on.
        LOG.error('json_traceback', 'skipping error write to client')
        debug = 0

      # write to log
      for e in traceback.format_exception(etype, evalue, etb):
        s = '%s' % e[:-1]
        LOG.error('json_traceback', s)

      if response is None:
        response = JSONResponse(
            status='500 Internal Server Error', code='/api/status/error/server')
      response.extend(**kws)

      stack = [
          dict(zip('file,line,func,source'.split(','), quad))
          for quad in traceback.extract_tb(etb, None)
      ]

      text = '%s: %s' % (etype, evalue)
      response.log(text, stack=stack, level='error')

      return response.response

    except Exception, e:
      # hit the backstop.  must be a bug in the normal exception handling code,
      #  do something simple.
      response = {
          'status': '500 Internal Server Error',
          'messages': [{
              'level': 'error',
              'text': traceback.format_exc()
          }],
      }
      return response

  finally:
    # erase the traceback
    etb = None


def wrap_query(querier, sq, varenv=None, transaction_id=None):
  """
    Run a query with the given querier (usually something like
    ctx.low_querier.read) - performing appropriate envelope packing and
    unpacking, multiple queries, error handling, etc
    """

  LOG.error(
      'deprecated',
      'mw.mql.pathexpr.wrap_query() is DEPRECATED and will go away soon!')

  if isinstance(sq, basestring):
    # convert to json query
    try:
      # XXX should eventually use unicode, for now utf8
      sq = json.loads(sq, encoding='utf-8', result_encoding='utf-8')

    except ValueError, e:
      # debug ME-907
      LOG.exception('mql.pathexpr.wrap_query()', sq=sq, varenv=varenv)

      SIMPLEJSON_ERR_RE = re.compile('^(.+): line (\d+) column (\d+)')
      m = SIMPLEJSON_ERR_RE.match(str(e))
      if not m:
        raise
      response = JSONResponse(
          status='400 Bad Request', code='/api/status/error/request')
      text = 'json parse error: ' + m.group(1)
      response.log(
          text, line=int(m.group(2)), column=int(m.group(3)), level='error')
      return response.response

    except Exception, e:
      return json_traceback(
          exception=e,
          status='400 Bad Request',
          code='/api/status/error/request')

  if not isinstance(sq, dict):
    response = JSONResponse(
        status='400 Bad Request', code='/api/status/error/request')
    text = 'json type error: query was not a dictionary'
    response.log(text, level='error')
    return response.response

  if varenv is None:
    varenv = {}

  # backwards compatibility until we remove the transaction_id parameter
  if 'tid' not in varenv:
    varenv['tid'] = transaction_id

  if 'cursor' in sq:
    varenv['cursor'] = sq['cursor']

  try:
    # should be JSONResponse(query=sq['query']) 'queries' to match
    # envelope spec
    response = JSONResponse(query=sq)
    results = {}

    # filter out these special keys for now - eventually some of
    # these will be filled in by the caller but only if we trust
    # them!
    reserved_names = ('request_id', 'cost', 'lang', 'transaction_id',
                      'permission', 'cursor', 'user')

    valid_queries = (
        (k, v) for k, v in sq.iteritems() if k not in reserved_names)

    # make sure to copy the request_id
    if 'request_id' in sq:
      response['request_id'] = sq['request_id']

    # should only looking either at sq['query'] for a single query or
    # sq['queries'] for multiple queries
    for id, subq in valid_queries:
      # assuming querier is a bound method here..
      LOG.notice(
          'Query',
          '%s.%s' % (querier.im_class.__name__, querier.__name__),
          subq=subq)
      try:
        results[id] = querier(subq, varenv)

        response.extend(status='200 OK')

      except EmptyResult, e:
        LOG.info('emptyresult', '%s' % e)
        response.log('empty result for query %s' % subq)
        result = None

      # exceptions should be packed into response['error']
      except ParameterizedError, e:
        if isinstance(e, MQLInternalError):
          response.extend(status='500 Internal Server Error')
        else:
          response.extend(status='400 Bad Request')

        tb = json_traceback(response=response, exception=e)
        response.log('parse exception: %s' % e, level='error')
        result = None
      except Exception, e:
        LOG.exception('python.exception')
        tb = json_traceback(response=response, exception=e)
        return tb

    response.extend(result=results)
    if 'cursor' in varenv:
      response.extend(cursor=varenv['cursor'])

    return response.response

  except Exception, e:
    LOG.exception('python.exception')
    return json_traceback(response=response, exception=e)
