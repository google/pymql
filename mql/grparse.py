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
"""quick and dirty parsing of graphd query language strings into python lists.
"""
import re
from grquoting import quote, unquote

from error import MQLGraphError, MQLDatelineInvalidError, MQLTimeoutError,\
    MQLCursorInvalidError, GraphIsSnapshottingError

from pymql.log import LOG

gstr_escape = quote

# there are several places in pymi where gstr_unescape is called on a string that
# is not escaped. One example is the result of result=(datatype) - a bareword
# like boolean is returned, not a quoted string.
#
# Rather than try and fix these cases, I've made gstr_unescape preserve
# this behaviour. Please use mw.mql.grquoting.unquote() instead.


def gstr_unescape(string):
  if string[0] == '"':
    return unquote(string)
  else:
    return string


cost_parameters = [
    ('tr', 'time/real',
     'number of milliseconds graphd spent executing to answer this query in '
     'general. This number will get larger on a system that is busy with other'
     ' things, even if graphd isn\'t involved in them.'
    ),
    ('tu', 'time/user',
     'number of milliseconds graphd spent executing in user mode while '
     'computing the answer to this request.'
    ),
    ('ts', 'time/system',
     'number of milliseconds graphd spent executing in system mode while '
     'computing the answer to this requests. "Executing in system mode" almost'
     ' always means "reading a lot of data from disk".'
    ),
    ('pr', 'page reclaims',
     'a benevolent form of page fault that doesn\'t actually do any work '
     'because the page is still in the local cache.'
    ),
    ('pf', 'page faults',
     'the thing we\'re trying to minimize. Higher pf will usually be '
     'accompanied by a higher ts.'
    ),
    ('dw', 'primitive data writes',
     'Usually, these will be what you expect, except for queries that create '
     'implicit type links and type system fragments.'
    ),
    ('dr', 'primitive data reads',
     'how many single primitive structs were read from disk (for example, as '
     'part of dismissing them as candiates for a qualified search).'
    ),
    ('in', 'index size reads',
     'how many indices were looked up with their starting address and size.'),
    ('ir', 'index element reads', 'get one member of one index.'),
    ('iw', 'index element write', 'add an element to an index.'),
    ('va', 'value allocation',
     'allocate a (possibly temporary or transient) result data structure.'),
    ('te', 'time/overall',
     'number of milliseconds from receipt of this query by the graph, to the '
     'start of sending the response'
    ),
    ('tg', 'time/graph',
     'time me observes from sending the first byte of the request to receiving'
     ' the last byte'
    ),
    ('tf', 'time/formatted',
     'time me takes from sending the request to handing off the formatted '
     'response'
    ), ('tm', 'time/mql', 'time taken inside the MQL subroutines'),
    ('cr', 'cache/read', 'number of requests sent to memcache'),
    ('cm', 'cache/miss', 'number of memcache misses'),
    ('ch', 'cache/hit', 'number of memcache hits'),
    ('lr', 'lojson-cache/read', 'number of schema requests sent to memcache'),
    ('lm', 'lojson-cache/miss', 'number of schema memcache misses'),
    ('lh', 'lojson-cache/hit', 'number of schema memcache hits'),
    ('rt', 'relevance/time',
     'time taken inside the relevance server (as measured by ME)'),
    ('gcr', 'graph connect retries',
     'the number of times that ME tried to open a connection to a graph'),
    ('gqr', 'graph query retries',
     'the number of times that ME tried to service a query from a single graph')
]

costcode_dict = dict([(cc[0], (cc[1], cc[2])) for cc in cost_parameters])

costitem_re = re.compile(r'([a-zA-Z]+)=(\d+)\s*')


def coststr_to_dict(coststr):
  if not coststr:
    return None
  matches = costitem_re.findall(coststr)
  return dict([(k, int(v)) for k, v in matches])


graphresult_re = re.compile(
    r'(\(|\)| |\-\>|\<\-|[a-z]+\=|[\-\:\._A-Za-z0-9]+|\"(?:[^\"\\]|\\[\\\"n])*\")'
)


class GraphResult(list):
  pass


class ReplyParser:
  """
    parses a graphd reply char by char.
      paren lists are broken up into python lists
      all list elements are returned as strings
    """

  def __init__(self):
    self.inbuf = []
    self.replyqueue = []

    self.reset_parser()

  def reset_parser(self):
    # parser state

    self.instring = 0  # true if we have read an open " but no close
    self.escaped = 0  # true if we just read a backslash
    # if instring is 1, curstr is a list of characters that
    #  will be joined to make the string
    self.curstr = []
    self.curreply = []  # list of strings - join when ready to
    # use (faster than string concat)

  def parsestr(self, s):
    if '\n' in s:
      # parse all of the 'completed' lines, and if there is an
      # uncompleted line at the end of s, leave it in curreply

      reply_list = s.split('\n')

      self.curreply.append(reply_list.pop(0))

      for reply in reply_list:

        # parse the previous reply
        replystr = ''.join(self.curreply)
        self.parse_full_reply(replystr)
        self.reset_parser()

        # now add the current line
        self.curreply.append(reply)

      # note that we're not processing the last line, because it is incomplete

    else:
      self.curreply.append(s)

  def parse_full_reply(self, replystr):
    """
        parse the given reply string from the graph into a bunch of
        nested lists of tokens. Results are in the form:
        [ 'ok', 'id=', '"me;..."', [[['010000..', '01...', ...]]]]
        """
    LOG.debug('graph.result', replystr)
    token_list = graphresult_re.findall(replystr)

    curlist = []

    stack = []
    push_state = stack.append
    pop_state = stack.pop

    for count, tok in enumerate(token_list):
      if tok == '(':
        push_state(curlist)
        curlist = []
      elif tok == ')':
        sublist = curlist
        curlist = pop_state()
        curlist.append(sublist)
      elif tok == '\n':
        raise MQLGraphError(
            None,
            'Not allowed a newline in parse_full_reply',
            reply=replystr,
            tokens=token_list)
      elif tok == ' ' or tok == '':
        pass
      else:
        curlist.append(tok)

    LOG.debug('graph.result.parsed', 'Parsed %d tokens' % count)
    if len(stack) != 0:
      raise MQLGraphError(
          None,
          'got linefeed in the middle of a reply?',
          reply=replystr,
          tokens=token_list,
          depth=len(stack))

    self.replyqueue.append(curlist)

  def get_reply_raw(self):
    return self.replyqueue.pop(0)

  def get_reply(self):
    l = self.get_reply_raw()
    result = GraphResult()
    result.status = l.pop(0)
    result.cost = None
    result.dateline = None

    if result.status == 'ok':
      result += l.pop()
    elif result.status == 'error':
      result.errcode = l.pop(0)
      result.errmsg = unquote(l.pop())
    else:
      raise MQLGraphError(
          None, 'grparse: unknown graphd reply type', header=l[0], reply=l)

    # what's left is info messages from graphd
    li = 0
    while li < len(l):
      rv = l[li]
      if type(rv) == str and rv in ('cost=', 'dateline=', 'id='):
        modifier = rv[:-1]
        setattr(result, modifier, unquote(l[li + 1]))
        li += 2
      else:
        raise MQLGraphError(
            None,
            'unknown response modifier from graphd',
            header=l[li],
            reply=l)

    if result.status == 'error' and result.errcode == 'BADCURSOR':
      raise MQLCursorInvalidError(None, result.errmsg)
    if result.status == 'error' and result.errcode == 'DATELINE':
      raise MQLDatelineInvalidError(None, result.errmsg)
    if result.status == 'error' and result.errcode == 'AGAIN':
      raise GraphIsSnapshottingError(None, result.errmsg)
    if result.status == 'error' and result.errcode == 'COST':
      raise MQLTimeoutError(None, 'Query too difficult.', cost=result.cost)
    if result.status == 'error' and result.errcode != 'EMPTY':
      raise MQLGraphError(
          None,
          'error %(subclass)s: %(detail)s',
          detail=result.errmsg,
          subclass=result.errcode,
          dateline=result.dateline)
    return result

  def put_buf(self, buf):
    self.inbuf.append(buf)

  def isready(self):
    return len(self.replyqueue) > 0


# this is different from a normal list printer because it
#  assumes that any sublists will come at the end.
# of course that's wrong.  hmmph.
def print_result(l, indent=''):
  if l is None:
    print indent + 'None'
    return
  #print type(l)
  if isinstance(l, list):
    dangle = 0
    for li in l:
      if isinstance(li, list):
        if dangle:
          print
          dangle = 0
        print_result(li, indent + '    ')
      else:
        if not dangle:
          print indent,
          dangle = 1
        print str(li),
    if dangle:
      print
