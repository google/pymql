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

import re
from xml.sax import saxutils
import urllib
import cgi

from pymql.error import FormattingError

######################################################################

# quoting rules
_internal_quoting_rules = [
    ('\"', '\\\"'),
    ('\\', '\\\\'),
    ('\n', '\\n'),
]

_internal_to_quote = dict(_internal_quoting_rules)
_internal_from_quote = dict([(a, b) for b, a in _internal_quoting_rules])
_internal_from_quote['\''] = ''
_internal_from_quote['\"'] = ''

# I love REs (aka read it and weep)
re_quoted_string_text = '^\"((?:[^\\\\\"]|\\\\[\\\\\"n])*)\"$'
re_quoted_string_part = '\\\\[\\\\\"n]'
# everything matches this, so we don't test (ie. all unquoted strings are legal)
re_unquoted_string_text = '^(?:[^\\\\\n\"]|([\\\\\n\"]))*$'
re_unquoted_string_part = '[\\\\\n\"]'

re_qs = re.compile(re_quoted_string_text)
re_qs_part = re.compile(re_quoted_string_part)
re_us_part = re.compile(re_unquoted_string_part)


def _internal_quote_sub(m):
  return _internal_to_quote[m.group()]


def _internal_unquote_sub(m):
  return _internal_from_quote[m.group()]


def _internal_leading_trailing(m):
  return


######################################################################


def quote(string):
  return '"' + re_us_part.sub(_internal_quote_sub, string) + '"'


def unquote(string):
  middlem = re_qs.match(string)
  if middlem is None:
    raise FormattingError('Badly formatted quoted string %s ' % string)
  return re_qs_part.sub(_internal_unquote_sub, middlem.group(1))


######################################################################

#
#  html escaping
#  url escaping
#
#  originally from mw/client/escaping.py
#


def escapeAttribute(data):
  """
    Prepares data to be used as an attribute value. The return value
    is a quoted version of data. The resulting string can be used
    directly as an attribute value:
    >>> print "<element attr=%s>" % quoteattr("ab ' cd \" ef")
    <element attr="ab ' cd &quot; ef">
    """
  return (saxutils.quoteattr(data))


def escapeUrl(data):
  """
    Replace special characters in string using the "%xx"
    escape. Letters, digits, and the characters "/_.-" are never
    escaped.
    """
  return (urllib.quote(data))


def escapeMarkup(data):
  """
    Convert the characters "&", "<" and ">" in data to HTML-safe
    sequences.
    """
  return (cgi.escape(data))


######################################################################

if __name__ == '__main__':
  print quote("\n\r\t\"\\foo\\\"")  # result is "\n\r\t\"\\foo\\\"" (duh)
  print unquote(
      "\"foo\\n\\\"\\\\\""
  )  # result is foo<newline>"\ -- note that python sees "foo\n\"\\"
  print unquote(
      "\"foo\\\"\\\"")  # should die with an "illegal quoted string" exception
