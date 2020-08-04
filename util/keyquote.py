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

import string
from pymql.mql import error

def quotekey(ustr):
    """
    quote a unicode string to turn it into a valid namespace key

    """
    valid_always = string.ascii_letters + string.digits + '_'
    valid_interior_only = valid_always + '-'

    if isinstance(ustr, str):
        s = unicode(ustr,'utf-8')
    elif isinstance(ustr, unicode):
        s = ustr
    else:
        raise ValueError, 'quotekey() expects utf-8 string or unicode'

    if len(s) == 0:
        return str(s)

    output = []
    if s[0] in valid_always:
        output.append(s[0])
    else:
        output.append('$%04X' % ord(s[0]))

    for c in s[1:-1]:
        if c in valid_interior_only:
            output.append(c)
        else:
            output.append('$%04X' % ord(c))

    if len(s) > 1:
        if s[-1] in valid_always:
            output.append(s[-1])
        else:
            output.append('$%04X' % ord(s[-1]))

    return str(''.join(output))


def unquotekey(key, encoding=None):
    """
    unquote a namespace key and turn it into a unicode string
    """

    valid_always = string.ascii_letters + string.digits + "_"

    output = []
    i = 0
    while i < len(key):
        if key[i] in valid_always:
            output.append(key[i])
            i += 1
        elif key[i] in '_-' and i != 0 and i != len(key):
            output.append(key[i])
            i += 1
        elif key[i] == '$' and i+4 < len(key):
            # may raise ValueError if there are invalid characters
            output.append(unichr(int(key[i+1:i+5],16)))
            i += 5
        else:
            msg = "key %s has invalid character %s at position %d" % (
                key,
                key[i],
                i
            )
            raise error.MQLInternalError(None, msg)

    ustr = u''.join(output)

    if encoding is None:
        return ustr

    return ustr.encode(encoding)


def unquote_id(id):
    """
    Turn an id into a user-readable string, for instance turning
    /media_type/application/rss$002Bxml into
    /media_type/application/rss+xml
    """

    if '/' not in id:
        return unquotekey(id)

    return '/'.join(unquotekey(k) for k in id.split('/'))

def id_to_urlid(id):
    """
    convert a mql id to an id suitable for embedding in a url path.
    """

    # XXX shouldn't be in metaweb.api!
    from mw.formats.http import urlencode_pathseg

    segs = id.split('/')

    assert isinstance(id, str) and id != '', 'bad id "%s"' % id

    if id[0] == '~':
        assert len(segs) == 1
        # assume valid, should check
        return id

    if id[0] == '#':
        assert len(segs) == 1
        # assume valid, should check
        return '%23' + id[1:]

    if id[0] != '/':
        raise ValueError, 'unknown id format %s' % id

    # ok, we have a slash-path
    # requote components as keys and rejoin.
    # urlids do not have leading slashes!!!
    return '/'.join(urlencode_pathseg(unquotekey(seg)) for seg in segs[1:])

