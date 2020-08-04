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


"""
routines for working with content-type headers
and other sources of media_types and text_encodings.

"""

import cgi
from mw.formats.uniqstr import UniqueStr
from mw.util import keyquote

class MediaType(UniqueStr):
    """
    this looks like an ordinary python str containing a media-type.
    it has some extra methods on it that are useful for the metaweb.
    """

    _valid_part0 = ('application', 'audio', 'image', 'message', 'model', 'multipart',
                    'text', 'text_encoding', 'video')

    @property
    def id(self):
        """the id property holds the metaweb id: value  """
        return '/media_type/%s' % '/'.join(keyquote.quotekey(part)
                                           for part in self.split('/'))

    metaweb_type = '/common/media_type'
    
    type = property(lambda self: str(self).split('/')[0].strip())
    subtype = property(lambda self: str(self).split('/')[1].strip())

    @classmethod
    def normalize(cls, s):
        s = UniqueStr.normalize(s)

        if len(s) > 128:
            raise ValueError('invalid media type "%s"' % s)

        parts = s.lower().split('/')
        if len(parts) != 2:
            raise ValueError('invalid media type "%s"' % s)

        if parts[0] not in cls._valid_part0:
            raise ValueError('invalid media type "%s"' % s)

        return s

    ###################################################

    @classmethod
    def from_id(cls, id):
        if id is None:
            return None
        assert id.startswith('/media_type/')
        idpath = id[len("/media_type/"):]

        return keyquote.unquote_id(idpath)

class TextEncoding(UniqueStr):
    """
    canonicalized text encoding string.

    # see http://WWW.IANA.ORG/assignments/character-sets
    """

    metaweb_type = '/common/text_encoding'

    @property
    def id(self):
        """the id property holds the metaweb id: value """
        return '/media_type/text_encoding/%s' % keyquote.quotekey(self.lower())

    @property
    def codec(self):
        """the codec property holds the python codec"""
        return self._codec

    @codec.setter
    def codec(self, value):
        self._codec = value

    @classmethod
    def normalize(cls, s):
        s = UniqueStr.normalize(s)

        # XXX check for valid token

        if len(s) > 20:
            raise ValueError, 'invalid charset "%s"' % s

        # STANDARDS PEOPLE DIG ALL CAPS.
        return s.upper()

    @classmethod
    def from_id(cls, id):
        if id is None:
            return None

        # better be ASCII, but make sure it's not unicode
        id = str(id)
        # XXX this is a bad namespace location!
        assert id.startswith('/media_type/text_encoding/')
        idpath = id[len('/media_type/text_encoding/'):]
        return cls(keyquote.unquotekey(idpath))


#
#  for now we list (and preload) some text encoding names.
#

# some well-known text-encodings
#  official names from http://www.iana.org/assignments/character-sets
#  python codec names are at .../lib/standard-encodings.html
ascii = TextEncoding('us-ascii')
ascii.addalias('ascii')
ascii.codec = 'ascii'

utf8 = TextEncoding('utf-8')
utf8.codec = 'utf_8'

utf16 = TextEncoding('utf-16')
utf16.codec = 'utf_16'

# XXX fill in the rest of the character sets we care about and
#  then turn on _exclusive
#TextEncoding._exclusive = True


def ContentType(value):
    mt, params = cgi.parse_header(value)
    mt = MediaType(mt)

    charset = params.get('charset')
    if charset is not None:
        # XXX whatever this is for, it's ugly...
        charset = charset.replace("'", '')
        te = TextEncoding(charset)
    else:
        te = None

    return (mt, te)

class LanguageCode(UniqueStr):
    """
    normalized language code string.

    mumble rfc-3066 inspired but more about common
    practice and the content we have.

    normalization may do surprising things.
    "en-US" gets normalized to "en".
    """

    metaweb_type = '/type/lang'

    @property
    def id(self):
        """the id property holds the metaweb id: value """
        return '/lang/%s' % keyquote.quotekey(self)

    @classmethod
    def normalize(cls, s):
        s = UniqueStr.normalize(s)

        if len(s) > 20:
            raise ValueError, 'invalid language code "%s"' % s

        # XXX for now we accept but do not require a leading '/lang/'
        #  choose one, i think.
        if s.startswith('/lang/'):
            s = s[len('/lang/'):]

        # cut off anything following '-' (e.g. "en-US" -> "en")
        # XXX this should be specified and documented
        return s.split('-', 1)[0]

    @classmethod
    def from_id(cls, id):
        if id is None:
            return None

        # better be ASCII, but make sure it's not unicode
        id = str(id)
        assert id.startswith('/lang/')
        return cls(keyquote.unquotekey(id[len('/lang/'):]))
