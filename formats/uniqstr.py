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

UniqueStr is a base class for implementing enums
as strings.

see MediaType and TextEncoding subclasses for example
subclasses.

"""

class UniqueStr(str):
    """
    UniqueStr looks like an ascii str, but it has been normalized.

    It's a string that behaves like an enum.

    Subclass this for values like media-types, charsets, 
    language names, locale, etc.
    """

    # dictionary mapping names to known values.
    # multiple names may match to the same unique str if it has aliases.
    # this looks like a mapping from str to str but it's really a mapping
    # from str to UniqueStr.
    _known = dict()

    # if set, attempts to create new values will fail
    _exclusive = False

    
    def __new__(cls, s):
        # make sure cls has its own _known and _exclusive -
        #  i'm sure there is a better way to do this...
        if '_known' not in cls.__dict__:
            cls._known = {}
            cls._exclusive = False

        s = cls.normalize(s)
        mt = cls._known.get(s)
        if not mt:
            if cls._exclusive:
                raise ValueError, "Unknown unique string"

            mt = str.__new__(cls, s)
            cls._known[s] = mt
        return mt


    @classmethod
    def normalize(cls, s):
        """
        normalize a string before intern-ing it.

        this is useful when there are multiple values of a string
        that are acceptable but you want to convert them to a
        preferred format, e.g. using a particular capitalization
        style for case-insensitive identifiers.

        this is also an opportunity to reject (with ValueError)
        invalid values.
        """
        if not isinstance(s, str):
            s = str(s)
            #raise ValueError('%s must be a string' % cls.__name__)
            
        return s.strip()


    def addalias(self, alias):
        """
        add an alias for this unique string.

        you can do more powerful things by overriding .normalize().
        """
        if alias in self._known:
            if self is not self._known[alias]:
                raise ValueError, 'attempt to change UniqueStr alias'
            # XXX should log a warning here, but it's safe to continue
            return
        self._known[alias] = self
