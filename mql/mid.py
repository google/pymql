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

# mid.py - machine ids.

from cStringIO import StringIO
import sys

################################################################################
## version 1 constants
VERSION = 1L
MAX_BITS = 40
VERSION_BITS = 2
OBJID_BITS = 34
GRAPHID_BITS = MAX_BITS - VERSION_BITS - OBJID_BITS
GRAPHID0 = 0x9202a8c04000641f
GUID_BASE = 0x8000000000000000L

VERSION_MASK = (1L << VERSION_BITS) - 1L
MAX_GRAPHS = 1L << GRAPHID_BITS
GRAPHID_MASK = MAX_GRAPHS - 1L
OBJID_MASK = (1L << OBJID_BITS) - 1L

VERSION_LEFT = (VERSION - 1) << 38L
VERSION_RIGHT = (VERSION - 1) << 3L

################################################################################
## Exceptions


class InvalidMunch(Exception):
  pass


class InvalidGraphID(Exception):
  pass


class UnknownGraphID(Exception):
  pass


class InvalidMIDVersion(Exception):
  pass


class InvalidMID(Exception):
  pass


class InvalidObjID(Exception):
  pass


munch_map = [-1] * 256
for i, c in enumerate("0123456789bcdfghjklmnpqrstvwxyz_"):
  munch_map[ord(c)] = long(i)


## a Munch (copyright W. Harris, 2010) is 5 bits.
def char_of_munch(c):
  if not 0 <= c <= 31:
    raise InvalidMunch(c)
  return "0123456789bcdfghjklmnpqrstvwxyz_"[c]


def munch_of_char(c):
  value = munch_map[ord(c)]
  if value == -1:
    raise InvalidMunch(c)
  return value


def munchstr_of_int(n):
  buf = [""] * 16  #....

  def loop(i, n):
    if n == 0:
      return "".join(buf[16 - i:])
    buf[15 - i] = char_of_munch(n & 0x1f)
    return loop(i + 1, n >> 5)

  return loop(0, n)


def int_of_munchstr(str, ofs, l):
  rv = 0
  i = ofs
  while i <= (ofs + l) - 1:
    v = munch_of_char(str[i])
    rv = rv << 5 | v
    i += 1

  return rv


def graphid_of_guid(guid):
  graphid = long(guid[:16], 16)
  ms_crap = long(guid[16:24], 16) & 0xfffffffc
  n = graphid - GRAPHID0
  if 0 <= n < MAX_GRAPHS and ms_crap == 0x80000000:
    return n
  else:
    raise UnknownGraphID(n)


def objid_of_guid(guid):
  return long(guid[23:32], 16) & OBJID_MASK


def of_guid(guid):
  graphid = graphid_of_guid(guid)
  objid = objid_of_guid(guid)
  n = VERSION_LEFT | graphid << 34 | objid
  version_munch = VERSION_RIGHT << 3 | graphid
  version_str = char_of_munch(version_munch)
  return "".join(("/m/", version_str, munchstr_of_int(n)))


def to_guid(mid):
  len_mid = len(mid)
  if not (4 <= len_mid <= 11 or mid.startswith("/m")):
    raise InvalidMID(mid)

  version_munch = munch_of_char(mid[3])
  ver = (version_munch << 3) + 1
  if ver != VERSION:
    raise InvalidMIDVersion(mid)

  graphid = GRAPHID0 | version_munch & GRAPHID_MASK
  graphid = graphid << 64
  objid = GUID_BASE | int_of_munchstr(mid, 4L, len_mid - 4)
  guid = graphid | objid
  return hex(guid)[2:-1]  # chop off 0x and L


if __name__ == "__main__":
  #o_guid = "9202a8c04000641f800000000172fcb8"
  #o_guid = "9202a8c04000641f800000000164382e"
  #o_guid = "9202a8c04000641f800000000172fcb8"
  o_guid = "9202a8c04000641f80000000013e068e"

  if len(sys.argv) < 2:
    print "usage: mid.py <mid to decode>"
    sys.exit(1)

  mid = sys.argv[1]
  print to_guid(mid)
  #mid    = of_guid(o_guid)
  #print mid
  #n_guid = to_guid(mid)
  #print n_guid
