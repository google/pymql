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


# imported from Client

# given a graphd datetime string (iso6801 format)
# parse it and format it

import re, datetime


ISO8601_TIME_PATTERN = r"(?P<hour>[0-9]{2})(:(?P<minute>[0-9]{2}))?(:(?P<second>[0-9]{2})(.(?P<fraction>[0-9]+))?)?"

ISO8601_TIME_REGEX = re.compile(ISO8601_TIME_PATTERN)

ISO8601_REGEX = \
    re.compile(r"(?P<bc>-)?(?P<year>[0-9]{4})(-(?P<month>[0-9]{1,2})(-(?P<day>[0-9]{1,2})"
               r"((?P<separator>.)" + ISO8601_TIME_PATTERN +
               r"(?P<timezone>Z|(([-+])([0-9]{2}):([0-9]{2})))?)?)?)?")

LABELS = ('year', 'month', 'day', 'hour', 'minute', 'second')

# This essentially maps the number of date components to a format,
# Especially annoying: these can't be unicode, strftime doesn't like that
FORMATS = [
    "%Y",                   # year only
    "%b %Y",                # year, month
    "%b %e, %Y",            # year, month, day
    "%b %e, %Y %l%p",       # year, month, day, hour
    "%b %e, %Y %l:%M%p",    # year, month, day, hour, minute
    "%b %e, %Y %l:%M:%S%p", # year, month, day, hour, minute, second
]
BC_FORMATS = [format.replace("%Y", "%Y B.C.E.") for format in FORMATS]
CE_FORMATS = [format.replace("%Y", "%Y C.E.") for format in FORMATS]


def parse_isodate(iso_date):
     """
     Given an iso8601-formatted string (or fraction thereof) return a
     tuple containing a python datetime object and a format string that
     should be used to display it. The format is passible to strftime()
     and should be locale-sensitive about ordering (though today it is
     not)
     """

     m = ISO8601_REGEX.match(iso_date)
     if not m:
         m = ISO8601_TIME_REGEX.match(iso_date)
         if not m: # bad data in the graph
             return None, None
         time_only = True
     else:
         time_only = False

     values = m.groupdict()

     args = []
     if time_only:
         today = datetime.date.today()
         args = [today.year, today.month, today.day]
         start = 3
     else:
         start = 0

     count = start
     for k in xrange(start, 6):
         value = values[LABELS[k]]
         if value is None:
             args.append(1)
         else:
             count += 1
             args.append(int(value))

     try:
         d = datetime.datetime(*args)
     except ValueError:
         return None, None

     if values.get('bc'):
         format = BC_FORMATS[count - 1]
     elif 0 <= d.year < 1000:
         format = CE_FORMATS[count - 1]
     else:
         format = FORMATS[count - 1]
         if time_only:
             format = format[10:]

     if iso_date.endswith('Z'):
         format += ' UTC'

     return d, format


def format_isodate(iso_date):
     """
     Given an iso8601 formatted string (or fraction thereof) return
     a timezone-independent display of the string.
     """

     d, format = parse_isodate(iso_date)
     if d is None:
         return None

     if d.year >= 1900:
         result = d.strftime(format)
     else:
         # make sure to pick something that is a leapyear, so that
         # 29-Feb is available! Note that 1900 is NOT a leapyear
         d_1904 = d.replace(year=1904)
         result = d_1904.strftime(format).replace("1904", str(d.year))

     if format.endswith("%p"):
         result = result[:-2] + result[-2:].lower()

     return result.replace("  ", " ").lstrip()


if __name__ == "__main__":
    import sys
    print format_isodate(sys.argv[1])
