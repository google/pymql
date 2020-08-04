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

import datetime
import re


# a datetime is a non-empty string containing one of
# yyyy, yyyy-mm, yyyy-mm-dd,
# Thh, Thh:mm, Thh:mm:ss Thh:mm:ss.dddd
# or yyyy-mm-dd followed by one of the T constructs.
# Note that this is more lenient than valid_timestamp in lojson - it matches the @timestamp clause only,
# not our extended ISO 8601 syntax


# Python datetime classes support only a year range between MINYEAR (1) and MAXYEAR(9999)
# we want to support anything from -9999 (== 10000BC) to 9999 (== 9999AD)
# and possibly support more in the future.

# and some other useful methods:
__datetime_re = re.compile(r'^(?:(?:(-?\d{4})(?:-(\d\d)(?:-(\d\d))?)?)|(?:(-?\d{4})-(\d\d)-(\d\d)T)?(\d\d)(?:\:(\d\d)(?:\:(\d\d)(?:\.(\d{1,6}))?)?)?(Z|[-+](?:0\d|1[0-4])\:(00|15|30|45))?)$')

# returns the graph format datetime (like ISO except for a leading T on times)
def coerce_datetime(dt):
    try:
        if dt == '__now__':
            return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        if dt == '__today__':
            return datetime.datetime.utcnow().strftime("%Y-%m-%d")

        match = __datetime_re.match(dt)
        if not match:
            return None
        elif match.group(1):
            if check_date(*match.group(1,2,3)):
                return dt
            else:
                return None
        elif match.group(4):
            # a date/time
            # we don't do subseconds as python thinks that '2' is "2 microseconds" not "2 deciseconds".
            if not check_date(*match.group(4,5,6)):
                return None
            if not check_time(*match.group(7,8,9)):
                return None

            return dt

        elif match.group(7):
            if not check_time(*match.group(7,8,9)):
                return None

            return 'T' + dt
        else:
            # no idea what the problem is, but it is invalid
            return None

    except TypeError:
        return None
    except ValueError:
        return None

def check_date(year,month,day):
    # returns true or false depending on whether the day is valid
    # handles strings and nulls
    fakeyear = int(year)
    if int(fakeyear) > 9999 or int(fakeyear) < -9999:
        return False

    if month is None:
        return True
    elif int(month) < 1 or int(month) > 12:
        return False
    elif day is None:
        return True
    else:
        while fakeyear <= 0:
            fakeyear += 8000

        try:
            datetime.date(fakeyear,int(month),int(day))
            return True
        except ValueError:
            return False

def check_time(hour,minute,second):
    if hour is None:
        return False
    elif int(hour) < 0 or int(hour) > 23:
        return False
    elif minute is None:
        return True
    elif int(minute) < 0 or int(minute) > 59:
        return False
    elif second is None:
        return True
    elif int(second) < 0 or int(second) > 59:
        return False
    else:
        return True


def uncoerce_datetime(graphdt):
    if graphdt[0] == 'T':
        return graphdt[1:]
    else:
        return graphdt
