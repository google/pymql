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

import mw

def bdb_lookup(me,guid,bdb):
    guid = guid.replace('#','/guid/')
    path = mw.blob.blobclient.BLOBClient.get_static_relative_url(bdb, guid)
    hostname,port=me.mss.ctx.clobd_read_addrs[0]
    hostname=hostname + ':' + str(port)
    url, connection = me.get_session().http_connect(hostname, path)
    connection.request('GET', url)
    response = connection.getresponse()
    result   = response.read()
    #TODO: how to do debugging? LOG if debug?
    #print "metacritic_adapter: result: %s" % result
    if response.status==200:
        return mw.json.loads(result)
    elif response.status==404:
        return None
    else:
        #TODO: Log unexpected status from BDB
        return None
