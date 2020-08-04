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
import urllib2

ip_address = re.compile(r"^\d+\.\d+\.\d+.\d+$").match

def parse_domain_from_host(host):
    host = host.split(':')[0]

    if not ip_address(host):
        # the domain is the last one or two dot-separated words
        domain = '.'.join(host.rsplit(".", 2)[-2:])
    else:
        domain = host
    
    return domain

def get_http_proxy_opener(mss):
    """
    Lazily retrieve proxy info
    """
    config = mss.full_config

    proxy_addr = config.get('me.external_proxy', '').strip()
    if not proxy_addr:
        return urllib2.urlopen
    else:
        proxy_handler = urllib2.ProxyHandler({'http': proxy_addr})
        return urllib2.build_opener(proxy_handler).open

def proxied_urlopen(request, mss):
    opener = get_http_proxy_opener(mss)
    return opener(request)
