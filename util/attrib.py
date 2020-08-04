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

from mw.mql import scope
from mw.log import LOG

def set_oauth_attribution_if_needed(mss):
    if not mss.authorized_app_id:
        return
    
    user_id = mss.get_user_id()
    
    query = [{
        "id": None,
        "creator": user_id,
        "type": "/freebase/written_by",
        "/freebase/written_by/application": {"id": mss.authorized_app_id}
    }]

    result = mss.mqlread(query, cache=False)
    if result:
        if len(result) > 1:
            # somehow we manage to get multiple attributions - fail gracefully and log an error
            LOG.warn("set_oauth_attribution_if_needed.duplicate",
                     "duplicate attributions for %s and %s" % (mss.authorized_app_id, user_id),
                     application_id=mss.authorized_app_id,
                     user_id=user_id,
                     attributions=result)
        result = result[0]
    else:
        query = {
             "create": "unconditional",
             "id": None,
             "/freebase/written_by/application": {
                "connect": "insert",
                "id": mss.authorized_app_id
             },
             "type": ["/freebase/written_by", "/type/attribution"]
        }
    
        with mss.push_variables(permission="/boot/oauth_permission",
                                privileged=scope.Privileged,
                                authority=None):
            result = mss.mqlwrite(query)
    mss.push_variables(attribution=result['id'] if result else None)
