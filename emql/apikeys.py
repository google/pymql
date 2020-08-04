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

import hmac, hashlib

null = None
from mw.user.sqlmodel import mwOAuthProviderToken, get_sql_connection
from sqlobject import AND, IN

def get_context(mss):
    """
    Get a unique string representing the combined user/app
    context.

    Note that this depends on mss.authenticate() having been
    called, if appropriate. This allows the context to be null if the
    call did not require authentication.
    """
    user_id = mss.get_user_id() or ''
    app_id = mss.get_app_id() or ''

    if not user_id and not app_id:
        return None

    # user_id&app_id, user_id&, or &app_id
    context = "%s&%s" % (user_id, app_id)

    # hmac-sha1 just like oauth
    magic_secret = "Sup3rAuth3nticated!eMQL"
    signed_context = hmac.new(magic_secret, context, hashlib.sha1).hexdigest()

    return signed_context

def get_extension_api_query(extension_id=None, optional=False):
    result = [{"id": null,
               "type": "/freebase/foreign_api",
               "consumer_token": {"id": null,
                                  "optional": True},
               "access_token": {"id": null,
                                "optional": True},
               "api_keys": [{
                   "id": null,
                   "optional": True
                   }]
               }]
    if extension_id:
        result[0]["extension"] = {"id": extension_id}

    if optional:
        result[0]["optional"] = True

    return result
        

def get_api_keys(mss, extension_id, apis=None):
    """
    For a given extension, get all the API keys out of the database

    `apis` is the result of something like get_extension_api_query() -
    if you don't provide it then mqlread will be run to fill it in for
    the given extension_id
    """

    # get a list of all keys that this extension needs, grouped by API
    # (because, in fact, an extension might use APIs that share
    # overlapping keys)

    if apis is None:
        q = get_extension_api_query(extension_id, optional=False)
        apis = mss.mqlread(q)

    if not apis:
        return None
    
    # ok, now authenticate
    mss.authenticate()
    context = get_context(mss)

    # to fetch them from the database, we want a flat list of all unique ids
    all_keys = set()
    for api in apis:
        for api_key in api["api_keys"]:
            all_keys.add(api_key)
        if api["access_token"]:
            all_keys.add(api["access_token"]["id"])
        if api["consumer_token"]:
            all_keys.add(api["consumer_token"]["id"])

    conn = get_sql_connection(mss)

    # now query the provider database for all of these specific keys
    foreign_key_list = mwOAuthProviderToken.select(
        AND(mwOAuthProviderToken.q.context == context,
            IN(mwOAuthProviderToken.q.apiKeyId, all_keys)),
        connection=conn
        )

    # generate a map of id->key data so we can access it below
    foreign_keys = {}
    for foreign_key in foreign_key_list:
        info = {
            "id" : foreign_key.apiKeyId,
            "key": foreign_key.key
            }
        if foreign_key.secret:
            info["secret"] = foreign_key.secret
            
        foreign_keys[foreign_key.apiKeyId] = info

    # now generate a datastructure similar to the mqlread
    # something like
    # [{ "id": "/netflix/queue_info",
    #    "consumer_token": {
    #        "id": "/netflix/consumer_token",
    #        "key": "ccc",
    #        "secret": "secretccc",
    #    },
    #    "access_token": {
    #        "id": "/netflix/access_token",
    #        "key": "aaa",
    #        "secret": "secretaaa",
    #    },
    #  },
    #  { "id": "/netflix/movie_info",
    #    "consumer_token": {
    #        "id": "/netflix/consumer_token",
    #        "key": "ccc",
    #        "secret": "secretccc",
    #    },
    #    "api_keys": [{
    #        "id": "/netflix/affiliate_code",
    #        "key": "fff"
    #     }]
    #  }]
        
    api_manifest = []
    for api in apis:
        api_info = {"id": api["id"]}
        api_manifest.append(api_info)

        for special_key in ("consumer_token", "access_token"):
            if api.get(special_key):
                # map "consumer_token" to "/netflix/consumer_token"
                special_key_id = api[special_key]["id"]
                
                # even if we dont' have the key, include dummy entry
                # meaning that the API requires the key
                api_info[special_key] = {
                    "id": special_key_id
                    }
                if special_key_id in foreign_keys:
                    # key and secret MUST be there
                    foreign_key = foreign_keys[special_key_id]
                    api_info[special_key]["key"] = foreign_key["key"]
                    api_info[special_key]["secret"] = foreign_key["secret"]
            
        for api_key in api["api_keys"]:
            api_key_id = api_key["id"]

            # put a dummy entry in, meaning the API requires/expects
            # the key
            api_key_info = {
                "id": api_key_id,
                }
            api_info.setdefault("api_keys",[]).append(api_key_info)
            
            if api_key_id in foreign_keys:
                
                foreign_key = foreign_keys[api_key_id]
                
                if foreign_key.get("key"):
                    api_key_info["key"] = foreign_key["key"]
                    
                if foreign_key.get("secret"):
                    api_key_info["secret"] = foreign_key["secret"]

    return api_manifest
