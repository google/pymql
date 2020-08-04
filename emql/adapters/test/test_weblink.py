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

from mw.tests.helpers import TestFixture
from mw.emql import emql
null = None
true = True
false = False
WEBLINK = "/common/topic/weblink"
class TestWeblinks_adapter(TestFixture):
    
    def setUp(self):
        super(TestWeblinks_adapter, self).setUp()
        self.cache = emql.emql_cache()


    def run_query(self, q):
        debug, cursors, results = self.mss.emqlread(None, q, {'debug': True, 'cache': False},
                                                    cache=self.cache)
        return results


    def test_bob_dylan(self):
        
        r = self.run_query({
                "id":"/en/bob_dylan",
                WEBLINK:[]
        })

        weblinks = r[WEBLINK] 
        self.assert_(weblinks, "Basic sanity test - make sure there are some weblinks returning which indiciate that at least emql is working and that the weblinks adapter is returning results.")

        #XXXXXX UNCOMMENT AFTER https://bugs.freebase.com/browse/DA-1093 ######

        #self.assert_("http://www.bobdylan.com/" in weblinks, "Test a key hanging off of a resource")

        self.assert_("http://en.wikipedia.org/wiki/Bob_Dylan" in weblinks, "Test a key hanging off a topic")

        
    def test_list_shape(self):
        """
        Let's test to make sure weblink works with just a [] shape, in which case it should
        just return a list of strings
        """
        
        r = self.run_query({
                "id":"/en/migraine",
                "/common/topic/weblink":[]
                })
        
        weblinks = r[WEBLINK]
        self.assert_(len(weblinks), "there should be some weblinks in here!")

        for w in weblinks:
            self.assert_(isinstance(w, str))

    def test_topic_with_all_types_of_weblinks(self):
        """
        This particular topic has a weblinks generated from keys in all three 
        places - off the topic, off the annotation, off the resource
        """
        q = {
           "id": "/en/royal_mail",
           WEBLINK: [{
               "url":null,
               "template":{
                   "id":null,
                   "template":null,
                   "ns":null
               },
               "category":{
                  "id":null,
                  "name":null,
                  "optional":true
               },
               "key":null
           }]
        }
        r = self.run_query(q)
        
        weblink_dict = {}
        for w in r[WEBLINK]:
            weblink_dict[w['url']] = w
        
        official_link = weblink_dict.get("http://www.royalmailgroup.com/")
        self.assert_(official_link, "The official link for royal mail is present. Key Hangs off resource.")
        self.assert_(official_link['category']['name'] == "Official Website", "Official Website category is....Official Website")
        

        guardian_link = weblink_dict.get("http://www.guardian.co.uk//uk/post")
        self.assert_(guardian_link, "Guardian link is present. Key hangs off annotation.")
        self.assert_(guardian_link['category']['name'] == "Tag", "Category is Tag")
        
        wiki_link = weblink_dict.get("http://en.wikipedia.org/wiki/index.html?curid=349823")
        self.assert_(wiki_link, "Wiki link is present. Key hangs off topic itself.")

        
