#!/usr/bin/python2.6
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

# -*- coding: utf-8 -*-
#
"""mqlwrite tests from the mql manual ."""

__author__ = 'bneutra@google.com (Brendan Neutra)'

import google3
from pymql.mql import error
from pymql.test import mql_fixture

# Some notes on testing writes:
# These tests come from the MQL Manual found at wiki.freebase.com.
# It's pretty much lifted query for query. I've created a test user
# (mw_brendan) and the "Note" and "Chord" types in "otg" for that user.
# In creating objects, I add some "fuzz" to the name attributes, in order
# to allow these tests to be run over and over. These tests will be run
# using a mocked graphd, but, as with other MQL tests, can be run against
# sandbox from time to time. You could run against "otg" but you should not.
# Finally, though each query represents a test of a particular functionality,
# it's not practical for each query to be it's own test: too many dependencies.
# So, each test is a scenario of many queries and assertions.

class MQLTest(mql_fixture.MQLTest):
  """mql write tests from the mql manual."""

  def setUp(self):
    self.SetMockPath('data/mql_manual_write.yaml')
    super(MQLTest, self).setUp()
    self.env = {'user': '/user/mw_brendan'}

  def testNotes(self):
    """the note examples."""

    fuzz = self.getFuzz('testNotes')

    query = """
    {
      "create":"unless_exists",
        "type":"/user/mw_brendan/default_domain/note",
        "name":"A%s",
        "id":null
    }
    """ % fuzz

    partial_response = {
        'create': 'created',
        'type': '/user/mw_brendan/default_domain/note',
        'name': 'A%s' % fuzz
    }

    self.DoQuery(query, mqlwrite=True)
    self.assertDictContainsSubset(partial_response,
                                  self.mql_result.result,
                                  msg='create a note')
    query = """
    {
      "create":"unless_exists",
        "type":"/user/mw_brendan/default_domain/note",
        "name":"A%s",
        "id":null,
        "guid":null
    }
    """ % fuzz

    partial_response = {
        'create': 'existed',
        'type': '/user/mw_brendan/default_domain/note',
        'name': 'A%s' % fuzz
    }
    self.DoQuery(query, mqlwrite=True)
    note1_id = self.mql_result.result['id']
    note1_guid = self.mql_result.result['guid']

    self.assertDictContainsSubset(partial_response,
                                  self.mql_result.result,
                                  msg='note already exists')

    # just to grab the timestamp
    query = """
    {
      "id":"%s",
      "timestamp":null
    }
    """ % note1_id

    self.DoQuery(query)
    note1_timestamp = self.mql_result.result['timestamp']

    query = """
    {
        "create":"unconditional",
        "type":"/user/mw_brendan/default_domain/note",
        "name":"A%s",
        "id":null
    }
    """ % fuzz

    partial_response = {
        'create': 'created',
        'type': '/user/mw_brendan/default_domain/note',
        'name': 'A%s' % fuzz
    }
    self.DoQuery(query, mqlwrite=True)
    note2_id = self.mql_result.result['id']

    self.assertDictContainsSubset(partial_response,
                                  self.mql_result.result,
                                  msg='create duplicate note')
    self.assertNotEquals(note1_id, note2_id, msg='new note, new id')

    query = """
    {
        "create":"unless_exists",
        "type":"/user/mw_brendan/default_domain/note",
        "name":"A%s",
        "id":null
    }
    """ % fuzz

    exc_response = (
        error.MQLResultError,
        'Need a unique result to attach here, not 2'
    )

    self.DoQuery(query, mqlwrite=True, exc_response=exc_response)

    query = """
      {
        "id":"%s",
        "name":{
          "connect":"update",
          "value":"B%s",
          "lang":"/lang/en"
        }
      }
    """ % (note2_id, fuzz)
    exp_response = """
      {
        "id":"%s",
        "name":{
          "connect":"updated",
          "value":"B%s",
          "lang":"/lang/en"
        }
      }
    """ % (note2_id, fuzz)

    self.DoQuery(query, mqlwrite=True, exp_response=exp_response)

    query = """
      {
        "id":"%s",
        "name":{
          "connect":"update",
          "value":"B%s",
          "lang":"/lang/en"
        }
      }
    """ % (note2_id, fuzz)
    exp_response = """
      {
        "id":"%s",
        "name":{
          "connect":"present",
          "value":"B%s",
          "lang":"/lang/en"
        }
      }
    """ % (note2_id, fuzz)

    self.DoQuery(query, mqlwrite=True, exp_response=exp_response)

    query = """
      {
        "id":"%s",
        "type":{
          "connect":"insert",
          "id":"/common/topic"
        }
      }
    """ % note1_id
    exp_response = """
      {
        "id":"%s",
        "type":{
          "connect":"inserted",
          "id":"/common/topic"
        }
      }
    """ % note1_id

    self.DoQuery(query, mqlwrite=True, exp_response=exp_response)

    query = """
      {
        "id":"%s",
        "type":{
          "connect":"insert",
          "id":"/common/topic"
        }
      }
    """ % note1_id
    exp_response = """
      {
        "id":"%s",
        "type":{
          "connect":"present",
          "id":"/common/topic"
        }
      }
    """ % note1_id

    self.DoQuery(query, mqlwrite=True, exp_response=exp_response)

    query = """
      {
        "id":"%s",
        "type":[]
      }
    """ % note1_id
    exp_response = """
      {
        "id":"%s",
        "type":[
          "/user/mw_brendan/default_domain/note",
          "/common/topic"
        ]
      }
    """ % note1_id

    self.DoQuery(query, exp_response=exp_response)

    query = """
      {
        "id":"%s",
        "type":{
          "connect":"delete",
          "id":"/common/topic"
        }
      }
    """ % note1_id
    exp_response = """
      {
        "id":"%s",
        "type":{
          "connect":"deleted",
          "id":"/common/topic"
        }
      }
    """ % note1_id

    self.DoQuery(query, mqlwrite=True, exp_response=exp_response)

    query = """
      {
        "id":"%s",
        "type":{
          "connect":"delete",
          "id":"/common/topic"
        }
      }
    """ % note1_id
    exp_response = """
      {
        "id":"%s",
        "type":{
          "connect":"absent",
          "id":"/common/topic"
        }
      }
    """ % note1_id

    self.DoQuery(query, mqlwrite=True, exp_response=exp_response)

    query = """
      [{
        "id":"%s",
        "type":{
          "connect":"delete",
          "id":"/user/mw_brendan/default_domain/note"
        },
        "name":{
          "connect":"delete",
          "value":"A%s",
          "lang":"/lang/en"
        }
      },{
        "id":"%s",
        "type":{
          "connect":"delete",
          "id":"/user/mw_brendan/default_domain/note"
        },
        "name":{
          "connect":"delete",
          "value":"B%s",
          "lang":"/lang/en"
        }
      }]
    """ % (note1_id, fuzz, note2_id, fuzz)

    exp_response = """
      [{
        "id":"%s",
        "type":{
          "connect":"deleted",
          "id":"/user/mw_brendan/default_domain/note"
        },
        "name":{
          "connect":"deleted",
          "value":"A%s",
          "lang":"/lang/en"
        }
      },{
        "id":"%s",
        "type":{
          "connect":"deleted",
          "id":"/user/mw_brendan/default_domain/note"
        },
        "name":{
          "connect":"deleted",
          "value":"B%s",
          "lang":"/lang/en"
        }
      }]
    """ % (note1_id, fuzz, note2_id, fuzz)

    self.DoQuery(query, mqlwrite=True, exp_response=exp_response)

    query = """
      {
        "id":"%s",
        "*":null,
        "/type/reflect/any_master": [{
            "id":null,
            "link":null,
            "optional":true
        }],
        "/type/reflect/any_reverse": [{
            "id":null,
            "link":null,
            "optional":true
        }],
        "/type/reflect/any_value": [{
            "link":null,
            "optional":true,
            "value":null
        }]
      }
    """ % note1_id

    exp_response = """
      {
        "/type/reflect/any_master": [{
          "id":   "/boot/all_permission",
          "link": "/type/object/permission"
        }],
        "/type/reflect/any_reverse": [],
        "/type/reflect/any_value": [],
        "attribution":   "/user/mw_brendan",
        "creator":       "/user/mw_brendan",
        "guid":          "%s",
        "id":            "%s",
        "key":           [],
        "mid": [
          "%s"
        ],
        "name":          null,
        "permission":    "/boot/all_permission",
        "search":        [],
        "timestamp":     "%s",
        "type":          []
      }
    """ % (note1_guid, note1_id, note1_id, note1_timestamp)
    self.DoQuery(query, exp_response=exp_response)

  def testNotesConnect(self):
    """note examples, continued."""

    fuzz = self.getFuzz('testNotesConnect')
    query = """
      [{
        "create":"unless_exists",
        "id":null,
        "type":"/user/mw_brendan/default_domain/note",
        "name":"C%s"
      },{
        "create":"unless_exists",
        "id":null,
        "type":"/user/mw_brendan/default_domain/note",
        "name":"G%s"
      }]
    """ % (fuzz, fuzz)

    self.DoQuery(query, mqlwrite=True)
    cnote_id = self.mql_result.result[0]['id']
    gnote_id = self.mql_result.result[1]['id']
    self.assertEquals(self.mql_result.result[0]['create'], 'created')
    self.assertEquals(self.mql_result.result[1]['create'], 'created')

    query = """
      {
        "id":     "%s",
        "/user/mw_brendan/default_domain/note/next":{
          "connect":"update",
          "id":     "%s"
        }
      }
    """ % (cnote_id, gnote_id)

    exp_response = """
      {
        "id":     "%s",
        "/user/mw_brendan/default_domain/note/next":{
          "connect":"inserted",
          "id":     "%s"
        }
      }
    """ % (cnote_id, gnote_id)
    self.DoQuery(query, mqlwrite=True, exp_response=exp_response)

    query = """
      {
        "type":"/user/mw_brendan/default_domain/note",
        "name":"G%s",
        "next":{
          "create":"unless_exists",
          "type":"/user/mw_brendan/default_domain/note",
          "name":"D%s"
        }
      }
    """ % (fuzz, fuzz)

    exp_response = """
      {
        "type":"/user/mw_brendan/default_domain/note",
        "name":"G%s",
        "next":{
          "create":"created",
          "type":"/user/mw_brendan/default_domain/note",
          "name":"D%s"
        }
      }
    """ % (fuzz, fuzz)
    self.DoQuery(query, mqlwrite=True, exp_response=exp_response)

    query = """
      {
        "create":"unless_exists",
        "type":"/user/mw_brendan/default_domain/note",
        "name":"B flat%s",
        "next":{
          "create":"unless_exists",
          "type":"/user/mw_brendan/default_domain/note",
          "name":"F%s",
          "next":{
            "create":"unless_exists",
            "type":"/user/mw_brendan/default_domain/note",
            "name":"C%s"
          }
        }
      }
    """ % (fuzz, fuzz, fuzz)

    exp_response = """
      {
        "create":"created",
        "type":"/user/mw_brendan/default_domain/note",
        "name":"B flat%s",
        "next":{
          "create":"created",
          "type":"/user/mw_brendan/default_domain/note",
          "name":"F%s",
          "next":{
            "create":"connected",
            "type":"/user/mw_brendan/default_domain/note",
            "name":"C%s"
          }
        }
      }
    """ % (fuzz, fuzz, fuzz)

    self.DoQuery(query, mqlwrite=True, exp_response=exp_response)

    query = """
      {
        "create":"unless_exists",
        "type":"/user/mw_brendan/default_domain/note",
        "name":"E flat%s",
        "next":{
          "create":"unless_connected",
          "type":"/user/mw_brendan/default_domain/note",
          "name":"B flat%s"
        }
      }
    """ % (fuzz, fuzz)
    exp_response = """
      {
        "create":"created",
        "type":"/user/mw_brendan/default_domain/note",
        "name":"E flat%s",
        "next":{
          "create":"created",
          "type":"/user/mw_brendan/default_domain/note",
          "name":"B flat%s"
        }
      }
    """ % (fuzz, fuzz)
    self.DoQuery(query, mqlwrite=True, exp_response=exp_response)

    query = """
      {
        "create":"unless_exists",
        "type":"/user/mw_brendan/default_domain/note",
        "name":"E flat%s",
        "next":{
          "create":"unless_connected",
          "type":"/user/mw_brendan/default_domain/note",
          "name":"B flat%s"
        }
      }
    """ % (fuzz, fuzz)
    exp_response = """
      {
        "create":"existed",
        "type":"/user/mw_brendan/default_domain/note",
        "name":"E flat%s",
        "next":{
          "create":"existed",
          "type":"/user/mw_brendan/default_domain/note",
          "name":"B flat%s"
        }
      }
    """ % (fuzz, fuzz)
    self.DoQuery(query, mqlwrite=True, exp_response=exp_response)

    query = """
      {
        "create":"unless_connected",
        "type":"/user/mw_brendan/default_domain/note",
        "name":"E flat%s",
        "next":{
          "create":"unless_connected",
          "type":"/user/mw_brendan/default_domain/note",
          "name":"B flat%s"
        }
      }
    """ % (fuzz, fuzz)

    exc_response = (
        error.MQLParseError,
        "Can't use 'create': 'unless_connected' at the root of the query"
    )
    self.DoQuery(query, mqlwrite=True, exc_response=exc_response)

    query = """
      {
        "type":"/user/mw_brendan/default_domain/note",
        "name":"E flat%s",
        "next":{
          "connect":"delete",
          "type":{
            "connect":"delete",
            "id":"/user/mw_brendan/default_domain/note"
          },
          "name":{
            "connect":"delete",
            "value":"B flat%s",
            "lang":"/lang/en"
           }
        }
      }
    """ % (fuzz, fuzz)

    exp_response = """
      {
        "type":"/user/mw_brendan/default_domain/note",
        "name":"E flat%s",
        "next":{
          "connect":"deleted",
          "type":{
            "connect":"deleted",
            "id":"/user/mw_brendan/default_domain/note"
          },
          "name":{
            "connect":"deleted",
            "value":"B flat%s",
            "lang":"/lang/en"
           }
        }
      }
    """ % (fuzz, fuzz)

    self.DoQuery(query, mqlwrite=True, exp_response=exp_response)

    query = """
      {
        "type":"/user/mw_brendan/default_domain/note",
        "name":"E flat%s",
        "next":{
          "connect":"insert",
          "type":"/user/mw_brendan/default_domain/note",
          "name":"B flat%s"
        }
      }
    """ % (fuzz, fuzz)

    exp_response = """
      {
        "type":"/user/mw_brendan/default_domain/note",
        "name":"E flat%s",
        "next":{
          "connect":"inserted",
          "type":"/user/mw_brendan/default_domain/note",
          "name":"B flat%s"
        }
      }
    """ % (fuzz, fuzz)
    self.DoQuery(query, mqlwrite=True, exp_response=exp_response)

  def testNotesChords(self):
    """note examples, continued."""

    # create some stuff
    fuzz = self.getFuzz('testNotesChords')
    query = """
      [{
        "create":"unless_exists",
        "id":null,
        "type":"/user/mw_brendan/default_domain/note",
        "name":"C%s"
      },{
        "create":"unless_exists",
        "id":null,
        "type":"/user/mw_brendan/default_domain/note",
        "name":"F%s"
      },{
        "create":"unless_exists",
        "id":null,
        "type":"/user/mw_brendan/default_domain/note",
        "name":"G%s"
      }]
    """ % (fuzz, fuzz, fuzz)

    self.DoQuery(query, mqlwrite=True)

    # and start testing
    query = """
      {
        "create":"unless_exists",
        "name":"CEG%s",
        "type":[
          "/common/topic",
          "/user/mw_brendan/default_domain/chord"
        ],
        "note":[{
          "create":"unless_exists",
          "type":"/user/mw_brendan/default_domain/note",
          "name":"C%s"
        },{
          "create":"unless_exists",
          "type":"/user/mw_brendan/default_domain/note",
          "name":"G%s"
        },{
          "create":"unless_exists",
          "type":"/user/mw_brendan/default_domain/note",
          "name":"E%s"
        }]
      }
    """ % (fuzz, fuzz, fuzz, fuzz)

    exp_response = """
      {
        "create":"created",
        "name":"CEG%s",
        "type":[
          "/common/topic",
          "/user/mw_brendan/default_domain/chord"
        ],
        "note":[{
          "create":"connected",
          "type":"/user/mw_brendan/default_domain/note",
          "name":"C%s"
        },{
          "create":"connected",
          "type":"/user/mw_brendan/default_domain/note",
          "name":"G%s"
        },{
          "create":"created",
          "type":"/user/mw_brendan/default_domain/note",
          "name":"E%s"
        }]
      }
    """ % (fuzz, fuzz, fuzz, fuzz)
    self.DoQuery(query, mqlwrite=True, exp_response=exp_response)

    query = """
      {
       "type":"/user/mw_brendan/default_domain/chord",
       "name":"CEG%s",
       "note":[]
      }
    """ % fuzz
    exp_response = """
      {
       "type":"/user/mw_brendan/default_domain/chord",
       "name":"CEG%s",
       "note":["C%s","G%s","E%s"]
      }
    """ % (fuzz, fuzz, fuzz, fuzz)

    self.DoQuery(query, exp_response=exp_response)

    query = """
      {
       "type":"/user/mw_brendan/default_domain/note",
       "name":"C%s",
       "chord": []
      }
    """ % fuzz
    exp_response = """
      {
       "type":"/user/mw_brendan/default_domain/note",
       "name":"C%s",
       "chord": ["CEG%s"]
      }
    """ % (fuzz, fuzz)

    self.DoQuery(query, exp_response=exp_response)

    query = """
      {
        "create":"unless_exists",
        "type":["/common/topic",
                "/user/mw_brendan/default_domain/chord"],
        "name":"BFG%s"
      }
    """ % fuzz

    exp_response = """
      {
        "create":"created",
        "type":["/common/topic",
                "/user/mw_brendan/default_domain/chord"],
        "name":"BFG%s"
      }
    """ % fuzz
    self.DoQuery(query, mqlwrite=True, exp_response=exp_response)

    query = """
      [{
        "create":"unless_exists",
        "type":"/user/mw_brendan/default_domain/note",
        "name":"B%s",
        "chord": {
          "connect":"insert",
          "type":"/user/mw_brendan/default_domain/chord",
          "name":"BFG%s"
        }
      },{
        "create":"unless_exists",
        "type":"/user/mw_brendan/default_domain/note",
        "name":"F%s",
        "chord": {
          "connect":"insert",
          "type":"/user/mw_brendan/default_domain/chord",
          "name":"BFG%s"
        }
      },{
        "create":"unless_exists",
        "type":"/user/mw_brendan/default_domain/note",
        "name":"G%s",
        "chord": {
          "connect":"insert",
          "type":"/user/mw_brendan/default_domain/chord",
          "name":"BFG%s"
        }
      }]
    """ % (fuzz, fuzz, fuzz, fuzz, fuzz, fuzz)

    exp_response = """
      [{
        "create":"created",
        "type":"/user/mw_brendan/default_domain/note",
        "name":"B%s",
        "chord": {
          "connect":"inserted",
          "type":"/user/mw_brendan/default_domain/chord",
          "name":"BFG%s"
        }
      },{
        "create":"existed",
        "type":"/user/mw_brendan/default_domain/note",
        "name":"F%s",
        "chord": {
          "connect":"inserted",
          "type":"/user/mw_brendan/default_domain/chord",
          "name":"BFG%s"
        }
      },{
        "create":"existed",
        "type":"/user/mw_brendan/default_domain/note",
        "name":"G%s",
        "chord": {
          "connect":"inserted",
          "type":"/user/mw_brendan/default_domain/chord",
          "name":"BFG%s"
        }
      }]
    """ % (fuzz, fuzz, fuzz, fuzz, fuzz, fuzz)

    self.DoQuery(query, mqlwrite=True, exp_response=exp_response)

    query = """
      {
       "type":"/user/mw_brendan/default_domain/chord",
       "name":"BFG%s",
       "note":[]
      }
    """ % fuzz
    exp_response = """
      {
       "type":"/user/mw_brendan/default_domain/chord",
       "name":"BFG%s",
       "note":["B%s","F%s","G%s"]
      }
    """ % (fuzz, fuzz, fuzz, fuzz)

    self.DoQuery(query, exp_response=exp_response)


if __name__ == '__main__':
  mql_fixture.main()
