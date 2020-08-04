#!/usr/bin/python2.4
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
"""mqlread mids tests.

This file is ported over from Metaweb svn (acquisition).
It follows a pretty simple pattern (see the query_assert method).
It does not pass gpylint. Sorry.
"""
__author__ = 'bneutra@google.com (Brendan Neutra)'

import json
import google3
from pymql.mql import error
from pymql.test import mql_fixture

class MQLTest(mql_fixture.MQLTest):
  """testing mqlread mid queries."""

  def setUp(self):
    self.SetMockPath('data/mids.yaml')
    super(MQLTest, self).setUp()

  def query_assert(self, q, r, exc_response=None, type="mqlread", asof="2010-05-21T00:00"):
    self.env = {"as_of_time": asof}
    self.DoQuery(q, exp_response=r, exc_response=exc_response)

  # sting is an uncontested winner (no merges)
  def test_id_uncontested_1(self):
    q= '{"guid":null, "id":"#9202a8c04000641f8000000000092a01", "mid":null}'
    r= '{"guid": "#9202a8c04000641f8000000000092a01", "id": "#9202a8c04000641f8000000000092a01","mid":"/m/0lbj1"}'
    self.query_assert(q,r)

  def test_id_uncontested_2(self):
    q= '{"guid":null, "id":"/guid/9202a8c04000641f8000000000092a01", "mid":null}'
    r= '{"guid": "#9202a8c04000641f8000000000092a01", "id": "/guid/9202a8c04000641f8000000000092a01","mid":"/m/0lbj1"}'
    self.query_assert(q,r)

  def test_id_uncontested_3(self):
    q= '{"guid":null, "id":"/en/sting", "mid":null}'
    r= '{"guid": "#9202a8c04000641f8000000000092a01", "id": "/en/sting","mid":"/m/0lbj1"}'
    self.query_assert(q,r)

  def test_id_uncontested_4(self):
    q= '{"guid":null, "id":"/m/0lbj1", "mid":null}'
    r= '{"guid": "#9202a8c04000641f8000000000092a01", "id": "/m/0lbj1","mid":"/m/0lbj1"}'
    self.query_assert(q,r)

  def test_id_uncontested_5(self):
    q= '{"guid":null, "id":"/m/0lbj1", "mid":[]}'
    r= '{"guid": "#9202a8c04000641f8000000000092a01", "id": "/m/0lbj1","mid":["/m/0lbj1", "/m/0c498c6", "/m/01v39md"]}'
    self.query_assert(q,r)

  def test_guid_uncontested_1(self):
    q= '{"id":null, "guid":"#9202a8c04000641f8000000000092a01", "mid":null}'
    r= '{"guid": "#9202a8c04000641f8000000000092a01", "id": "/en/sting","mid":"/m/0lbj1"}'
    self.query_assert(q,r)

  def test_guid_uncontested_2(self):
    q= '{"id":null, "guid":"/guid/9202a8c04000641f8000000000092a01", "mid":null}'
    self.query_assert(q, None, exc_response=(
        error.MQLParseError,
        "Can only use a hexadecimal guid here"
    ))

  def test_guid_uncontested_3(self):
    q= '{"id":null, "guid":"/en/sting", "mid":null}'
    self.query_assert(q, None, exc_response=(
        error.MQLParseError,
        "Can only use a hexadecimal guid here"
    ))

  def test_guid_uncontested_4(self):
    q= '{"id":null, "guid":"/m/0lbj1", "mid":null}'
    self.query_assert(q, None, exc_response=(
        error.MQLParseError,
        "Can only use a hexadecimal guid here"
    ))

  def test_guid_uncontested_5(self):
    q= '{"id":null, "guid":"#9202a8c04000641f8000000000092a01", "mid":[]}'
    r= '{"guid": "#9202a8c04000641f8000000000092a01", "id": "/en/sting","mid":["/m/0lbj1", "/m/0c498c6", "/m/01v39md"]}'
    self.query_assert(q,r)

  def test_mid_uncontested_1(self):
    q= '{"id":null, "mid":"#9202a8c04000641f8000000000092a01", "guid":null}'
    self.query_assert(q, None, exc_response=(
        error.MQLParseError,
        "MID is invalid (failed to parse)"
    ))

  def test_mid_uncontested_2(self):
    q= '{"id":null, "mid":"/guid/9202a8c04000641f8000000000092a01", "guid":null}'
    self.query_assert(q, None, exc_response=(
        error.MQLParseError,
        "MID is invalid (failed to parse)"
    ))

  def test_mid_uncontested_3(self):
    q= '{"id":null, "mid":"/en/sting", "guid":null}'
    self.query_assert(q, None, exc_response=(
        error.MQLParseError,
        "MID is invalid (failed to parse)"
    ))

  def test_mid_uncontested_4(self):
    q= '{"id":null, "mid":"/m/0lbj1", "guid":null}'
    r= '{"guid": "#9202a8c04000641f8000000000092a01", "id": "/en/sting","mid":"/m/0lbj1"}'
    self.query_assert(q,r)

  def test_mid_uncontested_5(self):
    q= '{"id":null, "mid":["/m/0lbj1"], "guid":null}'
    self.query_assert(q, None, exc_response=(
        error.MQLParseError,
        "Can't put raw values into a list, only dictionaries"
    ))

  # los angeles is merged, this is the  winner object
  def test_id_winner_1(self):
    q= '{"guid":null, "id":"/en/los_angeles", "mid":null}'
    r= '{"guid": "#9202a8c04000641f80000000060b2879", "id": "/en/los_angeles","mid":"/m/030qb3t"}'
    self.query_assert(q,r)

  def test_id_winner_2(self):
    q= '{"guid":null, "id":"#9202a8c04000641f80000000060b2879", "mid":null}'
    r= '{"guid": "#9202a8c04000641f80000000060b2879", "id": "#9202a8c04000641f80000000060b2879","mid":"/m/030qb3t"}'
    self.query_assert(q,r)

  def test_id_winner_3(self):
    q= '{"guid":null, "id":"/guid/9202a8c04000641f80000000060b2879", "mid":null}'
    r= '{"guid": "#9202a8c04000641f80000000060b2879", "id": "/guid/9202a8c04000641f80000000060b2879","mid":"/m/030qb3t"}'
    self.query_assert(q,r)

  def test_id_winner_4(self):
    q= '{"guid":null, "id":"/m/030qb3t", "mid":null}'
    r= '{"guid": "#9202a8c04000641f80000000060b2879", "id": "/m/030qb3t","mid":"/m/030qb3t"}'
    self.query_assert(q,r)

  def test_id_winner_5(self):
    q= '{"guid":null, "id":"/m/030qb3t", "mid":[]}'
    r= '{"guid": "#9202a8c04000641f80000000060b2879", "id": "/m/030qb3t","mid":["/m/030qb3t", "/m/04lr0", "/m/02h843w", "/m/0256dc7", "/m/0256ckz", "/m/0kjjkr", "/m/02nt5sr", "/m/0256b1m", "/m/065595y", "/m/07hcxp7", "/m/0dj42mf", "/m/0jzc22d"]}'
    self.query_assert(q,r)

  def test_guid_winner_1(self):
    q= '{"id":null, "guid":"#9202a8c04000641f80000000060b2879", "mid":null}'
    r= '{"id": "/en/los_angeles", "guid": "#9202a8c04000641f80000000060b2879","mid":"/m/030qb3t"}'
    self.query_assert(q,r)

  def test_guid_winner_2(self):
    q= '{"id":null, "guid":"#9202a8c04000641f80000000060b2879", "mid":[]}'
    r= '{"guid": "#9202a8c04000641f80000000060b2879", "id": "/en/los_angeles","mid":["/m/030qb3t", "/m/04lr0", "/m/02h843w", "/m/0256dc7", "/m/0256ckz", "/m/0kjjkr", "/m/02nt5sr", "/m/0256b1m", "/m/065595y", "/m/07hcxp7", "/m/0dj42mf", "/m/0jzc22d"]}'
    self.query_assert(q,r)

  def test_mid_winner_1(self):
    q= '{"id":null, "mid":"/m/030qb3t", "guid":null}'
    r= '{"id": "/en/los_angeles", "guid": "#9202a8c04000641f80000000060b2879","mid":"/m/030qb3t"}'
    self.query_assert(q,r)

  def test_mid_winner_2(self):
    q= '{"guid":null,"id":null, "mid":"/m/030qb3t", "a:mid":[]}'
    r= '{"guid": "#9202a8c04000641f80000000060b2879", "id": "/en/los_angeles", "mid":"/m/030qb3t", "a:mid":["/m/030qb3t", "/m/04lr0", "/m/02h843w", "/m/0256dc7", "/m/0256ckz", "/m/0kjjkr", "/m/02nt5sr", "/m/0256b1m", "/m/065595y", "/m/07hcxp7", "/m/0dj42mf", "/m/0jzc22d"]}'
    self.query_assert(q,r)



  # one of los angeles' loser objects
  def test_id_loser_1(self):
    q= '{"guid":null, "id":"/m/065595y", "mid":null}'
    r= '{"guid": "#9202a8c04000641f80000000060b2879", "id": "/m/065595y","mid":"/m/030qb3t"}'
    self.query_assert(q,r)

  def test_id_loser_2(self):
    q= '{"guid":null, "id":"/m/065595y", "mid":[]}'
    r= '{"guid": "#9202a8c04000641f80000000060b2879", "id": "/m/065595y","mid": ["/m/030qb3t", "/m/04lr0", "/m/02h843w", "/m/0256dc7", "/m/0256ckz", "/m/0kjjkr", "/m/02nt5sr", "/m/0256b1m", "/m/065595y", "/m/07hcxp7", "/m/0dj42mf", "/m/0jzc22d"]}'
    self.query_assert(q,r)

  def test_id_loser_3(self):
    q= '{"guid":null, "id":"#9202a8c04000641f800000000c52a4bd", "mid":null}'
    r= '{"guid": "#9202a8c04000641f800000000c52a4bd", "id": "#9202a8c04000641f800000000c52a4bd","mid": "/m/065595y"}'
    self.query_assert(q,r)

  def test_id_loser_4(self):
    q= '{"guid":null, "id":"/guid/9202a8c04000641f800000000c52a4bd", "mid":null}'
    r= '{"guid": "#9202a8c04000641f800000000c52a4bd", "id": "/guid/9202a8c04000641f800000000c52a4bd","mid": "/m/065595y"}'
    self.query_assert(q,r)

  def test_id_loser_5(self):
    q= '{"guid":null, "id":"/guid/9202a8c04000641f800000000c52a4bd", "mid":[]}'
    r= '{"guid": "#9202a8c04000641f800000000c52a4bd", "id": "/guid/9202a8c04000641f800000000c52a4bd","mid": ["/m/065595y"]}'
    self.query_assert(q,r)


  def test_guid_loser_1(self):
    q= '{"id":null, "guid":"#9202a8c04000641f800000000c52a4bd", "mid":null}'
    r= '{"guid": "#9202a8c04000641f800000000c52a4bd", "id": "/m/065595y","mid": "/m/065595y"}'
    self.query_assert(q,r)

  def test_guid_loser_2(self):
    q= '{"id":null, "guid":"#9202a8c04000641f800000000c52a4bd", "mid":[]}'
    r= '{"guid": "#9202a8c04000641f800000000c52a4bd", "id": "/m/065595y","mid": ["/m/065595y"]}'
    self.query_assert(q,r)

  def test_mid_loser_1(self):
    q= '{"id":null, "guid":null, "mid":"/m/065595y"}'
    r= '{"guid": "#9202a8c04000641f80000000060b2879", "id": "/en/los_angeles","mid": "/m/065595y"}'
    self.query_assert(q,r)

  def test_mid_loser_2(self):
    q= '{"id":null, "guid":null, "mid":"/m/065595y", "a:mid":null}'
    r= '{"guid": "#9202a8c04000641f80000000060b2879", "id": "/en/los_angeles","mid": "/m/065595y", "a:mid":"/m/030qb3t"}'
    self.query_assert(q,r)

  def test_mid_loser_3(self):
    q= '{"id":null, "guid":null, "mid":"/m/065595y", "a:mid":null, "b:mid":[]}'
    r= '{"guid": "#9202a8c04000641f80000000060b2879", "id": "/en/los_angeles","mid": "/m/065595y", "a:mid":"/m/030qb3t", "b:mid":["/m/030qb3t", "/m/04lr0", "/m/02h843w", "/m/0256dc7", "/m/0256ckz", "/m/0kjjkr", "/m/02nt5sr", "/m/0256b1m", "/m/065595y", "/m/07hcxp7", "/m/0dj42mf", "/m/0jzc22d"]}'
    self.query_assert(q,r)

  # too many ids
  def test_tmi1(self):
    q = '{"guid":"#9202a8c04000641f8000000000092a01", "id":"/en/sting"}'
    self.query_assert(q, None, exc_response=(
        error.MQLParseError,
        "Can't specify an id more than once in a single clause"
    ))

  def test_tmi2(self):
    q = '{"id":"/en/sting", "guid":"#9202a8c04000641f8000000000092a01", "mid":"/m/0lbj1"}'
    self.query_assert(q, None, exc_response=(
        error.MQLParseError,
        "Can't specify an id more than once in a single clause"
    ))

  def test_tmi3(self):
    q = '{"mid":"/m/0lbj1", "id":"/en/sting"}'
    self.query_assert(q, None, exc_response=(
        error.MQLParseError,
        "Can't specify an id more than once in a single clause"
    ))
  def test_tmi4(self):
    q = '{"guid":"#9202a8c04000641f8000000000092a01", "id":"/en/sting"}'
    self.query_assert(q, None, exc_response=(
        error.MQLParseError,
        "Can't specify an id more than once in a single clause"
    ))

  def test_tmi5(self):
    q = '{"guid":"#9202a8c04000641f8000000000092a01", "mid":"/m/0lbj1"}'
    self.query_assert(q, None, exc_response=(
        error.MQLParseError,
        "Can't specify an id more than once in a single clause"
    ))

  def test_root_mid(self):
    q= '{"id":null, "guid":null, "mid":"/m/0", "a:mid":null, "b:mid":[]}'
    r = '{"a:mid":"/m/0","b:mid": ["/m/0"],"guid": "#9202a8c04000641f8000000000000000","id": "/user/root","mid": "/m/0"}'
    self.query_assert(q, r)

  def test_bad_id_1(self):
    """empty name, not cool"""
    q= '{"guid":null, "id":"/m/", "a:mid":null, "b:mid":[]}'
    self.query_assert(q, None, exc_response=(
        error.MQLTypeError,
        "/m/ is a JSON string, but the expected type is /type/id"
    ))

  def test_bad_id_2(self):
    """invalid mid version """
    q= '{"guid":null, "id":"/m/9332j", "a:mid":null, "b:mid":[]}'
    self.query_assert(q,"null")

  def test_bad_id_3(self):
    """mids can't  be 9 characters"""
    q= '{"guid":null, "id":"/m/0bcdfgh12", "a:mid":null, "b:mid":[]}'
    self.query_assert(q, "null")

  def test_bad_id_4(self):
    q= '{"guid":null, "id":"/m/whatever", "a:mid":null, "b:mid":[]}'
    self.query_assert(q, "null")

  def test_bad_id_5(self):
    q= '{"guid":null, "id":"im_not_convinced_we_even_need_mids", "a:mid":null, "b:mid":[]}'
    self.query_assert(q, None, exc_response=(
        error.MQLTypeError,
        "im_not_convinced_we_even_need_mids is a JSON string, but the expected type is /type/id"
    ))

  # BUG:MQL-688
  def SKIPtest_wiki_de_id(self):
    q=json.dumps(
       {
        "name":"Helga Daub",
        "id":None
        }
      )
    r=json.dumps(
        {
        "name":"Helga Daub",
        "id":"/m/0ccrnl3"
        }
      )
    self.query_assert(q, r)

  def test_mid_namespace(self):
    q= '{"guid":null, "id":"/m", "a:mid":null, "b:mid":[]}'
    r= '{"a:mid": "/m/0bnqs_5","b:mid": [ "/m/0bnqs_5"],"guid": "#9202a8c04000641f80000000154b63e5","id":"/m" }'
    self.query_assert(q, r)

  def test_authority_bug(self):
    q=json.dumps(
[{
  "!/base/sameas/web_id/authority": [{
  "/type/namespace/keys": [{
    "/type/key/namespace": "/m/07s3m4g",
    "value": None
  }],
  "id": None
  }],
  "type": "/base/sameas/api_provider",
  "id":  None,
  "name": None
}]
      )
    r=json.dumps(
[
  {
    "!/base/sameas/web_id/authority": [
      {
        "/type/namespace/keys": [
          {
            "/type/key/namespace": "/m/07s3m4g",
            "value": "70125581"
          }
        ],
        "id": "/authority/netflix/movie"
      }
    ],
    "id": "/en/netflix",
    "name": "Netflix",
    "type": "/base/sameas/api_provider"
  },
  {
    "!/base/sameas/web_id/authority": [
      {
        "/type/namespace/keys": [
          {
            "/type/key/namespace": "/m/07s3m4g",
            "value": "tt1179904"
          }
        ],
        "id": "/authority/imdb/title"
      }
    ],
    "id": "/en/internet_movie_database",
    "name": "Internet Movie Database",
    "type": "/base/sameas/api_provider"
  },
  {
    "!/base/sameas/web_id/authority": [
      {
        "/type/namespace/keys": [
          {
            "/type/key/namespace": "/m/07s3m4g",
            "value": "24366494"
          }
        ],
        "id": "/wikipedia/en_id"
      },
      {
        "/type/namespace/keys": [
          {
            "/type/key/namespace": "/m/07s3m4g",
            "value": "Paranormal_Activity_$0028film$0029"
          },
          {
            "/type/key/namespace": "/m/07s3m4g",
            "value": "Paranormal_activity_$00282007_film$0029"
          },
          {
            "/type/key/namespace": "/m/07s3m4g",
            "value": "Paranormal_Activity_$00282009_film$0029"
          },
          {
            "/type/key/namespace": "/m/07s3m4g",
            "value": "Paranormal_activity_movie"
          },
          {
            "/type/key/namespace": "/m/07s3m4g",
            "value": "Paranormal_activity_$0028film$0029"
          },
          {
            "/type/key/namespace": "/m/07s3m4g",
            "value": "Paranormal_Activity_2"
          },
          {
            "/type/key/namespace": "/m/07s3m4g",
            "value": "Paranormal_activity_film"
          },
          {
            "/type/key/namespace": "/m/07s3m4g",
            "value": "Paranormal_Activity"
          }
        ],
        "id": "/wikipedia/en"
      }
    ],
    "id": "/en/wikipedia",
    "name": "Wikipedia",
    "type": "/base/sameas/api_provider"
  }
  ]
      )
    self.query_assert(q, r)

  def test_authority_bug_invert(self):
    q= json.dumps(
{
  "id": "/m/07s3m4g",
  "key": [{
  "value":None,
  "namespace": {
    "/base/sameas/web_id/authority": {
      "type": "/base/sameas/api_provider",
      "id":   None,
      "name": None
    }
  }
  }]
}
      )
    r=json.dumps(
{
  "id": "/m/07s3m4g",
  "key": [
    {
      "namespace": {
        "/base/sameas/web_id/authority": {
          "id": "/en/wikipedia",
          "name": "Wikipedia",
          "type": "/base/sameas/api_provider"
        }
      },
      "value": "24366494"
    },
    {
      "namespace": {
        "/base/sameas/web_id/authority": {
          "id": "/en/wikipedia",
          "name": "Wikipedia",
          "type": "/base/sameas/api_provider"
        }
      },
      "value": "Paranormal_Activity_$0028film$0029"
    },
    {
      "namespace": {
        "/base/sameas/web_id/authority": {
          "id": "/en/wikipedia",
          "name": "Wikipedia",
          "type": "/base/sameas/api_provider"
        }
      },
      "value": "Paranormal_activity_$00282007_film$0029"
    },
    {
      "namespace": {
        "/base/sameas/web_id/authority": {
          "id": "/en/wikipedia",
          "name": "Wikipedia",
          "type": "/base/sameas/api_provider"
        }
      },
      "value": "Paranormal_Activity_$00282009_film$0029"
    },
    {
      "namespace": {
        "/base/sameas/web_id/authority": {
          "id": "/en/wikipedia",
          "name": "Wikipedia",
          "type": "/base/sameas/api_provider"
        }
      },
      "value": "Paranormal_activity_movie"
    },
    {
      "namespace": {
        "/base/sameas/web_id/authority": {
          "id": "/en/wikipedia",
          "name": "Wikipedia",
          "type": "/base/sameas/api_provider"
        }
      },
      "value": "Paranormal_activity_$0028film$0029"
    },
    {
      "namespace": {
        "/base/sameas/web_id/authority": {
          "id": "/en/wikipedia",
          "name": "Wikipedia",
          "type": "/base/sameas/api_provider"
        }
      },
      "value": "Paranormal_Activity_2"
    },
    {
      "namespace": {
        "/base/sameas/web_id/authority": {
          "id": "/en/wikipedia",
          "name": "Wikipedia",
          "type": "/base/sameas/api_provider"
        }
      },
      "value": "Paranormal_activity_film"
    },
    {
      "namespace": {
        "/base/sameas/web_id/authority": {
          "id": "/en/internet_movie_database",
          "name": "Internet Movie Database",
          "type": "/base/sameas/api_provider"
        }
      },
      "value": "tt1179904"
    },
    {
      "namespace": {
        "/base/sameas/web_id/authority": {
          "id": "/en/wikipedia",
          "name": "Wikipedia",
          "type": "/base/sameas/api_provider"
        }
      },
      "value": "Paranormal_Activity"
    },
    {
      "namespace": {
        "/base/sameas/web_id/authority": {
          "id": "/en/netflix",
          "name": "Netflix",
          "type": "/base/sameas/api_provider"
        }
      },
      "value": "70125581"
    }
  ]
  }
      )
    self.query_assert(q, r)

  def SKIPtest_write(self):
    # TODO(bneutra): turn this one on when we have mocked write support
    return 0
    q= json.dumps(
      {"query":
      {
      "mid": "/m/0bsr2jk",
      "/biology/organism_classification/higher_classification": {
        "connect": "update",
        "id": "/en/tulip"
      }
      }
      }
      )
    r= json.dumps(
      {"result":
      {
      "/biology/organism_classification/higher_classification": {
          "connect": "present",
          "id":      "/en/tulip"
      },
      "mid": "/m/0bsr2jk"
      }
      }
      )
    u = 'mwmids' + self.store.dynstr
    self.create_user(u)
    self.login('/user/' + u)
    self.do_query(
          service='mqlwrite',
          q=q,
          r=r
      )

if __name__ == '__main__':
  mql_fixture.main()
