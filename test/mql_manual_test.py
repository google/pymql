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
"""Read examples from the MQL Manual at wiki.freebase.com."""

__author__ = 'bneutra@google.com (Brendan Neutra)'


import google3
from pymql.mql import error
from pymql.test import mql_fixture

class MQLTest(mql_fixture.MQLTest):
  """MQL Manual wiki examples."""

  def setUp(self):
    self.SetMockPath('data/mql_manual.yaml')
    super(MQLTest, self).setUp()
    self.env = {'as_of_time': '2011-10-24'}

  def testMqldocExample1(self):
    """mqldoc_example_1."""

    query = """
    {
      "type": "/film/director",
      "name": "Sofia Coppola",
      "film": []
    }
    """
    exp_response = """
    {
      "type": "/film/director",
      "name": "Sofia Coppola",
      "film": [
        "Lick the Star",
        "Lost in Translation",
        "Marie Antoinette",
        "The Virgin Suicides",
        "Somewhere"
      ]
    }
    """
    self.env['query_timeout_tu'] = 10
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample2(self):
    """mqldoc_example_2."""

    query = """
    {
      "type": "/film/director",
      "name": "Sofia Coppola",
      "id": null
    }
    """
    exp_response = """
    {
      "type": "/film/director",
      "name": "Sofia Coppola",
      "id": "/en/sofia_coppola"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample3(self):
    """mqldoc_example_3."""

    query = """
    {
      "type": "/film/director",
      "name": null,
      "id": "/m/01_f_5"
    }
    """
    exp_response = """
    {
      "type": "/film/director",
      "name": "Sofia Coppola",
      "id": "/m/01_f_5"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample4(self):
    """mqldoc_example_4."""

    query = """
    {
      "mid": null,
      "type": "/film/director",
      "name": null,
      "id": "/en/sofia_coppola"
    }
    """
    exp_response = """
    {
      "mid": "/m/01_f_5",
      "type": "/film/director",
      "name": "Sofia Coppola",
      "id": "/en/sofia_coppola"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample5(self):
    """mqldoc_example_5."""

    query = """
    {
      "type": null,
      "id": "/en/sofia_coppola",
      "name": null
    }
    """
    exc_response = (
        error.MQLTooManyValuesForUniqueQuery,
        "Unique query may have at most one result. Got 25"
    )
    self.DoQuery(query, exc_response=exc_response)

  def testMqldocExample6(self):
    """mqldoc_example_6."""

    query = """
    {
      "type": [],
      "id": "/en/sofia_coppola",
      "name": null
    }
    """
    exp_response = """
    {
  "type": [
    "/common/topic",
    "/people/person",
    "/film/actor",
    "/film/director",
    "/film/writer",
    "/film/producer",
    "/film/film_story_contributor",
    "/award/award_nominee",
    "/award/award_winner",
    "/user/narphorium/people/nndb_person",
    "/user/narphorium/people/topic",
    "/influence/influence_node",
    "/celebrities/celebrity",
    "/base/popstra/celebrity",
    "/base/popstra/topic",
    "/base/popstra/sww_base",
    "/people/family_member",
    "/base/markrobertdaveyphotographer/topic",
    "/tv/tv_actor",
    "/film/person_or_entity_appearing_in_film",
    "/book/author",
    "/tv/tv_producer",
    "/tv/tv_writer",
    "/film/cinematographer",
    "/film/film_costumer_designer"
  ],
  "id": "/en/sofia_coppola",
  "name": "Sofia Coppola"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample7(self):
    """mqldoc_example_7."""

    query = """
    [
      {
        "type": "/film/director",
        "name": "Sofia Coppola",
        "film": []
      }
    ]
    """
    exp_response = """
    [
      {
        "type": "/film/director",
        "name": "Sofia Coppola",
        "film": [
          "Lick the Star",
          "Lost in Translation",
          "Marie Antoinette",
          "The Virgin Suicides",
          "Somewhere"
        ]
      }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample8(self):
    """mqldoc_example_8."""

    query = """
    {
      "type": "/film/director",
      "name": "Sofia Coppola",
      "film": [
        {
          "name": null,
          "initial_release_date": null
        }
      ]
    }
    """
    exp_response = """
    {
      "type": "/film/director",
      "name": "Sofia Coppola",
      "film": [
        {
          "initial_release_date": "1998-10",
          "name": "Lick the Star"
        },
        {
          "initial_release_date": "2003-09-12",
          "name": "Lost in Translation"
        },
        {
          "initial_release_date": "2006-05-24",
          "name": "Marie Antoinette"
        },
        {
          "initial_release_date": "1999-05-19",
          "name": "The Virgin Suicides"
        },
        {
          "initial_release_date": "2010-09-03",
          "name": "Somewhere"
        }
      ]
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample9(self):
    """mqldoc_example_9."""

    query = """
    [
      {
        "limit": 3,
        "directed_by": [
          {
            "name": null,
            "film": []
          }
        ],
        "type": "/film/film",
        "name": null,
        "initial_release_date": "2003-09-12"
      }
    ]
    """
    exp_response = """
    [
  {
    "type": "/film/film",
    "initial_release_date": "2003-09-12",
    "name": "Lost in Translation",
    "directed_by": [
      {
        "name": "Sofia Coppola",
        "film": [
          "Lick the Star",
          "Lost in Translation",
          "Marie Antoinette",
          "The Virgin Suicides",
          "Somewhere"
        ]
      }
    ]
  },
  {
    "type": "/film/film",
    "initial_release_date": "2003-09-12",
    "name": "Once Upon a Time in Mexico",
    "directed_by": [
      {
        "name": "Robert Rodriguez",
        "film": [
          "Desperado",
          "El Mariachi",
          "From Dusk Till Dawn",
          "Grindhouse",
          "Madman",
          "Once Upon a Time in Mexico",
          "Roadracers",
          "Sin City",
          "Sin City 2",
          "The Adventures of Sharkboy and Lavagirl",
          "The Faculty",
          "Spy Kids",
          "Four Rooms",
          "Planet Terror",
          "Machete",
          "Spy Kids 3-D: Game Over",
          "Spy Kids 2: Island of Lost Dreams",
          "Bedhead",
          "Mexico Trilogy",
          "Shorts",
          "Spy Kids 4: All the Time in the World"
        ]
      }
    ]
  },
  {
    "type": "/film/film",
    "initial_release_date": "2003-09-12",
    "name": "Imagining Argentina",
    "directed_by": [
      {
        "name": "Christopher Hampton",
        "film": [
          "Imagining Argentina",
          "Carrington",
          "The Secret Agent "
        ]
      }
    ]
  }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample10(self):
    """mqldoc_example_10."""

    query = """
    {
      "type": "/film/director",
      "name": "Sofia Coppola",
      "film": [
        {
          "name": null,
          "initial_release_date": null
        }
      ]
    }
    """
    exp_response = """
    {
      "type": "/film/director",
      "name": "Sofia Coppola",
      "film": [
        {
          "initial_release_date": "1998-10",
          "name": "Lick the Star"
        },
        {
          "initial_release_date": "2003-09-12",
          "name": "Lost in Translation"
        },
        {
          "initial_release_date": "2006-05-24",
          "name": "Marie Antoinette"
        },
        {
          "initial_release_date": "1999-05-19",
          "name": "The Virgin Suicides"
        },
        {
          "initial_release_date": "2010-09-03",
          "name": "Somewhere"
        }
      ]
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample11(self):
    """mqldoc_example_11."""

    query = """
    {
      "type": "/film/film",
      "name": "Lost in Translation",
      "directed_by": [
        {
          "name": null
        }
      ]
    }
    """
    exp_response = """
    {
      "type": "/film/film",
      "name": "Lost in Translation",
      "directed_by": [
        {
          "name": "Sofia Coppola"
        }
      ]
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample12(self):
    """mqldoc_example_12."""

    query = """
    {
      "type": [
        {}
      ],
      "id": "/en/sofia_coppola",
      "name": null
    }
    """
    partial_response = [
  {
    "type": [
      "/type/type",
      "/freebase/type_profile"
    ],
    "id": "/common/topic",
    "name": "Topic"
  },
  {
    "type": [
      "/type/type",
      "/freebase/type_profile"
    ],
    "id": "/people/person",
    "name": "Person"
  }
    ]

    self.DoQuery(query)
    assert partial_response == self.mql_result.result['type'][:2]


  def testMqldocExample13(self):
    """mqldoc_example_13."""

    query = """
    {
      "type": "/film/film",
      "id": "/en/lost_in_translation",
      "directed_by": [
        {}
      ]
    }
    """
    exp_response = """
    {
  "type": "/film/film",
  "id": "/en/lost_in_translation",
  "directed_by": [
    {
      "type": [
        "/common/topic",
        "/people/person",
        "/film/actor",
        "/film/director",
        "/film/writer",
        "/film/producer",
        "/film/film_story_contributor",
        "/award/award_nominee",
        "/award/award_winner",
        "/user/narphorium/people/nndb_person",
        "/user/narphorium/people/topic",
        "/influence/influence_node",
        "/celebrities/celebrity",
        "/base/popstra/celebrity",
        "/base/popstra/topic",
        "/base/popstra/sww_base",
        "/people/family_member",
        "/base/markrobertdaveyphotographer/topic",
        "/tv/tv_actor",
        "/film/person_or_entity_appearing_in_film",
        "/book/author",
        "/tv/tv_producer",
        "/tv/tv_writer",
        "/film/cinematographer",
        "/film/film_costumer_designer"
      ],
      "id": "/en/sofia_coppola",
      "name": "Sofia Coppola"
    }
  ]
    }

    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample14(self):
    """mqldoc_example_14."""

    query = """
    {
      "id": "/en/united_states",
      "name": null
    }
    """
    exp_response = """
    {
      "id": "/en/united_states",
      "name": "United States of America"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample15(self):
    """mqldoc_example_15."""

    query = """
    {
      "id": "/en/united_states",
      "name": {}
    }
    """
    exp_response = """
    {
      "id": "/en/united_states",
      "name": {
        "lang": "/lang/en",
        "type": "/type/text",
        "value": "United States of America"
      }
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample16(self):
    """mqldoc_example_16."""

    query = """
    {
      "id": "/en/united_states",
      "name": []
    }
    """
    exp_response = """
    {
      "id": "/en/united_states",
      "name": [
        "United States of America"
      ]
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample17(self):
    """mqldoc_example_17."""

    query = """
    {
      "id": "/en/united_states",
      "name": [
        {}
      ]
    }
    """
    exp_response = """
    {
  "id": "/en/united_states",
  "name": [
    {
      "lang": "/lang/de",
      "type": "/type/text",
      "value": "Vereinigte Staaten"
    },
    {
      "lang": "/lang/it",
      "type": "/type/text",
      "value": "Stati Uniti d'America"
    },
    {
      "lang": "/lang/ja",
      "type": "/type/text",
      "value": "\u30a2\u30e1\u30ea\u30ab\u5408\u8846\u56fd"
    },
    {
      "lang": "/lang/sl",
      "type": "/type/text",
      "value": "Zdru\u017eene dr\u017eave Amerike"
    },
    {
      "lang": "/lang/bg",
      "type": "/type/text",
      "value": "\u0421\u044a\u0435\u0434\u0438\u043d\u0435\u043d\u0438 \u0430\u043c\u0435\u0440\u0438\u043a\u0430\u043d\u0441\u043a\u0438 \u0449\u0430\u0442\u0438"
    },
    {
      "lang": "/lang/sr",
      "type": "/type/text",
      "value": "\u0421\u0458\u0435\u0434\u0438\u045a\u0435\u043d\u0435 \u0410\u043c\u0435\u0440\u0438\u0447\u043a\u0435 \u0414\u0440\u0436\u0430\u0432\u0435"
    },
    {
      "lang": "/lang/lt",
      "type": "/type/text",
      "value": "JAV"
    },
    {
      "lang": "/lang/ko",
      "type": "/type/text",
      "value": "\ubbf8\uad6d"
    },
    {
      "lang": "/lang/ro",
      "type": "/type/text",
      "value": "Statele Unite ale Americii"
    },
    {
      "lang": "/lang/id",
      "type": "/type/text",
      "value": "Amerika Serikat"
    },
    {
      "lang": "/lang/tr",
      "type": "/type/text",
      "value": "Amerika Birle\u015fik Devletleri"
    },
    {
      "lang": "/lang/sk",
      "type": "/type/text",
      "value": "Spojen\u00e9 \u0161t\u00e1ty"
    },
    {
      "lang": "/lang/ca",
      "type": "/type/text",
      "value": "Estats Units d'Am\u00e8rica"
    },
    {
      "lang": "/lang/hu",
      "type": "/type/text",
      "value": "Amerikai Egyes\u00fclt \u00c1llamok"
    },
    {
      "lang": "/lang/da",
      "type": "/type/text",
      "value": "USA"
    },
    {
      "lang": "/lang/cs",
      "type": "/type/text",
      "value": "Spojen\u00e9 st\u00e1ty americk\u00e9"
    },
    {
      "lang": "/lang/he",
      "type": "/type/text",
      "value": "\u05d0\u05e8\u05e6\u05d5\u05ea \u05d4\u05d1\u05e8\u05d9\u05ea"
    },
    {
      "lang": "/lang/eo",
      "type": "/type/text",
      "value": "Usono"
    },
    {
      "lang": "/lang/no",
      "type": "/type/text",
      "value": "Amerikas forente stater"
    },
    {
      "lang": "/lang/fi",
      "type": "/type/text",
      "value": "Yhdysvallat"
    },
    {
      "lang": "/lang/ru",
      "type": "/type/text",
      "value": "\u0421\u043e\u0435\u0434\u0438\u043d\u0451\u043d\u043d\u044b\u0435 \u0428\u0442\u0430\u0442\u044b \u0410\u043c\u0435\u0440\u0438\u043a\u0438"
    },
    {
      "lang": "/lang/sv",
      "type": "/type/text",
      "value": "USA"
    },
    {
      "lang": "/lang/pl",
      "type": "/type/text",
      "value": "Stany Zjednoczone"
    },
    {
      "lang": "/lang/nl",
      "type": "/type/text",
      "value": "Verenigde Staten"
    },
    {
      "lang": "/lang/th",
      "type": "/type/text",
      "value": "\u0e2a\u0e2b\u0e23\u0e31\u0e10\u0e2d\u0e40\u0e21\u0e23\u0e34\u0e01\u0e32"
    },
    {
      "lang": "/lang/uk",
      "type": "/type/text",
      "value": "\u0421\u043f\u043e\u043b\u0443\u0447\u0435\u043d\u0456 \u0428\u0442\u0430\u0442\u0438 \u0410\u043c\u0435\u0440\u0438\u043a\u0438"
    },
    {
      "lang": "/lang/en",
      "type": "/type/text",
      "value": "United States of America"
    },
    {
      "lang": "/lang/zh",
      "type": "/type/text",
      "value": "\u7f8e\u56fd"
    },
    {
      "lang": "/lang/pt",
      "type": "/type/text",
      "value": "Estados Unidos"
    },
    {
      "lang": "/lang/es",
      "type": "/type/text",
      "value": "Estados Unidos"
    },
    {
      "lang": "/lang/fr",
      "type": "/type/text",
      "value": "\u00c9tats-Unis"
    }
  ]
}
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample18(self):
    """mqldoc_example_18."""

    query = """
    {
      "id": "/en/united_states",
      "name": {
        "lang": "/lang/fr",
        "value": null
      }
    }
    """
    exp_response = """
    {
      "id": "/en/united_states",
      "name": {
        "lang": "/lang/fr",
        "value": "\u00c9tats-Unis"
      }
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample19(self):
    """mqldoc_example_19."""

    query = """
    {
      "id": "/m/0gxbwm_",
      "type": "/film/film",
      "name": "Halloween: Bonus Material",
      "directed_by": null
    }
    """
    exp_response = """
    {
      "id": "/m/0gxbwm_",
      "type": "/film/film",
      "name": "Halloween: Bonus Material",
      "directed_by": "Rob Zombie"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample20(self):
    """mqldoc_example_20."""

    query = """
    {
      "type": [],
      "id": "/en/sofia_coppola",
      "name": null
    }
    """
    exp_response = """
    {
  "type": [
    "/common/topic",
    "/people/person",
    "/film/actor",
    "/film/director",
    "/film/writer",
    "/film/producer",
    "/film/film_story_contributor",
    "/award/award_nominee",
    "/award/award_winner",
    "/user/narphorium/people/nndb_person",
    "/user/narphorium/people/topic",
    "/influence/influence_node",
    "/celebrities/celebrity",
    "/base/popstra/celebrity",
    "/base/popstra/topic",
    "/base/popstra/sww_base",
    "/people/family_member",
    "/base/markrobertdaveyphotographer/topic",
    "/tv/tv_actor",
    "/film/person_or_entity_appearing_in_film",
    "/book/author",
    "/tv/tv_producer",
    "/tv/tv_writer",
    "/film/cinematographer",
    "/film/film_costumer_designer"
  ],
  "id": "/en/sofia_coppola",
  "name": "Sofia Coppola"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample21(self):
    """mqldoc_example_21."""

    query = """
    {
      "/people/person/date_of_birth": null,
      "type": "/film/director",
      "name": "Sofia Coppola",
      "film": []
    }
    """
    exp_response = """
    {
      "/people/person/date_of_birth": "1971-05-14",
      "type": "/film/director",
      "name": "Sofia Coppola",
      "film": [
        "Lick the Star",
        "Lost in Translation",
        "Marie Antoinette",
        "The Virgin Suicides",
        "Somewhere"
      ]
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample22(self):
    """mqldoc_example_22."""

    query = """
    {
      "/people/person/date_of_birth": null,
      "/type/object/type": "/film/director",
      "/film/director/film": [],
      "/type/object/name": "Sofia Coppola"
    }
    """
    exp_response = """
    {
      "/people/person/date_of_birth": "1971-05-14",
      "/type/object/type": "/film/director",
      "/film/director/film": [
        "Lick the Star",
        "Lost in Translation",
        "Marie Antoinette",
        "The Virgin Suicides",
        "Somewhere"
      ],
      "/type/object/name": "Sofia Coppola"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample23(self):
    """mqldoc_example_23."""

    query = """
    [
      {
        "b:film": "The Towering Inferno",
        "a:film": "King Kong",
        "type": "/film/director",
        "name": null
      }
    ]
    """
    exp_response = """
    [
      {
        "b:film": "The Towering Inferno",
        "a:film": "King Kong",
        "type": "/film/director",
        "name": "John Guillermin"
      }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample24(self):
    """mqldoc_example_24."""

    query = """
    {
      "constraint:type": "/film/director",
      "query:type": [],
      "name": "John Guillermin"
    }
    """
    exp_response = """
    {
      "constraint:type": "/film/director",
      "query:type": [
        "/common/topic",
        "/people/person",
        "/film/director",
        "/film/producer",
        "/user/narphorium/people/nndb_person",
        "/user/narphorium/people/topic",
        "/award/award_nominee",
        "/tv/tv_director",
        "/film/film_story_contributor"
      ],
      "name": "John Guillermin"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample25(self):
    """mqldoc_example_25."""

    query = """
    [
      {
        "limit" : 1,
        "b:type": "/music/artist",
        "type": "/film/director",
        "name": null,
        "film": []
      }
    ]
    """
    exp_response = """
    [
    {
    "b:type": "/music/artist",
    "type": "/film/director",
    "name": "Frank Zappa",
    "film": [
      "200 Motels",
      "Baby Snakes",
      "Uncle Meat",
      "Video from Hell",
      "The Dub Room Special",
      "Does Humor Belong in Music?",
      "The Amazing Mr. Bickford",
      "The True Story of Frank Zappa's 200 Motels"
    ]
    }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample26(self):
    """mqldoc_example_26."""

    query = """
    [
      {
        "b:type": "/music/artist",
        "name": null,
        "film": [],
        "a:type": "/film/director"
      }
    ]
    """
    exc_response = (
        error.MQLTypeError,
        "Type /type/object does not have property film"
    )
    self.DoQuery(query, exc_response=exc_response)

  def testMqldocExample27(self):
    """mqldoc_example_27."""

    query = """
    {
      "*": null,
      "id": "/m/0209xj"
    }
    """
    partial_response = {
  "search": [],
  "attribution": "/user/metaweb",
  "name": "Lost in Translation",
  "creator": "/user/metaweb",
  "timestamp": "2006-10-22T15:14:08.0061Z",
  "permission": "/boot/all_permission",
  "mid": [
    "/m/0209xj"
  ]
    }
    self.DoQuery(query)
    self.assertDictContainsSubset(partial_response, self.mql_result.result)

  def testMqldocExample28(self):
    """mqldoc_example_28."""

    query = """
    {
      "*": null,
      "type": "/film/film",
      "id": "/m/0209xj"
    }
    """
    partial_response = {
  "personal_appearances": [],
  "rating": [
    "R (USA)"
  ],
  "featured_song": [],
  "distributors": [
    None
  ],
  "creator": "/user/metaweb",
  "costume_design_by": [
    "Nancy Steiner"
  ],
  "locations": []
    }

    self.DoQuery(query)
    self.assertDictContainsSubset(partial_response, self.mql_result.result)

  def testMqldocExample29(self):
    """mqldoc_example_29."""

    query = """
    {
      "*": null,
      "type": "/film/film",
      "id": "/m/0209xj",
      "directed_by": [
        {
          "name": null,
          "id": null
        }
      ]
    }
    """
    partial_response = {
  "personal_appearances": [],
  "rating": [
    "R (USA)"
  ],
  "featured_song": [],
  "distributors": [
    None
  ],
  "creator": "/user/metaweb",
  "costume_design_by": [
    "Nancy Steiner"
  ],
  "locations": []
    }

    self.DoQuery(query)
    self.assertDictContainsSubset(partial_response, self.mql_result.result)

  def testMqldocExample30(self):
    """mqldoc_example_30."""

    query = """
    {
      "type": "/film/director",
      "name": "Sofia Coppola",
      "film": [
        {
          "*": null
        }
      ]
    }
    """
    partial_response = {
        "apple_movietrailer_id": [],
        "attribution":   "/user/metaweb",
        "cinematography": [
          "Lance Acord"
        ],
        "costume_design_by": [],
        "country": [
          "United States of America"
        ],
        "creator":       "/user/metaweb",
        "directed_by": [
          "Sofia Coppola"
        ],
        "distributors": [
          None
        ],
        "dubbing_performances": [],
        "edited_by": [
          "Eric Zumbrunnen"
        ],
        "estimated_budget": []
    }

    self.DoQuery(query)
    self.assertDictContainsSubset(partial_response, self.mql_result.result['film'][0])

  def testMqldocExample31(self):
    """mqldoc_example_31."""

    query = """
    {
      "type": "/film/director",
      "name": "Sofia Coppola",
      "film": [
        {
          "*": null,
          "type": "/common/topic",
          "name": "Lost in Translation"
        }
      ]
    }
    """
    partial_response = {
      "alias": [
        "Traduction infidèle",
        "Lost in Translation - L'amore tradotto",
        "O Amor É um Lugar Estranho",
        "Encontros e Desencontros"
      ],
      "mid": [
        "/m/0209xj"
      ],
      "name":          "Lost in Translation",
      "notable_for":   None,
      "notable_types": [],
      "official_website": [
        "http://www.lost-in-translation.com/"
      ]
    }

    self.DoQuery(query)
    self.assertDictContainsSubset(partial_response, self.mql_result.result['film'][0])

  def testMqldocExample32(self):
    """mqldoc_example_32."""

    query = """
    {
      "type": "/location/country",
      "name": "Monaco",
      "!/people/person/nationality": []
    }
    """
    exp_response = """
    {
  "type": "/location/country",
  "name": "Monaco",
  "!/people/person/nationality": [
    "Olivier Beretta",
    "Louis Chiron",
    "Sebastien Gattuso",
    "Armand Forcherio",
    "Torben Joneleit",
    "Sophiane Baghdad",
    "Manuel Vallaurio",
    "Andr\u00e9 Testut",
    "Clivio Piccione",
    "St\u00e9phane Ortelli",
    "Rainier III, Prince of Monaco",
    "Fabien Barel",
    "Patrice Servelle",
    "Anthony Rinaldi",
    "Stefano Coletti",
    "Maurice Revelli",
    "Tom Hateley",
    "Olivier Jenot",
    "Jeremy Bottin",
    "Alexandra Coletti",
    "Charlotte Casiraghi",
    "St\u00e9phane Richelmi"
  ]
    }

    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample33(self):
    """mqldoc_example_33."""

    query = """
    [
      {
        "limit": 1,
        "nationality": "Monaco",
        "type": "/people/person",
        "name": null
      }
    ]
    """
    exp_response = """
    [
    {
    "nationality": "Monaco",
    "type": "/people/person",
    "name": "Rainier III, Prince of Monaco"
    }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample34(self):
    """mqldoc_example_34."""

    query = """
    {
      "*": null,
      "type": "/type/property",
      "id": "/film/director/film"
    }
    """
    partial_response = {
    "attribution":   "/user/metaweb",
    "creator":       "/user/metaweb",
    "enumeration":   None,
    "expected_type": "/film/film",
    "guid":          "#9202a8c04000641f80000000010c394d",
    "id":            "/film/director/film",
    }
    self.DoQuery(query)
    self.assertDictContainsSubset(partial_response, self.mql_result.result)

  def testMqldocExample35(self):
    """mqldoc_example_35."""

    query = """
    {
      "*": null,
      "type": "/type/property",
      "id": "/film/film/directed_by"
    }
    """
    exp_response = """
    {
  "search": [],
  "unique": false,
  "attribution": "/user/metaweb",
  "name": "Directed by",
  "links": [
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by",
    "/film/film/directed_by"
  ],
  "creator": "/user/metaweb",
  "delegated": null,
  "timestamp": "2006-11-30T18:29:26.0007Z",
  "permission": "/m/010h",
  "authorities": null,
  "mid": [
    "/m/0jsg59"
  ],
  "enumeration": null,
  "requires_permission": null,
  "reverse_property": "/film/director/film",
  "key": [
    "directed_by"
  ],
  "master_property": null,
  "expected_type": "/film/director",
  "guid": "#9202a8c04000641f80000000010c38a9",
  "type": "/type/property",
  "id": "/film/film/directed_by",
  "unit": null,
  "schema": "/film/film"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample36(self):
    """mqldoc_example_36."""

    query = """
    {
      "/type/property/enumeration": null,
      "id": "/type/lang/iso639"
    }
    """
    exp_response = """
    {
      "/type/property/enumeration": "/lang",
      "id": "/type/lang/iso639"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample37(self):
    """mqldoc_example_37."""

    query = """
    {
      "expected_type": null,
      "type": "/type/property",
      "id": "/film/film_cut/runtime",
      "unit": null
    }
    """
    exp_response = """
    {
      "expected_type": "/type/float",
      "type": "/type/property",
      "id": "/film/film_cut/runtime",
      "unit": "/en/minute"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample38(self):
    """mqldoc_example_38."""

    query = """
    {
      "limit": 1,
      "type": "/film/film",
      "name": "King Kong",
      "id": null
    }
    """
    exp_response = """
    {
      "type": "/film/film",
      "id": "/en/king_kong_1933",
      "name": "King Kong"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample40(self):
    """mqldoc_example_40."""

    query = """
    {
      "type": "/film/director",
      "name": "Sofia Coppola",
      "film": [
        {
          "limit": 3,
          "name": null
        }
      ]
    }
    """
    exp_response = """
    {
      "type": "/film/director",
      "name": "Sofia Coppola",
      "film": [
        {
          "name": "Lick the Star"
        },
        {
          "name": "Lost in Translation"
        },
        {
          "name": "Marie Antoinette"
        }
      ]
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample41(self):
    """mqldoc_example_41."""

    query = """
    {
      "return": "count",
      "type": "/film/film",
      "directed_by": "Sofia Coppola"
    }
    """
    self.DoQuery(query)
    assert self.mql_result.result == 5

  def testMqldocExample42(self):
    """mqldoc_example_42."""

    query = """
    {
      "return": "count",
      "type": "/film/film",
      "name": "Foobar",
      "directed_by": "Sofia Coppola"
    }
    """
    self.DoQuery(query)
    assert self.mql_result.result == 0

  def testMqldocExample43(self):
    """mqldoc_example_43."""

    query = """
    {
      "type": "/film/director",
      "name": "Sofia Coppola",
      "film": {
        "return": "count",
        "name": "Foobar"
      }
    }
    """
    self.DoQuery(query)
    assert self.mql_result.result == None

  def testMqldocExample44(self):
    """mqldoc_example_44."""

    query = """
    {
      "return": "estimate-count",
      "type": "/music/artist"
    }
    """
    self.DoQuery(query)
    assert self.mql_result.result == 486562

  def testMqldocExample45(self):
    """mqldoc_example_45."""

    query = """
    [
      {
        "limit": 3,
        "count": null,
        "type": "/film/film",
        "initial_release_date": "1970",
        "name": null
      }
    ]
    """
    exp_response = """
    [
    {
      "count":         3,
      "initial_release_date": "1970",
      "name":          "Bombay Talkie",
      "type":          "/film/film"
    },
    {
      "count":         3,
      "initial_release_date": "1970",
      "name":          "Brancaleone alle Crociate",
      "type":          "/film/film"
    },
    {
      "count":         3,
      "initial_release_date": "1970",
      "name":          "Cherry, Harry &amp; Raquel!",
      "type":          "/film/film"
    }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

if __name__ == '__main__':
  mql_fixture.main()
