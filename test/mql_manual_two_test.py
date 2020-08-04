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
    self.SetMockPath('data/mql_manual_two.yaml')
    super(MQLTest, self).setUp()
    self.env = {'as_of_time': '2011-10-24'}

  def testMqldocExample46(self):
    """mqldoc_example_46."""

    query = """
    [
      {
        "limit": 3,
        "type": "/film/film",
        "initial_release_date": "1970",
        "estimate-count": null,
        "name": null
      }
    ]
    """
    exp_response = """
    [
    {
      "estimate-count": 52058,
      "initial_release_date": "1970",
      "name":          "Bombay Talkie",
      "type":          "/film/film"
    },
    {
      "estimate-count": 52058,
      "initial_release_date": "1970",
      "name":          "Brancaleone alle Crociate",
      "type":          "/film/film"
    },
    {
      "estimate-count": 52058,
      "initial_release_date": "1970",
      "name":          "Cherry, Harry &amp; Raquel!",
      "type":          "/film/film"
    }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample47(self):
    """mqldoc_example_47."""

    query = """
    [
      {
        "sort": "name",
        "type": "/film/film",
        "name": null,
        "directed_by": "Sofia Coppola"
      }
    ]
    """
    exp_response = """
    [
      {
        "type": "/film/film",
        "name": "Lick the Star",
        "directed_by": "Sofia Coppola"
      },
      {
        "type": "/film/film",
        "name": "Lost in Translation",
        "directed_by": "Sofia Coppola"
      },
      {
        "type": "/film/film",
        "name": "Marie Antoinette",
        "directed_by": "Sofia Coppola"
      },
      {
        "type": "/film/film",
        "name": "Somewhere",
        "directed_by": "Sofia Coppola"
      },
      {
        "type": "/film/film",
        "name": "The Virgin Suicides",
        "directed_by": "Sofia Coppola"
      }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample48(self):
    """mqldoc_example_48."""

    query = """
    [
      {
        "sort": "initial_release_date",
        "type": "/film/film",
        "name": null,
        "initial_release_date": null,
        "directed_by": "Sofia Coppola"
      }
    ]
    """
    exp_response = """
    [
      {
        "type": "/film/film",
        "initial_release_date": "1998-10",
        "name": "Lick the Star",
        "directed_by": "Sofia Coppola"
      },
      {
        "type": "/film/film",
        "initial_release_date": "1999-05-19",
        "name": "The Virgin Suicides",
        "directed_by": "Sofia Coppola"
      },
      {
        "type": "/film/film",
        "initial_release_date": "2003-09-12",
        "name": "Lost in Translation",
        "directed_by": "Sofia Coppola"
      },
      {
        "type": "/film/film",
        "initial_release_date": "2006-05-24",
        "name": "Marie Antoinette",
        "directed_by": "Sofia Coppola"
      },
      {
        "type": "/film/film",
        "initial_release_date": "2010-09-03",
        "name": "Somewhere",
        "directed_by": "Sofia Coppola"
      }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample49(self):
    """mqldoc_example_49."""

    query = """
    [
      {
        "sort": "-initial_release_date",
        "type": "/film/film",
        "name": null,
        "initial_release_date": null,
        "directed_by": "Sofia Coppola"
      }
    ]
    """
    exp_response = """
    [
      {
        "type": "/film/film",
        "initial_release_date": "2010-09-03",
        "name": "Somewhere",
        "directed_by": "Sofia Coppola"
      },
      {
        "type": "/film/film",
        "initial_release_date": "2006-05-24",
        "name": "Marie Antoinette",
        "directed_by": "Sofia Coppola"
      },
      {
        "type": "/film/film",
        "initial_release_date": "2003-09-12",
        "name": "Lost in Translation",
        "directed_by": "Sofia Coppola"
      },
      {
        "type": "/film/film",
        "initial_release_date": "1999-05-19",
        "name": "The Virgin Suicides",
        "directed_by": "Sofia Coppola"
      },
      {
        "type": "/film/film",
        "initial_release_date": "1998-10",
        "name": "Lick the Star",
        "directed_by": "Sofia Coppola"
      }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample50(self):
    """mqldoc_example_50."""

    query = """
    {
      "type": "/film/director",
      "name": "Francis Ford Coppola",
      "film": [
        {
          "sort": "initial_release_date",
          "limit": 1,
          "name": null,
          "initial_release_date": null
        }
      ]
    }
    """
    exp_response = """
    {
      "type": "/film/director",
      "name": "Francis Ford Coppola",
      "film": [
        {
          "initial_release_date": "1960",
          "name": "Battle Beyond the Sun"
        }
      ]
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample51(self):
    """mqldoc_example_51."""

    query = """
    [
      {
        "type": "/music/album",
        "id": "/en/zenyatta_mondatta",
        "releases": [
          {
            "sort": [
              "format",
              "release_date"
            ],
            "release_date": null,
            "format": null
          }
        ]
      }
    ]
    """
    exp_response = """
    [
      {
        "type": "/music/album",
        "id": "/en/zenyatta_mondatta",
        "releases": [
          {
            "release_date": "1983-12-19",
            "format": "Compact Disc"
          },
          {
            "release_date": "1991",
            "format": "Compact Disc"
          },
          {
            "release_date": "2003",
            "format": "Compact Disc"
          },
          {
            "release_date": "1980-10",
            "format": "Gramophone record"
          },
          {
            "release_date": "1980-10",
            "format": "Gramophone record"
          },
          {
            "release_date": "1980-10-03",
            "format": "Gramophone record"
          },
          {
            "release_date": "2007-11-05",
            "format": "Gramophone record"
          },
          {
            "release_date": "1980-10",
            "format": null
          },
          {
            "release_date": "2003",
            "format": null
          }
        ]
      }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample52(self):
    """mqldoc_example_52."""

    query = """
    [
      {
        "limit": 3,
        "sort": "releases.release_date",
        "type": "/music/album",
        "name": null,
        "releases": [
          {
            "sort": "release_date",
            "release_date": null,
            "limit": 1,
            "format": null
          }
        ],
        "artist": {
          "id": "/en/van_halen"
        }
      }
    ]
    """
    exp_response = """
    [
    {
      "artist": {
        "id": "/en/van_halen"
      },
      "name": "Van Halen",
      "releases": [{
        "format":       "Gramophone record",
        "release_date": "1978-02-10"
      }],
      "type": "/music/album"
    },
    {
      "artist": {
        "id": "/en/van_halen"
      },
      "name": "Van Halen II",
      "releases": [{
        "format":       "Gramophone record",
        "release_date": "1979"
      }],
      "type": "/music/album"
    },
    {
      "artist": {
        "id": "/en/van_halen"
      },
      "name": "Women and Children First",
      "releases": [{
        "format":       "Gramophone record",
        "release_date": "1980"
      }],
      "type": "/music/album"
    }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample53(self):
    """mqldoc_example_53."""

    query = """
    [
      {
        "sort": [
          "character",
          "film.name",
          "-actor./people/person/date_of_birth"
        ],
        "character": null,
        "type": "/film/performance",
        "actor": {
          "/people/person/date_of_birth": null,
          "name": null
        },
        "film": {
          "name": null,
          "directed_by": "George Lucas"
        }
      }
    ]
    """
    partial_response = {
    "type": "/film/performance", 
    "character": "Ackmena", 
    "film": {
      "name": "The Star Wars Holiday Special", 
      "directed_by": "George Lucas"
    }, 
    "actor": {
      "/people/person/date_of_birth": "1922-05-13", 
      "name": "Beatrice Arthur"
    }
    }
    self.DoQuery(query)
    assert partial_response == self.mql_result.result[0]

  def testMqldocExample54(self):
    """mqldoc_example_54."""

    query = """
    [
      {
        "type": "/film/film",
        "id": "/en/blade_runner",
        "starring": [
          {
            "sort": "index",
            "index": null,
            "character": null,
            "actor": null
          }
        ]
      }
    ]
    """
    exp_response = """
    [
      {
        "type": "/film/film",
        "id": "/en/blade_runner",
        "starring": [
          {
            "index": 0,
            "character": "Rick Deckard",
            "actor": "Harrison Ford"
          },
          {
            "index": 1,
            "character": "Roy Batty",
            "actor": "Rutger Hauer"
          },
          {
            "index": 2,
            "character": "Rachael",
            "actor": "Sean Young"
          },
          {
            "index": 3,
            "character": "Pris",
            "actor": "Daryl Hannah"
          },
          {
            "index": 4,
            "character": "Zhora",
            "actor": "Joanna Cassidy"
          },
          {
            "index": 5,
            "character": "Leon Kowalski",
            "actor": "Brion James"
          },
          {
            "index": 6,
            "character": "Holden",
            "actor": "Morgan Paull"
          },
          {
            "index": 7,
            "character": "Eldon Tyrell",
            "actor": "Joe Turkel"
          },
          {
            "index": 8,
            "character": "J.F. Sebastian",
            "actor": "William Sanderson"
          },
          {
            "index": 9,
            "character": "Gaff",
            "actor": "Edward James Olmos"
          },
          {
            "index": 10,
            "character": "Hannibal Chew",
            "actor": "James Hong"
          },
          {
            "index": 11,
            "character": "Bryant",
            "actor": "M. Emmet Walsh"
          },
          {
            "index": null,
            "character": "Taffey Lewis",
            "actor": "Hy Pyke"
          },
          {
            "index": null,
            "character": "Bear",
            "actor": "Kevin Thompson"
          },
          {
            "index": null,
            "character": "Kaiser",
            "actor": "John Edward Allen"
          }
        ]
      }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample55(self):
    """mqldoc_example_55."""

    query = """
    [
      {
        "type": "/film/film",
        "id": "/en/blade_runner",
        "starring": [
          {
            "sort": "-index",
            "index": null,
            "character": null,
            "limit": 5,
            "actor": null
          }
        ]
      }
    ]
    """
    exp_response = """
    [
      {
        "type": "/film/film",
        "id": "/en/blade_runner",
        "starring": [
          {
            "index": null,
            "character": "Taffey Lewis",
            "actor": "Hy Pyke"
          },
          {
            "index": null,
            "character": "Bear",
            "actor": "Kevin Thompson"
          },
          {
            "index": null,
            "character": "Kaiser",
            "actor": "John Edward Allen"
          },
          {
            "index": 1,
            "character": "Bryant",
            "actor": "M. Emmet Walsh"
          },
          {
            "index": 0,
            "character": "Hannibal Chew",
            "actor": "James Hong"
          }
        ]
      }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample56(self):
    """mqldoc_example_56."""

    query = """
    [
      {
        "track": "Lola",
        "limit": 5,
        "album": [
          {
            "optional": "optional",
            "name": "Greatest Hits"
          }
        ],
        "type": "/music/artist",
        "name": null
      }
    ]
    """
    exp_response = """
    [
      {
        "album": [],
        "track": "Lola",
        "type": "/music/artist",
        "name": "Herman Brood"
      },
      {
        "album": [
          {
            "name": "Greatest Hits"
          },
          {
            "name": "Greatest Hits"
          },
          {
            "name": "Greatest Hits"
          }
        ],
        "track": "Lola",
        "type": "/music/artist",
        "name": "Robbie Williams"
      },
      {
        "album": [],
        "track": "Lola",
        "type": "/music/artist",
        "name": "Madness"
      },
      {
        "album": [],
        "track": "Lola",
        "type": "/music/artist",
        "name": "Marlene Dietrich"
      },
      {
        "album": [],
        "track": "Lola",
        "type": "/music/artist",
        "name": "The Raincoats"
      }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample57(self):
    """mqldoc_example_57."""

    query = """
    [
      {
        "limit": 6,
        "type": "/music/track",
        "name": "Lola",
        "releases": [
          {
            "optional": "optional",
            "name": "Greatest Hits",
            "format": [
              {
                "optional": "optional",
                "name": "Compact Disc"
              }
            ]
          }
        ]
      }
    ]
    """
    exp_response = """
    [{
    "type": "/music/track", 
    "name": "Lola", 
    "releases": []
  }, 
  {
    "type": "/music/track", 
    "name": "Lola", 
    "releases": []
  }, 
  {
    "type": "/music/track", 
    "name": "Lola", 
    "releases": []
  }, 
  {
    "type": "/music/track", 
    "name": "Lola", 
    "releases": []
  }, 
  {
    "type": "/music/track", 
    "name": "Lola", 
    "releases": []
  }, 
  {
    "type": "/music/track", 
    "name": "Lola", 
    "releases": [
      {
        "name": "Greatest Hits", 
        "format": []
      }, 
      {
        "name": "Greatest Hits", 
        "format": []
      }, 
      {
        "name": "Greatest Hits", 
        "format": [
          {
            "name": "Compact Disc"
          }
        ]
      }
    ]
    }]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample58(self):
    """mqldoc_example_58."""

    query = """
    [
      {
        "limit": 3,
        "type": "/film/director",
        "name": null,
        "/common/topic/alias": []
      }
    ]
    """
    exp_response = """
    [
  {
    "type": "/film/director", 
    "name": "Blake Edwards", 
    "/common/topic/alias": [
      "William Blake Crump"
    ]
  }, 
  {
    "type": "/film/director", 
    "name": "D. A. Pennebaker", 
    "/common/topic/alias": [
      "Donn Alan Pennebaker", 
      "D.A. Pennabaker", 
      "Don Alan Pennebaker", 
      "Penny"
    ]
  }, 
  {
    "type": "/film/director", 
    "name": "Chris Hegedus", 
    "/common/topic/alias": []
    }]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample59(self):
    """mqldoc_example_59."""

    query = """
    [
      {
        "limit": 3,
        "type": "/film/director",
        "name": null,
        "/common/topic/alias": [
          {
            "lang": "/lang/en",
            "value": null
          }
        ]
      }
    ]
    """
    exp_response = """
    [
    {
      "/common/topic/alias": [{
        "lang":  "/lang/en",
        "value": "William Blake Crump"
      }],
      "name": "Blake Edwards",
      "type": "/film/director"
    },
    {
      "/common/topic/alias": [
        {
          "lang":  "/lang/en",
          "value": "Donn Alan Pennebaker"
        },
        {
          "lang":  "/lang/en",
          "value": "D.A. Pennabaker"
        },
        {
          "lang":  "/lang/en",
          "value": "Don Alan Pennebaker"
        },
        {
          "lang":  "/lang/en",
          "value": "Penny"
        }
      ],
      "name": "D. A. Pennebaker",
      "type": "/film/director"
    },
    {
      "/common/topic/alias": [
        {
          "lang":  "/lang/en",
          "value": "The Wizard"
        },
        {
          "lang":  "/lang/en",
          "value": "Zachary Edward Snyder"
        }
      ],
      "name": "Zack Snyder",
      "type": "/film/director"
    }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample60(self):
    """mqldoc_example_60."""

    query = """
    [
      {
        "limit": 3,
        "type": "/film/director",
        "name": null,
        "/common/topic/alias": [
          {
            "lang": "/lang/en",
            "optional": true,
            "value": null
          }
        ]
      }
    ]
    """
    exp_response = """
    [
    {
      "/common/topic/alias": [{
        "lang":  "/lang/en",
        "value": "William Blake Crump"
      }],
      "name": "Blake Edwards",
      "type": "/film/director"
    },
    {
      "/common/topic/alias": [
        {
          "lang":  "/lang/en",
          "value": "Donn Alan Pennebaker"
        },
        {
          "lang":  "/lang/en",
          "value": "D.A. Pennabaker"
        },
        {
          "lang":  "/lang/en",
          "value": "Don Alan Pennebaker"
        },
        {
          "lang":  "/lang/en",
          "value": "Penny"
        }
      ],
      "name": "D. A. Pennebaker",
      "type": "/film/director"
    },
    {
      "/common/topic/alias": [],
      "name":          "Chris Hegedus",
      "type":          "/film/director"
    }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample61(self):
    """mqldoc_example_61."""

    query = """
    {
      "album": {
        "optional": true,
        "return": "count",
        "name": "Arrested"
      },
      "type": "/music/artist",
      "name": "The Police"
    }
    """
    exp_response = """
    {
      "album": 0,
      "type": "/music/artist",
      "name": "The Police"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample62(self):
    """mqldoc_example_62."""

    query = """
    [
      {
        "track": "Masters of War",
        "album": {
          "optional": "forbidden",
          "name": "Greatest Hits"
        },
        "type": "/music/artist",
        "name": null,
        "limit": 3
      }
    ]
    """
    exp_response = """
    [
  {
    "album": null, 
    "track": "Masters of War", 
    "type": "/music/artist", 
    "name": "Don McLean"
  }, 
  {
    "album": null, 
    "track": "Masters of War", 
    "type": "/music/artist", 
    "name": "The Staple Singers"
  }, 
  {
    "album": null, 
    "track": "Masters of War", 
    "type": "/music/artist", 
    "name": "Judy Collins"
  }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample63(self):
    """mqldoc_example_63."""

    query = """
    [
      {
        "limit": 3,
        "nor:album": {
          "optional": "forbidden",
          "name": "The Best Of"
        },
        "type": "/music/artist",
        "name": null,
        "neither:album": {
          "optional": "forbidden",
          "name": "Greatest Hits"
        }
      }
    ]
    """
    exp_response = """
    [
  {
    "nor:album": null, 
    "type": "/music/artist", 
    "name": "Blonde Redhead", 
    "neither:album": null
  }, 
  {
    "nor:album": null, 
    "type": "/music/artist", 
    "name": "Bruce Cockburn", 
    "neither:album": null
  }, 
  {
    "nor:album": null, 
    "type": "/music/artist", 
    "name": "Buck Owens", 
    "neither:album": null
  }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample64(self):
    """mqldoc_example_64."""

    query = """
    [
      {
        "limit": 3,
        "album": {
          "optional": "forbidden",
          "id": null
        },
        "type": "/music/artist",
        "name": null
      }
    ]
    """
    exp_response = """
    [
  {
    "album": null, 
    "type": "/music/artist", 
    "name": "Bill Clinton"
  }, 
  {
    "album": null, 
    "type": "/music/artist", 
    "name": "Domenico Alberti"
  }, 
  {
    "album": null, 
    "type": "/music/artist", 
    "name": "Donny the Punk"
  }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample65(self):
    """mqldoc_example_65."""

    query = """
    {
      "track": [
        {
          "limit": 3,
          "length": null,
          "name": null
        }
      ],
      "type": "/music/artist",
      "id": "/en/the_police"
    }
    """
    exp_response = """
    {
    "track": [{
      "length": 272.666, 
      "name": "Roxanne '97 (Puff Daddy remix)"
    }, 
    {
      "length": 234.10599999999999, 
      "name": "Don't Stand So Close to Me"
    }, 
    {
      "length": 192.90600000000001, 
      "name": "Roxanne"
    }],
      "type": "/music/artist", 
      "id": "/en/the_police"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample66(self):
    """mqldoc_example_66."""

    query = """
    {
      "track": [
        {
          "length": null,
          "name": null,
          "length>": 475
        }
      ],
      "type": "/music/artist",
      "id": "/en/the_police"
    }
    """
    exp_response = """
    {
      "track": [
        {
          "length": 533.78599999999994,
          "name": "The Bed's Too Big Without You"
        },
        {
          "length": 476.82600000000002,
          "name": "Can't Stand Losing You"
        },
        {
          "length": 479.733,
          "name": "Walking on the Moon (Roger Sanchez Darkside of the Moon mix)"
        },
        {
          "length": 527.06600000000003,
          "name": "The Bed's Too Big Without You"
        },
        {
          "length": 490.89299999999997,
          "name": "Roxanne"
        }
      ],
      "type": "/music/artist",
      "id": "/en/the_police"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample67(self):
    """mqldoc_example_67."""

    query = """
    {
      "track": [
        {
          "length": null,
          "length>=": 420,
          "name": null,
          "length<": 480
        }
      ],
      "type": "/music/artist",
      "id": "/en/the_police"
    }
    """
    exp_response = """
    {
      "track": [
        {
          "length": 450.02600000000001,
          "name": "So Lonely"
        },
        {
          "length": 456,
          "name": "So Lonely"
        },
        {
          "length": 476.82600000000002,
          "name": "Can't Stand Losing You"
        },
        {
          "length": 479.733,
          "name": "Walking on the Moon (Roger Sanchez Darkside of the Moon mix)"
        },
        {
          "length": 456.39999999999998,
          "name": "Voices Inside My Head (E Smoove Pump mix)"
        },
        {
          "length": 474.44,
          "name": "I Can't Stand Losing You"
        },
        {
          "length": 424.12,
          "name": "Voices Inside My Head / When the World Is Running Down, You Make the Best of What's Still Around"
        }
      ],
      "type": "/music/artist",
      "id": "/en/the_police"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample68(self):
    """mqldoc_example_68."""

    query = """
    [
      {
        "initial_release_date": null,
        "initial_release_date<=": "2009",
        "initial_release_date>=": "1999",
        "directed_by": "Francis Ford Coppola",
        "type": "/film/film",
        "name": null
      }
    ]
    """
    exp_response = """
    [
      {
        "type": "/film/film",
        "initial_release_date": "2000",
        "name": "Supernova",
        "directed_by": "Francis Ford Coppola"
      },
      {
        "type": "/film/film",
        "initial_release_date": "2007-10-26",
        "name": "Youth Without Youth",
        "directed_by": "Francis Ford Coppola"
      },
      {
        "type": "/film/film",
        "initial_release_date": "2001-08-03",
        "name": "Apocalypse Now Redux",
        "directed_by": "Francis Ford Coppola"
      }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample69(self):
    """mqldoc_example_69."""

    query = """
    [
      {
        "name>=": "tl",
        "name<": "tn",
        "type": "/film/film",
        "name": null
      }
    ]
    """
    exp_response = """
    [
      {
        "type": "/film/film",
        "name": "TMNT"
      },
      {
        "type": "/film/film",
        "name": "TMZ on TV"
      },
      {
        "type": "/film/film",
        "name": "Tlayucan"
      },
      {
        "type": "/film/film",
        "name": "TMA Ultimate Idol THE BEST 2 discs 8 hours"
      },
      {
        "type": "/film/film",
        "name": "TLC: Now &amp; Forever: Video Hits"
      },
      {
        "type": "/film/film",
        "name": "TLC: Tables, Ladders &amp; Chairs"
      },
      {
        "type": "/film/film",
        "name": "Tlatelolco68"
      }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample70(self):
    """mqldoc_example_70."""

    query = """
    [
      {
        "limit": 3,
        "name~=": "love",
        "artist~=": "^The",
        "type": "/music/track",
        "name": null,
        "artist": null
      }
    ]
    """
    exp_response = """
    [
  {
    "type": "/music/track", 
    "name": "Love You Till Friday", 
    "artist": "The Replacements"
  }, 
  {
    "type": "/music/track", 
    "name": "Love Spreads", 
    "artist": "The Stone Roses"
  }, 
  {
    "type": "/music/track", 
    "name": "One Love", 
    "artist": "The Stone Roses"
  }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample71(self):
    """mqldoc_example_71."""

    query = """
    [
      {
        "limit": 3,
        "name~=": "^The *$",
        "type": "/music/artist",
        "name": null
      }
    ]
    """
    exp_response = """
    [
  {
    "type": "/music/artist", 
    "name": "The Doors"
  }, 
  {
    "type": "/music/artist", 
    "name": "The Beatles"
  }, 
  {
    "type": "/music/artist", 
    "name": "The Penguins"
  }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample72(self):
    """mqldoc_example_72."""

    query = """
    [
      {
        "limit": 3,
        "name~=": "^The * *s$",
        "type": "/music/artist",
        "name": null
      }
    ]
    """
    exp_response = """
    [
  {
    "type": "/music/artist", 
    "name": "The Beach Boys"
  }, 
  {
    "type": "/music/artist", 
    "name": "The Righteous Brothers"
  }, 
  {
    "type": "/music/artist", 
    "name": "The Rolling Stones"
  }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample73(self):
    """mqldoc_example_73."""

    query = """
    [
      {
        "limit": 3,
        "type": "/music/track",
        "name": null,
        "b:name~=": "love",
        "a:name~=": "I"
      }
    ]
    """
    exp_response = """
    [
  {
    "type": "/music/track", 
    "name": "I Want Your Love"
  }, 
  {
    "type": "/music/track", 
    "name": "P.S. I Love You"
  }, 
  {
    "type": "/music/track", 
    "name": "I Know My Love (feat. The Corrs)"
  }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample74(self):
    """mqldoc_example_74."""

    query = """
    [
      {
        "atomic_number": null,
        "sort": "atomic_number",
        "atomic_number|=": [
          1,
          2,
          3
        ],
        "type": "/chemistry/chemical_element",
        "name": null
      }
    ]
    """
    exp_response = """
    [
      {
        "atomic_number": 1,
        "type": "/chemistry/chemical_element",
        "name": "Hydrogen"
      },
      {
        "atomic_number": 2,
        "type": "/chemistry/chemical_element",
        "name": "Helium"
      },
      {
        "atomic_number": 3,
        "type": "/chemistry/chemical_element",
        "name": "Lithium"
      }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample75(self):
    """mqldoc_example_75."""

    query = """
    [
      {
        "english:name|=": [
          "England",
          "France"
        ],
        "english:name": null,
        "type": "/location/country",
        "foreign:name": [
          {
            "lang": null,
            "lang|=": [
              "/lang/fr",
              "/lang/es"
            ],
            "value": null
          }
        ]
      }
    ]
    """
    exp_response = """
    [
      {
        "english:name": "England",
        "type": "/location/country",
        "foreign:name": [
          {
            "lang": "/lang/fr",
            "value": "Angleterre"
          },
          {
            "lang": "/lang/es",
            "value": "Inglaterra"
          }
        ]
      },
      {
        "english:name": "France",
        "type": "/location/country",
        "foreign:name": [
          {
            "lang": "/lang/fr",
            "value": "France"
          },
          {
            "lang": "/lang/es",
            "value": "Francia"
          }
        ]
      }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample76(self):
    """mqldoc_example_76."""

    query = """
    [
      {
        "type": "/music/album",
        "release_type": null,
        "artist": "William Shatner"
      }
    ]
    """
    exp_response = """
    [
      {
        "type": "/music/album",
        "release_type": "Album",
        "artist": "William Shatner"
      },
      {
        "type": "/music/album",
        "release_type": "Album",
        "artist": "William Shatner"
      },
      {
        "type": "/music/album",
        "release_type": "Live Album",
        "artist": "William Shatner"
      },
      {
        "type": "/music/album",
        "release_type": "Single",
        "artist": "William Shatner"
      },
      {
        "type": "/music/album",
        "release_type": "Album",
        "artist": "William Shatner"
      },
      {
        "type": "/music/album",
        "release_type": null,
        "artist": "William Shatner"
      }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample77(self):
    """mqldoc_example_77."""

    query = """
    {
      "type": "/music/album",
      "return": "count",
      "release_type!=": "Live Album",
      "artist": "William Shatner"
    }
    """
    self.DoQuery(query)
    assert self.mql_result.result == 4

  def testMqldocExample78(self):
    """mqldoc_example_78."""

    query = """
    {
      "type": "/music/album",
      "return": "count",
      "release_type": {
        "optional": "forbidden",
        "name": "Live Album"
      },
      "artist": "William Shatner"
    }
    """
    self.DoQuery(query)
    assert self.mql_result.result == 5

  def testMqldocExample79(self):
    """mqldoc_example_79."""

    query = """
    {
      "id": "/en/sofia_coppola",
      "/film/director/film": {
        "link": {},
        "name": "Lost in Translation"
      }
    }
    """
    exp_response = """
    {
      "id": "/en/sofia_coppola",
      "/film/director/film": {
        "link": {
          "master_property": "/film/film/directed_by",
          "type": "/type/link",
          "reverse": true
        },
        "name": "Lost in Translation"
      }
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample80(self):
    """mqldoc_example_80."""

    query = """
    {
      "id": "/en/sofia_coppola",
      "/film/director/film": {
        "link": null,
        "name": "Lost in Translation"
      }
    }
    """
    exp_response = """
    {
      "id": "/en/sofia_coppola",
      "/film/director/film": {
        "link": "/film/film/directed_by",
        "name": "Lost in Translation"
      }
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample81(self):
    """mqldoc_example_81."""

    query = """
    {
      "id": "/en/sofia_coppola",
      "/film/director/film": {
        "link": {
          "*": null
        },
        "name": "Lost in Translation"
      }
    }
    """
    exp_response = """
    {
      "id": "/en/sofia_coppola",
      "/film/director/film": {
        "link": {
          "attribution": "/user/mwcl_infobox",
          "reverse": true,
          "creator": "/user/mwcl_infobox",
          "master_property": "/film/film/directed_by",
          "source": "Lost in Translation",
          "valid": true,
          "timestamp": "2006-11-30T19:17:55.0020Z",
          "operation": "insert",
          "type": "/type/link",
          "target_value": null,
          "target": "Sofia Coppola"
        },
        "name": "Lost in Translation"
      }
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample82(self):
    """mqldoc_example_82."""

    query = """
    {
      "timestamp": null,
      "id": "/en/sofia_coppola",
      "/film/director/film": {
        "timestamp": null,
        "link": {
          "timestamp": null,
          "creator": null
        },
        "name": "Lost in Translation",
        "creator": null
      },
      "creator": null
    }
    """
    exp_response = """
    {
      "timestamp": "2006-10-22T15:08:38.0048Z",
      "id": "/en/sofia_coppola",
      "/film/director/film": {
        "timestamp": "2006-10-22T15:14:08.0061Z",
        "link": {
          "timestamp": "2006-11-30T19:17:55.0020Z",
          "creator": "/user/mwcl_infobox"
        },
        "name": "Lost in Translation",
        "creator": "/user/metaweb"
      },
      "creator": "/user/metaweb"
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample83(self):
    """mqldoc_example_83."""

    query = """
    {
      "id": "/en/spain",
      "name": {
        "link": {
          "creator": null,
          "target": "French",
          "target_value": null
        }
      }
    }
    """
    exp_response = """
    {
      "id": "/en/spain",
      "name": {
        "link": {
          "creator": "/user/mwcl_wikipedia_en",
          "target": "French",
          "target_value": "Espagne"
        }
      }
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample84(self):
    """mqldoc_example_84."""

    query = """
    {
      "/type/reflect/any_master": [
        {
          "limit": 3,
          "link": null,
          "name": null
        }
      ],
      "/type/reflect/any_reverse": [
        {
          "limit": 3,
          "link": null,
          "name": null
        }
      ],
      "id": "/en/the_gumball_rally",
      "/type/reflect/any_value": [
        {
          "limit": 3,
          "link": null,
          "value": null
        }
      ]
    }
    """
    exp_response = """
    {
  "/type/reflect/any_master": [
    {
      "link": "/type/object/permission", 
      "name": "Global Write Permission"
    }, 
    {
      "link": "/type/object/type", 
      "name": "Topic"
    }, 
    {
      "link": "/common/topic/article", 
      "name": null
    }
  ], 
  "/type/reflect/any_reverse": [
    {
      "link": "/film/performance/film", 
      "name": null
    }, 
    {
      "link": "/film/performance/film", 
      "name": null
    }, 
    {
      "link": "/film/performance/film", 
      "name": null
    }
  ],
    "id": "/en/the_gumball_rally", 
  "/type/reflect/any_value": [
    {
      "link": "/type/object/name", 
      "value": "The Gumball Rally"
    }, 
    {
      "link": "/film/film/initial_release_date", 
      "value": "1976"
    }, 
    {
      "link": "/film/film/tagline", 
      "value": "It's a hilarious coast-to-coast, 180 mile-an-hour, go-for-broke, outrageous road race with the world's most expensive cars. And it's all just for glory and a gumball machine."
    }
  ]
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample85(self):
    """mqldoc_example_85."""

    query = """
    {
      "id": "/en/the_gumball_rally",
      "/type/reflect/any_value": [
        {
          "lang": null,
          "link": null,
          "type": "/type/text",
          "value": null
        }
      ]
    }
    """
    exp_response = """
    {
      "id": "/en/the_gumball_rally",
      "/type/reflect/any_value": [
        {
          "lang": "/lang/en",
          "type": "/type/text",
          "link": "/type/object/name",
          "value": "The Gumball Rally"
        },
        {
          "lang": "/lang/en",
          "type": "/type/text",
          "link": "/film/film/tagline",
          "value": "It's a hilarious coast-to-coast, 180 mile-an-hour, go-for-broke, outrageous road race with the world's most expensive cars. And it's all just for glory and a gumball machine."
        },
        {
          "lang": "/lang/it",
          "type": "/type/text",
          "link": "/type/object/name",
          "value": "La corsa pi\u00f9 pazza del mondo"
        }
      ]
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample86(self):
    """mqldoc_example_86."""

    query = """
    {
      "id": "/en/the_gumball_rally",
      "/type/reflect/any_value": [
        {
          "*": null,
          "link": {
            "master_property": null,
            "target": {
              "optional": true,
              "id": null
            }
          }
        }
      ]
    }
    """
    exp_response = """
    {
      "id": "/en/the_gumball_rally",
      "/type/reflect/any_value": [
        {
          "type": "/type/text",
          "link": {
            "master_property": "/type/object/name",
            "target": {
              "id": "/lang/en"
            }
          },
          "value": "The Gumball Rally"
        },
        {
          "type": "/type/datetime",
          "link": {
            "master_property": "/film/film/initial_release_date",
            "target": null
          },
          "value": "1976"
        },
        {
          "type": "/type/text",
          "link": {
            "master_property": "/film/film/tagline",
            "target": {
              "id": "/lang/en"
            }
          },
          "value": "It's a hilarious coast-to-coast, 180 mile-an-hour, go-for-broke, outrageous road race with the world's most expensive cars. And it's all just for glory and a gumball machine."
        },
        {
          "type": "/type/text",
          "link": {
            "master_property": "/type/object/name",
            "target": {
              "id": "/lang/it"
            }
          },
          "value": "La corsa pi\u00f9 pazza del mondo"
        }
      ]
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample87(self):
    """mqldoc_example_87."""

    query = """
    [
      {
        "limit": 3,
        "first:/type/reflect/any_master": {
          "link": null,
          "name": "Sting"
        },
        "type": [],
        "id": null,
        "second:/type/reflect/any_master": {
          "link": null,
          "name": "The Police"
        }
      }
    ]
    """
    exp_response = """
    [
  {
    "first:/type/reflect/any_master": {
      "link": "/music/group_membership/member", 
      "name": "Sting"
    }, 
    "type": [
      "/music/group_membership"
    ], 
    "id": "/m/01t4k16", 
    "second:/type/reflect/any_master": {
      "link": "/music/group_membership/group", 
      "name": "The Police"
    }
  }, 
  {
    "first:/type/reflect/any_master": {
      "link": "/freebase/user_profile/favorite_music_artists", 
      "name": "Sting"
    }, 
    "type": [
      "/type/user", 
      "/type/namespace", 
      "/freebase/user_profile"
    ], 
    "id": "/user/saraw524", 
    "second:/type/reflect/any_master": {
      "link": "/freebase/user_profile/favorite_music_artists", 
      "name": "The Police"
    }
  },
  {
    "first:/type/reflect/any_master": {
      "link": "/freebase/user_profile/favorite_music_artists", 
      "name": "Sting"
    }, 
    "type": [
      "/type/user", 
      "/type/namespace", 
      "/freebase/user_profile"
    ], 
    "id": "/user/webgrrlie", 
    "second:/type/reflect/any_master": {
      "link": "/freebase/user_profile/favorite_music_artists", 
      "name": "The Police"
    }
  }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample88(self):
    """mqldoc_example_88."""

    query = """
    [
      {
        "sort": "-timestamp",
        "target_value": null,
        "master_property": "/type/object/name",
        "source": {},
        "valid": false,
        "limit": 1,
        "timestamp": null,
        "type": "/type/link"
      }
    ]
    """
    exp_response = """
    [
  {
    "target_value": "Kim Possible - The Villain Files.jpg", 
    "master_property": "/type/object/name", 
    "source": {
      "type": [
        "/common/image", 
        "/type/content"
      ], 
      "id": "/m/0h88y5p", 
      "name": "Kim Possible: The Villain Files"
    }, 
    "valid": false, 
    "timestamp": "2011-10-23T11:46:50.0003Z", 
    "type": "/type/link"
  }
    ]
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample89(self):
    """mqldoc_example_89."""

    query = """
    {
      "id": "/finance/currency",
      "name": {
        "lang": "/lang/en",
        "link": {
          "timestamp": null
        },
        "value": null
      }
    }
    """
    exp_response = """
    {
      "id": "/finance/currency",
      "name": {
        "lang": "/lang/en",
        "link": {
          "timestamp": "2007-03-25T00:33:28.0000Z"
        },
        "value": "Currency"
      }
    }
    """
    self.DoQuery(query, exp_response=exp_response)

  def testMqldocExample90(self):
    """mqldoc_example_90."""

    query = """
    {
      "id": "/finance/currency",
      "name": [
        {
          "lang": "/lang/en",
          "link": {
            "timestamp": null,
            "valid": null
          },
          "value": null
        }
      ]
    }
    """
    exp_response = """
    {
      "id": "/finance/currency",
      "name": [
        {
          "lang": "/lang/en",
          "link": {
            "timestamp": "2006-10-22T07:34:51.0008Z",
            "valid": false
          },
          "value": "currency"
        },
        {
          "lang": "/lang/en",
          "link": {
            "timestamp": "2007-03-25T00:33:28.0000Z",
            "valid": true
          },
          "value": "Currency"
        }
      ]
    }
    """
    self.DoQuery(query, exp_response=exp_response)

if __name__ == '__main__':
  mql_fixture.main()
