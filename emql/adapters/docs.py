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

search_adapter_help = '''
===================
/type/object/search
===================

The ``/type/object/search`` property invokes Freebase's api/service/`search`_
service.

During the `pre`_ phase, the `search`_ api is invoked and the resulting
guids are inserted into the MQL query via with a ``guid|=`` clause.

During the `fetch`_ phase, the query and the relevance score, if it was
requested are returned for each match.

The ``pre`` phase expects either a query string as parameter or one or more
of ``query``, ``prefix``, ``prefixed``, ``type``, ``type_strict``,
``domain``, ``domain_strict``, ``type_exclude``, ``type_exclude_strict``,
``domain_exclude``, ``domain_exclude_strict``, ``limit``, ``denylist``,
``mql_filter`` and ``geo_filter`` which are implemented and documented by
the `search`_ service.

In addition to these parameters, the ``search`` property can return the
relevance score of matches via the ``score`` parameter. This score value can
then be sorted on.

Any other parameter is returned along with the matches in ``guid|=`` as an
extra MQL clause to satisfy.

Some of the containing query's constraints, such as ``type`` and ``limit``,
are passed on to the search call if they're not specified as parameters.

For example, the query below finds five ``/people/person`` instances
relevant to the term "gore", returning the query, id and score for every
match. The results are sorted by decreasing relevance score::

  [{ 
     "search": {"query": "gore", "id": null, "score": null},
     "/common/topic/article": [{"text": {"chars": null, "length": null},
                                "sort": "-text.length"}],
     "type": "/people/person",
     "name": null, 
     "limit": 5,
     "sort": "-search.score",
     "age": null
  }]

As with MQL's ``name`` clause, the results of ``search`` can be elided by
using the ``=~`` suffix. For example, the query below finds twelve companies
relevant to the term "energy", eliding the search result and returning
financial data for each of the matches via the `quote`_ property::

  [{ 
     "search~=": "energy",
     "type": "/business/business_operation",
     "name": null,
     "id": null,
     "limit": 12,
     "!/business/issue/issuer": [{
       "trading_symbol": [{"quote": {"price": null, "volume": null,
                           "high": null, "low": null, 
                           "ticker": null}}]
  }]


.. _search: search?help
.. _pre: mqlread?help=extended#pre
.. _fetch: mqlread?help=extended#fetch
.. _quote: mqlread?help=extended=/business/stock_ticker_symbol/quote
'''

geosearch_adapter_help = '''
=================================================
/location/location/inside /location/location/near
=================================================

These properties invokes Freebase's api/service/`geosearch`_ service.

During the `pre`_ phase, the `geosearch`_ api is invoked and the resulting
guids are inserted into the MQL query via with a ``guid|=`` clause.

During the `fetch`_ phase, the query and the distance - with ``near``, if it 
was requested - are returned for each match.

The ``inside`` property is meant to be used with a ``/location/location``
instance indexed with a shape.

The ``near`` property can be used with locations indexed with shapes or
points. A shape is used over a point if both are present and matches outside
the shape only are returned.

The ``pre`` phase expects either a location string as parameter or one or
more of ``location``, ``limit``,  ``type``, ``location_type``, ``within``,
``inside``, ``mql_filter``, ``geometry_type``, ``negate`` and
``outer_bounds`` which are implemented and documented by the `geosearch`_
service.

These additional parameters are also supported:

*distance*
  The ``near`` property can return the distance of matches via the
  ``distance`` parameter. This value can then be sorted on. 
  As a synonym for ``within``, this parameter supports the ``<=`` constraint.

*unit*
  By default, the distance is expressed in ``kms``. This can be changed to
  ``miles`` with this parameter.

*score*
  Both ``near`` and ``inside`` can return the popularity score for
  matches. This score is computed from the number Freebase links in and out
  of the ``/location/location`` instance and can be sorted on.

Any other parameter is returned along with the matches in ``guid|=`` as an
extra MQL clause to satisfy.

Some of the containing query's constraints, such as ``type`` and ``limit``,
are passed on to the search call if they're not specified as parameters.

For example, the query below finds up to fifty restaurants within two miles
of Berkeley. The results are then sorted by distance::

  [{
     "type" : "/dining/restaurant",
     "name" : null,
     "/location/location/near": {"location": "Berkeley", "unit": "miles",
                                 "distance": null, "distance<=": 2},
     "limit": 50,
     "sort": "/location/location/near.distance"
  }]

As with MQL's ``name`` clause, the results of ``geosearch`` can be elided by
using the ``=~`` suffix. For example, the query below finds fifty restaurants
in San Francisco eliding the ``inside`` property result::

  [{
     "type" : "/dining/restaurant",
     "name" : null,
     "/location/location/in~=": "San Francisco",
     "limit": 50
  }]


.. _geosearch: geosearch?help
.. _pre: mqlread?help=extended#pre
.. _fetch: mqlread?help=extended#fetch
'''

point_adapter_help = '''
========================
/location/location/point
========================

The ``/location/location/point`` property returns the longitude and latitude
of a location in geojson format.

If the location has no ``geocode`` information, ``null`` is returned.

For example, the query below::

    [{
        "id": null,
        "id|=": ["/en/harrison_ford", "/en/san_francisco"],
        "/location/location/point": null
    }]

returns::

    [
      {
        "/location/location/point": null,
        "id": "/en/harrison_ford"
      },
      {
        "/location/location/point": {
          "geometry": {
            "coordinates": [
              -122.4183,
              37.774999999999999
            ],
            "id": "#9202a8c04000641f80000000011523d4",
            "type": "Point"
          }
        },
        "id": "/en/san_francisco"
      }
    ]

'''

shape_adapter_help = '''
========================
/location/location/shape
========================

The ``/location/location/shape`` property returns the shape associated with
a location in geojson format.

If the location has no ``geometry`` information, ``null`` is returned.

The following parameters may be used on the right handside of the query and
their use is further documented by the geosearch_ api:

    ``accessor``
      This parameter makes it possible to simplify the returned shape by
      extracting its bounding box, its convex hull or its shell. It accepts
      one of ``envelope``, ``hull``, ``shell``, ``shape`` or ``centroid``
      respectively. The ``shell`` of a shape is a shape that was enlarged
      by 0.1 degree before being simplified again with a tolerance of 0.1
      degree.

    ``simplify``
      This parameter uses the PostGIS ``ST_Simplify()`` function to simplify
      complex shapes with the Douglas-Peuker algorithm. It takes a floating
      point number, a so-called ``tolerance`` value, expressed in
      degrees. The right tolerance to use depends on the actual geographical
      size of the shape.

For example, the query below returns the bounding boxes of the shapes
requested if they exist::

    [{
        "id": null,
        "id|=": ["/en/harrison_ford", "/en/san_francisco"],
        "/location/location/shape": {
            "accessor": "envelope"
        }
    }]

returns::

    [
      {
        "/location/location/shape": {
          "coordinates": [
            [
              [
                [
                  -122.61228942871099,
                  37.706718444824197
                ],
                [
                  -122.61228942871099,
                  37.929824829101598
                ],
                [
                  -122.281776428223,
                  37.929824829101598
                ],
                [
                  -122.281776428223,
                  37.706718444824197
                ],
                [
                  -122.61228942871099,
                  37.706718444824197
                ]
              ]
            ],
            [
              [
                [
                  -123.173828125,
                  37.639827728271499
                ],
                [
                  -123.173828125,
                  37.8230590820312
                ],
                [
                  -122.935707092285,
                  37.8230590820312
                ],
                [
                  -122.935707092285,
                  37.639827728271499
                ],
                [
                  -123.173828125,
                  37.639827728271499
                ]
              ]
            ]
          ],
          "id": "#9202a8c04000641f8000000008056a3b",
          "type": "MultiPolygon"
        },
        "id": "/en/san_francisco"
      }
    ]


.. _geosearch: geosearch?help
'''

quote_adapter_help = '''
===================================
/business/stock_ticker_symbol/quote
===================================

The ``/business/stock_ticker_symbol/quote`` property invokes the 
`Yahoo! Finance`_ service to return financial information about a publically
traded company via its ticker symbol.

During the `pre`_ phase, a clause to retrieve the stock ticker symbol from
the Freebase object is returned for insertion::

  { "/business/stock_ticker_symbol/ticker_symbol": null }

During the ``fetch`` phase, the parameters requested are used to construct an
HTTP request to the `Yahoo! Finance`_ service. The results returned are then
returned as individual values for the parameters requested which can one or
more of the list below:

*volume*
  the number of shares traded today

*price*
  the latest available share price

*high*
  the highest trading price today

*low*
  the lowest trading price today

*ticker*
  the company's ticker symbol

For example, the query below returns the ticker, latest stock price and
trading volume about twelve energy companies. The average trading volume is
computed during the `reduce`_ phase via the `average`_ extension property::

  [{ 
     "search~=": "energy",
     "type": "/business/business_operation",
     "name": null,
     "id": null,
     "limit": 12,
     "!/business/issue/issuer": [{ 
       "trading_symbol": [{
         "quote": {"price": null, "volume": null, "ticker": null}
       }]
     }],
     "@average:!/business/issue/issuer.trading_symbol.quote.volume": null
  }]

If no parameters are used, the latest available share price is returned::

  [{ 
     "search~=": {"query": "energy"},
     "type": "/business/business_operation",
     "name": null,
     "id": null,
     "limit": 12,
     "!/business/issue/issuer": [{ 
       "trading_symbol": {
         "quote": null
       }
     }]
  }]


.. _Yahoo! Finance: http://download.finance.yahoo.com
.. _pre: mqlread?help=extended#pre
.. _reduce: mqlread?help=extended#reduce
.. _average: mqlread?help=/freebase/emql/average
'''

text_adapter_help = '''
=====================
/common/document/text
=====================

The ``/common/document/text`` property returns the text pointed at by a
``/common/document`` instance.

During the `pre`_ phase, clauses to retrieve text location information
about the instance is returned for insertion::

  {
    "/common/document/content": {
      "optional": true, 
      "blob_id": null,
      "media_type": null
    },
    "/common/document/source_uri": null
  }

During the `fetch`_ phase, if a ``blob_id`` is found and its ``media_type``
is ``/media_type/text`` or one of its descendants, the blob's text is
obtained from the CDB service. Otherwise, if a ``source_uri`` was found, the
`blurb`_ service is invoked. If neither are found, no value is
returned.

The following parameters affect the resulting text:

*maxlength*
  The returned text is truncated to that many characters.

*length*
  The length of the returned text.

*char*
  The actual text if parameters are used.

If no parameters are used, the text is returned as-is.

For example, the query below returns article text about five people relevant
to the term "gore". The texts are sorted by decreasing length while the
matches are sorted by decreasing `relevance`_ score::

  [{
     "search": {"query": "gore", "score": null},
     "/common/topic/article": [{"text": {"chars": null, "length": null},
                                "sort": "-text.length"}],
     "type": "/people/person",
     "name": null, 
     "limit": 5,
     "sort": "-search.score",
     "age": null
  }]


.. _pre: mqlread?help=extended#pre
.. _fetch: mqlread?help=extended#fetch
.. _relevance: search?help
.. _blurb: /api/trans/blurb?help
'''

stats_adapter_help = '''
========================================
@average @median @sigma @min @max @total
========================================

These properties compute a basic `statistic`_ from a set of values.

The `sigma`_ property returns the sample standard deviation over the set of
values.

These properties are stored in the ``/freebase/emql`` namespace and are not
expressed relative to a type; they are considered global properties and can
be addressed with the '@' prefix character.

Their adapter implements only a `reduce`_ phase and expects one parameter,
``value``, that denotes the values to reduce.

For example, the query below finds ten people between the ages of 35 and 45,
sorted by their age. During the `reduce`_ phase, the median age is inserted
in every result::

  [{
     "type": "/people/person",
     "name": null,
     "age": null,
     "age>=": 35,
     "age<=": 45,
     "limit": 10,
     "sort": "age",
     "@median": {"value": "age"}
  }]

Because the parameter this adapter expects is called ``value``, the
value-suffix shorthand syntax for these properties is supported::

  [{
     "type": "/people/person",
     "name": null,
     "age": null,
     "age>=": 35,
     "age<=": 45,
     "limit": 10,
     "sort": "age",
     "@median:age": null
  }]

As with any property whose adapter implements a `reduce`_ phase, the
result of the reduction can take the place of the matches it is computed
from::

  [{         
     "type": "/people/person",
     "age": null,
     "age>=": 35,
     "age<=": 45,
     "limit": 10,
     "sort": "age",
     "return": {
        "@min:age": null,
        "@max:age": null,
        "@median:age": null
     }
  }]

See `reduce`_ for more information about the effects of ``sort`` and
``limit`` on the ``reduce`` phase.


.. _statistic: http://reference.wolfram.com/mathematica/tutorial/BasicStatistics.html
.. _sigma: http://en.wikipedia.org/wiki/Standard_deviation
.. _reduce: mqlread?help=extended#reduce
'''

properties_adapter_help = '''
========================
/common/topic/properties
========================

The ``/common/topic/properties`` property describes a topic's properties.

During the `fetch`_ phase, all the topics' properties, master and reverse,
are fetched and formatted into results according to the following
parameters:

*ordered*
  Request that the values of properties be returned in link ``index``
  order, the order used by the Freebase web client. The parameter is
  ``false`` by default.

*limit*
  Request that at most ``limit`` values be returned for any property.
  By default, the limit is 100. To retrieve all values for all properties,
  use ``limit: null``.

*text*
  Request that a text representation of the values be included in the
  results. By default, ``text`` is true. The textual results created depend
  on the type of the value and of the schema of the property they're
  generated for. See below for more information.

*thumbnail*
  Request that a URL for the thumbnail of image values be returned. This
  parameter is ``true`` by default.

*url*
  Request that a Freebase view URL be returned for each reference in the
  result values. This parameter is ``false`` by default.

*type*
  Request that the properties' ``/type/property/expected_type`` be returned
  in the results. This parameter is ``false`` by default.

*unit*
  Request that the properties' ``/freebase/unit_profile/abbreviation`` be
  returned in the results. This parameter is ``false`` by default.

*unique*
  Request that the properties' ``/type/property/unique`` be returned in the
  results. This parameter is ``false`` by default.

The conversion of values into text, requested via the ``text`` parameter, is
done as follows:

  - If the value is a literal and the property it is on has a unit abbreviation,
    the two are combined. For example, the text value of ``/en/austria``'s
    ``/location/location/area`` is ``"83872.0 km\xc2\xb2"``.

  - If the value is a floating point number, its graphd string
    representation is returned.

  - If the value is a date or datetime, it is formatted into a user readable
    date or datetime.

  - If the value is of type ``/common/document`` a blurb for its text
    content is retrieved using the `blurb`_ web API.

  - If the value is a reference to a Compound Value Type (CVT), the text
    values in the CVT's properties are joined together by `` - `` in the
    graph link ``index`` order as used by the Freebase web client.

  - If the value is a reference to another Freebase object, its name is
    returned


.. _fetch: mqlread?help=extended#fetch
.. _blurb: /api/trans/blurb?help
'''

weblink_adapter_help = '''
=====================
/common/topic/weblink
=====================

The ``/common/topic/weblink``'s extension works with URLs pertaining
to topics by covering all the schema variations in Freebase's topic URL data
model.

Two modes of operation apply to this extension:

- If given URL data, the ``weblink`` adapter searches a URL index to match
  this data against existing topics during the `pre`_ phase and inserts
  the resulting URL holder ids - topics or /common/resource instances - into
  the MQL query. During the `fetch`_ phase, the results of the data
  collected from the url index are inserted into the query's results. 

- If given no URL data, the adapters inserts the following query during
  the `pre`_ phase to find URLs pertaining to the containing query. During
  the `fetch`_ phase, the results of the data collected from the MQL query
  insertion are inserted into the query's results::

     {
       "key": [{
         "optional": true,
         "namespace": {
           "!/common/uri_template/ns": {
             "weblink:template": { "value": null },
             "weblink:guid": null
           }
         },
         "value": null
       }],
       "/common/topic/annotation": [{
         "optional": true,
         "guid": null,
         "resource": [{
           "optional": false,
           "guid": null,
           "key": [{
             "optional": false,
             "namespace": {
               "!/common/uri_template/ns": {
                 "weblink:template": { "value": null },
                 "weblink:guid": null
               }
             },
             "value": null
           }]
         }]
       }]
     }

In both cases, all URL template ``uri`` and ``name`` expressions inside {} are
expanded by running the corresponding MQL queries against the topics,
annotations or resources holding the URLs. Expressions that remain unexpanded
after the `fetch`_ phase are indications of errors in expanding them.

When using the URL index, the parameters below affect the operation of
this adapter. Please note that, at this time, the URL index only indexes
``http`` and ``https`` URLs:

*url*
  This parameter is expected to be a string argument to be used
  against the URL index for matching as a URL. The url is broken up into its
  constituent parts - host, path, query and fragment - and matched against
  the index accordingly. The url's host is matched case-insensitively, its
  path, query and fragments are matched case-sensitively. Matches against
  each components may be partial against the head or the tail of the
  component but cannot contain holes. For example, ``example.uk`` will match
  ``www.example.uk`` but not ``example.co.uk``. When the URL path is empty,
  empty path matches are required. To look for host-only matches, use
  the ``host`` parameter instead.

*host*
  This parameter may be used instead of ``url`` to request host-only matches,
  matches that match only the host of all indexed URLs. This query is
  considerably more expensive for hosts with many URLs such as
  ``http://wikipedia.org``.

*path*
  Instead of matching a full URL or just a host, the ``path`` parameter may be
  used to search URLs by path only. As when using the ``url`` parameter,
  this match can be partial but may not contain holes. See examples below.

When searching for URLs pertaining to topics in the surrounding query - not
using the URL index, not searching by URL components - the parameters
affecting the operation of this adapter are:

*url*
  Must be ``None`` otherwise the URL index is searched instead.

*template*
  A MQL query snippet to insert as additional constraints into the three
  template uses in the `pre`_ MQL query insertion described earlier.

*category*
  A MQL query snippet to insert as an additional ``category`` constraint into
  the first template use in the `pre`_ MQL query insertion described earlier.

*description*
  A MQL query snippet to insert as an additional ``name`` constraint into the
  three template uses in the `pre`_ MQL query insertion described earlier.

Tip: to see how your parameters affect the MQL query that is being run after
the `pre`_ phase, run your eMQL query with the ``debug`` envelope parameter
set to ``pre``.

Examples:

- Find 30 topics with URLs in Freebase::

    [{
       "type": "/common/topic",
       "weblink": [{"url": null, "optional": false}],
       "id": null,
       "limit": 30,
       "cursor": 500
    }]

- Find URLs pertaining to the ``/en/lesotho`` topic::

    [{
       "type": "/common/topic",
       "weblink": [],
       "id": "/en/lesotho",
       "limit": 30
    }]  

- Find three URLs for ``/en/madagascar``::

    [{
       "type": "/common/topic",
       "weblink": [{
         "url": null,
         "limit": 3
       }],
       "id": "/en/madagascar"
    }]

- Find up to 30 topics with URLs containing the word ``France`` in their
  path, returning up to three URLs with their description as well::

    [{
       "type": "/common/topic",
       "weblink": [{
         "path": "France",
         "limit": 3,
         "url": null,
         "description": null
       }],
       "limit": 30
    }]

- Find topics pertaining to Netflix URLs::

    [{
       "type": "/common/topic",
       "weblink": [{
          "host": "http://netflix.com",
          "limit": 3,
          "url": null,
          "description": null
       }],
       "limit": 30
    }]

- Find topics pertaining to a given exact URL::

    [{
       "type": "/common/topic",
       "weblink": "http://www.nytimes.com/top/reference/timestopics/people/f/aretha_franklin",
       "id": null
    }]

- Find topics pertaining to a given approximate URL::

    [{
       "type": "/common/topic",
       "weblink": "http://nytimes.com/aretha_franklin",
       "id": null
    }]

- Return just one URL for Nicole Kidman::

    [{
       "id": "/en/nicole_kidman",
       "type": "/common/topic",
       "weblink": null
    }]

- Return some detail about just one URL for Nicole Kidman::

    [{
       "id": "/en/nicole_kidman",
       "type": "/common/topic",
       "weblink": {
         "url": null,
         "description": null
       }
    }]

- Return the list of URLs pertaining to Nicole Kidman::

    [{
       "id": "/en/nicole_kidman",
       "type": "/common/topic",
       "weblink": []
    }]

- Return URLs for San Francisco, inserting template MQL constraints::

    [{
       "type": "/common/topic",
       "weblink": [{
         "template": {
           "template": null,
           "name": null,
           "ns": null
         }
       }],
       "id": "/en/san_francisco",
       "limit": 30
    }]

- Return URLS for France inserting a template constraint that excludes
  URLs coming from the ``/wikipedia/en`` topic::

    [{
       "type": "/common/topic",
       "weblink": [{
         "template": {
           "template": null,
           "name": null,
           "a:ns": {
             "id": "/wikipedia/en",
             "optional": "forbidden"
           },
           "ns": null
         }
       }],
       "id": "/en/france",
       "limit": 30
    }]

- Similar query about the Ford Mustang also extracting URL category and
  description::

    {
       "id": "/en/ford_mustang",
       "/common/topic/weblink": [{
         "category": null,
         "url": null,
         "description": null,
         "template": {
           "a:ns": {
             "id": "/wikipedia/en",
             "optional": "forbidden"
           }
         }
       }]
    }

- Find URLs for the movie "Inglorious Basterds" returning URL template,
  category and template key information::

     [{
       "type": "/common/topic",
       "id": "/en/inglorious_bastards",
       "weblink": [{
         "template": null,
         "category": null,
         "description": null,
         "key": null
       }],
       "limit": 300
     }]

- Return URLs for the Royal Mail using constraints on ``category`` and
  ``template``::

    {
       "/common/topic/weblink": [{
         "url": null,
         "category": {
           "id": null,
           "name": null
         },
         "key": null,
         "template": {
           "ns": null,
           "optional": true,
           "id": null,
           "template": null
         }
       }],
       "id": "/en/royal_mail"
    }

- Return properties of and URLs pertaining to San Francisco::

    [{
       "id": "/en/san_francisco",
       "/common/topic/properties": [{}]
       "/common/topic/weblink": []
    }]

- Return the URLs and their description pertaining to Penelope Cruz::

    [{
       "id": "/en/penelope_cruz",
       "/common/topic/weblink": [{
         "url": null,
         "description": null
       }]
    }]

- Return film topics which have URLs in the "Review" category including
  the topics' types::

    [{
       "type": "/film/film",
       "a:type": [],
       "/common/topic/weblink": [{
         "category": "Review",
         "url": null
       }]
    }]

- Return topics pointing at 'http://www.apple.com/' by way of an annotation,
  with the uri_template category and the annotation category included::

    [{
       "id": null,
       "/common/topic/annotation": [{
          "category": null,
          "/common/annotation/resource": [{
             "type": [],
             "/common/topic/weblink": [{
               "url": "http://www.apple.com/",
               "category": null,
               "description": null
             }]
          }]
       }]
    }]

     
.. _pre: mqlread?help=extended#pre
.. _fetch: mqlread?help=extended#fetch
'''


twitter_adapter_help = ''
nytimes_adapter_help = ''
