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

import os, sys, re
if __name__ == "__main__":
  sys.path.append(os.path.abspath("../.."))

from pymql.log import LOG
from pymql import json
import time

try:
  import cProfile
  profiler = "cProfile"
except ImportError, e:
  import hotshot
  profiler = "hotshot"


def wildcard_mql_query():
  return [{"id": None, "*": None}]


def get_all_domains_mql_query():
  return [{
      "id":
          None,
      "name":
          None,
      "type":
          "/type/domain",
      "key": {
          "value": None,
          "namespace": "/"
      },
      "/type/namespace/keys": [{
          "value": None,
          "type": None,
          "namespace": {
              "type":
                  "/type/type",
              "name":
                  None,
              "id":
                  None,
              "domain":
                  None,
              "/type/namespace/keys": [{
                  "value": None,
                  "type": None,
                  "namespace": {
                      "type": "/type/property",
                      "unique": None,
                      "id": None,
                      "schema": None,
                      "expected_type": None,
                      "master_property": None,
                      "name": None,
                      "reverse_property": []
                  }
              }]
          }
      }]
  }]


def get_domain_mql_query():
  return {
      "id":
          "/type",
      "name":
          None,
      "type":
          "/type/domain",
      "/type/namespace/keys": [{
          "value": None,
          "type": None,
          "namespace": {
              "type":
                  "/type/type",
              "name":
                  None,
              "id":
                  None,
              "domain":
                  None,
              "/type/namespace/keys": [{
                  "value": None,
                  "type": None,
                  "namespace": {
                      "type": "/type/property",
                      "unique": None,
                      "id": None,
                      "schema": None,
                      "expected_type": None,
                      "master_property": None,
                      "name": None,
                      "reverse_property": []
                  }
              }]
          }
      }]
  }


def get_type_mql_query():
  return {
      "type": [],
      "name":
          None,
      "id":
          "/type/object",
      "/type/type/domain":
          None,
      "/type/namespace/keys": [{
          "value": None,
          "type": None,
          "namespace": {
              "type": "/type/property",
              "unique": None,
              "id": None,
              "schema": None,
              "expected_type": None,
              "master_property": None,
              "name": None,
              "reverse_property": []
          }
      }]
  }


def get_schema_query(guid):
  return {
      "@guid":
          guid,
      "is_instance_of": {
          "@id": "/type/type"
      },
      "uses_properties_from": {
          "@guid": None,
          ":optional": True
      },
      "has_default_property_name": {
          ":value": None,
          ":optional": True
      },
      "has_key": [{
          ":optional": True,
          "@guid": None,
          ":value": None,
          "has_schema": {
              "@guid": None,
          },
          "has_expected_concept_type": {
              ":optional": True,
              "@guid": None
          },
          "has_master_property": {
              ":optional": True,
              "@guid": None,
              "is_unique_property": {
                  ":value": None,
                  ":datatype": "boolean",
                  ":optional": True
              }
          },
          "is_unique_property": {
              ":value": None,
              ":datatype": "boolean",
              ":optional": True
          },
          "is_instance_of": {
              "@id": "/type/property"
          }
      }]
  }


def get_object_query():
  q = get_schema_query(None)
  q["@id"] = "/type/object"
  return q


def get_domain_query():
  ns_query = {
      "@id": "/type",
      "is_instance_of": {
          "@id": "/type/domain"
      },
      "has_key": [get_schema_query(None)]
  }
  ns_query["has_key"][0][":value"] = None
  ns_query["has_key"][0]["has_domain"] = {"@id": "/type"}
  return ns_query


def get_wildcard_query():
  return [{
      "@guid": None,
      "*": [{
          "@guid": None,
          ":guid": None,
          ":value": None,
          ":optional": True
      }]
  }]


def test_run(ctx, varenv, options, query):
  graphq = ctx.gc
  ctx.gc.reset_cost()

  #ctx.gc.reopen()
  result = None

  start_time = time.time()

  for i in xrange(options.num):
    if options.flush:
      ctx.high_querier.schema_factory.flush("")

    if options.type == "graph":
      result = ctx.gc.read(
          query, transaction_id=varenv["tid"], policy=varenv["policy"])
    else:
      result = ctx.high_querier.read(query, varenv)

  stop_time = time.time()

  ctx.gc.totalcost["dt"] = stop_time - start_time

  return result


def cmdline_main():
  LOG.warning("benchmark", "test start")
  start_time = time.time()

  from mql.mql import cmdline
  op = cmdline.OP("testing")

  op.add_option(
      "-n", dest="num", default=1000, type="int", help="number of iterations")

  op.add_option(
      "-P",
      dest="profile",
      default=None,
      help="run profiler with output to file")

  op.add_option("-c", dest="call", default=None, help="function to call")

  op.add_option(
      "-f", dest="query_file", default=None, help="file containing query")

  op.add_option(
      "--flush",
      dest="flush",
      default=None,
      help="flush cache between every request")

  op.add_option("-t", dest="type", default="mql", help="graph or MQL query")

  options, args = op.parse_args()

  stop_time = time.time()
  op.ctx.gc.totalcost["dt"] = stop_time - start_time

  LOG.warning("start cost", {
      "nreqs": op.ctx.gc.nrequests,
      "cost": op.ctx.gc.totalcost
  })

  options, args = op.parse_args()

  queryfile = options.query_file
  if queryfile is not None:
    qf = open(queryfile, "r")
    query = "".join(qf.readlines())
    regex = re.compile("[\n\t]+")
    query = regex.sub(" ", query)
    qf.close()
  elif options.call:
    query = globals()[options.call]()
  elif len(args) == 1:
    query = args[0]
  else:
    op.error("Must specify a query argument")

  if options.type == "mql":
    # XXX should eventually use unicode, for now utf8
    query = json.loads(query, encoding="utf-8", result_encoding="utf-8")
  elif options.type == "graph":
    pass
  else:
    op.error("-t must be 'mql' or 'graph'")

  if options.profile:
    if profiler == "hotshot":
      profile = hotshot.Profile(options.profile)
      profile.runcall(test_run, op.ctx, op.varenv, options, query)
      LOG.warning(
          "benchmark",
          "Saving hotshot profile in Stats format to %s" % options.profile)

    elif profiler == "cProfile":
      profile = cProfile.Profile()
      profile.runcall(test_run, op.ctx, op.varenv, options, query)

      LOG.warning(
          "benchmark",
          "Saving cProfile data in kcachegrind format to %s" % options.profile)
      # get from http://jcalderone.livejournal.com/21124.html
      # and put in thirdparty/pyroot
      from mql.mql import lsprofcalltree
      k = lsprofcalltree.KCacheGrind(profile)
      k.output(open(options.profile, "w"))
    else:
      LOG.warning("benchmark", "No profiler available, not running benchmark")
  else:
    reslist = test_run(op.ctx, op.varenv, options, query)

  LOG.warning("run cost", {
      "nreqs": op.ctx.gc.nrequests,
      "cost": op.ctx.gc.totalcost
  })
  #print repr(reslist[0])
  #pprint.pprint(reslist)

  #LOG.warning("benchmark", "test finish")


if __name__ == "__main__":
  cmdline_main()
