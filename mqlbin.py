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
"""A simple wrapper to demonstrate basic mql reads and writes."""

__author__ = "bneutra@google.com (Brendan Neutra)"

import json

from absl import app
from absl import flags
from collections import OrderedDict
from pymql import MQLService
from pymql.mql.graph import TcpGraphConnector

FLAGS = flags.FLAGS
flags.DEFINE_string(
    "mqlenv", None, "a dict in the form of a string which "
    "contains valid mql env key/val pairs")

flags.DEFINE_string("mqlcmd", None, "'read' or 'write'")
flags.DEFINE_string("graphd_addr", "localhost:9100",
                    "host:port of graphd server")


def main(argv):
  if not FLAGS.graphd_addr:
    raise Exception("Must specify a --graphd_addr")

  conn = TcpGraphConnector(addrs=[("localhost", 8100)])
  mql = MQLService(connector=conn)

  q = json.loads(argv[1], object_pairs_hook=OrderedDict)
  env = {}
  if FLAGS.mqlenv:
    env = json.loads(FLAGS.mqlenv)

  if FLAGS.mqlcmd == "read":
    print mql.read(q, **env)
  elif FLAGS.mqlcmd == "write":
    print mql.write(q, **env)
  else:
    print "you must provie a --mqlcmd, either 'read' or 'write'"


if __name__ == "__main__":
  app.run(main)
