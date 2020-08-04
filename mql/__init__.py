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
"""This is the beginning of a public API for doing MQL/LoJSON reads and writes.

To use (using mql_read as an example)

from mw.mql import mql_read, MiniContext

query = {
  "query":[{
    "id":"/common/topic",
    "type":"/type/type",
    "properties":[{}]
  }]
}

ctx = MiniContext(("localhost", 1234))
result = mql_read(ctx, query)

"""

#from pathexpr import wrap_query
#from mw.log import LOG
#
#__all__ = ['mql_read', 'mql_write', 'MiniContext']
#
#def mql_read(ctx, query, varenv=None, transaction_id=None):
#    LOG.error("deprecated", "mw.mql.mql_read()")
#    return wrap_query(ctx.high_querier.read, query, varenv, transaction_id)
#
#def mql_write(ctx, query, varenv=None, transaction_id=None):
#    LOG.error("deprecated", "mw.mql.mql_write()")
#    assert not ctx.gc.readonly, "Context must be created with readonly=False"
#    return wrap_query(ctx.high_querier.write, query, varenv, transaction_id)
