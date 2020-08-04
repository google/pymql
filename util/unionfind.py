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

#
#
#   flexible union-find operations
#
#   you can use a particular attribute of the objects you're working
#     with as the union chain attribute.
#    
#
#   NOT WELL-TESTED  
#
#

#
# union-find: merge two nodes
#  the first argument is favored as the new common root
#
def union(node1, node2, chainattr):
    c1 = find(node1, chainattr)
    c2 = find(node2, chainattr)
    if c1 == c2: return
    setattr(c2, chainattr, c1)

#
# union-find: find the definitive member of a set,
#  collapsing lookup chains along the way
#
def find(node, chain_attr=None, chain_get=None, chain_put=None):
    if chain_get is None:
        chain_get = lambda p: getattr(p, chain_attr)
    if chain_put is None:
        chain_put = lambda p,v: setattr(p, chain_attr, v)

    #
    # find the root for this union
    #
    root = None
    c = node
    while 1:
        cc = chain_get(c)
        if c == cc:
            root = c
            break
        c = cc
    #print chainattr, node.id, root.id

    #
    # collapse the chain from us to the root
    #
    c = node
    while 1:
        cc = chain_get(c)
        if cc == root:
            break
        chain_put(c, root)
        c = cc

    return root

