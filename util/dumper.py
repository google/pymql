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

import pprint

def dump(object,depth=10, ctx=None):
    if ctx is None:
        ctx = {}
        
    # don't subclass these types. Please!
    if isinstance(object,(basestring,str,bool,int,float,long)):
        return object
    elif object is None:
        return object

    # subclasses of these types are interesting.
    if (type(object) in [dict, list, tuple]) and len(object) == 0:
        return object

    oid = id(object)
    if oid in ctx:
        return "!!REPEAT!!" + ctx[oid]
    
    typename = type(object).__name__
    if typename == 'instance':
        typename = object.__class__.__name__
    ctx[oid] = '<' + typename + ' instance at ' + hex(oid) + '>'

    if typename in ctx:
        return "!!SKIPPED!!" + ctx[oid]

    if depth < 0:
        return "!!DEPTH!!" + ctx[oid]
    
    if isinstance(object, dict):
        result = { '!!REPR!!' : ctx[oid] }
        for k in object:
            result[k] = dump(object[k],depth-1,ctx)
            
        return result

    elif isinstance(object,(list,tuple)):
        result = [ ctx[oid] ]
        for k in object:
            result.append(dump(k,depth-1,ctx))
            
        return result

    result = { '!!REPR!!' : ctx[oid] }
    try:
        for key in object.__dict__:
            if key not in ctx:
                result[key] = dump(object.__dict__[key],depth-1,ctx)
    except:
        pass
    return result
    
def dumper(object,depth=10,ctx=None):
    if ctx is None:
        ctx = {}
    pprint.pprint(dump(object,depth,ctx))

def dumps(object, **kws):
    return pprint.pformat(dump(object, **kws))

