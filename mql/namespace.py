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
# a namespace maps identifiers to guids
#
# an identifier is a string, "short enough to fit into a graph primitive"
#

import sys,os
if __name__ == '__main__':
    sys.path.append(os.path.abspath('../..'))


from pymql.log import LOG
from pymql.tid import generate_transaction_id
from grquoting import quote, unquote


from error import MQLInternalError, MQLParseError

# Maximum segments in an id.
MAX_ID_PARTS = 200

# this is only used in the multiple id lookup case.
class InternalNsMap(dict):
    def __init__(self,guid,namespace):
        self.guid = guid
        self.namespace = namespace
        # is everything about me and my children known?
        self.needed = False

class Namespace(object):
    def __init__(self):
        self.byname = {}

    def store(self, name, g):
        if g:
            self.byname[name] = g

    def lookup(self, name, varenv):
        if name in self.byname:
            return self.byname[name]

        return False

class BootNamespace(Namespace):
    '''Namespace to hold the initial two primitives, "/" and "has_key"
    '''

    def __init__(self, namemap):
        Namespace.__init__(self)

        # we require the following names to bootstrap
        tid = generate_transaction_id("boot_namespace")
        qs = '(name="ROOT_NAMESPACE" left=null right=null result=((contents)) ' + \
                 '(<-left typeguid->(name="HAS_KEY") comparator="octet" value="boot" scope->(name="ROOT_USER") result=(left typeguid scope right)) ' + \
                 '(<-left optional typeguid->(name="HAS_KEY") result=((value right)) )' + \
                 '(<-left optional typeguid->(name="HAS_KEY" result=()) comparator="octet" value=("boot" "type" "lang") result=((contents)) ' + \
                     'right->( left=null right=null result=((guid contents)) ' + \
                         '(<-left optional typeguid->(name="HAS_KEY") result=((value right)) ) ) ) )'
                           
        
        result = namemap.gc.read_varenv(qs, { "tid": tid, "policy": "bootstrap" })
        if len(result):
            self.root_namespace, self.has_key, self.root_user, self.boot = [ ('#' + x) for x in result[0][0] ]            
            
            self.store('/',self.root_namespace)
            self.store('has_key',self.has_key)
            self.store('root_user',self.root_user)
            self.store('boot',self.boot)

            # did we get any of the optional stuff?
            if result[0][1]:
                root_ns = namemap.get_or_add_namespace(self.root_namespace)
                root_ns.update_from_graph(result[0][1])

            for subresult in result[0][2]:
                guid = '#' + subresult[1][0][0]
                ns = namemap.get_or_add_namespace(guid)
                ns.update_from_graph(subresult[1][0][1])
            
        else:
            raise MQLInternalError(None,"can't find ROOT_NAMESPACE and HAS_KEY in graph!")


class NamespaceConcept(Namespace):
    '''
    a concept namespace is stored in the graph.
    note that it requires the boot namespace in order to
    load, because it uses the has_key attribute.

    create NamespaceConcepts using NamespaceCache.get(), not NamespaceConcept()
    '''

    def __init__(self, namemap, g):
        Namespace.__init__(self)

        self.namemap = namemap
        self.guid = g
        self.last_dateline = None

        # 1 if the namespace has been completely loaded from the graph
        # 0 if the namespace is too large to cache
        # -1 if we haven't checked yet
        self.complete = -1

        # if there are more than this many entries in a namespace,
        #  we don't try to cache the whole thing
        self.max_complete = 200


    def lookup(self, key, varenv):
        # if we have it, don't go any further
        if key in self.byname:
            return self.byname[key]

        # try to fetch the whole thing.
        if self.complete == -1:
            self.refresh(varenv)

        if key in self.byname:
            val = self.byname[key]
        else:
            val = self.fetch(key, varenv)

        return val

    def fetch(self, key, varenv):
        '''fetch a single namespace entry from the graph'''

        args = (self.namemap.bootstrap.has_key[1:], self.guid[1:], quote(key))
        qs = '(typeguid=%s left=%s value=%s comparator="octet" datatype=string pagesize=2 result=((value left right)))' % args
        
        r = self.namemap.gc.read_varenv(qs, varenv)

        # we asked for pagesize=2 just to check this
        if len(r) > 1:
            LOG.warning('mql.duplicate.key', '%s in namespace %s' % (key, self.guid))
        elif len(r) == 0:
            return False

        (value, nsg, g) = (unquote(r[0][0]), '#' + r[0][1], '#' + r[0][2])
        if nsg != self.guid:
            raise MQLInternalError(None,"Mismatched namespace query",value=value,namespace=self.guid,returned_namespace=nsg,guid=g)


        # this assert can fail because of graphd case-insensitivity
        #assert unquote(value) == key, "%s != %s" % (unquote(value), key)

        self.store(key, g)
        return g

    def refresh(self, varenv):
        '''try to refresh a complete namespace from the graph.
        set self.complete iff successful. '''

        # see if we can fetch the whole thing:
        # for large namespaces, don't try to fetch the whole thing
        if not self.complete:
            return

        datelineqs = ''
        if self.last_dateline is not None:
            datelineqs = 'dateline>%s' % self.last_dateline
        args = (self.namemap.bootstrap.has_key[1:], self.guid[1:], datelineqs, self.max_complete+1)
        qs = '(typeguid=%s left=%s comparator="octet" datatype=string %s pagesize=%d result=((value left right)))' % args

        r = self.namemap.gc.read_varenv(qs, varenv)

        # check if we hit the maximum size for cacheable namespaces
        if len(r) > self.max_complete:
            LOG.notice('mql.namespace.refresh', '%s too large to cache' % self.guid)
            self.complete = 0
        elif self.complete == -1:
            self.complete = 1

        if len(r) > 0:
            self.update_namespaces(r)
        else:
            if self.last_dateline is not None:
                # XXX should extract the dateline from the empty result and
                #  update self.last_dateline in order to minimize the dateline
                #  ranges in later queries.
                pass


    def update_from_graph(self,kv_list):
        '''
        take a list of (value,guid) tuples and add them to this namespace

        this allows external routines that gain knowledge to tell the namespace cache all about it!
        '''
        for (graph_value, graph_guid) in kv_list:
            self.store(unquote(graph_value),"#" + graph_guid)
            
           
    def update_namespaces(self, r):
        '''update this namespace cache based on the result of a graph query'''

        self.last_dateline = r.dateline

        for entry in r:
            (name, nsg, g) = (unquote(entry[0]), '#' + entry[1], '#' + entry[2])
            assert nsg == self.guid
            self.store(name,g)
        LOG.debug('updated namespace %s' % self.guid, '%d entries' % len(r))


class NameMap:
    '''a NameMap is a search path of Namespaces.
       the global namemap is found at gc.namemap, it is likely to contain:
          the true bootstrap namespace (guids of / and has_key)
          the metaweb bootstrap_namespace (contents of /boot)
    '''
    def __init__(self, gc, bootstrap=True):
        self.gc = gc
        self.namespaces = {}
        
        if bootstrap:
            self.bootstrap = BootNamespace(self)

    def refresh(self,varenv):
        for nsc in self.namespaces.itervalues():
            nsc.refresh(varenv)

    def flush(self):
        '''
        Empty the namespace stack completely except
        for the bootstrap namespace.
        '''
        
        self.namespaces = {}

    def lookup_multiple(self, id_list, varenv):
        '''lookup_multiple(id_list) returns a map from the listed ids to guids.
        '''

        # build the id_list dictionary tree
        ns_map = self.build_ns_dict_tree(id_list)

        # now we have a dictionary of lookups and their guids (if any) and namespace objects (if any)
        # let's go build a query for the missing stuff...
        query = self.recursive_ns_query_build(ns_map)
        if len(query):
            query = '(guid=%s result=(guid contents) %s)' % (
                self.bootstrap.root_namespace[1:], ''.join(query)
            )
            result = self.gc.read_varenv(query,varenv)
            self.recursive_ns_result_parse(result,ns_map)

        # now return all the results which must be in the tree now.
        retval = {}
        for id in id_list:
            retval[id] = self.lookup(id,varenv)

        return retval

    def build_ns_dict_tree(self,id_list):
        # create a nested dictionary of lookups.
        ns_map = InternalNsMap(self.bootstrap.root_namespace,
                               self.get_or_add_namespace(self.bootstrap.root_namespace))
        
        for id in id_list:
            split_id = id.split('/')
            if len(split_id) > MAX_ID_PARTS:
                raise MQLParseError(
                    id, 
                    "Id has too many segments. Maximum is %s" % (MAX_ID_PARTS,))

            if split_id[0] != '':
                # didn't start with a /
                raise MQLInternalError(None,"Invalid id %(id)s passed to lookup",id=id)

            # ignore trailing /.
            if split_id[-1] == '':
                # paths should have gone through the parser first, so the
                # only time this should happen is when the path is '/'
                #assert len(ids) == 2
                split_id = split_id[:-1]


            iddict = ns_map

            for key in split_id[1:]:
                if key not in iddict:
                    ns = None
                    if iddict.namespace:
                        guid = iddict.namespace.byname.get(key,None)
                        if guid:
                            ns = self.get_or_add_namespace(guid)
                    
                    iddict[key] = InternalNsMap(guid,ns)
                    
                iddict = iddict[key]

        return ns_map

    def recursive_ns_query_build(self,iddict):
        final_query = []
        for key in iddict:
            needed = False
            query = ['(<-left optional typeguid=%s comparator="octet" value="%s" ' % (
                    self.bootstrap.has_key[1:], key)]
            if len(iddict[key]):
                next = self.recursive_ns_query_build(iddict[key])
                if len(next):
                    needed = True
                    query.append('result=contents right->(result=(guid contents) ')
                    query += next
                    query.append(') ')
            elif not iddict[key].guid:
                # a terminal we need the result for.
                needed = True
                query.append('result=(right) ')
            else:
                # a terminal we already know about.
                needed = False

            query.append(') ')
            if needed:
                iddict[key].needed = True
                final_query += query
                
        return final_query

    def recursive_ns_result_parse(self,result,iddict):
        # we might get a list or a single null, or a [ null ]
        if result == 'null' or result[0] == 'null':
            # there was nothing here, bail recursively
            iddict.guid = False
            for key in iddict:
                self.recursive_ns_result_parse('null',iddict[key])
            return
        else:
            iddict.guid = '#' + result[0]
            
        i = 1

        if not iddict.namespace:
            iddict.namespace = self.get_or_add_namespace(iddict.guid)
        
        for key in iddict:
            if iddict[key].needed:
                # we generated you, so we have results.
                self.recursive_ns_result_parse(result[i],iddict[key])                
                iddict.namespace.store(key,iddict[key].guid)
                i += 1

    def lookup(self, id, varenv):
        '''lookup(id) returns the guid named by id.
        if id contains '/' characters, it is interpreted as a chain of has_key
        lookups.  a leading / starts at the root namespace.
        '''

        ids = id.split('/')
        if ids[0] != '':
            # didn't start with a /
            raise MQLInternalError(None,"Invalid id %(id)s passed to lookup",id=id)

        # ignore trailing /.
        if ids[-1] == '':
            # paths should have gone through the parser first, so the
            # only time this should happen is when the path is '/'
            #assert len(ids) == 2
            ids = ids[:-1]
            
        g = self.bootstrap.root_namespace

        for key in ids[1:]:
            # XXX fill this in
            # see if this is a namespace
            # if so, open it up and descend

            ns = self.get_or_add_namespace(g)
            g = ns.lookup(key, varenv)
            if g == False:
                return False

        return g

    def get_or_add_namespace(self,g):
        if g not in self.namespaces:
            nsconcept = NamespaceConcept(self, g)
            self.namespaces[g] = nsconcept

        return self.namespaces[g]        

    def lookup_by_guid_oneoff(self,g, varenv):
        root_ns_guid = self.bootstrap.root_namespace
        res = []
        name = []
        found = set()
        next = g
        # this is ridiculously deep - 18 ply.
        for i in xrange(6):
            res = self.lookup_by_guid_oneoff_internal(next, varenv)

            if not res:
                # we've ceased to make progress - bail 
                LOG.warning('mql.namespace.error','id for guid not found', guid=g)
                return g
                            
            for pair in res:
                name.append(pair[0])
                next = pair[1]
                if next == root_ns_guid:
                    name.reverse()
                    return "/" + "/".join(name)

                if next in found:
                    LOG.warning('mql.namespace.error','cycle in namespace looking for guid', guid=g)
                    return g

                found.add(next)                

        LOG.warning('mql.namespace.error','depth limit exceeded in namespace lookup', guid=g)
        return g                
                    
    def lookup_by_guid_oneoff_internal(self,g, varenv):
        # resolve 3 deep in one go...
        # relies on the boot/root cycle...

        # XXX temporary fix to get around graphd optimizer bug. Shouldn't last beyond 2006-09-01.
        
        qs = '''(right=%(guid)s
                  typeguid=%(has_key)s
                  comparator="octet"
                  pagesize=1
                  result=(value left contents)
                  left->(
                    result=contents
                    (<-right
                       typeguid=%(has_key)s
                       comparator="octet"
                       pagesize=1
                       result=(value left contents)
                       left->(
                         result=contents
                         (<-right
                           typeguid=%(has_key)s
                           comparator="octet"
                           pagesize=1
                           result=(value left guid)
                         )))))''' % { 'guid': g[1:], 'has_key': self.bootstrap.has_key[1:] }



        r = self.gc.read_varenv(qs, varenv)
        res = []
        while len(r) and isinstance(r,list):
            res.append([unquote(r[0]),'#' + r[1]])
            r = r[2]

        return res
