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
# lots of nasty hacks to get ids out of namespaces
#
# Nick -- I understand your point very well now...
#

from utils import valid_idname, valid_guid
from error import MQLParseError, MQLInternalError
from namespace import NameMap

from pymql.error import EmptyResult
from pymql.log import LOG

import mid


class NamespaceFactory:

  def __init__(self, querier):
    self.querier = querier
    self.guids = {}
    self.ids = {}
    self.namemap = NameMap(self.querier.gc)
    self.topic_en = None
    self.best_hrid_guid = None
    self.forbidden_namespaces = ()

  def flush(self):
    """
        Completely empty the caches.
        This takes care of flushing namespace.py caches as well.
        """

    self.guids = {}
    self.ids = {}
    self.namemap.flush()

  def preload(self, varenv):
    # load stuff that we know we will need later...
    if not self.topic_en:
      # it may be set to False after this...
      self.topic_en = self.lookup_guid("/en", varenv)

    if not self.best_hrid_guid:
      g = self.lookup_guid("/freebase/object_hints/best_hrid", varenv)
      if not isinstance(g, basestring) or g[0] != "#":
        raise MQLInternalError({}, "No /freebase/object_hints/best_hrid")
      self.best_hrid_guid = g

    if not self.forbidden_namespaces:
      self.forbidden_namespaces = \
          self.lookup_guids(("/wikipedia/en",
                             "/wikipedia/en_id",
                             "/wikipedia/de",
                             "/user/metaweb/datasource",
                             "/user/alecf/video_games",
                             "/user/avh/ellerdale",
                             "/base/zxspectrum/wos",
                             "/base/ourairports/ourairports_id",
                             "/authority",
                             "/source"),varenv).values()

  # this is the only part of low JSON that needs to resolve names.
  def lookup_guid(self, name, varenv):
    # check that we can't resolve it internally.

    if name.startswith("/m/"):
      guids = self.lookup_guids_of_mids([name], varenv)
      return guids.get(name, False)

    id_map = self.internal_lookup_checks([name])
    if id_map[name] is not None:
      return id_map[name]

    return self.namemap.lookup(name, varenv)

  def internal_lookup_checks(self, id_list):
    # we always use adorned guids here, except at the
    # very last stage in QueryPrimitive when
    # we generate the actual graph query itself.
    retval = {}
    for name in id_list:
      if isinstance(name, unicode):
        name = name.encode("utf-8")

      if not isinstance(name, str):
        raise MQLParseError(
            None, "didn't understand '%(id)s' as an id", id=str(name))

      # this implies raw guids are legal in :type and @id fields.
      # perhaps only the last is true. Perhaps not even that is true.
      elif valid_guid(name):
        retval[name] = name
      elif name.find("/guid/") == 0:
        retval[name] = self.internal_id_to_guid(name)
      elif valid_idname(name):
        retval[name] = None
      else:
        raise MQLParseError(None, "'%(id)s' is not a valid guid or id", id=name)

    return retval

  def internal_id_to_guid(self, id):
    # all we know going in is that we start /guid/
    if id.find("/guid/") == 0 and len(id) == 38 and valid_guid("#" + id[6:]):
      return "#" + id[6:]
    elif valid_idname(id):
      # well, it isn't a guid, but it's well formed so it just doesn't exist
      return False
    else:
      # it's a mess
      raise MQLParseError(None, "'%(id)s' is not a valid guid or id", id=id)

  def internal_guid_to_id(self, guid):
    # we only call this on a guid known to be OK
    return "/guid/" + guid[1:]

  def lookup_guids(self, id_list, varenv):
    # slightly hacky way to split out the things we can resolve here from the real idnames.
    id_map = self.internal_lookup_checks(id_list)
    mids = [m for m in id_list if m.startswith("/m/")]
    if mids:
      id_map.update(self.lookup_guids_of_mids(mids, varenv))
    varenv["gr_log_code"] = "id2guid"
    next_step = [id for id in id_map if id_map[id] is None]
    lookup_map = self.namemap.lookup_multiple(next_step, varenv)
    id_map.update(lookup_map)
    varenv.pop("gr_log_code")
    return id_map

  # nasty hacky function which contains "best available" namespace resolution.
  def lookup_id_internal(self, guid, varenv):
    if guid is None:
      return None

    elif guid in self.guids:
      return self.guids[guid]

    else:
      found_id = self.lookup_id_query(guid, varenv)
      self.guids[guid] = found_id
      return found_id

  # eek. see https://wiki.metaweb.com/index.php/Machine_IDs
  def lookup_guids_of_mids(self, mid_list, varenv):
    ask_list = set()
    result = {}
    rev = {}
    # arithmetically compute guids
    for m in mid_list:
      try:
        guid = "#" + mid.to_guid(m)
        ask_list.add(guid)
        # store the whole list here, down below we'll just
        # overwrite the things we got back.
        result[m] = guid  #self.internal_guid_to_id(guid)
        # i need to go back + forth.
        rev[guid] = m
      except (mid.InvalidMIDVersion, mid.InvalidMID) as e:
        result[m] = False
      except (mid.InvalidMunch) as e:
        raise MQLParseError(
            None, "'%(mid)s' is not a properly formatted mid", mid=m)

    if not len(ask_list):
      return result

    # i'm not caching these.
    LOG.debug(
        "mql.resolve.mids", "Looking up guids for mids", code=len(ask_list))
    # look for replaced by links off the guids
    # replaced_by links are unique, if they arent then this will signify some
    # end-of-the-world type event.
    query = [{"@guid": ask_list, "replaced_by": {"@guid": None}}]
    # read
    varenv["gr_log_code"] = "guids2mids"
    query_results = self.querier.read(query, varenv)
    varenv.pop("gr_log_code")
    # "now see what we found out..."
    for item in query_results:
      # [guid, replaced_by { guid }]
      guid = item["@guid"]
      rep_by = item["replaced_by"]["@guid"]
      m = rev[guid]
      result[m] = rep_by

    # pray.
    return result

  # harder
  def lookup_mids_of_guids(self, guid_list, varenv):
    # It's..sort of the same as before. We have some guids,
    # see if any of them are replaced_by.
    # If they are,
    if not guid_list:
      return {}

    ask_list = set()
    result = {}
    rev = {}
    for g in guid_list:
      # convert the mid directly.
      m = mid.of_guid(g[1:])
      ask_list.add(g)
      result[g] = [m]
      rev[m] = g

    LOG.debug("mql.lookup.mids", "Looking up mids for guids")

    # we look foward, up replaced_by links, and from that node
    # to other replaced_by links,
    # and backwards from the root, for previous ones.

    # +-+  r.b.    +-+
    # |A| -------> |B|
    # +-+          +-+
    #               |
    # +-+           |
    # |C|-----------+
    # +-+
    #
    # in this diagram, we root at B.
    # We list B first but also A and C if present.

    query = [{
        "@guid": ask_list,
        "@pagesize": len(ask_list) + 1,
        "-replaced_by": [{
            "@guid": None,
            ":optional": True
        }]
    }]

    varenv["gr_log_code"] = "mids2guids"
    query_results = self.querier.read(query, varenv)
    varenv.pop("gr_log_code")
    # each result is going to (hopefully) either haave a -replaced_by link
    # or a replaced_by one.
    for item in query_results:
      guid = item["@guid"]

      # otherwise, theres just links pointing at me.
      if item["-replaced_by"]:
        # me first
        result[guid] = [mid.of_guid(guid[1:])]
        # then everyone else
        for r in item["-replaced_by"]:
          result[guid].append(mid.of_guid(r["@guid"][1:]))

    return result

  def lookup_id(self, guid, varenv):
    # this function needs to have exactly the same semantics as
    # lookup_ids() (which now contains the "official" semantics)
    if isinstance(guid, unicode):
      guid = guid.encode("utf-8")

    return self.lookup_ids([guid], varenv)[guid]

  def lookup_ids(self, guid_list, varenv):
    """
        Given a list of guids returns an id for each one,
        using as few queries as possible.

        Returns a dictionary of guid->id.
        """

    ask_list = set()
    result = {}

    if not "asof" in varenv:
      # Step 1: maybe we already know.
      for guid in guid_list:
        if isinstance(guid, unicode):
          guid = guid.encode("utf-8")

        if guid in self.guids:
          LOG.debug(
              "mql.lookup.id.cached",
              "found %s in cache" % guid,
              value=self.guids[guid])
          result[guid] = self.guids[guid]
        elif guid not in ask_list:
          ask_list.add(guid)

      cache = len(ask_list) < 10000

    else:
      for guid in guid_list:
        if isinstance(guid, unicode):
          guid = guid.encode("utf-8")

        ask_list.add(guid)

      cache = False

    if not ask_list:
      return result

    LOG.debug("mql.lookup.ids", "Lookup ids", code=len(ask_list))

    self.preload(varenv)

    # Step 2: resolve the ask_list
    query = [{
        "@guid": ask_list,
        "@pagesize": len(ask_list) + 1,
        "best_hrid": [{
            ":typeguid": self.best_hrid_guid,
            ":value": None,
            ":optional": True,
        }],
        "-has_key": [{
            ":value":
                None,
            ":optional":
                True,
            ":comparator":
                "octet",
            ":pagesize":
                1000,
            "@guid":
                None,
            "-has_key": [{
                ":value":
                    None,
                ":optional":
                    True,
                ":comparator":
                    "octet",
                "@guid":
                    None,
                "-has_key": [{
                    ":value": None,
                    ":optional": True,
                    ":comparator": "octet",
                    "@guid": None,
                }]
            }]
        }],
        "is_instance_of": {
            "@id": "/type/namespace",
            ":optional": True
        }
    }]

    varenv["gr_log_code"] = "guid2id"
    query_results = self.querier.read(query, varenv)
    varenv.pop("gr_log_code")

    LOG.debug("mql.lookup.id.results", "", results=query_results)

    # now see what we found out...
    # these should be cached.
    leftover_guids = []
    for item in query_results:
      res = self.search_id_result(item, varenv)
      if res:
        result[item["@guid"]] = res

        if cache:
          self.guids[item["@guid"]] = res

    # every guid in guid_list has to be present in the result.
    for guid in guid_list:
      if guid not in result:
        LOG.debug("mql.lookup.id.notfound", "midifying %s" % guid)
        result[guid] = mid.of_guid(guid[1:])

    return result

  def search_id_result(self, head, varenv):
    """
        take the id result struct and attempt to produce an id.
        Here are the rules:

        - best_hrid is chosen if present
        - the shortest name is best
        - except that any three level name is better than a /boot name.
        - among names of the same length, pick any one at random.
        """

    hrids = head["best_hrid"]
    if hrids:
      if len(hrids) > 1:
        # This should never happen.
        # If it does, log an error but don't fail.
        LOG.error("mql.resolve.best_hrid",
                  "multiple /freebase/object_hints/best_hrid")
      hrid = hrids[0][":value"]
      return hrid

    # bfs_list format is an array of
    # ( value, parent, guid, keys, depth )
    bfs_list = [(None, None, head["@guid"], head.get("-has_key", []), 0)]
    root = self.namemap.bootstrap.root_namespace
    boot = self.namemap.bootstrap.boot
    is_namespace = False
    if isinstance(head["is_instance_of"], dict):
      is_namespace = True

    has_boot = None

    if head["@guid"] == root:
      return "/"
    elif head["@guid"] == boot:
      return "/boot"

    while bfs_list:
      front = bfs_list.pop(0)
      for item in front[3]:
        bfs_item = (item[":value"], front, item["@guid"],
                    item.get("-has_key", []), front[4] + 1)
        if bfs_item[2] == root:
          # we're done - what are we called?
          rv = []
          pos = bfs_item
          while pos[1]:
            rv.append(pos[0])
            pos = pos[1]

          return "/" + "/".join(rv)
        elif bfs_item[2] == boot:
          has_boot = bfs_item
        elif (self.topic_en and bfs_item[2] == self.topic_en and
              bfs_item[4] == 1):
          # hack for things *directly* in /en to short circuit early...
          return "/en/" + bfs_item[0]
        elif not is_namespace and bfs_item[2] in self.forbidden_namespaces:
          # terminate recursion at /wikipedia/en etc.
          pass
        else:
          bfs_list.append(bfs_item)

    # are we in /boot?
    if has_boot and has_boot[4] == 1:
      return "/boot/" + has_boot[0]

    # ok, we've searched the entire list. front is the last item...
    # try a regular lookup_id() on it. (so we can cache it too!)

    if front[4] == 3:
      leading_id = self.lookup_id_internal(front[2], varenv)
      if leading_id and leading_id[0] == "/":
        # we got something...
        rv = [leading_id]
        pos = front
        while pos[1]:
          rv.append(pos[0])
          pos = pos[1]

        return "/".join(rv)

    # failure
    return None

  def lookup_id_query(self, guid, varenv):
    """
        lots of nasty heuristics to find ids for id-identified objects:

        This seems to be called when lookup_ids can't finish the job,
        but it also seems to duplicate some of the logic there.

        Current rules:

        - do we have a /freebase/object_hints/best_hrid property
        - do we have a name in /
        - do we have a name in /XXX/YYY (XXX not boot or pub)
        - do we have a name in /XXX/YYY/ZZZ
        - do we have a name in /XXX/YYY (XXX may be boot or pub)
        - ask namespace.lookup_by_guid_oneoff...

        All of this trouble is mostly because some things (/type/object/type
        being the best example) have names in /bootstrap-namespace
        that we dont want to expose by accident.

        """

    query = {
        "@guid":
            guid,
        "best_hrid": [{
            ":typeguid": self.best_hrid_guid,
            ":value": None,
            ":optional": True,
        }],
        "has_root_name": [{
            ":type": "has_key",
            ":comparator": "octet",
            ":reverse": True,
            ":value": None,
            ":optional": True,
            "@id": "/"
        }],
        "has_2_level_name": [{
            ":type": "has_key",
            ":comparator": "octet",
            ":reverse": True,
            ":value": None,
            ":optional": True,
            "-has_key": [{
                ":comparator": "octet",
                ":value": None,
                "@id": "/"
            }]
        }],
        "has_3_level_name": [{
            ":type":
                "has_key",
            ":comparator":
                "octet",
            ":reverse":
                True,
            ":value":
                None,
            ":optional":
                True,
            "-has_key": [{
                ":comparator":
                    "octet",
                ":value":
                    None,
                "-has_key": [{
                    ":comparator": "octet",
                    ":value": None,
                    "@id": "/"
                }]
            }]
        }],
    }

    try:
      varenv["gr_log_code"] = "guid2id"
      result = self.querier.read(query, varenv)
      varenv.pop("gr_log_code")
    except EmptyResult:
      # everything was optional so we must not have found the guid itself
      # this code is unnecessary, but has key documentation value.
      raise

    # we may get nothing back if the guid has been deleted (or if we were deleting it)
    # in that case, just return the guid.
    if result is None:
      return guid

    idname = None

    hrids = result["best_hrid"]
    if hrids:
      if len(hrids) > 1:
        # This should never happen.
        # If it does, log an error but don't fail.
        LOG.error("mql.resolve.lookup_id_internal",
                  "multiple /freebase/object_hints/best_hrid")
      hrid = hrids[0][":value"]
      return hrid

    if result["has_root_name"]:
      idname = "/" + result["has_root_name"][0][":value"]

    elif (
        result["has_2_level_name"] and
        result["has_2_level_name"][0]["-has_key"][0][":value"] not in ("boot",
                                                                       "pub")):
      idname = "/" + result["has_2_level_name"][0]["-has_key"][0][
          ":value"] + "/" + result["has_2_level_name"][0][":value"]

    elif result["has_3_level_name"]:
      idname = (
          "/" +
          result["has_3_level_name"][0]["-has_key"][0]["-has_key"][0][":value"]
          + "/" + result["has_3_level_name"][0]["-has_key"][0][":value"] + "/" +
          result["has_3_level_name"][0][":value"])

    elif result["has_2_level_name"]:
      idname = "/" + result["has_2_level_name"][0]["-has_key"][0][
          ":value"] + "/" + result["has_2_level_name"][0][":value"]

    else:
      idname = self.namemap.lookup_by_guid_oneoff(guid, varenv)

    # special hack for the root namespace
    if idname == "/boot/root_namespace":
      return "/"
    elif idname == "/boot/root_user":
      return "/user/root"
    elif idname is not None and valid_idname(idname):
      return idname
    else:
      return guid
