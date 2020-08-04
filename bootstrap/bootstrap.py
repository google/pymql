#!/usr/bin/env python
"""bootstrap -- dump and load a bootstrap from an existing graph."""

import json
import re
import sys

from absl import app
from absl import flags
from absl import logging

from pymql import MQLService

from pymql.mql import graph
from pymql.mql import lojson

FLAGS = flags.FLAGS
flags.DEFINE_string(
    'mqlenv', None, 'a dict in the form of a string which '
    'contains valid mql env key/val pairs')
flags.DEFINE_string('graphd_addr', 'localhost:9100',
                    'host:port of graphd server')
flags.DEFINE_string('load', '', 'load bootstrap from given file')


class BootstrapError(Exception):
  pass


class Bootstrap(object):
  version = 1

  def __init__(self, gc):
    self.gc = gc

  def load_from_file(self, filename):
    loadfile = open(filename, 'r')
    data = ''.join(loadfile.readlines())
    regex = re.compile('[\n\t]+')
    data = regex.sub(' ', data)
    loadfile.close()

    d = json.loads(data)
    if d['0_version'] != self.version:
      raise BootstrapError('version mismatch')

    self.bootstrap = d['1_bootstrap']
    self.nodes = d['2_nodes']
    self.links = d['3_links']

  def mkprim(self, **kwds):
    if 'scope' not in kwds and self.root_user:
      kwds['scope'] = self.root_user
    params = ' '.join(['%s=%s' % (k, v) for (k, v) in kwds.items()])
    result = self.gc.write_varenv('(%s)' % params, {})
    return result[0]

  def load_bootstrap(self):
    self.xlate = {}
    self.xlate_link = {}

    if len(self.gc.read_varenv('(pagesize=1 result=(guid))', {})):
      logging.fatal("Can't bootstrap a non-empty graph")

    self.root_user = None  # avoid forward ref in mkprim
    self.root_user = self.mkprim(name='"ROOT_USER"')
    self.root_namespace = self.mkprim(name='"ROOT_NAMESPACE"')
    self.has_key = self.mkprim(name='"HAS_KEY"')

    self.xlate[self.bootstrap['ROOT_USER']] = self.root_user
    self.xlate[self.bootstrap['ROOT_NAMESPACE']] = self.root_namespace
    self.xlate[self.bootstrap['HAS_KEY']] = self.has_key

  def load_root_user(self):
    # we dumped them separately, but we want to load them together...
    node_pos = 0
    link_pos = 0
    while node_pos < len(self.nodes) or link_pos < len(self.links):
      if link_pos >= len(
          self.links) or (node_pos < len(self.nodes) and
                          self.nodes[node_pos] < self.links[link_pos]['guid']):
        # we will do the next node
        node = self.nodes[node_pos]
        self.write_node(node)
        node_pos += 1
      else:
        link = self.links[link_pos]
        self.write_link(link)
        link_pos += 1

  def write_node(self, node):
    if node not in self.xlate:
      self.xlate[node] = self.mkprim()

  def write_link(self, link):
    new_link = {'datatype': link['datatype'], 'value': link['value']}
    for ptr in ('left', 'right', 'scope', 'typeguid'):
      # translate the link
      if ptr in link:
        if link[ptr] == 'null':
          new_link[ptr] = 'null'
        elif link[ptr] not in self.xlate:
          raise BootstrapError('Saw dangling link %s' % repr(link))
        else:
          new_link[ptr] = self.xlate[link[ptr]]
    new_link['guid'] = self.mkprim(**new_link)

    self.xlate_link[link['guid']] = new_link


def main(argv):
  if not FLAGS.graphd_addr:
    raise Exception('Must specify a --graphd_addr')

  conn = graph.TcpGraphConnector(addrs=[('localhost', 8100)])

  bootstrap = Bootstrap(conn)
  bootstrap.load_from_file(FLAGS.load)
  bootstrap.load_bootstrap()
  bootstrap.load_root_user()


if __name__ == '__main__':
  app.run(main)
