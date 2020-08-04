#!/usr/bin/python2.6
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
"""Backward compatible support for mql LOG calls."""

__author__ = 'bneutra@google.com (Brendan Neutra)'

import datetime
import os
import socket
from pymql.log import log_util
from pymql.util import dumper
from absl import logging

__all__ = ['generate_tid', 'LOG', 'pprintlog', 'dumplog']

# allow google logging to discover the caller
# i.e. ignore these local functions
skip = [
    '_logit', 'fatal', 'error', 'warn', 'info', 'debug', 'spew', 'exception',
    'warning', 'alert', 'notice', 'log', 'pprintlog', 'dumplog'
]


def _logit(level, s, args=None, kwargs=None):
  # let's not waste any cycles
  if level > logging.get_verbosity():
    return
  msg = ''
  if args:
    msg += '\t'.join(str(arg) for arg in args)
  if kwargs:
    msg += '\t'.join('%s=%s' % (pair) for pair in kwargs.iteritems())
  logging.vlog(level, '%s %s' % (s, msg))


class LOG(object):

  @staticmethod
  def fatal(s, *args, **kwargs):
    _logit(logging.FATAL, s, args, kwargs)

  @staticmethod
  def error(s, *args, **kwargs):
    _logit(logging.ERROR, s, args, kwargs)

  @staticmethod
  def warn(s, *args, **kwargs):
    _logit(logging.WARN, s, args, kwargs)

  @staticmethod
  def info(s, *args, **kwargs):
    _logit(logging.INFO, s, args, kwargs)

  @staticmethod
  def debug(s, *args, **kwargs):
    _logit(logging.DEBUG, s, args, kwargs)

  @staticmethod
  def spew(s, *args, **kwargs):
    _logit(log_util.SPEW, s, args, kwargs)

  @staticmethod
  def log(level, s, *args, **kwargs):
    _logit(level, s, args, kwargs)

  exception = fatal
  notice = info
  warning = warn
  alert = warn


def dumplog(string, obj, level=log_util.SPEW):
  if level <= logging.get_verbosity():
    LOG.log(level, string, dumper.dumps(obj))


def pprintlog(string, obj, level=log_util.DEBUG, **kwargs):
  if level <= logging.get_verbosity():
    LOG.log(level, string, repr(obj))


tid_seqno = 0
hostname = socket.getfqdn()
del socket
pid = os.getpid()


def generate_tid(token=None, hostport=None):
  global tid_seqno

  # can't determine port without looking at WSGI environ or apache
  # config? perhaps we could read this from a config file?
  if not hostport:
    hostport = '%s:0' % hostname
  # hostport could be just a port, we prefix it with hostname then
  elif isinstance(hostport, (int, long)):
    hostport = '%s:%d' % (hostname, hostport)
  elif ':' not in hostport:
    hostport = '%s:%s' % (hostname, hostport)

  if not token:
    token = 'me'

  # small race condition here
  tid_seqno += 1

  return ('%s;%s;%05d;%sZ;%04d' %
          (token, hostport, pid, datetime.datetime.utcnow().isoformat('T'),
           tid_seqno))
