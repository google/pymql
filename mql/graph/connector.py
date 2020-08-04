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

#
"""Abstract transport to graphd database.

The encapsulates all reads and writes from the graph
maintaining statistics about various behaviours,
and making modifications to the inner GQL based on
various inputs to varenv

   GraphConnector(): Container for all graph related operations.
   log_grw(): append to the in-mem/ varenv graph write log.
"""

__author__ = 'nicholasv@google.com (Nicholas Veeser)'

# None of these method names are standard
# pylint: disable-msg=C6409

from collections import defaultdict
import time

from absl import flags
from pymql.error import EmptyResult
from pymql.error import GraphConnectionError
from pymql.log import LOG
from pymql.mql.error import MQLDatelineInvalidError
from pymql.mql.error import MQLParseError
from pymql.mql.grparse import coststr_to_dict
from pymql.mql.grparse import gstr_unescape
from pymql.mql.utils import ReadMode
from pymql.mql.utils import valid_guid
from pymql.mql.utils import valid_timestamp
from pymql.mql.utils import WriteMode
from pymql.tid import generate_transaction_id

FLAGS = flags.FLAGS
flags.DEFINE_integer('graphd_default_query_timeout_tu', 30,
                     'max graphd tu value')


def log_grw(varenv, flag, dateline_in, dateline_out):
  """Log values of graph behaviour.

  These are for generating evidence about how
  datelines are used for freshness/consistency
  across queries in a single request.
  Args:
     varenv:
     flag:
     dateline_in:
     dateline_out:
  """

  if 'grwlog' not in varenv:
    # bad maybe - this varenv does
    # not seem to be managed by mss.
    varenv['grwlog'] = []
    grwlog = varenv['grwlog']

    # grwlog is for diagnostic purposes only
    # - don't let it grow arbitrarily large.
    MAX_GRWLOG_LEN = 100
    if len(grwlog) < MAX_GRWLOG_LEN:
      grwlog.append((flag, dateline_in, dateline_out))


def log_graph_read(varenv, dateline_in, dateline_out):
  log_grw(varenv, 'r', dateline_in, dateline_out)


def log_graph_write(varenv, dateline_in, dateline_out):
  log_grw(varenv, 'w', dateline_in, dateline_out)


def log_graph_status(varenv, dateline_in, dateline_out):
  log_grw(varenv, 's', dateline_in, dateline_out)


class GraphConnector(object):
  """Handle for all context of all queries to the graph.

  Here's the logic behind the 'retry' array:

  First time, don't sleep before we connect

  Second time, connection might have been stale,
  so try to reconnect after a short interval.
  At this point the client would just like us to
  give up. So that's what we do.

  Third time, connection wasn't stale -- it was
  really down. So sleep for 8 seconds (enough time
  for a restart) before giving up entirely.

  Subclasses should implement the following:
    open(self, policy): open any necessary resources
     (like a TCP connection)

    close(self): Close the graph connector and
      any underlying open resources

    transmit_query(self, query, policy, epoch_deadline):
       send a GQL message with the specified policy

  """

  # TODO(bneutra): strip out all the policy stuff
  # it's not used anymore
  DEFAULT_POLICY_NAME = 'default'

  def __init__(self,
               no_timeouts=False,
               policy_map=None,
               default_policy=None,
               custom_policy=None):

    self.reset_cost()
    self.timeout_policies = policy_map

    if default_policy:
      self.default_policy = default_policy
    else:
      self.default_policy = self.DEFAULT_POLICY_NAME

    self.no_timeouts = no_timeouts

    if custom_policy:
      LOG.info('gc.custom.timeout.policy', '', policy=str(custom_policy))
      self.timeout_policies['custom'] = custom_policy
      self.default_policy = 'custom'

  def open(self, policy=None):
    """Open any necessary resources (like a TCP connection)."""
    pass

  def close(self):
    """Close the graph connector and any underlying open resources."""
    pass

  def transmit_query(self, q, policy, epoch_deadline, **kwargs):
    """Transmit query to the graph.

    Args:
      q: graph query
      policy: map of various timeouts to use
      epoch_deadline: float of time left before query becomes invalid
    """
    _ = q, policy, epoch_deadline, kwargs
    raise NotImplemented

  def validate_policy_map(self, required_keys):
    """Validate that the policy_map has the correct format.

    The policy should be a map of maps, where each sub-map
    contains an entry for all the specified keys.

    keys = [ 'key1', 'key2' ]
    {
       'default' : {
            'key1' : value,
            'key2' : value,
            },
       'bootstrap' : {
            'key1' : value,
            'key2' : value,
            },
       ...
    },
    Args:
      required_keys: Keys that must be present in every policy.

    Raises:
       GraphConnectionError: If timeout_policies are incorrectly specified.
    """

    for k, m in self.timeout_policies.iteritems():
      if not isinstance(m, dict):
        raise GraphConnectionError('Policy map is specified incorrectly')

      for k in required_keys:
        if k not in m:
          raise GraphConnectionError('Policy %s must contain key: %s', k)

  def _get_default_policy(self):
    return self._default_policy

  def _set_default_policy(self, policy):
    if isinstance(policy, basestring):
      assert policy in self.timeout_policies, ('Policy Name is invalid: %s' %
                                               policy)
      self._default_policy = policy

    elif isinstance(policy, dict):
      self.timeout_policies['custom'] = policy
      self._default_policy = 'custom'

    else:
      raise Exception('Invalid policy specified')

  _doc_default_policy = """Set the default policy.

  Must be a policy key in the policy map
  or
  a map of maps specifying a custom policy
  """

  default_policy = property(_get_default_policy, _set_default_policy, None,
                            _doc_default_policy)

  def reset_cost(self):

    LOG.debug('resetting graphd costs')
    # these 3 counters remain for backward compatiblity
    self.nrequests = 0
    # -1 because the first attempt is not really a 'retry'
    self.dbretries = -1
    self.qretries = -1

    # all cost info is tracked in this dict
    # this includes cost info returned by GQL
    self.totalcost = defaultdict(float)

  def _get_policy(self, policy=None):
    if policy is None:
      return self.timeout_policies[self.default_policy]
    elif isinstance(policy, basestring):
      return self.timeout_policies[policy]
    elif isinstance(policy, dict):
      return policy

    else:
      raise GraphConnectionError(
          'Bad Context Policy',
          http_code=500,
          app_code='/mql/backend/bad_context_policy')

  def status(self, varenv, name):
    """Send a simple status query to the graph."""

    dateline_in = None
    transaction_id = varenv.get('tid')
    r = self.transmit_query('status id="%s" (%s)\n' % (transaction_id, name),
                            varenv)
    dateline_out = r.dateline
    log_graph_status(varenv, dateline_in, dateline_out)

    def fix_quoting(l):
      if isinstance(l, list):
        return [fix_quoting(x) for x in l]
      elif isinstance(l, str):
        if l[0] == '"':
          return gstr_unescape(l)
        else:
          return l

    return fix_quoting(r)

  def _generate_and_transmit_query(self, gql, varenv, mode):
    """Generate Modifiers for "envelope" of query and send."""

    policy = self._get_policy(varenv.get('policy'))
    # epoch_deadline is passed in by the caller
    # its unix epoch float by which time all work must be done here.
    epoch_deadline = varenv.get('epoch_deadline', None)
    modifiers = []

    # we always set a maximum graphd 'user time' in ms
    # value.  It represents work done by graphd.
    query_timeout_tu = varenv.get('query_timeout_tu')
    if isinstance(query_timeout_tu, (int, long)):
      tu_max = query_timeout_tu
    else:
      tu_max = FLAGS.graphd_default_query_timeout_tu
    cost = '"tu=%d"' % tu_max

    modifiers.append(('cost', cost))

    # Modifier: Dateline
    #
    dateline_in = varenv.get('write_dateline', None) or ''

    if mode is ReadMode:
      dateline = '"' + dateline_in + '"'
    else:
      dateline = '""'

    modifiers.append(('dateline', dateline))

    # Modifier: AsOf
    #
    # It used to be the case that we'd check the varenv for this
    # 'mql_query' thing. It was used to signal to the gc when to use
    # an asof if one existed in the varenv.
    # That was silly, though, we really want to cut the database off
    # if someone asked us to, so if there's an asof, then there's an asof,
    # all your queries are modified with the asof - not just some of them.
    if varenv.get('asof') and mode is ReadMode and varenv.get('mql_query'):
      if valid_guid(varenv.get('asof')):
        modifiers.append(('asof', varenv.get('asof')[1:]))
      elif valid_timestamp(varenv.get('asof')):
        modifiers.append(('asof', varenv.get('asof')))
      else:
        raise MQLParseError(
            None, 'asof must be a valid guid '
            'or timestamp not %(asof)s',
            asof=varenv.get('asof'))

    # Modifier: Tid
    #   Legacy Transaction Id
    #
    transaction_id = varenv.get('tid')
    if transaction_id is None:
      transaction_id = generate_transaction_id('graph_%s' % str(mode))
    modifiers.append(('id', '"%s"' % transaction_id))

    modifiers = ' '.join(('%s=%s' % x) for x in modifiers)

    # set up quota user:
    quota_user_id = None
    if varenv.get('project_id'):
      quota_user_id = 'project_id:' + varenv.get('project_id')

    is_continuation = varenv.get('is_write_continuation', False)
    is_idempotent = varenv.get('is_idempotent', False)

    full_query = '%s %s %s' % (mode, modifiers, gql)
    return self.transmit_query(
        full_query,
        policy,
        epoch_deadline,
        quota_user_id=quota_user_id,
        continuation=is_continuation,
        idempotent=is_idempotent)

  def read_varenv(self, qs, varenv):
    """Read from the graph the specified "query"."""
    try:
      # the pymql user provides a 'write_dateline', which should be a valid
      # dateline returned to said user by a previous mqlwrite query
      dateline_in = varenv.get('write_dateline', None)

      r = self._generate_and_transmit_query(qs, varenv, ReadMode)

    except MQLDatelineInvalidError:
      # Drop the datelines out of the varenv,
      # re-generate the query and try again.
      # the main use case here is when sandbox is refreshed
      # and the instance id in the dateline changes. The user's dateline
      # (usually in a cookie) is now invalid until they do a write, or a touch
      LOG.info('mqlread.dateline.delete',
               'got an invalid dateline, deleting from varenv',
               varenv.get('write_dateline'))
      varenv['write_dateline'] = ''

      r = self._generate_and_transmit_query(qs, varenv, ReadMode)

    if not r and varenv.get('graph_noisy'):
      raise EmptyResult('query %s' % qs)

    dateline_out = r.dateline

    # 'dateline' is returned to the original caller of pymql read.
    # though, in practice, it is not passed on by frapi and
    # they really should only update their dateline after doing
    # a write.
    # we do *not* update the internal 'write_dateline' varenv, here.
    # in the case of reads, the idea being the user only needs to
    # demand the level of freshness of their last write, so
    # subsequent reads in this session will use the original
    # 'write_dateline' provided by the caller of pymql read/write.
    # the 'write_dateline' is updated in the event that a write
    # occurs in this session.
    varenv['dateline'] = dateline_out
    log_graph_read(varenv, dateline_in, dateline_out)

    LOG.debug('graph.dateline.set', '', dateline=varenv['dateline'])

    return r

  def write_varenv(self, qs, varenv):
    """Write to the graph the specified "query"."""

    if getattr(self, 'readonly', None):
      raise GraphConnectionError(
          'Tried to write to a read-only graph',
          http_code=500,
          app_code='/mqlwrite/backend/read_only')

    dateline_in = varenv.get('write_dateline', None)

    self.write_occurred = 1

    try:
      r = self._generate_and_transmit_query(qs, varenv, WriteMode)

    except MQLDatelineInvalidError:
      # see read_varenv comment on this
      LOG.info('mqlwrite.dateline.delete',
               'got an invalid dateline, deleting from varenv',
               varenv.get('write_dateline'))
      varenv['write_dateline'] = ''

      r = self._generate_and_transmit_query(qs, varenv, WriteMode)

    dateline_out = r.dateline

    # update our write_dateline in case we do subsequent reads
    # or writes. The new 'write_dateline' is returned to the
    # user for use with subsequent mqlreads or mqlwrites they do
    varenv['write_dateline'] = dateline_out
    varenv['last_write_time'] = time.time()
    log_graph_write(varenv, dateline_in, dateline_out)

    LOG.debug(
        'graph.write_dateline.set',
        '',
        last_write_time=varenv['last_write_time'],
        write_dateline=varenv['write_dateline'])

    # Record that a write has happened and following writes should set
    # the continuation flag.
    varenv['is_write_continuation'] = True

    return r

  def add_graph_costs(self, costs, dbtime, tries):
    """feed costs from graphdb into self.totalcost."""

    request_cost = coststr_to_dict(costs)
    request_cost['mql_dbtime'] = dbtime
    request_cost['mql_dbtries'] = tries or 1
    request_cost['mql_dbreqs'] = 1
    LOG.debug('graphdrequest.cost %s', request_cost)
    for k, v in request_cost.iteritems():
      if k in ['mm', 'fm']:
        # These are high water marks. Don't sum them.
        self.totalcost[k] = max(v, self.totalcost.get(k))
      else:
        if k in self.totalcost:
          self.totalcost[k] += v
        else:
          self.totalcost[k] = v
