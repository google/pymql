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

import re
from datetime import datetime, timedelta

from pymql.log import LOG
from pymql.mql.error import MQLAccessError
from pymql.mql.error import MQLWriteQuotaError

# This object is very very special. If you give it to me as the user
# field, you get to bypass all the access control checks.
# This means (at least) that you need to be in the same python
# environment as me (or you need to be using eval() -- snicker)
Privileged = object()

# this object when passed as the $privileged field
# enables you to pass another user id as the $authority field
# the write will still be attributed to $user, but $authority's
# permissions will be checked in addition to $user's.
Authority = object()

# this is a very special case because user creation cannot be escalated as a
# privilege to anybody else. created a list because there might be other
# catch-22 situations
MAX_WRITE_EXCEPTED_USERS = [
    '#9202a8c04000641f80000000000000aa',  # /user/user_administrator
]


class Permission(object):
  """
    A little wrapper around a permission guid that allows us to break
    out the actual permission queries from the rest of lojson.

    This object is more or less temporary - don't keep it anywhere
    """

  def __init__(self, querier, guid):
    self.querier = querier
    self.guid = guid

  def user_has_permission_permission(self, userguid, varenv):
    """
        Can the user administer objects with this permission?
        """
    # do this in one query.
    # don't cache the results at the moment.
    # (if non-caching is a problem we can always cache later.)
    query = {
        '@guid': self.guid,
        'is_instance_of': {
            '@id': '/type/permission'
        },
        'has_write_group': [{
            'is_instance_of': {
                '@id': '/type/usergroup'
            },
            # should check /type/user as the user loader does
            # not.
            'has_member': {
                '@guid': userguid,
                'is_instance_of': {
                    '@id': '/type/user'
                }
            },
            ':optional': False,
        }],
        'has_permission': {
            '@guid':
                None,
            'is_instance_of': {
                '@id': '/type/permission'
            },
            'has_write_group': [{
                'is_instance_of': {
                    '@id': '/type/usergroup'
                },
                # should check /type/user as the user loader
                # does not.
                'has_member': {
                    '@guid': userguid,
                    'is_instance_of': {
                        '@id': '/type/user'
                    }
                },
                ':optional': False,
            }]
        }
    }
    result = self.querier.read(query, varenv)

    # slight paranoia - result is not None should be enough; but
    # optional=false might break in the future.
    if (result is not None and
        result['has_write_group'][0]['has_member']['@guid'] == userguid and
        result['has_permission']['has_write_group'][0]['has_member']['@guid'] ==
        userguid):
      return True

    return False

  # XXX revokation should work properly...
  def user_has_write_permission(self, userguid, varenv):
    """
        Can the user write to objects with this permission?
        """
    # this is the only query ever run to verify permissions, so it
    # should be properly paranoid.
    #
    # Currently it checks the following.
    #
    # 0 - the user actually is a member of a group with permission!
    # 1 - the permission is of /type/permission
    # 2 - the attached group is of /type/usergroup
    # 3 - the attached user is of /type/user
    #
    # It is possible we put futher restrictions on what it is to
    # be a valid user, valid permission and/or valid group in the
    # future. Perhaps you need to be in the /user namespace as a
    # user. Perhaps groups and permissions also have namespaces.

    query = {
        '@guid':
            self.guid,
        'is_instance_of': {
            '@id': '/type/permission'
        },
        'has_write_group': [{
            'is_instance_of': {
                '@id': '/type/usergroup'
            },
            # should check /type/user as the user loader does not.
            'has_member': {
                '@guid': userguid,
                'is_instance_of': {
                    '@id': '/type/user'
                }
            },
            ':optional': False,
        }]
    }
    result = self.querier.read(query, varenv)

    # slight paranoia - result is not None should be enough; but
    # optional=false might break in the future.
    if (result is not None and
        result['has_write_group'][0]['has_member']['@guid'] == userguid):
      return True

    return False


#
# we're a bit more restrictive with user-ids than with regular ids
# we insist on lower-case only, and a max-len of 38 (32 significant characters)
#
__userid_re = re.compile('^/user/[a-z](?:_?[a-z0-9])*$')


def valid_user_id(userid):
  return __userid_re.match(userid) and len(userid) <= 38


def check_attribution_to_user(querier, varenv, attributionguid):
  query = {
      '@guid': attributionguid,
      '@scope': varenv.get_user_guid(),
      'is_instance_of': {
          '@id': '/type/attribution'
      }
  }
  result = querier.read(query, varenv)
  if result is None:
    return False
  else:
    return True


def check_write_defaults(querier, varenv):
  """
    It is painful to deal with $user, $permission, $authority and $attribution
    all the time, so this function verifies them and the sets them to member
    variables.
    """

  if not varenv.get_user_guid():
    raise MQLAccessError(
        None, 'You must specify a valid user to write', user=None)

  # must check authority before permission as authority affects the check_permission() call later
  if varenv.get('$authority'):
    if not varenv.get('$privileged') is Authority:
      # *****************************************************************************************************************
      raise MQLAccessError(
          None,
          'user %(user)s cannot use authority %(authority)s without scope.Authority',
          user=varenv.get_user_id(),
          authority=varenv.get('$authority'))
      # *****************************************************************************************************************

    varenv.authority_guid = querier.lookup.lookup_guid(
        varenv.get('$authority'), varenv)
  else:
    varenv.authority_guid = None

  if varenv.get('$permission'):
    permission_guid = querier.lookup.lookup_guid(
        varenv.get('$permission'), varenv)
    if not check_permission(querier, varenv, permissionguid=permission_guid):
      # *****************************************************************************************************************
      raise MQLAccessError(
          None,
          'User %(user)s cannot create with permission %(permission)s',
          user=varenv.get_user_id(),
          permission=permission_guid)
      # *****************************************************************************************************************

    # permission checks out OK (this implies the user checked out OK too)
    varenv.default_permission_guid = permission_guid
  else:
    # *****************************************************************************************************************
    raise MQLAccessError(
        None,
        'You must specify a default permission to write with',
        permission=None)
    # *****************************************************************************************************************

  if varenv.get('$attribution'):
    attribution_guid = querier.lookup.lookup_guid(
        varenv.get('$attribution'), varenv)
    if not check_attribution_to_user(querier, varenv, attribution_guid):
      # *****************************************************************************************************************
      raise MQLAccessError(
          None,
          'User %(user)s cannot attribute to a node  %(attribution)s that they did not create, or is not of type /type/attribution',
          user=varenv.get_user_id(),
          attribution=varenv.get('$attribution'))
      # *****************************************************************************************************************

    # attribution checks out OK
    varenv.attribution_guid = attribution_guid
  else:
    varenv.attribution_guid = varenv.get_user_guid()


def check_permission(querier, varenv, permissionguid):
  """
    Check if the user can write to objects permitted by permission_guid
    """
  write_permission = varenv.setdefault('write_permission', {})

  if permissionguid not in write_permission:
    userguid = varenv.get_user_guid()
    authorityguid = varenv.authority_guid
    permission = Permission(querier, permissionguid)

    has_access = permission.user_has_write_permission(userguid, varenv)
    if not has_access and authorityguid:
      has_access = permission.user_has_write_permission(authorityguid, varenv)
      if has_access:
        LOG.notice(
            'access.authority', 'for user %s, permission %s and authority %s' %
            (userguid, permissionguid, authorityguid))

    if not has_access and varenv.get('$privileged') is Privileged:
      LOG.notice('access.privileged',
                 'for user %s and permission %s' % (userguid, permissionguid))
      has_access = True

    write_permission[permissionguid] = has_access

  return write_permission[permissionguid]


def check_change_permission_by_user(querier, varenv, old_permission_guid,
                                    new_permission_guid):

  has_old_permission = \
      check_permission_permission(querier, varenv, old_permission_guid)
  has_new_permission = \
      check_permission_permission(querier, varenv, new_permission_guid)

  # privileged access bypass
  if varenv.get('$privileged') is Privileged:
    LOG.notice(
        'access.privileged', 'for user %s changing permission %s to %s' %
        (varenv.get_user_guid(), old_permission_guid, new_permission_guid))
    return True

  # no privileged block because I don't have any need to
  # privilege this operation (yet) when there is a need a
  # privileged block can be put here.
  return has_old_permission and has_new_permission


def check_permission_permission(querier, varenv, permission_guid):
  """
    Check if the user has permission to administer the given permission
    """
  permission_permission = varenv.setdefault('permission_permission', {})

  if permission_guid not in permission_permission:
    userguid = varenv.get_user_guid()
    authorityguid = varenv.authority_guid
    permission = Permission(querier, permission_guid)

    has_access = permission.user_has_permission_permission(userguid, varenv)
    if not has_access and authorityguid:
      has_access = permission.user_has_permission_permission(
          authorityguid, varenv)
      if has_access:
        LOG.notice(
            'access.authority', 'for user %s, permission %s and authority %s' %
            (userguid, permission_guid, authorityguid))

    permission_permission[permission_guid] = has_access

  return permission_permission[permission_guid]


def check_write_throttle(querier, varenv):
  userguid = varenv.get_user_guid()
  max_writes = varenv.get('max_writes', None)
  if max_writes is None or userguid in MAX_WRITE_EXCEPTED_USERS:
    LOG.error('write.throttle.skipped',
              'user=%s skipped write throttle' % userguid)
    return True

  # userguid starts with a '#' while max_writes['guid'] does not.
  # We need to strip the '#' in order for the comparison to succeed.
  if userguid[0] == '#':
    userguid = userguid[1:]
  if max_writes['guid'] != userguid:
    LOG.notice(
        'write.throttle.different_users',
        'Logged in user: %s different from mqlwrite user: %s' %
        (max_writes['guid'], userguid))

  # 1 day
  tdelta = timedelta(1)
  yesterday = (datetime.utcnow() - tdelta).isoformat()

  # MQL attribution models documented at:
  # https://wiki.metaweb.com/index.php/MQL_Attribution_for_OAuth%2C_Acre%2C_etc
  # normal attribution query
  # need the optional to suppress EMPTY on count=0
  graphq = ('(scope=%s timestamp>%s live=dontcare newest>=0 result=(count) '
            'optional)') % (
      max_writes['guid'], yesterday)
  gresult = querier.gc.read_varenv(graphq, varenv)
  count = int(gresult[0])

  # oauth/open social attribution query
  graphq = ('(scope->(scope=%s) timestamp>%s live=dontcare newest>=0 '
            'result=(count) optional)') % (
      max_writes['guid'], yesterday)
  gresult = querier.gc.read_varenv(graphq, varenv)

  count += int(gresult[0])

  if count > max_writes['limit']:
    LOG.alert(
        'write.throttle.exceeded', 'user=%s count=%s max=%d delta=%s' %
        (max_writes['guid'], count, max_writes['limit'], str(tdelta)))
    msg = 'Daily write limit of %s was exceeded.' % max_writes['limit']
    raise MQLWriteQuotaError(
        None,
        msg,
        user='/guid/' + max_writes['guid'],
        count=count,
        max_writes=max_writes['limit'],
        period=str(tdelta))
  else:
    LOG.notice(
        'write.throttle.ok', 'user=%s count=%s max=%s' %
        (max_writes['guid'], count, max_writes['limit']))
    return True
