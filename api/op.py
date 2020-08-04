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

import os
from mw.log import LOG
import logging

from optparse import OptionParser
from ConfigParser import ConfigParser, NoSectionError, NoOptionError
from mw.user.cache import get_user_by_name

class OP(OptionParser):
    def __init__(self, *args, **kws):
        usage = kws.get('usage','')
        kws['usage'] = "%%prog  [-d] [-g HOST:PORT] %s [...]" % usage
        OptionParser.__init__(self, *args, **kws)

        config_file = None
        if 'ME_SITE_CONFIG' in os.environ:
            config_file = os.environ['ME_SITE_CONFIG']
            if not os.path.exists(config_file):
                config_file = None

            
        if config_file == None:
            # default look in me/mwbuild/_site.cfg
            config_file = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                                       '../../../mwbuild/_site.cfg'))

            # walk up the directory structure, stopping at project.mw4
            # (i.e. the root of whatever project we're in)
            path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
            config_file = os.path.join(path, "_site.cfg")
            
            while (not os.path.exists(config_file) and
                   not os.path.exists(os.path.join(path, "project.mw4"))):
                path = os.path.abspath(os.path.join(path, ".."))
                config_file = os.path.join(path, "_site.cfg")
                
            if not os.path.exists(config_file):
                config_file = None


        self.add_option('-c', '--config', dest='config_file',
                        default=config_file,
                        help="location of _site.cfg with graph configuration")

        self.add_option('-d', '--debug', dest='debug',
                        default=False, action='store_true',
                        help="turn on debugging output")

        self.add_option('-l', '--loglevel', dest='loglevel',
                        default='WARNING', action='store',
                        help="set the log level")
        self.add_option('-g', '--graph', dest='graphd_addr',
                        metavar="HOST:PORT",
                        help="address of graphd in the form host:port")
        self.add_option('-b', '--blob', dest='blobd_addr',
                        metavar="HOST:PORT",
                        help="address of blobd in the form host:port")
        self.add_option('-D', '--define', dest='defines',
                        default=[], action='append',
                        help='override other site.cfg options in the form section.entry=value')
        self.add_option("-a", "--as_user", dest="as_user",
                        metavar="/user/USERID",
                        help="User ID to write with")

        self.add_option("-r", "--relevance", dest="relevance_addr",
                        metavar="HOST:PORT",
                        help="host:port of relevance server")
        self.add_option("-s", "--geo", dest="geo_addr",
                        metavar="HOST:PORT",
                        help="host:port of geo server")

        self.add_option("-T", "--no_timeouts", dest="no_timeouts",
                        default=False, action='store_true',
                        help="turn off socket timeouts (off by default)")

    def parse_args(self, *args, **kws):
        # this is an all-in-one function. It parses the args, loads the config and creates the session.
        # most of the time in simple scripts you don't need any more control than this.

        options, args = self.parse_args_only(*args,**kws)

        config = self.load_config(options)

        self.create_session(config,options)

        return (options, args)

    def parse_args_only(self, *args, **kws):
        # this strictly parses the args without loading the config or creating the session
        return OptionParser.parse_args(self, *args, **kws)

    def load_config(self,options):
        # this loads the configuration file without attempting to connect to any services

        from paste.deploy import appconfig

        config = {}
        if options.config_file is not None:
            LOG.debug("parse.args", "Trying to open %s" % options.config_file)
            try:
                config = appconfig("config:%s" % options.config_file)
            except LookupError as e:
                LOG.debug("parse.args", "Error loading config file, missing paste sections", options.config_file, e)
                # fall through

        for k,v in (li.split('=', 1)
                    for li in options.defines):
            config[k] = v

        loglevels = 'EMERG ALERT CRIT ERR WARNING NOTICE INFO DEBUG'.split()
        if options.loglevel in loglevels:
            LOG.setLevel(logging.getLevelName(options.loglevel))
        else:
            self.error('unknown log level %s\n  valid log levels are %s'
                     % (options.loglevel, ', '.join(loglevels)))
            sys.exit(1)

        # go through the config file for these options, keeps things
        # simple
        if options.graphd_addr:
            config["graphd.address"] = options.graphd_addr

        if options.blobd_addr:
            config["clobd.address"] = options.blobd_addr
            config["clobd.masteraddress"] = options.blobd_addr

        if options.relevance_addr:
            config["relevance.address"] = options.relevance_addr

        if options.geo_addr:
            config["geo.address"] = options.geo_addr

        if options.no_timeouts:
            config["debug.no_timeouts"] = options.no_timeouts and 'true'

        self.config = config
        return config

    def create_session(self,config,options):
        # this opens the connections to services

        from mw.api.service import ServiceContext, Session
        self.ctx = ServiceContext()

        self.ctx.load_config(config)
        self.ctx.connect()

        self.session = Session(self.ctx)

        # do further configuration of Session

        self.session.finish_init()

        if options.as_user:
            if not options.as_user.startswith("/user/"):
                raise Exception("User must be in the form /user/USERID")
            user_name = options.as_user[len("/user/"):]
            self.session.push_variables(user=options.as_user)
            self.session._signed_user = get_user_by_name(user_name)
            self.session.get_user().validate(self.session)

        return self.session
