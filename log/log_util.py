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
"""Backward compatible support for mql LOG calls, Levels."""

__author__ = 'bneutra@google.com (Brendan Neutra)'

from absl import logging

FATAL = logging.FATAL
ERROR = logging.ERROR
CRIT = ALERT = ERROR
WARN = logging.WARN
WARNING = WARN
INFO = logging.INFO
NOTICE = INFO
DEBUG = logging.DEBUG
SPEW = 2  # e.g. mql.utils.dumplog: for things that are expensive and verbose
