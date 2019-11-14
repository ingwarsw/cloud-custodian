# Copyright 2019 Microsoft Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import absolute_import, division, print_function, unicode_literals

from gcp_common import BaseTest
from mock import patch, Mock

from c7n.filters import FilterValidationError


def get_policy(actions=None, filters=None):
    policy = {'name': 'test-label',
              'resource': 'gcp.instance'}
    if filters:
        policy['filters'] = filters
    if actions:
        policy['actions'] = actions
    return policy


class LabelTest(BaseTest):

    def test_schema_validate(self):
        self.assertTrue(
            self.load_policy(
                get_policy([
                    {'type': 'label',
                     'label': 'test',
                     'value': 'test_value'}
                ])))

        self.assertTrue(
            self.load_policy(
                get_policy([
                    {'type': 'label',
                     'labels': {'value1': 'test',
                                'value2': 'test_value'}}
                ])))

        self.assertTrue(
            self.load_policy(
                get_policy([
                    {'type': 'label',
                     'label': 'test',
                     'value': {'type': 'resource',
                               'key': 'test_value'}}
                ])))

        with self.assertRaises(FilterValidationError):
            # Must specify labels to add
            self.load_policy(get_policy([
                {'type': 'label'}
            ]))

        with self.assertRaises(FilterValidationError):
            # Must not specify label and labels at once
            self.load_policy(get_policy([
                {'type': 'label',
                'label': 'test',
                'labels': {'label1': 'test1'}}
            ]))


class RemoveLabelTest(BaseTest):

    def test_schema_validate(self):
        self.assertTrue(
            self.load_policy(
                get_policy([
                    {'type': 'unlabel',
                     'labels': ['test']}
                ])))

        with self.assertRaises(FilterValidationError):
            # Must specify labels to remove
            self.load_policy(get_policy([
                {'type': 'unlabel'}
            ]))


class LabelDelayedActionTest(BaseTest):

    def test_schema_validate(self):
        self.assertTrue(
            self.load_policy(
                get_policy([
                    {'type': 'mark-for-op',
                     'op': 'stop'}
                ])))

        with self.assertRaises(FilterValidationError):
            # Must specify op
            self.load_policy(get_policy([
                {'type': 'mark-for-op'}
            ]))

        with self.assertRaises(FilterValidationError):
            # Must specify right op
            self.load_policy(get_policy([
                {'type': 'mark-for-op',
                 'op': 'no-such-op'}
            ]))
