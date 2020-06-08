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
from c7n_azure.filters import ParentFilter
from c7n_azure.resources.key_vault import KeyVault

from c7n.filters.core import ValueFilter
from .azure_common import BaseTest


class ParentFilterTest(BaseTest):

    def test_schema(self):
        self.assertTrue(self.load_policy({
            'name': 'test-policy',
            'resource': 'azure.keyvault-key',
            'filters': [
                {'type': 'parent',
                 'filter': {
                     'type': 'value',
                     'key': 'name',
                     'op': 'glob',
                     'value': 'cctestkv*'
                 }}]
        }, validate=True))

        self.assertTrue(self.load_policy({
            'name': 'test-policy',
            'resource': 'azure.cosmosdb-collection',
            'filters': [
                {'type': 'parent',
                 'filter': {
                     'type': 'value',
                     'key': 'name',
                     'op': 'glob',
                     'value': 'cctestkv*'
                 }}]
        }, validate=True))

    def test_verify_parent_filter(self):
        p = self.load_policy({
            'name': 'test-policy',
            'resource': 'azure.keyvault-key',
            'filters': [
                {'type': 'parent',
                 'filter': {
                     'type': 'value',
                     'key': 'name',
                     'op': 'glob',
                     'value': 'cctestkv*'
                 }}]})

        self.assertEqual(len(p.resource_manager.filters), 1)

        filter = p.resource_manager.filters[0]
        self.assertTrue(isinstance(filter, ParentFilter))
        self.assertTrue(isinstance(filter.parent_manager, KeyVault))
        self.assertTrue(isinstance(filter.parent_filter, ValueFilter))
