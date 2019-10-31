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

from c7n.utils import local_session, type_schema
from c7n_gcp.actions import MethodAction
from c7n.filters import FilterValidationError
from c7n_azure.lookup import Lookup


class Tag(MethodAction):
    """Adds tags to GCP resources

    :example:

    This policy will tag all existing resource groups with a value such as environment

    .. code-block:: yaml

      policies:
        - name: gcp-tag-resourcegroups
          resource: gcp.instance
          description: |
            Tag all existing instances with a value such as environment
          actions:
           - type: tag
             tag: environment
             value: test
    """


    schema = type_schema('setLabels')
    method_spec = {'op': 'setLabels'}

    schema = type_schema(
        'tag',
        **{
            'value': Lookup.lookup_type({'type': 'string'}),
            'tag': Lookup.lookup_type({'type': 'string'}),
            'tags': {'type': 'object'}
        }
    )

    def validate(self):
        if not self.data.get('tags') and not (self.data.get('tag') and self.data.get('value')):
            raise FilterValidationError(
                "Must specify either tags or a tag and value")

        if self.data.get('tags') and self.data.get('tag'):
            raise FilterValidationError(
                "Can't specify both tags and tag, choose one")

        return self

    def get_resource_params(self, resource):
        new_tags = self._get_tags(resource)
        TagHelper.add_tags(self, resource, new_tags)

    def _get_tags(self, resource):
        return self.data.get('tags') or {Lookup.extract(
            self.data.get('tag'), resource): Lookup.extract(self.data.get('value'), resource)}


class RemoveTag(MethodAction):
    """Removes tags from GCP resources

    :example:

    This policy will remove tag for all existing resource groups with a key such as environment

        .. code-block:: yaml

          policies:
            - name: gcp-remove-tag-resourcegroups
              resource: gcp.instance
              description: |
                Remove tag for all existing instances with a key such as environment
              actions:
               - type: untag
                 tags: ['environment']
    """
    schema = type_schema(
        'untag',
        tags={'type': 'array', 'items': {'type': 'string'}})

    def validate(self):
        if not self.data.get('tags'):
            raise FilterValidationError("Must specify tags")
        return self

    def _prepare_processing(self,):
        self.tags_to_delete = self.data.get('tags')

    def _process_resource(self, resource):
        # TagHelper.remove_tags(self, resource, self.tags_to_delete)


