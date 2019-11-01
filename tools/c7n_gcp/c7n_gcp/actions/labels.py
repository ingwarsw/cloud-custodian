# Copyright 2019 Karol Lassak
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

from c7n.utils import type_schema
from c7n_gcp.actions import MethodAction
from c7n.filters import FilterValidationError
from c7n_azure.lookup import Lookup


def register_labeling(action_registry):
    action_registry.register('label', Label)
    action_registry.register('unlabel', RemoveLabel)


class BaseLabelAction(MethodAction):
    schema = type_schema('setLabels')
    method_spec = {'op': 'setLabels'}

    def get_labels_to_add(self, resource):
        return None

    def get_labels_to_delete(self, resource):
        return None

    def _merge_labels(self, current_labels, new_labels, remove_labels):
        result = current_labels
        if new_labels:
            result.update(new_labels)
        if remove_labels:
            result = {k: v for k, v in current_labels.items() if k not in remove_labels}
        return result

    def get_resource_params(self, model, resource):
        params = model.get_label_params(resource)
        current_labels = self._get_current_labels(resource)
        new_labels = self.get_labels_to_add(resource)
        remove_labels = self.get_labels_to_delete(resource)
        all_labels = self._merge_labels(current_labels, new_labels, remove_labels)

        params['body'] = {
            'labels': all_labels,
            'labelFingerprint': resource['labelFingerprint']
        }

        return params

    def _get_current_labels(self, resource):
        return resource.get('labels', {})


class Label(BaseLabelAction):
    """Adds labels to GCP resources

    :example:

    This policy will label all existing resource groups with a value such as environment

    .. code-block:: yaml

      policies:
        - name: gcp-label-resourcegroups
          resource: gcp.instance
          description: |
            Label all existing instances with a value such as environment
          actions:
           - type: label
             label: environment
             value: test
    """

    schema = type_schema(
        'label',
        **{
            'value': Lookup.lookup_type({'type': 'string'}),
            'label': Lookup.lookup_type({'type': 'string'}),
            'labels': {'type': 'object'}
        }
    )

    def validate(self):
        if not self.data.get('labels') and not (self.data.get('label') and self.data.get('value')):
            raise FilterValidationError(
                "Must specify either labels or a label and value")

        if self.data.get('labels') and self.data.get('label'):
            raise FilterValidationError(
                "Can't specify both labels and label, choose one")

        return self

    def get_labels_to_add(self, resource):
        return self.data.get('labels') or {Lookup.extract(
            self.data.get('label'), resource): Lookup.extract(self.data.get('value'), resource)}


class RemoveLabel(BaseLabelAction):
    """Removes labels from GCP resources

    :example:

    This policy will remove label for all existing resource groups with a key such as environment

        .. code-block:: yaml

          policies:
            - name: gcp-remove-label-resourcegroups
              resource: gcp.instance
              description: |
                Remove label for all existing instances with a key such as environment
              actions:
               - type: unlabel
                 labels: ['environment']
    """
    schema = type_schema(
        'unlabel',
        labels={'type': 'array', 'items': {'type': 'string'}})

    def validate(self):
        if not self.data.get('labels'):
            raise FilterValidationError("Must specify labels")
        return self

    def get_labels_to_delete(self, resource):
        return self.data.get('labels')
