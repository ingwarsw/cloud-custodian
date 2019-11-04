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

from datetime import datetime, timedelta
from dateutil import tz as tzutil

from c7n.utils import type_schema
from c7n.filters import Filter, FilterValidationError
from c7n.filters.offhours import Time
from c7n_gcp.actions import MethodAction
from c7n_gcp.filters.labels import LabelActionFilter
from c7n_azure.lookup import Lookup

from c7n_gcp.provider import resources as gcp_resources


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

    @staticmethod
    def register_label_actions(registry, _):
        for resource in registry.keys():
            klass = registry.get(resource)

            if klass.resource_type.labels:
                print("Adding labels commands to {}", klass)
                klass.action_registry.register('label', Label)
                klass.action_registry.register('unlabel', RemoveLabel)
                klass.action_registry.register('mark-for-op', LabelDelayedAction)

                klass.filter_registry.register('marked-for-op', LabelActionFilter)


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


DEFAULT_TAG = "custodian_status"


class LabelDelayedAction(BaseLabelAction):
    """Label resources for future action.

    The optional 'tz' parameter can be used to adjust the clock to align
    with a given timezone. The default value is 'utc'.

    If neither 'days' nor 'hours' is specified, Cloud Custodian will default
    to marking the resource for action 4 days in the future.

    :example:

    .. code-block :: yaml

       policies:
        - name: vm-mark-for-stop
          resource: gcp.instance
          filters:
            - type: value
              key: name
              value: instance-to-stop-in-four-days
          actions:
            - type: mark-for-op
              op: stop
              days: 2
    """

    schema = type_schema(
        'mark-for-op',
        label={'type': 'string'},
        msg={'type': 'string'},
        days={'type': 'integer', 'minimum': 0, 'exclusiveMinimum': False},
        hours={'type': 'integer', 'minimum': 0, 'exclusiveMinimum': False},
        tz={'type': 'string'},
        op={'type': 'string'})

    default_template = 'resource_policy-{op}-{action_date}'

    def __init__(self, data=None, manager=None, log_dir=None):
        super(LabelDelayedAction, self).__init__(data, manager, log_dir)
        self.tz = tzutil.gettz(
            Time.TZ_ALIASES.get(self.data.get('tz', 'utc')))

        msg_tmpl = self.data.get('msg', self.default_template)

        op = self.data.get('op', 'stop')
        days = self.data.get('days', 0)
        hours = self.data.get('hours', 0)
        action_date = self.generate_timestamp(days, hours)

        self.label = self.data.get('label', DEFAULT_TAG)
        self.msg = msg_tmpl.format(
            op=op, action_date=action_date)

    def validate(self):
        op = self.data.get('op')
        if self.manager and op not in self.manager.action_registry.keys():
            raise FilterValidationError(
                "mark-for-op specifies invalid op:%s in %s" % (
                    op, self.manager.data))

        self.tz = tzutil.gettz(
            Time.TZ_ALIASES.get(self.data.get('tz', 'utc')))
        if not self.tz:
            raise FilterValidationError(
                "Invalid timezone specified %s in %s" % (
                    self.tz, self.manager.data))
        return self

    def generate_timestamp(self, days, hours):
        n = datetime.now(tz=self.tz)
        if days is None or hours is None:
            # maintains default value of days being 4 if nothing is provided
            days = 4
        action_date = (n + timedelta(days=days, hours=hours))
        if hours > 0:
            action_date_string = action_date.strftime('%Y_%m_%d__%H_%M')
        else:
            action_date_string = action_date.strftime('%Y_%m_%d__0_0')

        return action_date_string

    def get_labels_to_add(self, resource):
        return {self.label: self.msg}

gcp_resources.subscribe(
    gcp_resources.EVENT_FINAL, BaseLabelAction.register_label_actions)
