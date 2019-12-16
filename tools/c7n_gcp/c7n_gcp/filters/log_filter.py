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

import itertools

from c7n.filters import FilterValidationError, ValueFilter
from c7n.utils import chunks, local_session, type_schema, get_annotation_prefix
from c7n_gcp.provider import resources as gcp_resources
from datetime import datetime, timedelta


class StackdriverLogFilter(ValueFilter):
    """The stackdriver log filter is implicitly just the ValueFilter
    on the stackdriver log for an GCP resource.

    In `filter` you need to specify filter for logs.
     You can use python `format string <https://pyformat.info/>`
    Inside format string there are defined variables:
      - `resource`: whole resource that we are applying it to

    `filter_days` specify how many days from now we want to look for logs (default 30 days)

    :example:

    Find all instances that was stopped more than 7 days ago.

    .. code-block:: yaml

        policies
          - name: find-instances-stopped-more-than-week-ago
            resource: gcp.instance
            filters:
              - type: value
                key: status
                value: TERMINATED
              - type: stackdriver-logs
                filter_days: 7
                filter: |
                    resource.type=gce_instance AND
                    resource.labels.instance_id={resource[id]} AND
                    (
                        jsonPayload.event_subtype:compute.instances.stop OR
                        jsonPayload.event_subtype:compute.instances.guestTerminate OR
                        protoPayload.request.@type:type.googleapis.com/compute.instances.stop
                    )
                key: filtered_logs
                value: empty

    """

    schema = type_schema('stackdriver-logs',
                         rinherit=ValueFilter.schema,
                         required=['filter'],
                         filter={'type': 'string'},
                         filter_days={'type': 'number', 'minimum': 0})
    schema_alias = True

    def __init__(self, data, manager=None):
        super(StackdriverLogFilter, self).__init__(data, manager)
        
        self.filter = self.data.get('filter', '')
        self.time_from = datetime.now() - timedelta(days=self.data.get('filter_days', 30))

    def validate(self):
        if self.data.get('filter_days') < 0:
            raise FilterValidationError("Filter '{}': invalid filter_days < 0".format(self.type))
        if not self.data.get('filter'):
            raise FilterValidationError("Filter '{}': filter field must exists".format(self.type))
        super(StackdriverLogFilter, self).validate()

    def process(self, resources, event=None):
        self.project_id = local_session(self.manager.source.query.session_factory).get_default_project()
        self.client = local_session(self.manager.source.query.session_factory).client('logging', 'v2', 'entries')

        results = []
        for resource_set in chunks(resources, 100):
            results.extend(self.process_resources(resource_set))

        return super(StackdriverLogFilter, self).process(results, event=None)

    def process_resources(self, resource_set):
        resource_map = {x['id']: x for x in resource_set}
        filter_ = self.get_filter(resource_map)

        params = {'body': {
            'resourceNames': "projects/{}".format(self.project_id),
            'filter': filter_,
        }}
        print("Filter: {}".format(filter_))
        entries = self.client.execute_command('list', params).get('entries', [])

        id_function = lambda e: e['resource']['labels']['instance_id']
        sorted_entries = sorted(entries, key=id_function)

        for resource_id, logs in itertools.groupby(sorted_entries, key=id_function):
            self._write_logs_to_resource(resource_map[resource_id], list(logs))

        return resource_set

    def get_filter(self, resource_map):
        filter_ = self.data.get('filter')
        filter_ = "timestamp>={time_from:%Y-%m-%d} AND " \
                  "resource.labels.instance_id = ({resource_ids}) AND " \
                  "({filter_})".format(
            resource_ids=" OR ".join(resource_map.keys()),
            time_from=self.time_from,
            project_id=self.project_id,
            filter_=filter_)
        return filter_

    def _get_metrics_cache_key(self):
        return self.type

    def _write_logs_to_resource(self, resource, logs):
        resource_metrics = resource.setdefault(get_annotation_prefix('filtered_logs'), {})
        resource_metrics[self._get_metrics_cache_key()] = logs

    @classmethod
    def register_resources(cls, registry, resource_class):
        resource_class.filter_registry.register('stackdriver-logs', cls)


gcp_resources.subscribe(gcp_resources.EVENT_REGISTER, StackdriverLogFilter.register_resources)
