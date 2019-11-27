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

from google.cloud.logging import Client as LogClient
from google.cloud.logging.entries import LogEntry
from google.api_core.exceptions import TooManyRequests
from retrying import retry

from c7n.utils import chunks, local_session, type_schema
from c7n.filters import FilterValidationError, ValueFilter

from c7n_gcp.provider import resources as gcp_resources


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

    def validate(self):
        if self.data.get('filter_days') < 0:
            raise FilterValidationError("Filter '{}': invalid filter_days < 0".format(self.type))
        if not self.data.get('filter'):
            raise FilterValidationError("Filter '{}': filter field must exists".format(self.type))
        super(StackdriverLogFilter, self).validate()

    def process(self, resources, event=None):

        project_id = local_session(self.manager.source.query.session_factory).get_default_project()
        client = LogClient(project=project_id, _use_grpc=False)

        time_from = datetime.now() - timedelta(days=self.data.get('filter_days', 30))

        results = []
        for resource in resources:
            results.extend([self.process_resource(resource, client, time_from)])

        return super(StackdriverLogFilter, self).process(results, event=None)

    def is_retryable_exception(e):
        return isinstance(e, TooManyRequests)

    @retry(retry_on_exception=is_retryable_exception,
           wait_exponential_multiplier=1000,
           wait_exponential_max=10000,
           stop_max_attempt_number=10)
    def process_resource(self, resource, client, time_from):
        filter_ = self.data.get('filter').format(resource=resource)
        filter_ = "timestamp>={time_from:%Y-%m-%d} AND ({filter_})".format(
                    time_from=time_from,
                    filter_=filter_)

        # print("Filter: {}".format(filter_))
        entries = client.list_entries(filter_=filter_)

        def map_entry(entry):
            json_entry = LogEntry.to_api_repr(entry)
            json_entry["payload"] = entry.payload
            return json_entry

        entries = list(map(map_entry, entries))

        # We are modifying resource to save logs in result
        resource['filtered_logs'] = entries

        return resource

    @classmethod
    def register_resources(cls, registry, resource_class):
        resource_class.filter_registry.register('stackdriver-logs', cls)


gcp_resources.subscribe(gcp_resources.EVENT_REGISTER, StackdriverLogFilter.register_resources)
