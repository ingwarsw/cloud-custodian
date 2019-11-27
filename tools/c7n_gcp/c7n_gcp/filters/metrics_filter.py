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

import operator
import isodate

from datetime import datetime, timedelta

from c7n.utils import chunks, local_session, type_schema, get_annotation_prefix
from c7n.filters import FilterValidationError, Filter

from c7n_gcp.provider import resources as gcp_resources

from c7n_azure.utils import (Math)

scalar_ops = {
    'eq': operator.eq,
    'equal': operator.eq,
    'ne': operator.ne,
    'not-equal': operator.ne,
    'gt': operator.gt,
    'greater-than': operator.gt,
    'ge': operator.ge,
    'gte': operator.ge,
    'le': operator.le,
    'lte': operator.le,
    'lt': operator.lt,
    'less-than': operator.lt
}

class MetricsFilter(Filter):
    """

    Filters GCP resources based on live metrics from the Stackdriver monitor

    Click `here
    <https://cloud.google.com/monitoring/api/metrics/>`_
    for a full list of metrics supported by GCP resources.

    :example:

    Find all VMs with an average Percentage CPU greater than 75% over last 2 hours

    .. code-block:: yaml

        policies:
          - name: vm-percentage-cpu
            resource: azure.vm
            filters:
              - type: metric
                metric: Percentage CPU
                aggregation: average
                op: gt
                threshold: 75
                timeframe: 2

    :example:

    Find KeyVaults with more than 1000 API hits in the last hour

    .. code-block:: yaml

        policies:
          - name: keyvault-hits
            resource: azure.keyvault
            filters:
              - type: metric
                metric: ServiceApiHit
                aggregation: total
                op: gt
                threshold: 1000
                timeframe: 1

    :example:

    Find SQL servers with less than 10% average DTU consumption
    across all databases over last 24 hours

    .. code-block:: yaml

        policies:
          - name: dtu-consumption
            resource: azure.sqlserver
            filters:
              - type: metric
                metric: dtu_consumption_percent
                aggregation: average
                op: lt
                threshold: 10
                timeframe: 24
                filter:  "DatabaseResourceId eq '*'"

    """

    DEFAULT_TIMEFRAME = 24
    DEFAULT_ALIGNMENT_PERIOD = 'PT1M'
    DEFAULT_ALIGNAER = 'mean'
    DEFAULT_AGGREGATION = 'mean'

    schema = {
        'type': 'object',
        'required': ['type', 'metric', 'op', 'threshold'],
        'additionalProperties': False,
        'properties': {
            'type': {'enum': ['metric']},
            'metric': {'type': 'string'},
            'op': {'enum': list(scalar_ops.keys())},
            'threshold': {'type': 'number'},
            'timeframe': {'type': 'number'},
            'alignment_period': {'enum': [
                'PT1M', 'PT5M', 'PT15M', 'PT30M', 'PT1H', 'PT6H', 'PT12H', 'P1D']},
            'aligner': {'enum': ['none',
                                 'delta',
                                 'rate',
                                 'interpolate',
                                 'next_older',
                                 'min',
                                 'max',
                                 'mean',
                                 'count',
                                 'sum',
                                 'stddev',
                                 'count_true',
                                 'count_false',
                                 'fraction_true',
                                 'percentile_99',
                                 'percentile_95',
                                 'percentile_50',
                                 'percentile_05',
                                 'percent_change',
                                 ]},
            'aggregation': {'enum': ['none',
                                     'mean',
                                     'min',
                                     'max',
                                     'sum',
                                     'stddev',
                                     'count',
                                     'count_true',
                                     'count_false',
                                     'fraction_true',
                                     'percentile_99',
                                     'percentile_95',
                                     'percentile_50',
                                     'percentile_05'
                                     ]},
            'no_data_action': {'enum': ['include', 'exclude']},
            'filter': {'type': 'string'}
        }
    }
    schema_alias = True

    def __init__(self, data, manager=None):
        super(MetricsFilter, self).__init__(data, manager)
        # Metric name as defined by Stackdriver SDK
        self.metric = self.data.get('metric')
        # gt (>), ge  (>=), eq (==), le (<=), lt (<)
        self.op = scalar_ops[self.data.get('op')]
        # Value to compare metric value with self.op
        self.threshold = self.data.get('threshold')
        # Number of hours from current UTC time
        self.timeframe = float(self.data.get('timeframe', self.DEFAULT_TIMEFRAME))
        # Alignment Period as defined by 
        # https://cloud.google.com/monitoring/api/ref_v3/rest/v3/projects.alertPolicies#Aggregation
        self.alignment_period = isodate.parse_duration(self.data.get('alignment_period', self.DEFAULT_ALIGNMENT_PERIOD))
        # Aligner
        self.aligner = self.data.get('aligner', self.DEFAULT_ALIGNAER)
        # Aggregation as defined by Stackdriver SDK
        self.aggregation = self.data.get('aggregation', self.DEFAULT_AGGREGATION)
        # Used to reduce the set of metric data returned
        self.filter = self.data.get('filter', None)
        # Include or exclude resources if there is no metric data available
        self.no_data_action = self.data.get('no_data_action', 'exclude')
        

    def process(self, resources, event=None):
        # Project id
        self.project_id = local_session(self.manager.source.query.session_factory).get_default_project()
        # Create Stackdriver Monitor client
        self.client = local_session(self.manager.source.query.session_factory).client('monitoring', 'v3', 'projects.timeSeries')
        
        # Process each resource in a separate thread, returning all that pass filter
        with self.executor_factory(max_workers=3) as w:
            processed = list(w.map(self.process_resource, resources))
            return [item for item in processed if item is not None]

    def get_metric_data(self, resource):
        cached_metric_data = self._get_cached_metric_data(resource)
        if cached_metric_data:
            return cached_metric_data['measurement']
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=self.timeframe)

        params = {'name': "projects/{}".format(self.project_id),
                  'interval_startTime': start_time.isoformat('T') + 'Z',
                  'interval_endTime': end_time.isoformat('T') + 'Z',
                  'aggregation_crossSeriesReducer': 'REDUCE_{}'.format(self.aggregation.upper()),
                  'aggregation_alignmentPeriod': '{}s'.format(int(self.alignment_period.total_seconds())),
                  'aggregation_perSeriesAligner': 'ALIGN_{}'.format(self.aligner.upper()),
                  'filter': self.get_filter(resource),
        }
            
        print("Params {}".format(params))
        metrics_data = self.client.execute_command('list', params)
        print("result {}".format(metrics_data))

        values = [item['value'].values() for item in metrics_data['timeSeries'][0]['points']]
        
        # print("Modified {}".format(values))

        self._write_metric_to_resource(resource, metrics_data, values)

        return values

    def get_filter(self, resource):
        filter = 'resource.labels.instance_id="{instance_id}" AND ' \
                 'resource.labels.project_id="{project_id}" AND ' \
                 'metric.type="{metric}"'.format(
            instance_id=resource['id'], project_id=self.project_id, metric=self.metric)
        return filter

    def _write_metric_to_resource(self, resource, metrics_data, values):
        resource_metrics = resource.setdefault(get_annotation_prefix('metrics'), {})
        resource_metrics[self._get_metrics_cache_key()] = {
            'metrics_data': metrics_data,
            'measurement': values,
        }

    def _get_metrics_cache_key(self):
        return "{}, {}, {}, {}, {}".format(
            self.metric,
            self.aggregation,
            self.timeframe,
            self.alignment_period,
            self.filter,
        )

    def _get_cached_metric_data(self, resource):
        metrics = resource.get(get_annotation_prefix('metrics'))
        if not metrics:
            return None
        return metrics.get(self._get_metrics_cache_key())

    def passes_op_filter(self, resource):
        m_data = self.get_metric_data(resource)
        if m_data is None:
            return self.no_data_action == 'include'
        # aggregate_value = self.func(m_data)
        return self.op(aggregate_value, self.threshold)

    def process_resource(self, resource):
        return resource if self.passes_op_filter(resource) else None


    @classmethod
    def register_resources(cls, registry, resource_class):
        resource_class.filter_registry.register('metric', cls)


gcp_resources.subscribe(gcp_resources.EVENT_REGISTER, MetricsFilter.register_resources)
