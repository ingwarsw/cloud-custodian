# Copyright 2018 Capital One Services, LLC
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
import time

from c7n.config import Bag
from c7n.output import metrics_outputs
from c7n_gcp.output import StackDriverMetrics

from gcp_common import BaseTest


class MetricsOutputTest(BaseTest):

    def test_metrics_selector(self):
        self.assertEqual(
            metrics_outputs.get('gcp'),
            StackDriverMetrics)

    def test_metrics_output(self):
        project_id = 'cloud-custodian'
        factory = self.replay_flight_data('output-metrics', project_id=project_id)
        ctx = Bag(session_factory=factory,
                  policy=Bag(name='custodian-works', resource_type='gcp.function'))
        conf = Bag()
        metrics = StackDriverMetrics(ctx, conf)
        metrics.put_metric('ResourceCount', 43, 'Count', Scope='Policy')
        metrics.flush()
    
    def test_metrics_output_set_write_project_id(self):
        project_id = 'cloud-custodian-sub'
        write_project_id = 'cloud-custodian'
        factory = self.replay_flight_data('output-metrics', project_id=project_id)
        ctx = Bag(session_factory=factory,
                  policy=Bag(name='custodian-works', resource_type='gcp.function'))
        conf = Bag(project_id=write_project_id)
        metrics = StackDriverMetrics(ctx, conf)
        metrics.put_metric('ResourceCount', 43, 'Count', Scope='Policy')
        metrics.flush()
