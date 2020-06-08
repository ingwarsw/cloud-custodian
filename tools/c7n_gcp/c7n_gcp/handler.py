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

import json
import logging
import os
import uuid

from c7n.config import Config
from c7n.policy import PolicyCollection
from c7n.resources import load_resources
from c7n.structure import StructureParser
# Load resource plugins
from c7n_gcp.entry import initialize_gcp

initialize_gcp()

log = logging.getLogger('custodian.gcp.functions')

logging.getLogger().setLevel(logging.INFO)

# config.json policy data dict
policy_data = None

# execution options for the policy
policy_config = None

def run(event, context=None):
    # one time initialization for cold starts.
    global policy_config, policy_data
    if policy_config is None:
        with open('config.json') as f:
            policy_data = json.load(f)
        options_overrides = \
            policy_data['policies'][0].get('mode', {}).get('execution-options', {})

        # if output_dir specified use that, otherwise make a temp directory
        if 'output_dir' not in options_overrides:
            options_overrides['output_dir'] = get_tmp_output_dir()

        policy_config = Config.empty(**options_overrides)
        load_resources(StructureParser().get_resource_types(policy_data))

    if not policy_data or not policy_data.get('policies'):
        log.error('Invalid policy config')
        return False

    policies = PolicyCollection.from_data(policy_data, policy_config)
    if policies:
        for p in policies:
            log.info("running policy %s", p.name)
            p.validate()
            p.push(event, context)
    return True


def get_tmp_output_dir():
    output_dir = '/tmp/' + str(uuid.uuid4())
    if not os.path.exists(output_dir):
        try:
            os.mkdir(output_dir)
        except OSError as error:
            log.warning("Unable to make output directory: {}".format(error))
    return output_dir
