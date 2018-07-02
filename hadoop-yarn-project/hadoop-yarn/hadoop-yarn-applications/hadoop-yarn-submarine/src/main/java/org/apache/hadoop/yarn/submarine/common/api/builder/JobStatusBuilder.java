/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package org.apache.hadoop.yarn.submarine.common.api.builder;

import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.Container;
import org.apache.hadoop.yarn.service.api.records.ContainerState;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.api.records.ServiceState;
import org.apache.hadoop.yarn.submarine.client.cli.param.JobRunParameters;
import org.apache.hadoop.yarn.submarine.common.ClientContext;
import org.apache.hadoop.yarn.submarine.common.api.JobComponentStatus;
import org.apache.hadoop.yarn.submarine.common.api.JobState;
import org.apache.hadoop.yarn.submarine.common.api.JobStatus;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.yarn.submarine.common.Constants.WORKER_COMPONENT_NAME;

public class JobStatusBuilder {
  public static JobStatus fromServiceSepc(Service serviceSpec,
      ClientContext clientContext) {
    JobStatus status = new JobStatus();
    status.setState(fromServiceState(serviceSpec.getState()));

    // If it is a final state, return.
    if (JobState.isFinal(status.getState())) {
      return status;
    }

    List<JobComponentStatus> componentStatusList = new ArrayList<>();

    for (Component component : serviceSpec.getComponents()) {
      componentStatusList.add(
          JobComponentStatusBuilder.fromServiceComponent(component));
    }
    status.setComponentStatus(componentStatusList);

    status.setTensorboardLink(getTensorboardLink(serviceSpec, clientContext));

    status.setJobName(serviceSpec.getName());

    return status;
  }

  private static String getTensorboardLink(Service serviceSpec,
      ClientContext clientContext) {
    JobRunParameters jobRunParameters = clientContext.getRunJobParameters(
        serviceSpec.getName());
    if (jobRunParameters == null || !jobRunParameters.isTensorboardEnabled()) {
      return null;
    }

    for (Component component : serviceSpec.getComponents()) {
      if (component.getName().equals(WORKER_COMPONENT_NAME)) {
        for (Container c : component.getContainers()) {
          if (c.getComponentInstanceName().equals(WORKER_COMPONENT_NAME + "-0")
              && (c.getState() == ContainerState.READY
              || c.getState() == ContainerState.RUNNING_BUT_UNREADY)) {
            String hostname = c.getHostname();
            int port = clientContext.getTaskNetworkPortManager().getPort(
                serviceSpec.getName(), "tensorboard", 0);
            return "http://" + hostname + ":" + port;
          }
        }
      }
    }

    return null;
  }

  private static JobState fromServiceState(ServiceState serviceState) {
    switch (serviceState){
    case STOPPED:
      return JobState.SUCCEEDED;
    case FAILED:
      return JobState.FAILED;
    }

    return JobState.RUNNING;
  }
}
