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

package org.apache.hadoop.yarn.submarine.common.job;

import org.apache.hadoop.yarn.submarine.common.ClientContext;
import org.apache.hadoop.yarn.submarine.common.api.JobState;
import org.apache.hadoop.yarn.submarine.common.api.JobStatus;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.client.ServiceClient;
import org.apache.hadoop.yarn.submarine.common.api.builder.JobStatusBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Monitor status of job
 */
public class JobMonitor {
  private static final Logger LOG =
      LoggerFactory.getLogger(JobMonitor.class);
  /**
   * Returns status of training job.
   * @return JobStatus
   */
  public JobStatus getTrainingJobStatus(String jobName, ClientContext clientContext)
      throws IOException, YarnException {
    ServiceClient serviceClient = clientContext.getServiceClient();
    Service serviceSpec = serviceClient.getStatus(jobName);
    JobStatus jobStatus = JobStatusBuilder.fromServiceSpec(serviceSpec);
    return jobStatus;
  }

  /**
   * Continue wait and print status if job goes to ready or final state.
   * @param jobName
   * @param clientContext
   * @throws IOException
   * @throws YarnException
   */
  public void waitTrainingJobReadyOrFinal(String jobName,
      ClientContext clientContext)
      throws IOException, YarnException {
    // Wait 5 sec between each fetch.
    int waitSec = 5;
    JobStatus js;
    while (true) {
      js = getTrainingJobStatus(jobName, clientContext);
      JobState jobState = js.getState();
      js.nicePrint(System.err);

      if (JobState.isFinal(jobState)) {
        LOG.info("Job exited with state=" + jobState);
        break;
      }

      try {
        Thread.sleep(waitSec * 1000);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
  }
}
