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

package org.apache.hadoop.yarn.submarine.common.job.submitter;

import org.apache.hadoop.yarn.submarine.common.ClientContext;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineConfiguration;
import org.apache.hadoop.yarn.submarine.common.exception.SubmarineException;

/**
 * Get job Submitter by SubmarineConfiguration
 */
public class JobSubmitterFactory {
  /**
   * Create JobSubmitter instance by config
   * @param clientContext clientContext
   * @return JobSubmitter
   * @throws SubmarineException when anything bad related to Submarine happens.
   */
  public static YarnServiceJobSubmitter createJobSubmitter(
      ClientContext clientContext) throws SubmarineException {
    SubmarineConfiguration submarineConfiguration =
        clientContext.getSubmarineConfig();

    String runtime = submarineConfiguration.get(SubmarineConfiguration.RUNTIME,
        SubmarineConfiguration.DEFAULT_RUNTIME);

    if (runtime.equals(SubmarineConfiguration.YARN_SERVICE_FRAMEWORK_RUNTIME)) {
      return new YarnServiceJobSubmitter(clientContext);
    }

    throw new SubmarineException("Runtime = " + runtime + " is not supported");
  }
}
