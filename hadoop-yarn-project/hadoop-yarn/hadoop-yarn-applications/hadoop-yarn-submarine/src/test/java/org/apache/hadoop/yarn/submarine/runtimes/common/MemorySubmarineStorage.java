/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.submarine.runtimes.common;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MemorySubmarineStorage extends SubmarineStorage {
  private Map<String, Map<String, String>> jobsInfo = new HashMap<>();
  private Map<String, Map<String, Map<String, String>>> modelsInfo =
      new HashMap<>();

  @Override
  public synchronized void addNewJob(String jobName, Map<String, String> jobInfo)
      throws IOException {
    jobsInfo.put(jobName, jobInfo);
  }

  @Override
  public synchronized Map<String, String> getJobInfoByName(String jobName)
      throws IOException {
    return jobsInfo.get(jobName);
  }

  @Override
  public synchronized void addNewModel(String modelName, String version,
      Map<String, String> modelInfo) throws IOException {
    if (!modelsInfo.containsKey(modelName)) {
      modelsInfo.put(modelName, new HashMap<>());
    }
    modelsInfo.get(modelName).put(version, modelInfo);
  }

  @Override
  public synchronized Map<String, String> getModelInfoByName(String modelName,
      String version) throws IOException {
    return modelsInfo.get(modelName).get(version);
  }
}
