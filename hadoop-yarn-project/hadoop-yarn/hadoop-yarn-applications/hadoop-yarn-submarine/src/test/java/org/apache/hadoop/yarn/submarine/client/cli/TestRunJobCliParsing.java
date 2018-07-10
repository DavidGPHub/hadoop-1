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


package org.apache.hadoop.yarn.submarine.client.cli;

import org.apache.hadoop.yarn.submarine.client.cli.param.JobRunParameters;
import org.apache.hadoop.yarn.submarine.common.MockClientContext;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineLogs;
import org.apache.hadoop.yarn.submarine.common.job.submitter.JobSubmitter;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class TestRunJobCliParsing {
  @Before
  public void before() {
    SubmarineLogs.verboseOff();
  }

  @Test
  public void testPrintHelp() {
    MockClientContext mockClientContext = new MockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    runJobCli.printUsages();
  }

  @Test
  public void testBasicRunJobForDistributedTraining() throws Exception {
    MockClientContext mockClientContext = new MockClientContext();
    JobSubmitter mockJobSubmitter = mock(JobSubmitter.class);
    RunJobCli runJobCli = new RunJobCli(mockClientContext, mockJobSubmitter);

    Assert.assertFalse(SubmarineLogs.isVerbose());

    runJobCli.run(
        new String[] { "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "hdfs://input", "--checkpoint_path", "hdfs://output",
            "--num_workers", "3", "--num_ps", "2", "--worker_launch_cmd",
            "python run-job.py", "--worker_resources", "memory=2048M,vcores=2",
            "--ps_resources", "memory=4G,vcores=4", "--tensorboard", "true",
            "--ps_launch_cmd", "python run-ps.py", "--verbose" });

    JobRunParameters jobRunParameters = runJobCli.getRunJobParameters();

    Assert.assertEquals(jobRunParameters.getInputPath(), "hdfs://input");
    Assert.assertEquals(jobRunParameters.getCheckpointPath(), "hdfs://output");
    Assert.assertEquals(jobRunParameters.getNumPS(), 2);
    Assert.assertEquals(jobRunParameters.getPSLaunchCmd(), "python run-ps.py");
    Assert.assertEquals(Resources.createResource(4096, 4),
        jobRunParameters.getPsResource());
    Assert.assertEquals(jobRunParameters.getWorkerLaunchCmd(),
        "python run-job.py");
    Assert.assertEquals(Resources.createResource(2048, 2),
        jobRunParameters.getWorkerResource());
    Assert.assertEquals(jobRunParameters.getDockerImageName(),
        "tf-docker:1.1.0");
    Assert.assertTrue(SubmarineLogs.isVerbose());
  }

  @Test
  public void testBasicRunJobForSingleNodeTraining() throws Exception {
    MockClientContext mockClientContext = new MockClientContext();
    JobSubmitter mockJobSubmitter = mock(JobSubmitter.class);
    RunJobCli runJobCli = new RunJobCli(mockClientContext, mockJobSubmitter);
    Assert.assertFalse(SubmarineLogs.isVerbose());

    runJobCli.run(
        new String[] { "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "hdfs://input", "--checkpoint_path", "hdfs://output",
            "--num_workers", "1", "--worker_launch_cmd", "python run-job.py",
            "--worker_resources", "memory=4g,vcores=2", "--tensorboard",
            "true", "--verbose" });

    JobRunParameters jobRunParameters = runJobCli.getRunJobParameters();

    Assert.assertEquals(jobRunParameters.getInputPath(), "hdfs://input");
    Assert.assertEquals(jobRunParameters.getCheckpointPath(), "hdfs://output");
    Assert.assertEquals(jobRunParameters.getNumWorkers(), 1);
    Assert.assertEquals(jobRunParameters.getWorkerLaunchCmd(),
        "python run-job.py");
    Assert.assertEquals(Resources.createResource(4096, 2),
        jobRunParameters.getWorkerResource());
    Assert.assertTrue(SubmarineLogs.isVerbose());
  }

  @Test
  public void testLaunchCommandPatternReplace() throws Exception {
    MockClientContext mockClientContext = new MockClientContext();
    JobSubmitter mockJobSubmitter = mock(JobSubmitter.class);
    RunJobCli runJobCli = new RunJobCli(mockClientContext, mockJobSubmitter);
    Assert.assertFalse(SubmarineLogs.isVerbose());

    runJobCli.run(
        new String[] { "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "hdfs://input", "--checkpoint_path", "hdfs://output",
            "--num_workers", "3", "--num_ps", "2", "--worker_launch_cmd",
            "python run-job.py --input=%input_path% --model_dir=%checkpoint_path% --export_dir=%saved_model_path%/savedmodel",
            "--worker_resources", "memory=2048,vcores=2", "--ps_resources",
            "memory=4096,vcores=4", "--tensorboard", "true", "--ps_launch_cmd",
            "python run-ps.py --input=%input_path% --model_dir=%checkpoint_path%/model",
            "--verbose" });

    Assert.assertEquals(
        "python run-job.py --input=hdfs://input --model_dir=hdfs://output "
            + "--export_dir=hdfs://output/savedmodel",
        runJobCli.getRunJobParameters().getWorkerLaunchCmd());
    Assert.assertEquals(
        "python run-ps.py --input=hdfs://input --model_dir=hdfs://output/model",
        runJobCli.getRunJobParameters().getPSLaunchCmd());
  }
}
