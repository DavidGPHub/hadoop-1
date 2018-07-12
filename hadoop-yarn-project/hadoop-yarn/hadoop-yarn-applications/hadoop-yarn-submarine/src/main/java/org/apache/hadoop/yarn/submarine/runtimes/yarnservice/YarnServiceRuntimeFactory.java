package org.apache.hadoop.yarn.submarine.runtimes.yarnservice;

import org.apache.hadoop.yarn.submarine.common.ClientContext;
import org.apache.hadoop.yarn.submarine.runtimes.RuntimeFactory;
import org.apache.hadoop.yarn.submarine.runtimes.common.JobMonitor;
import org.apache.hadoop.yarn.submarine.runtimes.common.JobSubmitter;

public class YarnServiceRuntimeFactory extends RuntimeFactory {
  private JobSubmitter jobSubmitter;
  private JobMonitor jobMonitor;

  public YarnServiceRuntimeFactory(ClientContext clientContext) {
    super(clientContext);
  }

  @Override
  public synchronized JobSubmitter getJobSubmitterInstance() {
    if (jobSubmitter == null) {
      jobSubmitter = new YarnServiceJobSubmitter(super.clientContext);
    }
    return jobSubmitter;
  }

  @Override
  public JobMonitor getJobMonitorInstance() {
    if (jobMonitor == null) {
      jobMonitor = new YarnServiceJobMonitor(super.clientContext);
    }
    return jobMonitor;
  }
}
