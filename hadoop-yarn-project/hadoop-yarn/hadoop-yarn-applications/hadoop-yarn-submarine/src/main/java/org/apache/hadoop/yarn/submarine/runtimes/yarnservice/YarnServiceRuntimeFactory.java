package org.apache.hadoop.yarn.submarine.runtimes.yarnservice;

import org.apache.hadoop.yarn.submarine.common.ClientContext;
import org.apache.hadoop.yarn.submarine.runtimes.RuntimeFactory;
import org.apache.hadoop.yarn.submarine.runtimes.common.FSBasedSubmarineStorageImpl;
import org.apache.hadoop.yarn.submarine.runtimes.common.JobMonitor;
import org.apache.hadoop.yarn.submarine.runtimes.common.JobSubmitter;
import org.apache.hadoop.yarn.submarine.runtimes.common.SubmarineStorage;

public class YarnServiceRuntimeFactory extends RuntimeFactory {

  public YarnServiceRuntimeFactory(ClientContext clientContext) {
    super(clientContext);
  }

  @Override
  protected JobSubmitter internalCreateJobSubmitter() {
    return new YarnServiceJobSubmitter(super.clientContext);
  }

  @Override
  protected JobMonitor internalCreateJobMonitor() {
    return new YarnServiceJobMonitor(super.clientContext);
  }

  @Override
  protected SubmarineStorage internalCreateSubmarineStorage() {
    return new FSBasedSubmarineStorageImpl(super.clientContext);
  }
}
