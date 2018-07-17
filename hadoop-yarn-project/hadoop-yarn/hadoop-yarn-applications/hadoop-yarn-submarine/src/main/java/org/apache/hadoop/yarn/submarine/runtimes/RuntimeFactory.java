package org.apache.hadoop.yarn.submarine.runtimes;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.submarine.common.ClientContext;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineConfiguration;
import org.apache.hadoop.yarn.submarine.common.exception.SubmarineRuntimeException;
import org.apache.hadoop.yarn.submarine.runtimes.common.FSBasedSubmarineStorageImpl;
import org.apache.hadoop.yarn.submarine.runtimes.common.JobMonitor;
import org.apache.hadoop.yarn.submarine.runtimes.common.JobSubmitter;
import org.apache.hadoop.yarn.submarine.runtimes.common.SubmarineStorage;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.YarnServiceJobMonitor;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.YarnServiceJobSubmitter;

import java.lang.reflect.InvocationTargetException;

public abstract class RuntimeFactory {
  protected ClientContext clientContext;
  private JobSubmitter jobSubmitter;
  private JobMonitor jobMonitor;
  private SubmarineStorage submarineStorage;

  public RuntimeFactory(ClientContext clientContext) {
    this.clientContext = clientContext;
  }

  public static RuntimeFactory getRuntimeFactory(
      ClientContext clientContext) {
    SubmarineConfiguration submarineConfiguration =
        clientContext.getSubmarineConfig();
    String runtimeClass = submarineConfiguration.get(
        SubmarineConfiguration.RUNTIME_CLASS,
        SubmarineConfiguration.DEFAULT_RUNTIME_CLASS);

    try {
      Class<?> runtimeClazz = Class.forName(runtimeClass);
      if (RuntimeFactory.class.isAssignableFrom(runtimeClazz)) {
        return (RuntimeFactory) runtimeClazz.getConstructor(ClientContext.class).newInstance(clientContext);
      } else {
        throw new SubmarineRuntimeException("Class: " + runtimeClass
            + " not instance of " + RuntimeFactory.class.getCanonicalName());
      }
    } catch (ClassNotFoundException | IllegalAccessException |
             InstantiationException | NoSuchMethodException |
             InvocationTargetException e) {
      throw new SubmarineRuntimeException(
          "Could not instantiate RuntimeFactory: " + runtimeClass, e);
    }
  }

  protected abstract JobSubmitter internalCreateJobSubmitter();

  protected abstract JobMonitor internalCreateJobMonitor();

  protected abstract SubmarineStorage internalCreateSubmarineStorage();

  public synchronized JobSubmitter getJobSubmitterInstance() {
    if (jobSubmitter == null) {
      jobSubmitter = internalCreateJobSubmitter();
    }
    return jobSubmitter;
  }

  public synchronized JobMonitor getJobMonitorInstance() {
    if (jobMonitor == null) {
      jobMonitor = internalCreateJobMonitor();
    }
    return jobMonitor;
  }

  public synchronized SubmarineStorage getSubmarineStorage() {
    if (submarineStorage == null) {
      submarineStorage = internalCreateSubmarineStorage();
    }
    return submarineStorage;
  }

  @VisibleForTesting
  public synchronized void setJobSubmitterInstance(JobSubmitter jobSubmitter) {
    this.jobSubmitter = jobSubmitter;
  }

  @VisibleForTesting
  public synchronized void setJobMonitorInstance(JobMonitor jobMonitor) {
    this.jobMonitor = jobMonitor;
  }

  @VisibleForTesting
  public synchronized void setSubmarineStorage(SubmarineStorage storage) {
    this.submarineStorage = storage;
  }
}
