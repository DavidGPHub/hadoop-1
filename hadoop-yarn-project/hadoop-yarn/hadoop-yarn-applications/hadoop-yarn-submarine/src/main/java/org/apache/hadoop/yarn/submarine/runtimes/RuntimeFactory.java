package org.apache.hadoop.yarn.submarine.runtimes;

import org.apache.hadoop.yarn.submarine.common.ClientContext;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineConfiguration;
import org.apache.hadoop.yarn.submarine.common.exception.SubmarineRuntimeException;
import org.apache.hadoop.yarn.submarine.runtimes.common.JobMonitor;
import org.apache.hadoop.yarn.submarine.runtimes.common.JobSubmitter;

import java.lang.reflect.InvocationTargetException;

public abstract class RuntimeFactory {
  protected ClientContext clientContext;

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

  public abstract JobSubmitter getJobSubmitterInstance();

  public abstract JobMonitor getJobMonitorInstance();
}
