/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.o2.proxima.core.metrics;

import com.google.auto.service.AutoService;
import java.lang.management.ManagementFactory;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import lombok.extern.slf4j.Slf4j;

@AutoService(MetricsRegistrar.class)
@Slf4j
public class JmxMetricsRegistrar implements MetricsRegistrar {

  private final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

  @Override
  public void register(Metric<?> metric) {
    registerWithMBeanServer(metric);
  }

  /** Register this metric with {@link MBeanServer}. */
  protected void registerWithMBeanServer(Metric<?> m) {
    try {
      ObjectName mxbeanName =
          new ObjectName(
              m.getGroup() + "." + m.getName() + ":type=" + m.getClass().getSimpleName());
      mbs.registerMBean(m, mxbeanName);
    } catch (InstanceAlreadyExistsException
        | MBeanRegistrationException
        | NotCompliantMBeanException
        | MalformedObjectNameException ex) {
      log.warn("Failed to register metric {} with MBeanServer", m, ex);
    }
  }
}
