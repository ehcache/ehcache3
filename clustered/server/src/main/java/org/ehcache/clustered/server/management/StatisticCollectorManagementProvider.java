/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ehcache.clustered.server.management;

import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.management.registry.ManagementRegistry;
import org.terracotta.management.registry.action.ExposedObject;
import org.terracotta.management.registry.action.Named;
import org.terracotta.management.registry.action.RequiredContext;
import org.terracotta.management.registry.collect.DefaultStatisticCollector;
import org.terracotta.management.registry.collect.StatisticCollector;
import org.terracotta.management.registry.collect.StatisticCollectorProvider;
import org.terracotta.management.service.monitoring.MonitoringService;
import org.terracotta.management.service.monitoring.registry.provider.MonitoringServiceAware;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Named("StatisticCollectorCapability")
@RequiredContext({@Named("consumerId")})
class StatisticCollectorManagementProvider extends StatisticCollectorProvider<StatisticCollector> implements MonitoringServiceAware {

  private volatile MonitoringService monitoringService;
  private final DefaultStatisticCollector statisticCollector;

  StatisticCollectorManagementProvider(ManagementRegistry managementRegistry, StatisticConfiguration statisticConfiguration, ScheduledExecutorService scheduledExecutorService, String[] statsCapabilitynames) {
    super(StatisticCollector.class, Context.create(managementRegistry.getContextContainer().getName(), managementRegistry.getContextContainer().getValue()));

    long timeToDisableMs = TimeUnit.MILLISECONDS.convert(statisticConfiguration.timeToDisable(), statisticConfiguration.timeToDisableUnit());
    long pollingIntervalMs = Math.round(timeToDisableMs * 0.75); // we poll at 75% of the time to disable (before the time to disable happens)

    statisticCollector = new DefaultStatisticCollector(
      managementRegistry,
      scheduledExecutorService,
      statistics -> monitoringService.pushServerEntityStatistics(statistics.toArray(new ContextualStatistics[statistics.size()])),
      // TODO FIXME: there is no timesource service in voltron: https://github.com/Terracotta-OSS/terracotta-apis/issues/167
      System::currentTimeMillis,
      pollingIntervalMs,
      TimeUnit.MILLISECONDS,
      statsCapabilitynames
    );
  }

  @Override
  public void setMonitoringService(MonitoringService monitoringService) {
    this.monitoringService = monitoringService;
  }

  @Override
  protected void dispose(ExposedObject<StatisticCollector> exposedObject) {
    exposedObject.getTarget().stopStatisticCollector();
  }

  void init() {
    register(statisticCollector);
    statisticCollector.startStatisticCollector();
  }

}
