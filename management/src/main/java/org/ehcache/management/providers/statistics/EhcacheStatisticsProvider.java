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
package org.ehcache.management.providers.statistics;

import org.ehcache.Ehcache;
import org.ehcache.management.config.StatisticsProviderConfiguration;
import org.ehcache.management.providers.ManagementProvider;
import org.ehcache.management.utils.ContextHelper;
import org.ehcache.util.ConcurrentWeakIdentityHashMap;
import org.terracotta.management.capabilities.context.CapabilityContext;
import org.terracotta.management.capabilities.descriptors.Descriptor;
import org.terracotta.management.stats.Statistic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author Ludovic Orban
 */
public class EhcacheStatisticsProvider implements ManagementProvider<Ehcache> {

  private final ConcurrentMap<Ehcache, EhcacheStatistics> statistics = new ConcurrentWeakIdentityHashMap<Ehcache, EhcacheStatistics>();

  private final StatisticsProviderConfiguration configuration;
  private final ScheduledExecutorService executor;

  public EhcacheStatisticsProvider(StatisticsProviderConfiguration statisticsProviderConfiguration, ScheduledExecutorService executor) {
    this.configuration = statisticsProviderConfiguration;
    this.executor = executor;
  }

  public StatisticsProviderConfiguration getConfiguration() {
    return configuration;
  }

  @Override
  public void register(Ehcache ehcache) {
    statistics.putIfAbsent(ehcache, createStatistics(ehcache));
  }

  EhcacheStatistics createStatistics(Ehcache ehcache) {
    return new EhcacheStatistics(ehcache, configuration, executor);
  }

  @Override
  public void unregister(Ehcache ehcache) {
    EhcacheStatistics removed = statistics.remove(ehcache);
    if (removed != null) {
      removed.dispose();
    }
  }

  @Override
  public Class<Ehcache> managedType() {
    return Ehcache.class;
  }

  @Override
  public Set<Descriptor> descriptions() {
    Set<Descriptor> capabilities = new HashSet<Descriptor>();
    for (EhcacheStatistics ehcacheStatistics : this.statistics.values()) {
      capabilities.addAll(ehcacheStatistics.capabilities());
    }
    return capabilities;
  }

  @Override
  public CapabilityContext capabilityContext() {
    return new CapabilityContext(Arrays.asList(new CapabilityContext.Attribute("cacheManagerName", true), new CapabilityContext.Attribute("cacheName", true)));
  }

  @Override
  public <T extends Statistic<?>> Collection<T> collectStatistics(Map<String, String> context, String... statisticNames) {
    String cacheManagerName = context.get("cacheManagerName");
    if (cacheManagerName == null) {
      throw new IllegalArgumentException("Missing cache manager name from context");
    }
    String cacheName = context.get("cacheName");
    if (cacheName == null) {
      throw new IllegalArgumentException("Missing cache name from context");
    }

    for (Map.Entry<Ehcache, EhcacheStatistics> entry : statistics.entrySet()) {
      if (!ContextHelper.findCacheManagerName(entry.getKey()).equals(cacheManagerName) ||
          !ContextHelper.findCacheName(entry.getKey()).equals(cacheName)) {
        continue;
      }

      Collection<T> result = new ArrayList<T>();
      for (String statisticName : statisticNames) {
        result.addAll(entry.getValue().<T>queryStatistic(statisticName));
      }
      return result;
    }

    throw new IllegalArgumentException("No such cache manager / cache pair : [" + cacheManagerName + " / " + cacheName + "]");
  }

  @Override
  public Object callAction(Map<String, String> context, String methodName, String[] argClassNames, Object[] args) {
    throw new UnsupportedOperationException("Not an action provider : " + getClass().getName());
  }

}
