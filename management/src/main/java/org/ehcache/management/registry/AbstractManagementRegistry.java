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
package org.ehcache.management.registry;

import org.ehcache.management.ManagementRegistry;
import org.ehcache.management.config.StatisticsProviderConfiguration;
import org.ehcache.management.providers.CapabilityContextProvider;
import org.ehcache.management.providers.statistics.EhcacheStatisticsProvider;
import org.ehcache.management.providers.ManagementProvider;
import org.ehcache.spi.ServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.management.capabilities.ActionsCapability;
import org.terracotta.management.capabilities.Capability;
import org.terracotta.management.capabilities.StatisticsCapability;
import org.terracotta.management.capabilities.context.CapabilityContext;
import org.terracotta.management.capabilities.descriptors.Descriptor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Ludovic Orban
 */
public abstract class AbstractManagementRegistry implements ManagementRegistry {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractManagementRegistry.class);

  private final Map<Class<?>, List<ManagementProvider<?>>> managementProviders = new HashMap<Class<?>, List<ManagementProvider<?>>>();
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final CapabilityContextProvider capabilityContextProvider = new CapabilityContextProvider();

  private boolean started = false;

  protected AbstractManagementRegistry() {
  }

  protected void addSupportFor(ManagementProvider<?> managementProvider) {
    Lock lock = this.lock.writeLock();
    lock.lock();
    try {
      List<ManagementProvider<?>> managementProviders = this.managementProviders.get(managementProvider.managedType());
      if (managementProviders == null) {
        managementProviders = new ArrayList<ManagementProvider<?>>();
        this.managementProviders.put(managementProvider.managedType(), managementProviders);
      }
      managementProviders.add(managementProvider);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public <T> Collection<T> contexts() {
    return (Collection<T>) capabilityContextProvider.contexts();
  }

  @Override
  public <T> Collection<T> collectStatistics(Map<String, String> context, String capabilityName, String... statisticNames) {
    Lock lock = this.lock.readLock();
    lock.lock();
    try {
      for (List<ManagementProvider<?>> providers : managementProviders.values()) {
        for (ManagementProvider<?> provider : providers) {
          if (provider.getClass().getName().equals(capabilityName)) {
            return (Collection<T>) provider.collectStatistics(context, statisticNames);
          }
        }
      }
      throw new IllegalArgumentException("No such capability registered : " + capabilityName);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public <T> T callAction(Map<String, String> context, String capabilityName, String methodName, String[] argClassNames, Object[] args) {
    Lock lock = this.lock.readLock();
    lock.lock();
    try {
      for (List<ManagementProvider<?>> providers : managementProviders.values()) {
        for (ManagementProvider<?> provider : providers) {
          if (provider.getClass().getName().equals(capabilityName)) {
            return (T) provider.callAction(context, methodName, argClassNames, args);
          }
        }
      }
      throw new IllegalArgumentException("No such capability registered : " + capabilityName);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public <T> void register(Class<T> managedType, T managedObject) {
    Lock lock = this.lock.writeLock();
    lock.lock();
    try {
      if (!started) {
        throw new IllegalStateException("Management registry not started");
      }

      List<ManagementProvider<?>> managementProviders = this.managementProviders.get(managedType);
      if (managementProviders == null) {
        LOGGER.warn("No registered management provider that supports {}", managedType);
        return;
      }
      for (ManagementProvider<?> managementProvider : managementProviders) {
        ManagementProvider<T> typedManagementProvider = (ManagementProvider<T>) managementProvider;
        typedManagementProvider.register(managedObject);
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public <T> void unregister(Class<T> managedType, T managedObject) {
    Lock lock = this.lock.writeLock();
    lock.lock();
    try {
      if (!started) {
        throw new IllegalStateException("Management registry not started");
      }

      List<ManagementProvider<?>> managementProviders = this.managementProviders.get(managedType);
      if (managementProviders == null) {
        LOGGER.warn("No registered management provider that supports {}", managedType);
        return;
      }
      for (ManagementProvider<?> managementProvider : managementProviders) {
        ManagementProvider<T> typedManagementProvider = (ManagementProvider<T>) managementProvider;
        typedManagementProvider.unregister(managedObject);
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public <T> Collection<T> capabilities() {
    Lock lock = this.lock.readLock();
    lock.lock();
    try {
      Collection<Capability> result = new ArrayList<Capability>();

      for (Map.Entry<Class<?>, List<ManagementProvider<?>>> entry : managementProviders.entrySet()) {
        List<ManagementProvider<?>> managementProviders = entry.getValue();
        for (ManagementProvider<?> managementProvider : managementProviders) {
          if (managementProvider.descriptions().isEmpty()) {
            continue;
          }

          if (managementProvider instanceof EhcacheStatisticsProvider) {
            Set<Descriptor> descriptors = managementProvider.descriptions();
            String name = managementProvider.getClass().getName();
            CapabilityContext context = managementProvider.capabilityContext();
            EhcacheStatisticsProvider ehcacheStatisticsProvider = (EhcacheStatisticsProvider) managementProvider;
            StatisticsProviderConfiguration configuration = ehcacheStatisticsProvider.getConfiguration();
            StatisticsCapability.Properties properties = new StatisticsCapability.Properties(configuration.averageWindowDuration(),
                configuration.averageWindowUnit(), configuration.historySize(), configuration.historyInterval(),
                configuration.historyIntervalUnit(), configuration.timeToDisable(), configuration.timeToDisableUnit());

            StatisticsCapability statisticsCapability = new StatisticsCapability(name, properties, descriptors, context);
            result.add(statisticsCapability);
          } else {
            Set<Descriptor> descriptors = managementProvider.descriptions();
            String name = managementProvider.getClass().getName();
            CapabilityContext context = managementProvider.capabilityContext();

            ActionsCapability actionsCapability = new ActionsCapability(name, descriptors, context);
            result.add(actionsCapability);
          }
        }
      }

      return (Collection<T>) result;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void start(ServiceProvider serviceProvider) {
    Lock lock = this.lock.writeLock();
    lock.lock();
    try {
      addSupportFor(capabilityContextProvider);
      started = true;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void stop() {
    Lock lock = this.lock.writeLock();
    lock.lock();
    try {
      managementProviders.clear();
      started = false;
    } finally {
      lock.unlock();
    }
  }
}
