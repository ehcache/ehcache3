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

import org.ehcache.Cache;
import org.ehcache.EhcacheManager;
import org.ehcache.Status;
import org.ehcache.events.CacheManagerListener;
import org.ehcache.internal.concurrent.ConcurrentHashMap;
import org.ehcache.management.ManagementRegistry;
import org.ehcache.management.SharedManagementService;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.service.CacheManagerProviderService;
import org.ehcache.spi.service.ServiceDependencies;
import org.terracotta.management.capabilities.Capability;
import org.terracotta.management.context.ContextContainer;
import org.terracotta.management.stats.Statistic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * This service can be registered across several cache managers and provides a way to access per-cache manager management registry
 *
 * @author Mathieu Carbou
 */
@ServiceDependencies({CacheManagerProviderService.class, ManagementRegistry.class})
public class DefaultSharedManagementService implements SharedManagementService {

  private final ConcurrentMap<String, ManagementRegistry> delegates = new ConcurrentHashMap<String, ManagementRegistry>();

  @Override
  public void start(final ServiceProvider serviceProvider) {
    final ManagementRegistry managementRegistry = serviceProvider.getService(ManagementRegistry.class);
    final String alias = managementRegistry.getConfiguration().getCacheManagerAlias();
    final EhcacheManager ehcacheManager = serviceProvider.getService(CacheManagerProviderService.class).getCacheManager();

    ehcacheManager.registerListener(new CacheManagerListener() {
      @Override
      public void cacheAdded(String alias, Cache<?, ?> cache) {
      }

      @Override
      public void cacheRemoved(String alias, Cache<?, ?> cache) {
      }

      @Override
      public void stateTransition(Status from, Status to) {
        if (from == Status.UNINITIALIZED && to == Status.AVAILABLE) {
          if (delegates.put(alias, managementRegistry) != null) {
            throw new IllegalStateException("Duplicate cache manager alias in ManagementRegistry : " + alias);
          }
        } else if (from == Status.AVAILABLE && to == Status.UNINITIALIZED) {
          delegates.remove(alias);
          ehcacheManager.deregisterListener(this);
        }
      }
    });
  }

  @Override
  public void stop() {
  }

  @Override
  public Collection<ContextContainer> getContexts() {
    Collection<ContextContainer> contexts = new ArrayList<ContextContainer>();
    for (ManagementRegistry delegate : delegates.values()) {
      contexts.add(delegate.getContext());
    }
    return contexts;
  }

  @Override
  public Map<String, Collection<Capability>> getCapabilities() {
    Map<String, Collection<Capability>> capabilities = new LinkedHashMap<String, Collection<Capability>>();
    for (Map.Entry<String, ManagementRegistry> entry : delegates.entrySet()) {
      capabilities.put(entry.getKey(), entry.getValue().getCapabilities());
    }
    return capabilities;
  }

  @Override
  public <T extends Statistic<?>> List<Collection<T>> collectStatistics(List<Map<String, String>> contextList, String capabilityName, String... statisticNames) {
    // pre-validation
    for (Map<String, String> ctx : contextList) {
      if (ctx.get("cacheManagerName") == null) {
        throw new IllegalArgumentException("Missing cache manager name from context : " + ctx + " in context list " + contextList);
      }
    }
    List<Collection<T>> statistics = new ArrayList<Collection<T>>(contextList.size());
    for (Map<String, String> context : contextList) {
      ManagementRegistry registry = delegates.get(context.get("cacheManagerName"));
      if (registry == null) {
        statistics.add(Collections.<T>emptyList());
      } else {
        statistics.add(registry.<T>collectStatistics(context, capabilityName, statisticNames));
      }
    }
    return statistics;
  }

  @Override
  public <T> List<T> callAction(List<Map<String, String>> contextList, String capabilityName, String methodName, String[] argClassNames, Object[] args) {
    // pre-validation
    for (Map<String, String> ctx : contextList) {
      if (ctx.get("cacheManagerName") == null) {
        throw new IllegalArgumentException("Missing cache manager name from context : " + ctx + " in context list " + contextList);
      }
    }
    List<T> returns = new ArrayList<T>();
    for (Map<String, String> context : contextList) {
      ManagementRegistry registry = delegates.get(context.get("cacheManagerName"));
      if (registry == null) {
        returns.add(null);
      } else {
        returns.add(registry.<T>callAction(context, capabilityName, methodName, argClassNames, args));
      }
    }
    return returns;
  }

}
