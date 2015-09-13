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

import org.ehcache.EhcacheManager;
import org.ehcache.internal.concurrent.ConcurrentHashMap;
import org.ehcache.management.ManagementRegistry;
import org.ehcache.management.SharedManagementService;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.alias.AliasService;
import org.ehcache.spi.lifecycle.LifeCycleListener;
import org.ehcache.spi.lifecycle.LifeCycleService;
import org.ehcache.spi.service.ServiceDependencies;
import org.terracotta.management.capabilities.Capability;
import org.terracotta.management.context.ContextContainer;
import org.terracotta.management.stats.Statistic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * This service can be registered across several cache managers and provides a way to access per-cache manager management registry
 *
 * @author Mathieu Carbou
 */
@ServiceDependencies({ManagementRegistry.class, LifeCycleService.class, AliasService.class})
public class DefaultSharedManagementService implements SharedManagementService {

  private final ConcurrentMap<String, ManagementRegistry> delegates = new ConcurrentHashMap<String, ManagementRegistry>();

  @Override
  public void start(final ServiceProvider serviceProvider) {
    serviceProvider.getService(LifeCycleService.class).register(EhcacheManager.class, new LifeCycleListener<EhcacheManager>() {
      @Override
      public void afterInitialization(EhcacheManager instance) {
        String alias = serviceProvider.getService(AliasService.class).getCacheManagerAlias();
        ManagementRegistry registry = serviceProvider.getService(ManagementRegistry.class);
        if (delegates.putIfAbsent(alias, registry) != null) {
          throw new IllegalStateException("Duplicate cache manager alias: " + alias);
        }
      }

      @Override
      public void afterClosing(EhcacheManager instance) {
        String alias = serviceProvider.getService(AliasService.class).getCacheManagerAlias();
        delegates.remove(alias);
      }
    });
  }

  @Override
  public void stop() {
    // we do not stop because this service is shared
  }

  @Override
  public Collection<ContextContainer> contexts() {
    Collection<ContextContainer> contexts = new ArrayList<ContextContainer>();
    for (ManagementRegistry delegate : delegates.values()) {
      contexts.addAll(delegate.contexts());
    }
    return contexts;
  }

  @Override
  public List<Collection<Capability>> capabilities(List<Map<String, String>> contextList) {
    List<Collection<Capability>> capabilities = new ArrayList<Collection<Capability>>();
    for (Map<String, String> context : contextList) {
      String cacheManagerName = context.get("cacheManagerName");
      if (cacheManagerName == null) {
        throw new IllegalArgumentException("Missing cache manager name from context");
      }
      ManagementRegistry registry = delegates.get(cacheManagerName);
      capabilities.add(registry == null ? Collections.<Capability>emptyList() : registry.capabilities());
    }
    return capabilities;
  }

  @Override
  public <T extends Statistic<?>> List<Collection<T>> collectStatistics(List<Map<String, String>> contextList, String capabilityName, String... statisticNames) {
    List<Collection<T>> results = new ArrayList<Collection<T>>(contextList.size());
    for (Map<String, String> context : contextList) {
      String cacheManagerName = context.get("cacheManagerName");
      if (cacheManagerName == null) {
        throw new IllegalArgumentException("Missing cache manager name from context");
      }
      ManagementRegistry registry = delegates.get(cacheManagerName);
      results.add(registry == null ? Collections.<T>emptyList() : registry.<T>collectStatistics(context, capabilityName, statisticNames));
    }
    return results;
  }

  @Override
  public <T> List<T> callAction(List<Map<String, String>> contextList, String capabilityName, String methodName, String[] argClassNames, Object[] args) {
    List<T> results = new ArrayList<T>();
    for (Map<String, String> context : contextList) {
      String cacheManagerName = context.get("cacheManagerName");
      if (cacheManagerName == null) {
        throw new IllegalArgumentException("Missing cache manager name from context");
      }
      ManagementRegistry registry = delegates.get(cacheManagerName);
      results.add(registry == null ? null : registry.<T>callAction(context, capabilityName, methodName, argClassNames, args));
    }
    return results;
  }

}
