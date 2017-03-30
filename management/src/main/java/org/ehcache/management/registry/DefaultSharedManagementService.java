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
import org.ehcache.Status;
import org.ehcache.core.events.CacheManagerListener;
import org.ehcache.core.spi.service.CacheManagerProviderService;
import org.ehcache.core.spi.store.InternalCacheManager;
import org.ehcache.management.ManagementRegistryService;
import org.ehcache.management.SharedManagementService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.spi.service.ServiceProvider;
import org.terracotta.management.model.capabilities.Capability;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.context.ContextContainer;
import org.terracotta.management.registry.CapabilityManagement;
import org.terracotta.management.registry.DefaultCapabilityManagement;
import org.terracotta.management.registry.ManagementProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This service can be registered across several cache managers and provides a way to access per-cache manager management registry
 */
@ServiceDependencies({CacheManagerProviderService.class, ManagementRegistryService.class})
public class DefaultSharedManagementService implements SharedManagementService {

  private final ConcurrentMap<Context, ManagementRegistryService> delegates = new ConcurrentHashMap<Context, ManagementRegistryService>();

  @Override
  public void start(final ServiceProvider<Service> serviceProvider) {
    final ManagementRegistryService managementRegistry = serviceProvider.getService(ManagementRegistryService.class);
    final Context cmContext = managementRegistry.getConfiguration().getContext();
    final InternalCacheManager cacheManager =
      serviceProvider.getService(CacheManagerProviderService.class).getCacheManager();

    cacheManager.registerListener(new CacheManagerListener() {
      @Override
      public void cacheAdded(String alias, Cache<?, ?> cache) {
      }

      @Override
      public void cacheRemoved(String alias, Cache<?, ?> cache) {
      }

      @Override
      public void stateTransition(Status from, Status to) {
        switch (to) {

          case AVAILABLE:
            delegates.put(cmContext, managementRegistry);
            break;

          case UNINITIALIZED:
            delegates.remove(cmContext);
            cacheManager.deregisterListener(this);
            break;

          case MAINTENANCE:
            // in case we need management capabilities in maintenance mode
            break;

          default:
            throw new AssertionError("Unsupported state: " + to);
        }
      }
    });
  }

  @Override
  public void stop() {
  }

  @Override
  public Map<Context, ContextContainer> getContextContainers() {
    Map<Context, ContextContainer> contexts = new HashMap<Context, ContextContainer>();
    for (Map.Entry<Context, ManagementRegistryService> entry : delegates.entrySet()) {
      contexts.put(entry.getKey(), entry.getValue().getContextContainer());
    }
    return contexts;
  }

  @Override
  public Collection<? extends Capability> getCapabilities() {
    Collection<Capability> capabilities = new ArrayList<Capability>();
    for (ManagementRegistryService registryService : delegates.values()) {
      capabilities.addAll(registryService.getCapabilities());
    }
    return capabilities;
  }

  @Override
  public Collection<String> getCapabilityNames() {
    Collection<String> names = new TreeSet<String>();
    for (ManagementRegistryService registryService : delegates.values()) {
      names.addAll(registryService.getCapabilityNames());
    }
    return names;
  }

  @Override
  public Map<Context, Collection<? extends Capability>> getCapabilitiesByContext() {
    Map<Context, Collection<? extends Capability>> capabilities = new HashMap<Context, Collection<? extends Capability>>();
    for (Map.Entry<Context, ManagementRegistryService> entry : delegates.entrySet()) {
      capabilities.put(entry.getKey(), entry.getValue().getCapabilities());
    }
    return capabilities;
  }

  @Override
  public Collection<ManagementProvider<?>> getManagementProvidersByCapability(String capabilityName) {
    List<ManagementProvider<?>> allProviders = new ArrayList<ManagementProvider<?>>();
    for (ManagementRegistryService managementRegistry : delegates.values()) {
      allProviders.addAll(managementRegistry.getManagementProvidersByCapability(capabilityName));
    }
    return allProviders;
  }

  @Override
  public CapabilityManagement withCapability(String capabilityName) {
    return new DefaultCapabilityManagement(this, capabilityName);
  }

}
