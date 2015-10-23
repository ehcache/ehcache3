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
import org.ehcache.config.CacheConfiguration;
import org.ehcache.events.CacheManagerListener;
import org.ehcache.management.CapabilityManagement;
import org.ehcache.management.ManagementRegistry;
import org.ehcache.management.ManagementRegistryConfiguration;
import org.ehcache.management.providers.ManagementProvider;
import org.ehcache.management.providers.actions.EhcacheActionProvider;
import org.ehcache.management.providers.statistics.EhcacheStatisticsProvider;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.service.CacheManagerProviderService;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.spi.service.ThreadPoolsService;
import org.terracotta.management.capabilities.Capability;
import org.terracotta.management.context.ContextContainer;
import org.terracotta.statistics.StatisticsManager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Ludovic Orban
 */
@ServiceDependencies({CacheManagerProviderService.class, ThreadPoolsService.class})
public class DefaultManagementRegistry implements ManagementRegistry, CacheManagerListener {

  private final ManagementRegistryConfiguration configuration;
  private final List<ManagementProvider<?>> managementProviders = new CopyOnWriteArrayList<ManagementProvider<?>>();

  private volatile ThreadPoolsService threadPoolsService;
  private volatile EhcacheManager cacheManager;

  public DefaultManagementRegistry() {
    this(new DefaultManagementRegistryConfiguration());
  }

  public DefaultManagementRegistry(ManagementRegistryConfiguration configuration) {
    if (configuration == null) {
      throw new NullPointerException();
    }
    this.configuration = configuration;
  }

  @Override
  public void start(final ServiceProvider serviceProvider) {
    this.threadPoolsService = serviceProvider.getService(ThreadPoolsService.class);
    this.cacheManager = serviceProvider.getService(CacheManagerProviderService.class).getCacheManager();

    this.cacheManager.registerListener(this);
  }

  @Override
  public void stop() {
    this.cacheManager.deregisterListener(this);

    for (ManagementProvider<?> managementProvider : managementProviders) {
      managementProvider.close();
    }

    managementProviders.clear();
    threadPoolsService = null;
    cacheManager = null;
  }

  @Override
  public void cacheAdded(String alias, Cache<?, ?> cache) {
    StatisticsManager.associate(cache).withParent(cacheManager);

    register(cache);
    register(new CacheBinding(alias, cache));
  }

  @Override
  public void cacheRemoved(String alias, Cache<?, ?> cache) {
    unregister(cache);
    unregister(new CacheBinding(alias, cache));

    StatisticsManager.dissociate(cache).fromParent(cacheManager);
  }

  @Override
  public void stateTransition(Status from, Status to) {
    // we are only interested when cache manager is initializing (but at the end of the initialization) 
    if (from == Status.UNINITIALIZED && to == Status.AVAILABLE) {

      // initialize management capabilities (stats, action calls, etc)
      addManagementProvider(new EhcacheActionProvider(getConfiguration().getCacheManagerAlias()));
      addManagementProvider(new EhcacheStatisticsProvider(
          getConfiguration().getCacheManagerAlias(),
          getConfiguration().getConfigurationFor(EhcacheStatisticsProvider.class),
          threadPoolsService.getStatisticsExecutor()));

      register(cacheManager);

      // we need to fire cacheAdded events because cacheAdded are not fired when caches are created at init because we are within a transition
      for (Map.Entry<String, CacheConfiguration<?, ?>> entry : cacheManager.getRuntimeConfiguration().getCacheConfigurations().entrySet()) {
        String alias = entry.getKey();
        CacheConfiguration<?, ?> configuration = entry.getValue();
        Cache<?, ?> cache = cacheManager.getCache(alias, configuration.getKeyType(), configuration.getValueType());
        cacheAdded(alias, cache);
      }
    }
  }

  @Override
  public ManagementRegistryConfiguration getConfiguration() {
    return configuration;
  }

  @Override
  public void addManagementProvider(ManagementProvider<?> provider) {
    String name = provider.getCapabilityName();
    for (ManagementProvider<?> managementProvider : managementProviders) {
      if (managementProvider.getCapabilityName().equals(name)) {
        throw new IllegalStateException("Duplicated management provider name : " + name);
      }
    }
    managementProviders.add(provider);
  }

  @Override
  public void removeManagementProvider(ManagementProvider<?> provider) {
    managementProviders.remove(provider);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void register(Object managedObject) {
    for (ManagementProvider managementProvider : managementProviders) {
      if (managementProvider.managedType().isInstance(managedObject)) {
        managementProvider.register(managedObject);
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void unregister(Object managedObject) {
    for (ManagementProvider managementProvider : managementProviders) {
      if (managementProvider.managedType().isInstance(managedObject)) {
        managementProvider.unregister(managedObject);
      }
    }
  }

  @Override
  public ContextContainer getContext() {
    Collection<ContextContainer> cacheCtx = new ArrayList<ContextContainer>();
    for (String cacheName : this.cacheManager.getRuntimeConfiguration().getCacheConfigurations().keySet()) {
      cacheCtx.add(new ContextContainer("cacheName", cacheName, null));
    }
    return new ContextContainer("cacheManagerName", getConfiguration().getCacheManagerAlias(), cacheCtx);
  }

  @Override
  public CapabilityManagement withCapability(String capabilityName) {
    return new DefaultCapabilityManagement(this, capabilityName);
  }

  @Override
  public Collection<Capability> getCapabilities() {
    Collection<Capability> capabilities = new ArrayList<Capability>();
    for (ManagementProvider<?> managementProvider : managementProviders) {
      capabilities.add(managementProvider.getCapability());
    }
    return capabilities;
  }

  @Override
  public List<ManagementProvider<?>> getManagementProvidersByCapability(String capabilityName) {
    List<ManagementProvider<?>> allProviders = new ArrayList<ManagementProvider<?>>();
    for (ManagementProvider<?> provider : managementProviders) {
      if (provider.getCapabilityName().equals(capabilityName)) {
        allProviders.add(provider);
      }
    }
    return allProviders;
  }

}
