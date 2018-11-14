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
import org.ehcache.config.CacheConfiguration;
import org.ehcache.core.events.CacheManagerListener;
import org.ehcache.core.spi.service.CacheManagerProviderService;
import org.ehcache.core.spi.service.ExecutionService;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.core.spi.store.InternalCacheManager;
import org.ehcache.core.spi.time.TimeSourceService;
import org.ehcache.management.ManagementRegistryService;
import org.ehcache.management.ManagementRegistryServiceConfiguration;
import org.ehcache.management.cluster.Clustering;
import org.ehcache.management.cluster.ClusteringManagementService;
import org.ehcache.management.cluster.DefaultClusteringManagementServiceConfiguration;
import org.ehcache.management.providers.CacheBinding;
import org.ehcache.management.providers.EhcacheStatisticCollectorProvider;
import org.ehcache.management.providers.actions.EhcacheActionProvider;
import org.ehcache.management.providers.settings.EhcacheSettingsProvider;
import org.ehcache.management.providers.statistics.EhcacheStatisticsProvider;
import org.ehcache.spi.service.OptionalServiceDependencies;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.spi.service.ServiceProvider;
import org.terracotta.management.model.context.ContextContainer;
import org.terracotta.management.registry.DefaultManagementRegistry;
import org.terracotta.statistics.StatisticsManager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

@ServiceDependencies({CacheManagerProviderService.class, StatisticsService.class, TimeSourceService.class, ExecutionService.class})
@OptionalServiceDependencies({
  "org.ehcache.clustered.client.service.EntityService",
  "org.ehcache.clustered.client.service.ClusteringService"})
public class DefaultManagementRegistryService extends DefaultManagementRegistry implements ManagementRegistryService, CacheManagerListener {

  private final ManagementRegistryServiceConfiguration configuration;
  private volatile InternalCacheManager cacheManager;
  private volatile ClusteringManagementService clusteringManagementService;
  private volatile boolean clusteringManagementServiceAutoStarted;

  public DefaultManagementRegistryService() {
    this(new DefaultManagementRegistryConfiguration());
  }

  public DefaultManagementRegistryService(ManagementRegistryServiceConfiguration configuration) {
    super(null); // context container creation is overriden here
    this.configuration = configuration == null ? new DefaultManagementRegistryConfiguration() : configuration;
  }

  @Override
  public void start(final ServiceProvider<Service> serviceProvider) {
    this.cacheManager = serviceProvider.getService(CacheManagerProviderService.class).getCacheManager();

    StatisticsService statisticsService = serviceProvider.getService(StatisticsService.class);
    TimeSourceService timeSourceService = serviceProvider.getService(TimeSourceService.class);

    // initialize management capabilities (stats, action calls, etc)
    addManagementProvider(new EhcacheActionProvider(getConfiguration()));
    addManagementProvider(new EhcacheStatisticsProvider(getConfiguration(), statisticsService, timeSourceService.getTimeSource()));
    addManagementProvider(new EhcacheStatisticCollectorProvider(getConfiguration()));
    addManagementProvider(new EhcacheSettingsProvider(getConfiguration(), cacheManager));

    this.cacheManager.registerListener(this);

    // optional clustering support. Management works both in standalone and clustering mode
    // this feature detection is done to avoid having another "cluster-management" module that would depend on both management and clustering
    this.clusteringManagementService = serviceProvider.getService(ClusteringManagementService.class);
    if (this.clusteringManagementService == null && Clustering.isAvailable(serviceProvider)) {
      this.clusteringManagementService = Clustering.newClusteringManagementService(new DefaultClusteringManagementServiceConfiguration());
      this.clusteringManagementService.start(serviceProvider);
      this.clusteringManagementServiceAutoStarted = true;
    } else {
      this.clusteringManagementServiceAutoStarted = false;
    }
  }

  @Override
  public void stop() {
    if (this.clusteringManagementService != null && this.clusteringManagementServiceAutoStarted) {
      this.clusteringManagementService.stop();
    }
    this.clusteringManagementService = null;

    super.close();
  }

  @Override
  public void cacheAdded(String alias, Cache<?, ?> cache) {
    StatisticsManager.associate(cache).withParent(cacheManager);

    register(new CacheBinding(alias, cache));
  }

  @Override
  public void cacheRemoved(String alias, Cache<?, ?> cache) {
    unregister(new CacheBinding(alias, cache));

    StatisticsManager.dissociate(cache).fromParent(cacheManager);
  }

  @Override
  public void stateTransition(Status from, Status to) {
    // we are only interested when cache manager is initializing (but at the end of the initialization)
    switch (to) {

      case AVAILABLE:
        register(cacheManager);

        // we need to fire cacheAdded events because cacheAdded are not fired when caches are created at init because we are within a transition
        for (Map.Entry<String, CacheConfiguration<?, ?>> entry : cacheManager.getRuntimeConfiguration().getCacheConfigurations().entrySet()) {
          String alias = entry.getKey();
          CacheConfiguration<?, ?> configuration = entry.getValue();
          Cache<?, ?> cache = cacheManager.getCache(alias, configuration.getKeyType(), configuration.getValueType());
          cacheAdded(alias, cache);
        }
        break;

      case UNINITIALIZED:
        this.cacheManager.deregisterListener(this);
        break;

      case MAINTENANCE:
        // in case we need management capabilities in maintenance mode
        break;

      default:
        throw new AssertionError("Unsupported state: " + to);
    }
  }

  @Override
  public ManagementRegistryServiceConfiguration getConfiguration() {
    return configuration;
  }

  @Override
  public ContextContainer getContextContainer() {
    Collection<ContextContainer> cacheCtx = new ArrayList<>();
    for (String cacheName : this.cacheManager.getRuntimeConfiguration().getCacheConfigurations().keySet()) {
      cacheCtx.add(new ContextContainer("cacheName", cacheName));
    }
    return new ContextContainer("cacheManagerName", getConfiguration().getContext().get("cacheManagerName"), cacheCtx);
  }

}
