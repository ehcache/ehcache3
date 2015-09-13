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

import org.ehcache.Ehcache;
import org.ehcache.EhcacheBinding;
import org.ehcache.EhcacheManager;
import org.ehcache.management.ManagementRegistry;
import org.ehcache.management.ManagementRegistryConfiguration;
import org.ehcache.management.config.EhcacheStatisticsProviderConfiguration;
import org.ehcache.management.config.StatisticsProviderConfiguration;
import org.ehcache.management.providers.CapabilityContextProvider;
import org.ehcache.management.providers.ManagementProvider;
import org.ehcache.management.providers.actions.EhcacheActionProvider;
import org.ehcache.management.providers.statistics.EhcacheStatisticsProvider;
import org.ehcache.spi.lifecycle.DeferredLifeCycleListener;
import org.ehcache.util.Deferred;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.alias.AliasService;
import org.ehcache.spi.lifecycle.LifeCycleListener;
import org.ehcache.spi.lifecycle.LifeCycleService;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.spi.service.ThreadPoolsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.context.annotations.ContextAttribute;
import org.terracotta.management.capabilities.ActionsCapability;
import org.terracotta.management.capabilities.Capability;
import org.terracotta.management.capabilities.StatisticsCapability;
import org.terracotta.management.capabilities.context.CapabilityContext;
import org.terracotta.management.capabilities.descriptors.Descriptor;
import org.terracotta.management.context.ContextContainer;
import org.terracotta.management.stats.Statistic;
import org.terracotta.statistics.StatisticsManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * @author Ludovic Orban
 */
@ServiceDependencies({AliasService.class, LifeCycleService.class, ThreadPoolsService.class})
public class DefaultManagementRegistry implements ManagementRegistry {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultManagementRegistry.class);

  private final ManagementRegistryConfiguration configuration;
  private final ConcurrentMap<Class<?>, List<ManagementProvider<?>>> managementProviders = new ConcurrentHashMap<Class<?>, List<ManagementProvider<?>>>();
  private final CapabilityContextProvider capabilityContextProvider = new CapabilityContextProvider();

  private DeferredLifeCycleListener<EhcacheManager> cacheManager;
  private ServiceProvider serviceProvider;

  // must be kept as a strong reference because the StatisticsManager class is using weak references
  private EhcacheManagerStatsSetting ehcacheManagerStatsSetting;
  private final ConcurrentMap<String, EhcacheStatsSetting> ehcacheStatsSettings = new ConcurrentHashMap<String, EhcacheStatsSetting>();

  public DefaultManagementRegistry() {
    this(null);
  }

  public DefaultManagementRegistry(ManagementRegistryConfiguration configuration) {
    this.configuration = configuration != null ?
        configuration :
        new DefaultManagementRegistryConfiguration() {
          @Override
          public StatisticsProviderConfiguration getConfigurationFor(Class<? extends ManagementProvider<?>> managementProviderClass) {
            return new EhcacheStatisticsProviderConfiguration(5 * 60, TimeUnit.SECONDS, 100, 1, TimeUnit.SECONDS, 30, TimeUnit.SECONDS);
          }
        };
  }

  @Override
  public void start(final ServiceProvider serviceProvider) {

    // detection to ensure a registry is only used in one cache manager 
    if (this.serviceProvider == null) {
      this.serviceProvider = serviceProvider;
    }
    if (this.serviceProvider != serviceProvider) {
      throw new IllegalStateException("The " + ManagementRegistry.class.getSimpleName() + " cannot be used across several cache managers");
    }

    // 0. prepare a new deferred cache manager
    cacheManager = new DeferredLifeCycleListener<EhcacheManager>();

    // 1. when cache manager will be ready, expose its name
    cacheManager.afterInitialization().done(new Deferred.Consumer<EhcacheManager>() {
      @Override
      public void consume(EhcacheManager ehCacheManager) {
        AliasService aliasService = serviceProvider.getService(AliasService.class);
        String cacheManagerName = aliasService.getCacheManagerAlias();
        ehcacheManagerStatsSetting = new EhcacheManagerStatsSetting(cacheManagerName, Collections.<String, Object>singletonMap("Setting", "CacheManagerName"));
        StatisticsManager.associate(ehcacheManagerStatsSetting).withParent(ehCacheManager);
      }
    });

    // 2. when cache manager will be ready, prepare the management registry. The management registry must be prepared BEFORE the caches are registered into (step 4)
    cacheManager.afterInitialization().done(new Deferred.Consumer<EhcacheManager>() {
      @Override
      public void consume(EhcacheManager ehCacheManager) {
        // get the services required for this management registry to work properly
        ThreadPoolsService threadPoolsService = serviceProvider.getService(ThreadPoolsService.class);

        // initialize management capabilities (stats, action calls, etc)
        addSupportFor(new EhcacheActionProvider());
        addSupportFor(capabilityContextProvider);
        addSupportFor(new EhcacheStatisticsProvider(
            configuration.getConfigurationFor(EhcacheStatisticsProvider.class),
            threadPoolsService.getStatisticsExecutor()));

        // register this cache manager with all its caches in the management registry
        register(EhcacheManager.class, ehCacheManager);
      }
    });

    // END: unregister cache manager from registry
    cacheManager.afterClosing().done(new Deferred.Consumer<EhcacheManager>() {
      @Override
      public void consume(EhcacheManager obj) {
        unregister(EhcacheManager.class, obj);
        StatisticsManager.dissociate(ehcacheManagerStatsSetting).fromParent(obj);
        managementProviders.clear();
        cacheManager = null;
        ehcacheManagerStatsSetting = null;
      }
    });
    
    LifeCycleService lifeCycleService = serviceProvider.getService(LifeCycleService.class);
    lifeCycleService.register(EhcacheManager.class, cacheManager);
    lifeCycleService.register(EhcacheBinding.class, new LifeCycleListener<EhcacheBinding>() {
      @Override
      public void afterInitialization(final EhcacheBinding binding) {
        
        // 3. when a cache is added, expose its name  
        cacheManager.afterInitialization().done(new Deferred.Consumer<EhcacheManager>() {
          @Override
          public void consume(EhcacheManager ehcacheManager) {
            EhcacheStatsSetting ehcacheStatsSetting = new EhcacheStatsSetting(binding.getAlias(), Collections.<String, Object>singletonMap("Setting", "CacheName"));
            ehcacheStatsSettings.put(binding.getAlias(), ehcacheStatsSetting);
            StatisticsManager.associate(binding.getCache()).withParent(ehcacheManager);
            StatisticsManager.associate(ehcacheStatsSetting).withParent(binding.getCache());
          }
        });
        
        // 4. when a cache is added, and AFTER step 2, register the cache
        cacheManager.afterInitialization().done(new Deferred.Consumer<EhcacheManager>() {
          @Override
          public void consume(EhcacheManager ehcacheManager) {
            register(Ehcache.class, binding.getCache());
          }
        });
      }

      @Override
      public void afterClosing(final EhcacheBinding binding) {
        cacheManager.afterInitialization().done(new Deferred.Consumer<EhcacheManager>() {
          @Override
          public void consume(EhcacheManager instance) {
            unregister(Ehcache.class, binding.getCache());
            StatisticsManager.dissociate(binding.getCache()).fromParent(instance);
            ehcacheStatsSettings.remove(binding.getAlias());
          }
        });
      }
    });
  }

  @Override
  public void stop() {
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> void register(Class<T> managedType, T managedObject) {
    List<ManagementProvider<?>> managementProviders = this.managementProviders.get(managedType);
    if (managementProviders == null) {
      LOGGER.warn("No registered management provider that supports {}", managedType);
      return;
    }
    for (ManagementProvider<?> managementProvider : managementProviders) {
      ManagementProvider<T> typedManagementProvider = (ManagementProvider<T>) managementProvider;
      typedManagementProvider.register(managedObject);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> void unregister(Class<T> managedType, T managedObject) {
    List<ManagementProvider<?>> managementProviders = this.managementProviders.get(managedType);
    if (managementProviders == null) {
      LOGGER.warn("No registered management provider that supports {}", managedType);
      return;
    }
    for (ManagementProvider<?> managementProvider : managementProviders) {
      ManagementProvider<T> typedManagementProvider = (ManagementProvider<T>) managementProvider;
      typedManagementProvider.unregister(managedObject);
    }
  }

  @Override
  public Collection<ContextContainer> contexts() {
    return capabilityContextProvider.contexts();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends Statistic<?>> Collection<T> collectStatistics(Map<String, String> context, String capabilityName, String... statisticNames) {
    return this.<T>collectManyStatistics(Collections.singletonList(context), capabilityName, statisticNames).get(0);
  }

  @Override
  public <T extends Statistic<?>> List<Collection<T>> collectManyStatistics(List<Map<String, String>> contextList, String capabilityName, String... statisticNames) {
    List<Collection<T>> list = new ArrayList<Collection<T>>(contextList.size());
    for (List<ManagementProvider<?>> providers : managementProviders.values()) {
      for (ManagementProvider<?> provider : providers) {
        if (provider.getClass().getName().equals(capabilityName)) {
          for (Map<String, String> context : contextList) {
            list.add(provider.<T>collectStatistics(context, statisticNames));
          }
        }
      }
    }
    return list;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T callAction(Map<String, String> context, String capabilityName, String methodName, String[] argClassNames, Object[] args) {
    for (List<ManagementProvider<?>> providers : managementProviders.values()) {
      for (ManagementProvider<?> provider : providers) {
        if (provider.getClass().getName().equals(capabilityName)) {
          return (T) provider.callAction(context, methodName, argClassNames, args);
        }
      }
    }
    throw new IllegalArgumentException("No such capability registered : " + capabilityName);
  }

  @Override
  public Collection<Capability> capabilities() {
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

    return result;
  }

  private void addSupportFor(ManagementProvider<?> managementProvider) {
    List<ManagementProvider<?>> managementProviders = this.managementProviders.get(managementProvider.managedType());
    if (managementProviders == null) {
      this.managementProviders.putIfAbsent(managementProvider.managedType(), new CopyOnWriteArrayList<ManagementProvider<?>>());
      managementProviders = this.managementProviders.get(managementProvider.managedType());
    }
    managementProviders.add(managementProvider);
  }

  private static final class EhcacheStatsSetting {
    @ContextAttribute("CacheName") private final String alias;
    @ContextAttribute("properties") private final Map<String, Object> properties;
    @ContextAttribute("tags") private final Set<String> tags = new HashSet<String>(Arrays.asList("cache", "exposed"));

    EhcacheStatsSetting(String alias, Map<String, Object> properties) {
      this.alias = alias;
      this.properties = properties;
    }
  }

  private static final class EhcacheManagerStatsSetting {
    @ContextAttribute("CacheManagerName") private final String name;
    @ContextAttribute("properties") private final Map<String, Object> properties;
    @ContextAttribute("tags") private final Set<String> tags = new HashSet<String>(Arrays.asList("cacheManager", "exposed"));

    EhcacheManagerStatsSetting(String name, Map<String, Object> properties) {
      this.name = name;
      this.properties = properties;
    }
  }

}
