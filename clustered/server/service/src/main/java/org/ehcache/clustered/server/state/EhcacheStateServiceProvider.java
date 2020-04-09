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

package org.ehcache.clustered.server.state;

import com.tc.classloader.BuiltinService;
import org.ehcache.clustered.server.EhcacheStateServiceImpl;
import org.ehcache.clustered.server.state.config.EhcacheStateServiceConfig;
import org.ehcache.clustered.server.state.config.EhcacheStoreStateServiceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.PlatformConfiguration;
import org.terracotta.entity.ServiceConfiguration;
import org.terracotta.entity.ServiceProvider;
import org.terracotta.entity.ServiceProviderConfiguration;
import org.terracotta.entity.StateDumpCollector;
import org.terracotta.offheapresource.OffHeapResources;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * {@link ServiceProvider} for {@link EhcacheStateService}
 */
@BuiltinService
public class EhcacheStateServiceProvider implements ServiceProvider, Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(EhcacheStateServiceProvider.class);

  private final ConcurrentMap<String, EhcacheStateService> serviceMap = new ConcurrentHashMap<>();
  private OffHeapResources offHeapResourcesProvider;

  @Override
  public void addStateTo(StateDumpCollector dump) {
    for (Map.Entry<String, EhcacheStateService> entry : new HashMap<>(serviceMap).entrySet()) {
      StateDumpCollector clusterTierManagerStateDump = dump.subStateDumpCollector(entry.getKey());
      EhcacheStateService clusterTierManagerState = entry.getValue();
      EhcacheStateServiceDump.dump(clusterTierManagerState, clusterTierManagerStateDump);
    }
  }

  @Override
  public boolean initialize(ServiceProviderConfiguration configuration, PlatformConfiguration platformConfiguration) {
    Collection<OffHeapResources> extendedConfiguration = platformConfiguration.getExtendedConfiguration(OffHeapResources.class);
    if (extendedConfiguration.size() > 1) {
      throw new UnsupportedOperationException("There are " + extendedConfiguration.size() + " OffHeapResourcesProvider, this is not supported. " +
        "There must be only one!");
    }
    Iterator<OffHeapResources> iterator = extendedConfiguration.iterator();
    if (iterator.hasNext()) {
      offHeapResourcesProvider = iterator.next();
      if (offHeapResourcesProvider.getAllIdentifiers().isEmpty()) {
        LOGGER.warn("No offheap-resource defined - this will prevent provider from offering any EhcacheStateService.");
      }
    } else {
      throw new UnsupportedOperationException("There are no offheap-resource defined, this is not supported");
    }
    return true;
  }

  @Override
  public <T> T getService(long consumerID, ServiceConfiguration<T> configuration) {
    if (configuration != null && configuration.getServiceType().equals(EhcacheStateService.class)) {

      EhcacheStateService result;
      if (configuration instanceof EhcacheStateServiceConfig) {
        EhcacheStateServiceConfig stateServiceConfig = (EhcacheStateServiceConfig) configuration;
        EhcacheStateServiceImpl storeManagerService = new EhcacheStateServiceImpl(
          offHeapResourcesProvider, stateServiceConfig.getConfig().getConfiguration(), stateServiceConfig.getMapper(),
          service -> serviceMap.remove(stateServiceConfig.getConfig().getIdentifier(), service));
        result = serviceMap.putIfAbsent(stateServiceConfig.getConfig().getIdentifier(), storeManagerService);
        if (result == null) {
          result = storeManagerService;
        }
      } else if (configuration instanceof EhcacheStoreStateServiceConfig) {
        EhcacheStoreStateServiceConfig storeStateServiceConfig = (EhcacheStoreStateServiceConfig) configuration;
        result = serviceMap.get(storeStateServiceConfig.getManagerIdentifier());
      } else {
        throw new IllegalArgumentException("Unexpected configuration type: " + configuration);
      }

      return configuration.getServiceType().cast(result);
    }
    throw new IllegalArgumentException("Unexpected configuration type.");
  }

  @Override
  public Collection<Class<?>> getProvidedServiceTypes() {
    List<Class<?>> classes = new ArrayList<>();
    classes.add(EhcacheStateService.class);
    return classes;
  }

  @Override
  public void prepareForSynchronization() {
    serviceMap.clear();
  }

  @Override
  public void close() {
    //passthrough test cleanup
    serviceMap.values().forEach(EhcacheStateService::destroy);
  }

  public interface DestroyCallback {
    void destroy(EhcacheStateService service);
  }
}
