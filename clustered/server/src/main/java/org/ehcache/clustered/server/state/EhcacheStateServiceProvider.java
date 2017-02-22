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

import org.ehcache.clustered.server.EhcacheStateServiceImpl;
import org.ehcache.clustered.server.state.config.EhcacheStateServiceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.PlatformConfiguration;
import org.terracotta.entity.ServiceConfiguration;
import org.terracotta.entity.ServiceProvider;
import org.terracotta.entity.ServiceProviderCleanupException;
import org.terracotta.entity.ServiceProviderConfiguration;
import org.terracotta.offheapresource.OffHeapResources;

import com.tc.classloader.BuiltinService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * {@link ServiceProvider} for {@link EhcacheStateService}
 */
@BuiltinService
public class EhcacheStateServiceProvider implements ServiceProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(EhcacheStateServiceProvider.class);

  private ConcurrentMap<Long, EhcacheStateService> serviceMap = new ConcurrentHashMap<>();
  private OffHeapResources offHeapResourcesProvider;

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
        throw new UnsupportedOperationException("There are no offheap-resource defined, this is not supported. There must be at least one!");
      }
    } else {
      LOGGER.warn("No offheap-resource defined - this will prevent provider from offering any EhcacheStateService.");
    }
    return true;
  }

  @Override
  public <T> T getService(long consumerID, ServiceConfiguration<T> configuration) {
    if (configuration != null && configuration.getServiceType().equals(EhcacheStateService.class)) {
      if (offHeapResourcesProvider == null) {
        LOGGER.warn("EhcacheStateService requested but no offheap-resource was defined - returning null");
        return null;
      }
      EhcacheStateService storeManagerService = new EhcacheStateServiceImpl(offHeapResourcesProvider);
      EhcacheStateService result = serviceMap.putIfAbsent(consumerID, storeManagerService);
      if (result == null) {
        result = storeManagerService;
      }
      return (T) result;
    }
    throw new IllegalArgumentException("Unexpected configuration type.");
  }

  @Override
  public Collection<Class<?>> getProvidedServiceTypes() {
    List<Class<?>> classes = new ArrayList<Class<?>>();
    classes.add(EhcacheStateService.class);
    return classes;
  }

  @Override
  public void prepareForSynchronization() throws ServiceProviderCleanupException {
    serviceMap.clear();
  }
}
