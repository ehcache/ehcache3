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
import org.terracotta.entity.PlatformConfiguration;
import org.terracotta.entity.ServiceConfiguration;
import org.terracotta.entity.ServiceProvider;
import org.terracotta.entity.ServiceProviderCleanupException;
import org.terracotta.entity.ServiceProviderConfiguration;

import com.tc.classloader.BuiltinService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * {@link ServiceProvider} for {@link EhcacheStateService}
 */
@BuiltinService
public class EhcacheStateServiceProvider implements ServiceProvider {

  private ConcurrentMap<Long, EhcacheStateService> serviceMap = new ConcurrentHashMap<>();

  @Override
  public boolean initialize(ServiceProviderConfiguration configuration, PlatformConfiguration platformConfiguration) {
    return true;
  }

  @Override
  public <T> T getService(long consumerID, ServiceConfiguration<T> configuration) {
    if (configuration != null && configuration.getServiceType().equals(EhcacheStateService.class)) {
      EhcacheStateServiceConfig stateServiceConfig = (EhcacheStateServiceConfig) configuration;
      EhcacheStateService storeManagerService = new EhcacheStateServiceImpl(
        stateServiceConfig.getServiceRegistry(), stateServiceConfig.getOffHeapResourceIdentifiers(), stateServiceConfig.getMapper());
      EhcacheStateService result = serviceMap.putIfAbsent(consumerID, storeManagerService);
      if (result == null) {
        result = storeManagerService;
      }
      @SuppressWarnings("unchecked")
      T typedResult = (T) result;
      return typedResult;
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
  public void clear() throws ServiceProviderCleanupException {
    serviceMap.clear();
  }
}
