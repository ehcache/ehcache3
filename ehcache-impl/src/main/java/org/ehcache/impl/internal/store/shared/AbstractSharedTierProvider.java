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

package org.ehcache.impl.internal.store.shared;

import org.ehcache.config.ResourceType;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.impl.internal.store.shared.store.SharedStoreProvider;
import org.ehcache.spi.service.OptionalServiceDependencies;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.spi.service.ServiceProvider;

import java.util.Arrays;
import java.util.Set;

@ServiceDependencies({SharedStorageProvider.class})
@OptionalServiceDependencies("org.ehcache.core.spi.service.StatisticsService")
public abstract class AbstractSharedTierProvider implements Service {

  protected SharedStorageProvider sharedStorageProvider;
  protected StatisticsService statisticsService;

  @Override
  public void start(ServiceProvider<Service> serviceProvider) {
    sharedStorageProvider = serviceProvider.getService(SharedStorageProvider.class);
    statisticsService = serviceProvider.getService(StatisticsService.class);
  }

  @Override
  public void stop() {
    sharedStorageProvider = null;
    statisticsService = null;
  }

  protected void associateStoreStatsWithPartition(Object toAssociate, Object parent) {
    if (statisticsService != null) {
      statisticsService.registerWithParent(toAssociate, parent);
    }
  }

  protected <T> int rank(Class<T> type, Set<ResourceType<?>> resourceTypes) {
    if (resourceTypes.size() == 1) {
      ResourceType<?> resourceType = resourceTypes.iterator().next();
      if (resourceType instanceof ResourceType.SharedResource && sharedStorageProvider.supports(type, ((ResourceType.SharedResource<?>) resourceType).getResourceType())) {
        return 1;
      } else {
        return 0;
      }
    } else {
      return 0;
    }
  }

  protected static ResourceType.SharedResource<?> assertResourceIsShareable(Set<ResourceType<?>> resourceTypes) {
    if (resourceTypes.size() == 1) {
      ResourceType<?> resourceType = resourceTypes.iterator().next();
      if (resourceType instanceof ResourceType.SharedResource) {
        return (ResourceType.SharedResource<?>) resourceType;
      } else {
        throw new AssertionError();
      }
    } else {
      throw new AssertionError();
    }
  }

  protected String extractAlias(ServiceConfiguration<?,?>[] serviceConfigs) {
    return Arrays.stream(serviceConfigs)
      .filter(SharedStoreProvider.SharedPersistentSpaceIdentifier.class::isInstance)
      .map(SharedStoreProvider.SharedPersistentSpaceIdentifier.class::cast)
      .findFirst().map(SharedStoreProvider.SharedPersistentSpaceIdentifier::getName).orElse(null);
  }
}
