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

import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceType;
import org.ehcache.core.spi.service.ServiceFactory;
import org.ehcache.impl.config.store.shared.SharedStoreProviderConfiguration;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.osgi.service.component.annotations.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import static org.ehcache.config.ResourceType.Core.*;

@Component
public class SharedStoreProviderFactory implements ServiceFactory<SharedStoreProvider> {

  @Override
  public SharedStoreProvider create(ServiceCreationConfiguration<SharedStoreProvider, ?> configuration) {
    return null;
    //return new SharedStoreProvider(null);
  }

  @Override
  public Collection<SharedStoreProvider> multiCreate(ServiceCreationConfiguration<SharedStoreProvider, ?> configuration) {
    Collection<SharedStoreProvider> sharedStores = new ArrayList<>();
    if (configuration instanceof SharedStoreProviderConfiguration) {
      ResourcePools sharedPools = ((SharedStoreProviderConfiguration) configuration).getSharedResourcePools();
      Set<ResourceType<?>> sharedResourceTypes = sharedPools.getResourceTypeSet();
      sharedResourceTypes.forEach(r -> {
        ResourcePool pool = sharedPools.getPoolForResource(r);
        if (r == OFFHEAP ) {
          sharedStores.add(new SharedOffHeapStoreProvider(pool));
        } else if (r == HEAP) {
          sharedStores.add(new SharedOnHeapStoreProvider(pool));
        } else if (r == DISK) {
          sharedStores.add(new SharedOffHeapDiskStoreProvider(pool));
        } else {
          throw new IllegalArgumentException();
        }
      });
    }
    return sharedStores;
  }

  @Override
  public Class<? extends SharedStoreProvider> getServiceType() {
    return SharedStoreProvider.class;
  }
}

