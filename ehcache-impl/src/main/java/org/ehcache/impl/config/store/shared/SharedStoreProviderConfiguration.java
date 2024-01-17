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

package org.ehcache.impl.config.store.shared;

import org.ehcache.config.ResourcePools;
import org.ehcache.config.SharedResourcePools;
import org.ehcache.impl.config.ResourcePoolsImpl;
import org.ehcache.impl.internal.store.shared.SharedStoreProvider;
import org.ehcache.spi.service.ServiceCreationConfiguration;


public class SharedStoreProviderConfiguration implements ServiceCreationConfiguration<SharedStoreProvider, String> {

  private final ResourcePools sharedResourcePools;


  public SharedStoreProviderConfiguration(SharedResourcePools sharedResourcePools) {
    this.sharedResourcePools = new ResourcePoolsImpl(sharedResourcePools.getSharedResourcePools());
  }


  public ResourcePools getSharedResourcePools() {
    return sharedResourcePools;
  }

  @Override
  public Class<SharedStoreProvider> getServiceType() {
    return SharedStoreProvider.class;
  }
}
