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

package org.ehcache.impl.internal.store.disk;

import org.ehcache.core.spi.service.ServiceFactory;
import org.ehcache.impl.config.store.disk.OffHeapDiskStoreProviderConfiguration;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.osgi.service.component.annotations.Component;

/**
 * @author Chris Dennis
 */
@Component
public class OffHeapDiskStoreProviderFactory implements ServiceFactory<OffHeapDiskStore.Provider> {

  @Override
  public OffHeapDiskStore.Provider create(ServiceCreationConfiguration<OffHeapDiskStore.Provider, ?> configuration) {
    if (configuration == null) {
      return new OffHeapDiskStore.Provider();
    } else if (configuration instanceof OffHeapDiskStoreProviderConfiguration) {
      return new OffHeapDiskStore.Provider(((OffHeapDiskStoreProviderConfiguration) configuration).getThreadPoolAlias());
    } else {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public Class<OffHeapDiskStore.Provider> getServiceType() {
    return OffHeapDiskStore.Provider.class;
  }
}
