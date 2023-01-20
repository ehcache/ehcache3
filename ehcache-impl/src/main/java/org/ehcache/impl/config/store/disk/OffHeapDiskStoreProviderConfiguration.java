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

package org.ehcache.impl.config.store.disk;

import org.ehcache.impl.internal.store.disk.OffHeapDiskStore;
import org.ehcache.spi.service.ServiceCreationConfiguration;

/**
 * {@link ServiceCreationConfiguration} for the default {@link org.ehcache.core.spi.store.Store off heap disk store}.
 */
public class OffHeapDiskStoreProviderConfiguration implements ServiceCreationConfiguration<OffHeapDiskStore.Provider, String> {

  private final String threadPoolAlias;

  /**
   * Creates a new configuration instance using the provided parameter.
   *
   * @param threadPoolAlias the thread pool alias
   *
   * @see org.ehcache.impl.config.executor.PooledExecutionServiceConfiguration
   */
  public OffHeapDiskStoreProviderConfiguration(String threadPoolAlias) {
    this.threadPoolAlias = threadPoolAlias;
  }

  /**
   * Returns the configured thread pool alias.
   *
   * @return the thread pool alias
   *
   * @see org.ehcache.impl.config.executor.PooledExecutionServiceConfiguration
   */
  public String getThreadPoolAlias() {
    return threadPoolAlias;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Class<OffHeapDiskStore.Provider> getServiceType() {
    return OffHeapDiskStore.Provider.class;
  }

  @Override
  public String derive() {
    return getThreadPoolAlias();
  }

  @Override
  public OffHeapDiskStoreProviderConfiguration build(String alias) {
    return new OffHeapDiskStoreProviderConfiguration(alias);
  }
}
