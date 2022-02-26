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

package org.ehcache.impl.config.event;

import org.ehcache.core.events.CacheEventDispatcherFactory;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * {@link ServiceConfiguration} for the default {@link CacheEventDispatcherFactory} implementation.
 * <p>
 * Enables configuring the thread pool to be used by a {@link org.ehcache.core.events.CacheEventDispatcher} for
 * a given cache.
 */
public class DefaultCacheEventDispatcherConfiguration implements ServiceConfiguration<CacheEventDispatcherFactory, String> {

  private final String threadPoolAlias;

  /**
   * Creates a new configuration with the provided pool alias
   *
   * @param threadPoolAlias the pool alias
   */
  public DefaultCacheEventDispatcherConfiguration(String threadPoolAlias) {
    this.threadPoolAlias = threadPoolAlias;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Class<CacheEventDispatcherFactory> getServiceType() {
    return CacheEventDispatcherFactory.class;
  }

  /**
   * Returns the thread pool alias.
   *
   * @return the pool alias
   */
  public String getThreadPoolAlias() {
    return threadPoolAlias;
  }

  @Override
  public String derive() {
    return getThreadPoolAlias();
  }

  @Override
  public DefaultCacheEventDispatcherConfiguration build(String alias) {
    return new DefaultCacheEventDispatcherConfiguration(alias);
  }
}
