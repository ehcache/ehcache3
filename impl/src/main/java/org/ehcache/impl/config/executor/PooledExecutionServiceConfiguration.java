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

package org.ehcache.impl.config.executor;

import java.util.HashMap;
import java.util.Map;
import org.ehcache.core.spi.service.ExecutionService;
import org.ehcache.spi.service.ServiceCreationConfiguration;

import static java.util.Collections.unmodifiableMap;

/**
 * {@link ServiceCreationConfiguration} for the pooled {@link ExecutionService} implementation.
 * <P>
 *   Enables configuring named thread pools that can then be used by the different Ehcache services
 *   that require threads to function.
 * </P>
 *
 * @see org.ehcache.impl.config.event.CacheEventDispatcherFactoryConfiguration
 * @see org.ehcache.impl.config.event.DefaultCacheEventDispatcherConfiguration
 * @see org.ehcache.impl.config.loaderwriter.writebehind.WriteBehindProviderConfiguration
 * @see org.ehcache.impl.config.loaderwriter.writebehind.DefaultWriteBehindConfiguration
 * @see org.ehcache.impl.config.store.disk.OffHeapDiskStoreProviderConfiguration
 * @see org.ehcache.impl.config.store.disk.OffHeapDiskStoreConfiguration
 */
public class PooledExecutionServiceConfiguration implements ServiceCreationConfiguration<ExecutionService> {

  private final Map<String, PoolConfiguration> poolConfigurations = new HashMap<String, PoolConfiguration>();

  private String defaultAlias;

  /**
   * Adds a new default pool with the provided minimum and maximum.
   * <P>
   *   The default pool will be used by any service requiring threads but not specifying a pool alias.
   *   It is not mandatory to have a default pool.
   *   But without one <I>ALL</I> services have to specify a pool alias.
   * </P>
   *
   * @param alias the pool alias
   * @param minSize the minimum size
   * @param maxSize the maximum size
   *
   * @throws NullPointerException if alias is null
   * @throws IllegalArgumentException if another default was configured already or if another pool with the same
   *                                  alias was configured already
   */
  public void addDefaultPool(String alias, int minSize, int maxSize) {
    if (alias == null) {
      throw new NullPointerException("Pool alias cannot be null");
    }
    if (defaultAlias == null) {
      addPool(alias, minSize, maxSize);
      defaultAlias = alias;
    } else {
      throw new IllegalArgumentException("'" + defaultAlias + "' is already configured as the default pool");
    }
  }

  /**
   * Adds a new pool with the provided minimum and maximum.
   *
   * @param alias the pool alias
   * @param minSize the minimum size
   * @param maxSize the maximum size
   *
   * @throws NullPointerException if alias is null
   * @throws IllegalArgumentException if another pool with the same alias was configured already
   */
  public void addPool(String alias, int minSize, int maxSize) {
    if (alias == null) {
      throw new NullPointerException("Pool alias cannot be null");
    }
    if (poolConfigurations.containsKey(alias)) {
      throw new IllegalArgumentException("A pool with the alias '" + alias + "' is already configured");
    } else {
      poolConfigurations.put(alias, new PoolConfiguration(minSize, maxSize));
    }
  }

  /**
   * Returns the map from alias to {@link PoolConfiguration} defined by this configuration object.
   *
   * @return a map from alias to pool configuration
   */
  public Map<String, PoolConfiguration> getPoolConfigurations() {
    return unmodifiableMap(poolConfigurations);
  }

  /**
   * Returns the default pool alias, or {@code null} if none configured.
   *
   * @return the default pool alias or {@code null}
   */
  public String getDefaultPoolAlias() {
    return defaultAlias;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Class<ExecutionService> getServiceType() {
    return ExecutionService.class;
  }

  /**
   * Configuration class representing a pool configuration.
   */
  public static final class PoolConfiguration {

    private final int minSize;
    private final int maxSize;

    private PoolConfiguration(int minSize, int maxSize) {
      this.minSize = minSize;
      this.maxSize = maxSize;
    }

    /**
     * Returns the minimum size of the pool.
     *
     * @return the minimum size
     */
    public int minSize() {
      return minSize;
    }

    /**
     * Returns the maximum size of the pool.
     *
     * @return the maximum size
     */
    public int maxSize() {
      return maxSize;
    }
  }
}
