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

package org.ehcache.config.builders;

import org.ehcache.config.Builder;
import org.ehcache.impl.config.executor.PooledExecutionServiceConfiguration;

import java.util.HashSet;
import java.util.Set;

/**
 * The {@code PooledExecutionServiceConfigurationBuilder} enables building configurations for an
 * {@link org.ehcache.core.spi.service.ExecutionService} that is pool based using a fluent style.
 * <p>
 * As with all Ehcache builders, all instances are immutable and calling any method on the builder will return a new
 * instance without modifying the one on which the method was called.
 * This enables the sharing of builder instances without any risk of seeing them modified by code elsewhere.
 */
public class PooledExecutionServiceConfigurationBuilder implements Builder<PooledExecutionServiceConfiguration> {

  private Pool defaultPool;
  private final Set<Pool> pools = new HashSet<>();

  private PooledExecutionServiceConfigurationBuilder() {
  }

  private PooledExecutionServiceConfigurationBuilder(PooledExecutionServiceConfigurationBuilder other) {
    this.defaultPool = other.defaultPool;
    this.pools.addAll(other.pools);
  }

  private PooledExecutionServiceConfigurationBuilder(PooledExecutionServiceConfiguration seed) {
    seed.getPoolConfigurations().forEach((alias, config) -> {
      Pool pool = new Pool(alias, config.minSize(), config.maxSize());
      if (alias.equals(seed.getDefaultPoolAlias())) {
        defaultPool = pool;
      }
      pools.add(pool);
    });
  }

  /**
   * Creates a new instance of {@code PooledExecutionServiceConfigurationBuilder}
   *
   * @return the builder
   */
  public static PooledExecutionServiceConfigurationBuilder newPooledExecutionServiceConfigurationBuilder() {
    return new PooledExecutionServiceConfigurationBuilder();
  }

  /**
   * Creates a seeded instance of {@code PooledExecutionServiceConfigurationBuilder}
   *
   * @return the builder
   */
  public static PooledExecutionServiceConfigurationBuilder newPooledExecutionServiceConfigurationBuilder(PooledExecutionServiceConfiguration seed) {
    return new PooledExecutionServiceConfigurationBuilder(seed);
  }

  /**
   * Adds a default pool configuration to the returned builder.
   *
   * @param alias the pool alias
   * @param minSize the minimum number of threads in the pool
   * @param maxSize the maximum number of threads in the pool
   * @return a new builder with the added default pool
   */
  public PooledExecutionServiceConfigurationBuilder defaultPool(String alias, int minSize, int maxSize) {
    PooledExecutionServiceConfigurationBuilder other = new PooledExecutionServiceConfigurationBuilder(this);
    other.defaultPool = new Pool(alias, minSize, maxSize);
    return other;
  }

  /**
   * Adds a pool configuration to the returned builder.
   *
   * @param alias the pool alias
   * @param minSize the minimum number of threads in the pool
   * @param maxSize the maximum number of threads in the pool
   * @return a new builder with the added default pool
   */
  public PooledExecutionServiceConfigurationBuilder pool(String alias, int minSize, int maxSize) {
    PooledExecutionServiceConfigurationBuilder other = new PooledExecutionServiceConfigurationBuilder(this);
    other.pools.add(new Pool(alias, minSize, maxSize));
    return other;
  }

  /**
   * Builds the {@link PooledExecutionServiceConfiguration}
   *
   * @return the built configuration
   */
  @Override
  public PooledExecutionServiceConfiguration build() {
    PooledExecutionServiceConfiguration config = new PooledExecutionServiceConfiguration();
    if (defaultPool != null) {
      config.addDefaultPool(defaultPool.alias, defaultPool.minSize, defaultPool.maxSize);
    }
    for (Pool pool : pools) {
      config.addPool(pool.alias, pool.minSize, pool.maxSize);
    }
    return config;
  }


  private static class Pool {
    private final String alias;
    private final int minSize;
    private final int maxSize;

    Pool(String alias, int minSize, int maxSize) {
      this.alias = alias;
      this.minSize = minSize;
      this.maxSize = maxSize;
    }
  }

}
