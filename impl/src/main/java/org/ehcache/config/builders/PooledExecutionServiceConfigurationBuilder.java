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

import org.ehcache.impl.config.executor.PooledExecutionServiceConfiguration;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Ludovic Orban
 */
public class PooledExecutionServiceConfigurationBuilder implements Builder<PooledExecutionServiceConfiguration> {

  private Pool defaultPool;
  private final Set<Pool> pools = new HashSet<Pool>();

  private PooledExecutionServiceConfigurationBuilder() {
  }

  private PooledExecutionServiceConfigurationBuilder(PooledExecutionServiceConfigurationBuilder other) {
    this.defaultPool = other.defaultPool;
    this.pools.addAll(other.pools);
  }

  public static PooledExecutionServiceConfigurationBuilder newPooledExecutionServiceConfigurationBuilder() {
    return new PooledExecutionServiceConfigurationBuilder();
  }

  public PooledExecutionServiceConfigurationBuilder defaultPool(String alias, int minSize, int maxSize) {
    PooledExecutionServiceConfigurationBuilder other = new PooledExecutionServiceConfigurationBuilder(this);
    other.defaultPool = new Pool(alias, minSize, maxSize);
    return other;
  }

  public PooledExecutionServiceConfigurationBuilder pool(String alias, int minSize, int maxSize) {
    PooledExecutionServiceConfigurationBuilder other = new PooledExecutionServiceConfigurationBuilder(this);
    other.pools.add(new Pool(alias, minSize, maxSize));
    return other;
  }

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
    private String alias;
    private int minSize;
    private int maxSize;

    public Pool(String alias, int minSize, int maxSize) {
      this.alias = alias;
      this.minSize = minSize;
      this.maxSize = maxSize;
    }
  }

}
