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

package org.ehcache.xml.provider;

import org.ehcache.impl.config.executor.PooledExecutionServiceConfiguration;
import org.ehcache.xml.model.ConfigType;
import org.ehcache.xml.model.ThreadPoolsType;

import java.math.BigInteger;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class PooledExecutionServiceConfigurationParser
  extends SimpleCoreServiceCreationConfigurationParser<ThreadPoolsType, PooledExecutionServiceConfiguration> {

  public PooledExecutionServiceConfigurationParser() {
    super(PooledExecutionServiceConfiguration.class,
      ConfigType::getThreadPools, ConfigType::setThreadPools,
      config -> {
        PooledExecutionServiceConfiguration poolsConfiguration = new PooledExecutionServiceConfiguration();
        for (ThreadPoolsType.ThreadPool pool : config.getThreadPool()) {
          if (pool.isDefault()) {
            poolsConfiguration.addDefaultPool(pool.getAlias(), pool.getMinSize().intValue(), pool.getMaxSize().intValue());
          } else {
            poolsConfiguration.addPool(pool.getAlias(), pool.getMinSize().intValue(), pool.getMaxSize().intValue());
          }
        }
        return poolsConfiguration;
      },
      config -> {
        List<ThreadPoolsType.ThreadPool> threadPools = config.getPoolConfigurations().entrySet().stream().map(entry -> {
          PooledExecutionServiceConfiguration.PoolConfiguration poolConfig = entry.getValue();
          String alias = entry.getKey();
          ThreadPoolsType.ThreadPool threadPool = new ThreadPoolsType.ThreadPool()
            .withAlias(alias)
            .withMinSize(BigInteger.valueOf(poolConfig.minSize()))
            .withMaxSize(BigInteger.valueOf(poolConfig.maxSize()));
          if (alias.equals(config.getDefaultPoolAlias())) {
            threadPool.setDefault(true);
          }
          return threadPool;
        }).collect(toList());
        return new ThreadPoolsType().withThreadPool(threadPools);
      }
    );
  }
}
