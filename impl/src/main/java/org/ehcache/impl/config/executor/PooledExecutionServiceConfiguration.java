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
 *
 * @author cdennis
 */
public class PooledExecutionServiceConfiguration implements ServiceCreationConfiguration<ExecutionService> {

  private final Map<String, PoolConfiguration> poolConfigurations = new HashMap<String, PoolConfiguration>();
  
  private String defaultAlias;
  
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

  public Map<String, PoolConfiguration> getPoolConfigurations() {
    return unmodifiableMap(poolConfigurations);
  }

  public String getDefaultPoolAlias() {
    return defaultAlias;
  }
  
  @Override
  public Class<ExecutionService> getServiceType() {
    return ExecutionService.class;
  }

  public static final class PoolConfiguration {

    private final int minSize;
    private final int maxSize;

    private PoolConfiguration(int minSize, int maxSize) {
      this.minSize = minSize;
      this.maxSize = maxSize;
    }

    public int minSize() {
      return minSize;
    }

    public int maxSize() {
      return maxSize;
    }
  }
}
