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

package org.ehcache.clustered.config;

import org.ehcache.CacheManager;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.service.ClusteringService;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.CacheManagerConfiguration;
import org.ehcache.spi.service.ServiceCreationConfiguration;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.terracotta.offheapstore.util.MemoryUnit;

import static java.util.Collections.unmodifiableMap;

/**
 * Specifies the configuration for a {@link ClusteringService}.
 *
 * @author Clifford W. Johnson
 */
// TODO: Should this accept/hold a *list* of URIs?
// TODO: Add validation for connection URI(s)
public final class ClusteringServiceConfiguration
    implements ServiceCreationConfiguration<ClusteringService>,
    CacheManagerConfiguration<PersistentCacheManager> {

  private final URI clusterUri;
  private final String defaultPool;
  private final Map<String, PoolDefinition> pools;

  public ClusteringServiceConfiguration(final URI clusterUri, String defaultPool, Map<String, PoolDefinition> pools) {
    if (clusterUri == null) {
      throw new NullPointerException("connectionUrl");
    }
    if (defaultPool == null) {
      throw new NullPointerException("defaultPool");
    }
    this.clusterUri = clusterUri;
    this.defaultPool = defaultPool;
    this.pools = unmodifiableMap(new HashMap<String, PoolDefinition>(pools));
  }

  public ClusteringServiceConfiguration(URI clusterUri, Map<String, PoolDefinition> pools) {
    if (clusterUri == null) {
      throw new NullPointerException("connectionUrl");
    }
    this.clusterUri = clusterUri;
    this.defaultPool = null;
    this.pools = unmodifiableMap(new HashMap<String, PoolDefinition>(pools));
  }

  public URI getConnectionUrl() {
    return clusterUri;
  }

  public String getDefaultPool() {
    return defaultPool;
  }

  public Map<String, PoolDefinition> getPools() {
    return pools;
  }

  @Override
  public Class<ClusteringService> getServiceType() {
    return ClusteringService.class;
  }

  @Override
  public CacheManagerBuilder<PersistentCacheManager> builder(final CacheManagerBuilder<? extends CacheManager> other) {
    return (CacheManagerBuilder<PersistentCacheManager>) other.using(this);
  }

  public static final class PoolDefinition {

    private final long size;
    private final MemoryUnit unit;
    private final String from;

    public PoolDefinition(long size, MemoryUnit unit, String from) {
      if (unit == null) {
        throw new NullPointerException("Unit cannot be null");
      }
      if (from == null) {
        throw new NullPointerException("Source resource cannot be null");
      }
      this.size = size;
      this.unit = unit;
      this.from = from;
    }

    public PoolDefinition(long size, MemoryUnit unit) {
      if (unit == null) {
        throw new NullPointerException("Unit cannot be null");
      }
      this.size = size;
      this.unit = unit;
      this.from = null;
    }

    public long getSize() {
      return size;
    }

    public MemoryUnit getUnit() {
      return unit;
    }

    public String getFrom() {
      return from;
    }
  }
}
