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

package org.ehcache.clustered.client.config;

import org.ehcache.CacheManager;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.CacheManagerConfiguration;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.spi.service.ServiceCreationConfiguration;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static java.util.Collections.unmodifiableMap;

/**
 * Specifies the configuration for a {@link ClusteringService}.
 */
// TODO: Should this accept/hold a *list* of URIs?
public final class ClusteringServiceConfiguration
    implements ServiceCreationConfiguration<ClusteringService>,
    CacheManagerConfiguration<PersistentCacheManager> {

  private final URI clusterUri;
  private final String defaultServerResource;
  private final Map<String, PoolDefinition> pools;

  public ClusteringServiceConfiguration(final URI clusterUri, String defaultServerResource, Map<String, PoolDefinition> pools) {
    if (clusterUri == null) {
      throw new NullPointerException("Cluster URI cannot be null");
    }
    if (defaultServerResource == null) {
      throw new NullPointerException("Default server resource cannot be null");
    }
    this.clusterUri = clusterUri;
    this.defaultServerResource = defaultServerResource;
    this.pools = unmodifiableMap(new HashMap<String, PoolDefinition>(pools));
  }

  public ClusteringServiceConfiguration(URI clusterUri, Map<String, PoolDefinition> pools) {
    if (clusterUri == null) {
      throw new NullPointerException("Cluster URI cannot be null");
    }
    StringBuilder issues = new StringBuilder();
    for (Entry<String, PoolDefinition> e : pools.entrySet()) {
      if (e.getValue().getServerResource() == null) {
        issues.append("Pool '").append(e.getKey()).append("' has no defined server resource, and no default value was supplied").append("\n");
      }
    }
    if (issues.length() > 0) {
      throw new IllegalArgumentException(issues.toString());
    }
    this.clusterUri = clusterUri;
    this.defaultServerResource = null;
    this.pools = unmodifiableMap(new HashMap<String, PoolDefinition>(pools));
  }

  /**
   * The {@code URI} of the cluster that will be connected to.
   *
   * @return the cluster {@code URI}
   */
  public URI getClusterUri() {
    return clusterUri;
  }

  /**
   * The default server resource to use for caches and pools, or {@code null} if one is not defined.
   *
   * @return the default server resource
   */
  public String getDefaultServerResource() {
    return defaultServerResource;
  }

  /**
   * The map of pool definitions that can be used for clustered caches.
   *
   * @return the set of pools
   */
  public Map<String, PoolDefinition> getPools() {
    return pools;
  }

  @Override
  public Class<ClusteringService> getServiceType() {
    return ClusteringService.class;
  }

  @SuppressWarnings("unchecked")
  @Override
  public CacheManagerBuilder<PersistentCacheManager> builder(final CacheManagerBuilder<? extends CacheManager> other) {
    return (CacheManagerBuilder<PersistentCacheManager>) other.using(this);   // unchecked
  }

  /**
   * The definition of a pool that can be shared by multiple caches.
   */
  public static final class PoolDefinition {

    private final long size;
    private final MemoryUnit unit;
    private final String serverResource;

    /**
     * Creates a new pool definition with the given size, consuming the given server resource.
     *
     * @param size pool size
     * @param unit pool size unit
     * @param serverResource the server resource to consume
     */
    public PoolDefinition(long size, MemoryUnit unit, String serverResource) {
      if (unit == null) {
        throw new NullPointerException("Unit cannot be null");
      }
      if (size <= 0) {
        throw new IllegalArgumentException("Pool must have a positive size");
      }
      if (serverResource == null) {
        throw new NullPointerException("Source resource cannot be null");
      }
      this.size = size;
      this.unit = unit;
      this.serverResource = serverResource;
    }

    /**
     * Creates a new pool definition with the given size, consuming the default server resource.
     *
     * @param size pool size
     * @param unit pool size unit
     */
    public PoolDefinition(long size, MemoryUnit unit) {
      if (unit == null) {
        throw new NullPointerException("Unit cannot be null");
      }
      if (size <= 0) {
        throw new IllegalArgumentException("Pool must have a positive size");
      }
      this.size = size;
      this.unit = unit;
      this.serverResource = null;
    }

    /**
     * Returns the size of the pool.
     *
     * @return pool size
     */
    public long getSize() {
      return size;
    }

    /**
     * Returns the memory unit used for size.
     *
     * @return pool size unit
     */
    public MemoryUnit getUnit() {
      return unit;
    }

    /**
     * Returns the server resource consumed by this pool, or {@code null} if the default pool will be used.
     *
     * @return the server resource to consume
     */
    public String getServerResource() {
      return serverResource;
    }
  }
}
