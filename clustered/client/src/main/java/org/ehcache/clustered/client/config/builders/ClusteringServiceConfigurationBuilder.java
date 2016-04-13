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
package org.ehcache.clustered.client.config.builders;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.client.config.ClusteringServiceConfiguration.PoolDefinition;
import org.ehcache.config.Builder;
import org.ehcache.config.units.MemoryUnit;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

/**
 * A builder of ClusteringService configurations.
 */
public final class ClusteringServiceConfigurationBuilder implements Builder<ClusteringServiceConfiguration> {

  private final URI clusterUri;
  private final String defaultServerResource;
  private final Map<String, PoolDefinition> pools;

  /**
   * Creates a new builder connecting to the given cluster.
   *
   * @param clusterUri cluster URI
   *
   * @return a clustering service configuration builder
   */
  public static ClusteringServiceConfigurationBuilder cluster(URI clusterUri) {
    return new ClusteringServiceConfigurationBuilder(clusterUri);
  }

  private ClusteringServiceConfigurationBuilder(URI clusterUri) {
    this.clusterUri = clusterUri;
    this.defaultServerResource = null;
    this.pools = emptyMap();
  }

  private ClusteringServiceConfigurationBuilder(ClusteringServiceConfigurationBuilder original, String poolName, PoolDefinition poolDefinition) {
    this.clusterUri = original.clusterUri;
    this.defaultServerResource = original.defaultServerResource;
    Map<String, PoolDefinition> pools = new HashMap<String, PoolDefinition>(original.pools);
    if (pools.put(poolName, poolDefinition) != null) {
      throw new IllegalArgumentException("Pool '" + poolName + "' already defined");
    }
    this.pools = unmodifiableMap(pools);
  }

  private ClusteringServiceConfigurationBuilder(ClusteringServiceConfigurationBuilder original, String defaultServerResource) {
    this.clusterUri = original.clusterUri;
    this.defaultServerResource = defaultServerResource;
    this.pools = original.pools;
  }

  /**
   * Sets the default server resource for pools and caches.
   *
   * @param defaultServerResource default server resource
   *
   * @return a clustering service configuration builder
   */
  public ClusteringServiceConfigurationBuilder defaultServerResource(String defaultServerResource) {
    return new ClusteringServiceConfigurationBuilder(this, defaultServerResource);
  }

  /**
   * Adds a resource pool with the given name and size and consuming the given server resource.
   *
   * @param name pool name
   * @param size pool size
   * @param unit pool size unit
   * @param serverResource server resource to consume
   *
   * @return a clustering service configuration builder
   */
  public ClusteringServiceConfigurationBuilder resourcePool(String name, long size, MemoryUnit unit, String serverResource) {
    return resourcePool(name, new PoolDefinition(size, unit, serverResource));
  }

  /**
   * Adds a resource pool with the given name and size and consuming the default server resource.
   *
   * @param name pool name
   * @param size pool size
   * @param unit pool size unit
   *
   * @return a clustering service configuration builder
   */
  public ClusteringServiceConfigurationBuilder resourcePool(String name, long size, MemoryUnit unit) {
    return resourcePool(name, new PoolDefinition(size, unit));
  }

  /**
   * Adds a resource pool with the given name and definition
   *
   * @param name pool name
   * @param definition pool definition
   *
   * @return a clustering service configuration builder
   */
  public ClusteringServiceConfigurationBuilder resourcePool(String name, PoolDefinition definition) {
    return new ClusteringServiceConfigurationBuilder(this, name, definition);
  }

  @Override
  public ClusteringServiceConfiguration build() {
    if (defaultServerResource == null) {
      return new ClusteringServiceConfiguration(clusterUri, pools);
    } else {
      return new ClusteringServiceConfiguration(clusterUri, defaultServerResource, pools);
    }
  }
}
