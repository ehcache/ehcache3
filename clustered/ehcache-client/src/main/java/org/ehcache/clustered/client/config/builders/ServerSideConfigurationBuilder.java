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

import java.util.HashMap;
import java.util.Map;

import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.ServerSideConfiguration.Pool;
import org.ehcache.config.Builder;
import org.ehcache.config.units.MemoryUnit;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

/**
 * Constructs the server-side portion of a {@link ClusteringServiceConfiguration}.  An instance of this
 * class is used in conjunction with {@link ClusteringServiceConfigurationBuilder} and is obtained from
 * the {@link ClusteringServiceConfigurationBuilder#autoCreate() autoCreate} and
 * {@link ClusteringServiceConfigurationBuilder#expecting() expecting} methods of that class.
 */
public class ServerSideConfigurationBuilder implements Builder<ClusteringServiceConfiguration> {

  private final ClusteringServiceConfigurationBuilder clientSideBuilder;
  private final String defaultServerResource;
  private final Map<String, Pool> pools;

  ServerSideConfigurationBuilder() {
    this.clientSideBuilder = null;
    this.defaultServerResource = null;
    this.pools = emptyMap();
  }

  ServerSideConfigurationBuilder(ClusteringServiceConfigurationBuilder clientSideBuilder) {
    if (clientSideBuilder == null) {
      throw new NullPointerException("clientSideBuilder can not be null");
    }
    this.clientSideBuilder = clientSideBuilder;
    this.defaultServerResource = null;
    this.pools = emptyMap();
  }

  protected ServerSideConfigurationBuilder(ServerSideConfiguration serverSideConfiguration) {
    this.clientSideBuilder = null;
    this.defaultServerResource = serverSideConfiguration.getDefaultServerResource();
    this.pools = serverSideConfiguration.getResourcePools();
  }

  private ServerSideConfigurationBuilder(ServerSideConfigurationBuilder original, String defaultServerResource) {
    this.clientSideBuilder = original.clientSideBuilder;
    this.pools = original.pools;
    this.defaultServerResource = defaultServerResource;
  }

  private ServerSideConfigurationBuilder(ServerSideConfigurationBuilder original, String poolName, Pool poolDefinition) {
    this.clientSideBuilder = original.clientSideBuilder;
    this.defaultServerResource = original.defaultServerResource;
    Map<String, Pool> pools = new HashMap<>(original.pools);
    if (pools.put(poolName, poolDefinition) != null) {
      throw new IllegalArgumentException("Pool '" + poolName + "' already defined");
    }
    this.pools = unmodifiableMap(pools);
  }

  /**
   * Sets the default server resource for pools and caches.
   *
   * @param defaultServerResource default server resource
   *
   * @return a clustering service configuration builder
   */
  public ServerSideConfigurationBuilder defaultServerResource(String defaultServerResource) {
    return new ServerSideConfigurationBuilder(this, defaultServerResource);
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
  public ServerSideConfigurationBuilder resourcePool(String name, long size, MemoryUnit unit, String serverResource) {
    return resourcePool(name, new Pool(unit.toBytes(size), serverResource));
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
  public ServerSideConfigurationBuilder resourcePool(String name, long size, MemoryUnit unit) {
    return resourcePool(name, new Pool(unit.toBytes(size)));
  }

  /**
   * Adds a resource pool with the given name and definition
   *
   * @param name pool name
   * @param definition pool definition
   *
   * @return a clustering service configuration builder
   */
  public ServerSideConfigurationBuilder resourcePool(String name, Pool definition) {
    return new ServerSideConfigurationBuilder(this, name, definition);
  }

  @Override
  public ClusteringServiceConfiguration build() {
    return clientSideBuilder.build(buildServerSideConfiguration());
  }

  ServerSideConfiguration buildServerSideConfiguration() {
    if (defaultServerResource == null) {
      return new ServerSideConfiguration(pools);
    } else {
      return new ServerSideConfiguration(defaultServerResource, pools);
    }
  }
}
