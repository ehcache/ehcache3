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
package org.ehcache.clustered.config.builder;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.ehcache.clustered.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.config.ClusteringServiceConfiguration.PoolDefinition;
import org.ehcache.config.builders.Builder;
import org.terracotta.offheapstore.util.MemoryUnit;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

/**
 *
 * @author cdennis
 */
public class ClusteringServiceConfigurationBuilder implements Builder<ClusteringServiceConfiguration> {

  public static ClusteringServiceConfigurationBuilder cluster(URI clusterUri) {
    return new ClusteringServiceConfigurationBuilder(clusterUri);
  }

  private final URI clusterUri;
  private final Map<String, PoolDefinition> pools;

  private ClusteringServiceConfigurationBuilder(URI clusterUri) {
    this.clusterUri = clusterUri;
    this.pools = emptyMap();
  }

  private ClusteringServiceConfigurationBuilder(ClusteringServiceConfigurationBuilder original, String poolName, PoolDefinition poolDefinition) {
    this.clusterUri = original.clusterUri;
    Map<String, PoolDefinition> pools = new HashMap<String, PoolDefinition>(original.pools);
    if (pools.put(poolName, poolDefinition) != null) {
      throw new IllegalArgumentException("Pool '" + poolName + "' already defined");
    }
    this.pools = unmodifiableMap(pools);
  }

  public Defaulted defaultResource(String resource) {
    return new Defaulted(this, resource);
  }

  public ClusteringServiceConfigurationBuilder resourcePool(String name, long size, MemoryUnit unit, String from) {
    return new ClusteringServiceConfigurationBuilder(this, name, new PoolDefinition(size, unit, from));
  }

  @Override
  public ClusteringServiceConfiguration build() {
    return new ClusteringServiceConfiguration(clusterUri, pools);
  }

  public class Defaulted implements Builder<ClusteringServiceConfiguration> {

    private final URI clusterUri;
    private final String defaultResource;
    private final Map<String, PoolDefinition> pools;

    private Defaulted(ClusteringServiceConfigurationBuilder original, String defaultResource) {
      this.clusterUri = original.clusterUri;
      this.pools = unmodifiableMap(original.pools);
      this.defaultResource = defaultResource;
    }

    private Defaulted(Defaulted original, String poolName, PoolDefinition poolDefinition) {
      this.clusterUri = original.clusterUri;
      this.defaultResource = original.defaultResource;
      Map<String, PoolDefinition> pools = new HashMap<String, PoolDefinition>(original.pools);
      if (pools.put(poolName, poolDefinition) != null) {
        throw new IllegalArgumentException("Pool '" + poolName + "' already defined");
      }
      this.pools = unmodifiableMap(pools);
    }

    public Defaulted resourcePool(String name, long size, MemoryUnit unit) {
      return new Defaulted(this, name, new PoolDefinition(size, unit));
    }

    public Defaulted resourcePool(String name, long size, MemoryUnit unit, String from) {
      return new Defaulted(this, name, new PoolDefinition(size, unit, from));
    }

    @Override
    public ClusteringServiceConfiguration build() {
      return new ClusteringServiceConfiguration(clusterUri, defaultResource, pools);
    }
  }
}
