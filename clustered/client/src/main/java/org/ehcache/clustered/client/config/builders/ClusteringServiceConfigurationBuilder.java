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

import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import java.util.concurrent.TimeUnit;
import org.ehcache.clustered.client.config.TimeoutDuration;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.config.Builder;

/**
 * A builder of ClusteringService configurations.
 */
public final class ClusteringServiceConfigurationBuilder implements Builder<ClusteringServiceConfiguration> {

  private final URI clusterUri;
  private final TimeoutDuration readOperationTimeout;

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
    this.readOperationTimeout = null;
  }

  private ClusteringServiceConfigurationBuilder(ClusteringServiceConfigurationBuilder original, TimeoutDuration readOperationTimeout) {
    this.clusterUri = original.clusterUri;
    this.readOperationTimeout = readOperationTimeout;
  }

  /**
   * Support connection to an existing entity or create if the entity if absent.
   *
   * @return a clustering service configuration builder
   */
  public ServerSideConfigurationBuilder autoCreate() {
    return new ServerSideConfigurationBuilder(getClientSideConfiguration(true));
  }

  /**
   * Only support connection to an existing entity.
   *
   * @return a clustering service configuration builder
   */
  public ServerSideConfigurationBuilder expecting() {
    return new ServerSideConfigurationBuilder(getClientSideConfiguration(false));
  }

  private ClusteringServiceClientSideConfiguration getClientSideConfiguration(boolean autoCreate) {
    return new ClusteringServiceClientSideConfigurationImpl(clusterUri, autoCreate, readOperationTimeout);
  }

  /**
   * Adds a read operation timeout.  Read operations which time out return a result comparable to
   * a cache miss.
   *
   * @param duration the amount of time permitted for read operations
   * @param unit the time units for {@code duration}
   *
   * @return a clustering service configuration builder
   *
   * @throws NullPointerException if {@code unit} is {@code null}
   * @throws IllegalArgumentException if {@code amount} is negative
   */
  public ClusteringServiceConfigurationBuilder readOperationTimeout(long duration, TimeUnit unit) {
    return new ClusteringServiceConfigurationBuilder(this, TimeoutDuration.of(duration, unit));
  }

  @Override
  public ClusteringServiceConfiguration build() {
    return new ClusteringServiceConfiguration(clusterUri, readOperationTimeout);
  }

  /**
   * Internal method to build a new {@link ClusteringServiceConfiguration} from the {@link ServerSideConfigurationBuilder}.
   *
   * @param clientSideConfiguration the {@code ClusteringServiceClientSideConfiguration} passed to the
   *                                {@link ServerSideConfigurationBuilder#ServerSideConfigurationBuilder(ClusteringServiceClientSideConfiguration)
   *                                ServerSideConfigurationBuilder(ClusteringServiceClientSideConfiguration)}
   *                                constructor
   * @param serverSideConfiguration the {@code ServerSideConfiguration} to use
   *
   * @return a new {@code ClusteringServiceConfiguration} instance built from {@code clientSideConfiguration} and
   *        {@code serverSideConfiguration}
   */
  static ClusteringServiceConfiguration build(ClusteringServiceClientSideConfiguration clientSideConfiguration,
                                              ServerSideConfiguration serverSideConfiguration) {
    return new ClusteringServiceConfiguration(
        clientSideConfiguration.getClusterUri(),
        clientSideConfiguration.getReadOperationTimeout(),
        clientSideConfiguration.isAutoCreate(),
        serverSideConfiguration);
  }

  /**
   * The client-side portion of the {@link ClusteringServiceConfiguration} used during
   * configuration building.
   */
  private static class ClusteringServiceClientSideConfigurationImpl implements ClusteringServiceClientSideConfiguration {
    private final URI clusterUri;
    private final boolean autoCreate;
    private final TimeoutDuration readOperationTimeout;

    ClusteringServiceClientSideConfigurationImpl(URI clusterUri, boolean autoCreate, TimeoutDuration readOperationTimeout) {
      this.clusterUri = clusterUri;
      this.autoCreate = autoCreate;
      this.readOperationTimeout = readOperationTimeout;
    }

    @Override
    public URI getClusterUri() {
      return clusterUri;
    }

    @Override
    public boolean isAutoCreate() {
      return autoCreate;
    }

    @Override
    public TimeoutDuration getReadOperationTimeout() {
      return readOperationTimeout;
    }
  }

  /**
   * The client-side portion of the {@link ClusteringServiceConfiguration} used during
   * configuration building.
   */
  interface ClusteringServiceClientSideConfiguration {
    /**
     * Gets the URI of the clustering server.
     *
     * @return the configured URI
     */
    URI getClusterUri();

    /**
     * Indicates if auto-create is enabled for the clustering server and the caches it serves.
     *
     * @return {@code true} if auto-create is enabled; {@code false} otherwise
     */
    boolean isAutoCreate();

    /**
     * Gets the timeout to use for cache read operations for a clustered store.
     *
     * @return the timeout to use for clustered read operations
     */
    TimeoutDuration getReadOperationTimeout();
  }
}
