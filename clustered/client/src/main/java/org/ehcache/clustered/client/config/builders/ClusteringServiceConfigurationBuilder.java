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
  private final Boolean autoCreate;

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
    this.autoCreate = null;
  }

  private ClusteringServiceConfigurationBuilder(ClusteringServiceConfigurationBuilder original, TimeoutDuration readOperationTimeout) {
    this.clusterUri = original.clusterUri;
    this.readOperationTimeout = readOperationTimeout;
    this.autoCreate = original.autoCreate;
  }

  private ClusteringServiceConfigurationBuilder(ClusteringServiceConfigurationBuilder original, boolean autoCreate) {
    this.clusterUri = original.clusterUri;
    this.readOperationTimeout = original.readOperationTimeout;
    this.autoCreate = autoCreate;
  }

  /**
   * Support connection to an existing entity or create if the entity if absent.
   *
   * @return a clustering service configuration builder
   */
  public ServerSideConfigurationBuilder autoCreate() {
    return new ServerSideConfigurationBuilder(new ClusteringServiceConfigurationBuilder(this, true));
  }

  /**
   * Only support connection to an existing entity.
   *
   * @return a clustering service configuration builder
   */
  public ServerSideConfigurationBuilder expecting() {
    return new ServerSideConfigurationBuilder(new ClusteringServiceConfigurationBuilder(this, false));
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
    if (readOperationTimeout == null) {
      return new ClusteringServiceConfiguration(clusterUri);
    } else {
      return new ClusteringServiceConfiguration(clusterUri, readOperationTimeout);
    }
  }

  /**
   * Internal method to build a new {@link ClusteringServiceConfiguration} from the {@link ServerSideConfigurationBuilder}.
   *
   * @param serverSideConfiguration the {@code ServerSideConfiguration} to use
   *
   * @return a new {@code ClusteringServiceConfiguration} instance built from {@code this}
   *        {@code ClusteringServiceConfigurationBuilder} and the {@code serverSideConfiguration} provided
   */
  ClusteringServiceConfiguration build(ServerSideConfiguration serverSideConfiguration) {
    ClusteringServiceConfiguration configuration;
    if (autoCreate != null) {
      if (readOperationTimeout != null) {
        configuration = new ClusteringServiceConfiguration(clusterUri, readOperationTimeout, autoCreate, serverSideConfiguration);
      } else {
        configuration = new ClusteringServiceConfiguration(clusterUri, autoCreate, serverSideConfiguration);
      }
    } else {
      if (readOperationTimeout != null) {
        configuration = new ClusteringServiceConfiguration(clusterUri, readOperationTimeout, serverSideConfiguration);
      } else {
        configuration = new ClusteringServiceConfiguration(clusterUri, serverSideConfiguration);
      }
    }
    return configuration;
  }

}
