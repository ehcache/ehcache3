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
import org.ehcache.config.Builder;
import org.ehcache.clustered.common.ServerSideConfiguration;

/**
 * A builder of ClusteringService configurations.
 */
public final class ClusteringServiceConfigurationBuilder implements Builder<ClusteringServiceConfiguration> {

  private final URI clusterUri;

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
  }

  /**
   * Support connection to an existing entity or create if the entity if absent.
   *
   * @return a clustering service configuration builder
   */
  public ServerSideConfigurationBuilder autoCreate() {
    return new ServerSideConfigurationBuilder(clusterUri, true);
  }

  /**
   * Only support connection to an existing entity.
   *
   * @return a clustering service configuration builder
   */
  public ServerSideConfigurationBuilder expecting() {
    return new ServerSideConfigurationBuilder(clusterUri, false);
  }

  @Override
  public ClusteringServiceConfiguration build() {
    return new ClusteringServiceConfiguration(clusterUri);
  }
}
