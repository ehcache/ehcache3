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
import org.ehcache.core.HumanReadable;
import org.ehcache.spi.service.ServiceCreationConfiguration;

import java.net.URI;

import org.ehcache.clustered.common.ServerSideConfiguration;

import static org.ehcache.clustered.client.internal.Timeouts.DEFAULT_READ_OPERATION_TIMEOUT;

/**
 * Specifies the configuration for a {@link ClusteringService}.
 */
// TODO: Should this accept/hold a *list* of URIs?
// TODO: Determine proper place for setting readOperationTimeout default
public class ClusteringServiceConfiguration
    implements ServiceCreationConfiguration<ClusteringService>,
    CacheManagerConfiguration<PersistentCacheManager>,
    HumanReadable {

  private final URI clusterUri;
  private final boolean autoCreate;
  private final ServerSideConfiguration serverConfiguration;
  private final TimeoutDuration readOperationTimeout;

  /**
   * Creates a {@code ClusteringServiceConfiguration} from the properties provided.
   *
   * @param clusterUri the non-{@code null} URI identifying the cluster server
   *
   * @throws NullPointerException if {@code clusterUri} is {@code null}
   * @throws IllegalArgumentException if {@code clusterUri} is not URI valid for cluster operations
   */
  public ClusteringServiceConfiguration(final URI clusterUri) {
    validateClusterUri(clusterUri);
    this.clusterUri = clusterUri;
    this.autoCreate = false;
    this.serverConfiguration = null;
    this.readOperationTimeout = DEFAULT_READ_OPERATION_TIMEOUT;
  }

  /**
   * Creates a {@code ClusteringServiceConfiguration} from the properties provided.
   *
   * @param clusterUri the non-{@code null} URI identifying the cluster server
   * @param readOperationTimeout the {@code TimeoutDuration} specifying the time limit for clustered cache
   *                            read operations; if {@code null}, the default value is used
   *
   * @throws NullPointerException if {@code clusterUri} is {@code null}
   * @throws IllegalArgumentException if {@code clusterUri} is not URI valid for cluster operations
   */
  public ClusteringServiceConfiguration(final URI clusterUri, final TimeoutDuration readOperationTimeout) {
    validateClusterUri(clusterUri);
    validateReadOperationTimeout(readOperationTimeout);
    this.clusterUri = clusterUri;
    this.autoCreate = false;
    this.serverConfiguration = null;
    this.readOperationTimeout = readOperationTimeout;
  }

  /**
   * Creates a {@code ClusteringServiceConfiguration} from the properties provided.
   *
   * @param clusterUri the non-{@code null} URI identifying the cluster server
   * @param serverConfig  the server side entity configuration required
   *
   * @throws NullPointerException if {@code clusterUri} or {@code serverConfig} is {@code null}
   * @throws IllegalArgumentException if {@code clusterUri} is not URI valid for cluster operations
   */
  public ClusteringServiceConfiguration(final URI clusterUri, final ServerSideConfiguration serverConfig) {
    validateClusterUri(clusterUri);
    if (serverConfig == null) {
      throw new NullPointerException("Server configuration cannot be null");
    }
    this.clusterUri = clusterUri;
    this.autoCreate = false;
    this.serverConfiguration = serverConfig;
    this.readOperationTimeout = DEFAULT_READ_OPERATION_TIMEOUT;
  }

  /**
   * Creates a {@code ClusteringServiceConfiguration} from the properties provided.
   *
   * @param clusterUri the non-{@code null} URI identifying the cluster server
   * @param readOperationTimeout the {@code TimeoutDuration} specifying the time limit for clustered cache
   *                            read operations; if {@code null}, the default value is used
   * @param serverConfig  the server side entity configuration required
   *
   * @throws NullPointerException if {@code clusterUri} or {@code serverConfig} is {@code null}
   * @throws IllegalArgumentException if {@code clusterUri} is not URI valid for cluster operations
   */
  public ClusteringServiceConfiguration(final URI clusterUri, final TimeoutDuration readOperationTimeout, ServerSideConfiguration serverConfig) {
    validateClusterUri(clusterUri);
    validateReadOperationTimeout(readOperationTimeout);
    if (serverConfig == null) {
      throw new NullPointerException("Server configuration cannot be null");
    }
    this.clusterUri = clusterUri;
    this.autoCreate = false;
    this.serverConfiguration = serverConfig;
    this.readOperationTimeout = readOperationTimeout;
  }

  /**
   * Creates a {@code ClusteringServiceConfiguration} from the properties provided.
   *
   * @param clusterUri the non-{@code null} URI identifying the cluster server
   * @param autoCreate {@code true} if server components should be auto created
   * @param serverConfig  the server side entity configuration required
   *
   * @throws NullPointerException if {@code clusterUri} or {@code serverConfig} is {@code null}
   * @throws IllegalArgumentException if {@code clusterUri} is not URI valid for cluster operations
   */
  public ClusteringServiceConfiguration(final URI clusterUri, boolean autoCreate, ServerSideConfiguration serverConfig) {
    validateClusterUri(clusterUri);
    if (serverConfig == null) {
      throw new NullPointerException("Server configuration cannot be null");
    }
    this.clusterUri = clusterUri;
    this.autoCreate = autoCreate;
    this.serverConfiguration = serverConfig;
    this.readOperationTimeout = DEFAULT_READ_OPERATION_TIMEOUT;
  }

  /**
   * Creates a {@code ClusteringServiceConfiguration} from the properties provided.
   *
   * @param clusterUri the non-{@code null} URI identifying the cluster server
   * @param readOperationTimeout the {@code TimeoutDuration} specifying the time limit for clustered cache
   *                            read operations; if {@code null}, the default value is used
   * @param autoCreate {@code true} if server components should be auto created
   * @param serverConfig  the server side entity configuration required
   *
   * @throws NullPointerException if {@code clusterUri} or {@code serverConfig} is {@code null}
   * @throws IllegalArgumentException if {@code clusterUri} is not URI valid for cluster operations
   */
  public ClusteringServiceConfiguration(final URI clusterUri, final TimeoutDuration readOperationTimeout, boolean autoCreate, ServerSideConfiguration serverConfig) {
    validateClusterUri(clusterUri);
    validateReadOperationTimeout(readOperationTimeout);
    if (serverConfig == null) {
      throw new NullPointerException("Server configuration cannot be null");
    }
    this.clusterUri = clusterUri;
    this.autoCreate = autoCreate;
    this.serverConfiguration = serverConfig;
    this.readOperationTimeout = readOperationTimeout;
  }

  protected ClusteringServiceConfiguration(ClusteringServiceConfiguration baseConfig) {
    if (baseConfig == null) {
      throw new NullPointerException("Base configuration cannot be null");
    }

    this.clusterUri = baseConfig.getClusterUri();
    this.readOperationTimeout = baseConfig.getReadOperationTimeout();
    this.autoCreate = baseConfig.isAutoCreate();
    this.serverConfiguration = baseConfig.getServerConfiguration();
  }

  private static void validateClusterUri(URI clusterUri) {
    if (clusterUri == null) {
      throw new NullPointerException("Cluster URI cannot be null.");
    }
  }

  private void validateReadOperationTimeout(TimeoutDuration readOperationTimeout) {
    if (readOperationTimeout == null) {
      throw new NullPointerException("readOperationTimeout cannot be null");
    }
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
   * Returns {@code true} is server side components should be automatically created.
   *
   * @return {@code true} is auto-create is enabled
   */
  public boolean isAutoCreate() {
    return autoCreate;
  }

  /**
   * The default server resource to use for caches and pools, or {@code null} if one is not defined.
   *
   * @return the default server resource
   */
  public ServerSideConfiguration getServerConfiguration() {
    return serverConfiguration;
  }

  /**
   * The timeout for cache read operations.
   *
   * @return the cache read operation timeout; may be {@code null} indicating the default timeout is used
   */
  public TimeoutDuration getReadOperationTimeout() {
    return readOperationTimeout;
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

  @Override
  public String readableString() {
    return this.getClass().getName() + ":\n    " +
        "clusterUri: " + getClusterUri()+ "\n    " +
        "readOperationTimeout: " + getReadOperationTimeout()+ "\n    " +
        "autoCreate: " + isAutoCreate();
  }
}
