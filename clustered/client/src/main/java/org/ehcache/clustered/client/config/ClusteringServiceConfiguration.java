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
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;

import org.ehcache.clustered.common.ServerSideConfiguration;

/**
 * Specifies the configuration for a {@link ClusteringService}.
 */
// TODO: Should this accept/hold a *list* of URIs?
public class ClusteringServiceConfiguration
    implements ServiceCreationConfiguration<ClusteringService>,
    CacheManagerConfiguration<PersistentCacheManager>,
    HumanReadable {

  public static final boolean DEFAULT_AUTOCREATE = false;
  private final URI clusterUri;
  private final boolean autoCreate;
  private final ServerSideConfiguration serverConfiguration;
  private final Timeouts timeouts;
  private final Properties properties;

  /**
   * Creates a {@code ClusteringServiceConfiguration} from the properties provided.
   *
   * @param clusterUri the non-{@code null} URI identifying the cluster server
   *
   * @throws NullPointerException if {@code clusterUri} is {@code null}
   * @throws IllegalArgumentException if {@code clusterUri} is not URI valid for cluster operations
   */
  public ClusteringServiceConfiguration(URI clusterUri) {
    this(clusterUri, Timeouts.DEFAULT);
  }

  /**
   * Creates a {@code ClusteringServiceConfiguration} from the properties provided.
   *
   * @param clusterUri the non-{@code null} URI identifying the cluster server
   * @param timeouts the {@link Timeouts} specifying the time limit for clustered cache operations
   *
   * @throws NullPointerException if {@code clusterUri} is {@code null}
   * @throws IllegalArgumentException if {@code clusterUri} is not URI valid for cluster operations
   */
  public ClusteringServiceConfiguration(URI clusterUri, Timeouts timeouts) {
    this(clusterUri, timeouts, null);
  }

  /**
   * Creates a {@code ClusteringServiceConfiguration} from the properties provided.
   *
   * @param clusterUri the non-{@code null} URI identifying the cluster server
   * @param serverConfig  the server side entity configuration required
   *
   * @throws NullPointerException if {@code clusterUri} is {@code null}
   * @throws IllegalArgumentException if {@code clusterUri} is not URI valid for cluster operations
   */
  public ClusteringServiceConfiguration(URI clusterUri, ServerSideConfiguration serverConfig) {
    this(clusterUri, Timeouts.DEFAULT, serverConfig);
  }

  /**
   * Creates a {@code ClusteringServiceConfiguration} from the properties provided.
   *
   * @param clusterUri the non-{@code null} URI identifying the cluster server
   * @param timeouts the {@link Timeouts} specifying the time limit for clustered cache operations
   * @param serverConfig  the server side entity configuration required
   *
   * @throws NullPointerException if {@code clusterUri} is {@code null}
   * @throws IllegalArgumentException if {@code clusterUri} is not URI valid for cluster operations
   */
  public ClusteringServiceConfiguration(URI clusterUri, Timeouts timeouts, ServerSideConfiguration serverConfig) {
    this(clusterUri, timeouts, DEFAULT_AUTOCREATE, serverConfig);
  }

  /**
   * Creates a {@code ClusteringServiceConfiguration} from the properties provided.
   *
   * @param clusterUri the non-{@code null} URI identifying the cluster server
   * @param autoCreate {@code true} if server components should be auto created
   * @param serverConfig  the server side entity configuration required
   *
   * @throws NullPointerException if {@code clusterUri} is {@code null}
   * @throws IllegalArgumentException if {@code clusterUri} is not URI valid for cluster operations
   */
  public ClusteringServiceConfiguration(URI clusterUri, boolean autoCreate, ServerSideConfiguration serverConfig) {
    this(clusterUri, Timeouts.DEFAULT, autoCreate, serverConfig);
  }

  /**
   * Creates a {@code ClusteringServiceConfiguration} from the properties provided.
   *
   * @param clusterUri the non-{@code null} URI identifying the cluster server
   * @param timeouts the {@link Timeouts} specifying the time limit for clustered cache operations
   * @param autoCreate {@code true} if server components should be auto created
   * @param serverConfig  the server side entity configuration required
   *
   * @throws NullPointerException if {@code clusterUri} is {@code null}
   * @throws IllegalArgumentException if {@code clusterUri} is not URI valid for cluster operations
   */
  public ClusteringServiceConfiguration(URI clusterUri, Timeouts timeouts, boolean autoCreate, ServerSideConfiguration serverConfig) {
    this(clusterUri, timeouts, autoCreate, serverConfig, new Properties());
  }

  /**
   * Creates a {@code ClusteringServiceConfiguration} from the properties provided.
   *
   * @param clusterUri the non-{@code null} URI identifying the cluster server
   * @param timeouts the {@link Timeouts} specifying the time limit for clustered cache operations
   * @param autoCreate {@code true} if server components should be auto created
   * @param serverConfig  the server side entity configuration required
   * @param properties the non-{@code null} connection Properties
   *
   * @throws NullPointerException if {@code clusterUri} is {@code null}
   * @throws IllegalArgumentException if {@code clusterUri} is not URI valid for cluster operations
   */
  public ClusteringServiceConfiguration(URI clusterUri, Timeouts timeouts, boolean autoCreate, ServerSideConfiguration serverConfig, Properties properties) {
    this.clusterUri = Objects.requireNonNull(clusterUri, "Cluster URI cannot be null");
    this.autoCreate = autoCreate;
    this.serverConfiguration = serverConfig;
    this.timeouts = Objects.requireNonNull(timeouts, "Operation timeouts cannot be null");
    this.properties = (Properties) Objects.requireNonNull(properties, "Properties cannot be null").clone();
  }

  protected ClusteringServiceConfiguration(ClusteringServiceConfiguration baseConfig) {
    Objects.requireNonNull(baseConfig, "Base configuration cannot be null");
    this.clusterUri = baseConfig.getClusterUri();
    this.timeouts = baseConfig.getTimeouts();
    this.autoCreate = baseConfig.isAutoCreate();
    this.serverConfiguration = baseConfig.getServerConfiguration();
    this.properties = baseConfig.getProperties();
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
   * The timeouts for all cache operations
   *
   * @return the cache timeouts
   */
  public Timeouts getTimeouts() {
    return timeouts;
  }

  /**
   * The {@code Properties} for the connection.
   *
   * @return the connection {@code Properties}
   */
  public Properties getProperties() {
    return (Properties) properties.clone();
  }

  /**
   * The timeout for cache read operations.
   *
   * @return the cache read operation timeout
   *
   * @deprecated Use {@link #getTimeouts()}
   */
  @Deprecated
  public Duration getReadOperationTimeout() {
    return timeouts.getReadOperationTimeout();
  }

  @Override
  public Class<ClusteringService> getServiceType() {
    return ClusteringService.class;
  }

  @SuppressWarnings("unchecked")
  @Override
  public CacheManagerBuilder<PersistentCacheManager> builder(CacheManagerBuilder<? extends CacheManager> other) {
    return (CacheManagerBuilder<PersistentCacheManager>) other.using(this);   // unchecked
  }

  @Override
  public String readableString() {
    return this.getClass().getName() + ":\n    " +
        "clusterUri: " + getClusterUri()+ "\n    " +
        "timeouts: " + getTimeouts()+ "\n    " +
        "autoCreate: " + isAutoCreate() + "\n    " +
        "defaultServerResource: " + serverConfiguration.getDefaultServerResource() + "\n    " +
        readablePoolsString();
  }

  private String readablePoolsString() {
    StringBuilder pools = new StringBuilder("resourcePools:\n");
    serverConfiguration.getResourcePools().forEach((key, value) -> {
      pools.append("        ");
      pools.append(key);
      pools.append(": ");
      pools.append(value);
      pools.append("\n");
    });
    return pools.toString();
  }
}
