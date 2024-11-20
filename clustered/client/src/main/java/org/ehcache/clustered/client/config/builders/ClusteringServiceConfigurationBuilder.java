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

import java.net.InetSocketAddress;
import java.net.URI;

import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import org.ehcache.clustered.client.config.ClusteringServiceConfiguration.ClientMode;
import org.ehcache.clustered.client.config.Timeouts;
import org.ehcache.clustered.client.internal.ConnectionSource;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.config.Builder;

import static org.ehcache.clustered.client.config.ClusteringServiceConfiguration.DEFAULT_CLIENT_MODE;

/**
 * A builder of ClusteringService configurations.
 */
public final class ClusteringServiceConfigurationBuilder implements Builder<ClusteringServiceConfiguration> {

  private final ConnectionSource connectionSource;
  private final Timeouts timeouts;
  private final ClientMode clientMode;
  private final ServerSideConfigurationBuilder serverSideConfiguration;
  private final Properties properties;

  /**
   * Creates a new builder connecting to the given cluster.
   *
   * @param clusterUri cluster URI
   *
   * @return a clustering service configuration builder
   */
  public static ClusteringServiceConfigurationBuilder cluster(URI clusterUri) {
    return new ClusteringServiceConfigurationBuilder(new ConnectionSource.ClusterUri(clusterUri), TimeoutsBuilder.timeouts().build(), DEFAULT_CLIENT_MODE, null, new Properties());
  }

  /**
   * Creates a new builder connecting to the given cluster.
   *
   * @param servers the non-{@code null} iterable of servers in the cluster
   * @param clusterTierManager the non-{@code null} cluster tier manager identifier
   *
   * @return a clustering service configuration builder
   */
  public static ClusteringServiceConfigurationBuilder cluster(Iterable<InetSocketAddress> servers, String clusterTierManager) {
    return new ClusteringServiceConfigurationBuilder(new ConnectionSource.ServerList(servers, clusterTierManager), TimeoutsBuilder.timeouts().build(), DEFAULT_CLIENT_MODE, null, new Properties());
  }

  /**
   * Creates a new builder seeded from an existing configuration.
   *
   * @param configuration existing clustering configuration
   * @return a clustering service configuration builder
   */
  public static ClusteringServiceConfigurationBuilder seededFrom(ClusteringServiceConfiguration configuration) {

    ServerSideConfiguration serverSideConfiguration = configuration.getServerConfiguration();
    if (serverSideConfiguration == null) {
      return new ClusteringServiceConfigurationBuilder(configuration.getConnectionSource(), configuration.getTimeouts(),
        configuration.getClientMode(), null, configuration.getProperties());
    } else {
      return new ClusteringServiceConfigurationBuilder(configuration.getConnectionSource(), configuration.getTimeouts(),
        configuration.getClientMode(), new ServerSideConfigurationBuilder(serverSideConfiguration), configuration.getProperties());
    }
  }

  private ClusteringServiceConfigurationBuilder(ConnectionSource connectionSource, Timeouts timeouts, ClientMode clientMode, ServerSideConfigurationBuilder serverSideConfiguration, Properties properties) {
    this.connectionSource = connectionSource;
    this.timeouts = Objects.requireNonNull(timeouts, "Timeouts can't be null");
    this.clientMode = clientMode;
    this.serverSideConfiguration = serverSideConfiguration;
    this.properties = properties;
  }

  /**
   * Reconfigure to connect to a different URI.
   *
   * @return a clustering service configuration builder
   */
  public ClusteringServiceConfigurationBuilder usingUri(URI clusterUri) {
    return new ClusteringServiceConfigurationBuilder(new ConnectionSource.ClusterUri(clusterUri), timeouts, clientMode, serverSideConfiguration, properties);
  }

  /**
   * Reconfigure to connect to a different cluster.
   *
   * @return a clustering service configuration builder
   */
  public ClusteringServiceConfigurationBuilder usingServers(Iterable<InetSocketAddress> servers) {
    return new ClusteringServiceConfigurationBuilder(new ConnectionSource.ServerList(servers, connectionSource.getClusterTierManager()), timeouts, clientMode, serverSideConfiguration, properties);
  }

  /**
   * Reconfigure to connect to a different cluster and manager name.
   *
   * @return a clustering service configuration builder
   */
  public ClusteringServiceConfigurationBuilder usingServers(Iterable<InetSocketAddress> servers, String clusterTierManager) {
    return new ClusteringServiceConfigurationBuilder(new ConnectionSource.ServerList(servers, clusterTierManager), timeouts, clientMode, serverSideConfiguration, properties);
  }

  /**
   * Support connection to an existing entity or create if the entity if absent.
   *
   * @return a clustering service configuration builder
   * @deprecated in favor of {@link ClusteringServiceConfigurationBuilder#autoCreate(UnaryOperator)}
   */
  @Deprecated
  public ServerSideConfigurationBuilder autoCreate() {
    return new ServerSideConfigurationBuilder(new ClusteringServiceConfigurationBuilder(this.connectionSource, this.timeouts, ClientMode.AUTO_CREATE, serverSideConfiguration, properties));
  }

  /**
   * Only support connection to an existing entity.
   *
   * @return a clustering service configuration builder
   * @deprecated in favor of {@link ClusteringServiceConfigurationBuilder#expecting(UnaryOperator)}
   */
  @Deprecated
  public ServerSideConfigurationBuilder expecting() {
    return new ServerSideConfigurationBuilder(new ClusteringServiceConfigurationBuilder(this.connectionSource, this.timeouts, ClientMode.EXPECTING, serverSideConfiguration, properties));
  }

  /**
   * Support connection to an existing entity or create if the entity if absent.
   * <p>
   * An empty server-side configuration can be created by performing no operations on the supplied builder:
   * {@code builder.autoCreate(b -> b)}
   *
   * @return a clustering service configuration builder
   */
  public ClusteringServiceConfigurationBuilder autoCreate(UnaryOperator<ServerSideConfigurationBuilder> serverSideConfig) {
    return new ClusteringServiceConfigurationBuilder(this.connectionSource, this.timeouts, ClientMode.AUTO_CREATE,
      serverSideConfig.apply(new ServerSideConfigurationBuilder()), properties);
  }

  /**
   * Support connection to an existing entity or create if the entity if absent on initial connection or any subsequent reconnect attempt.
   * <p>
   * An empty server-side configuration can be created by performing no operations on the supplied builder:
   * {@code builder.autoCreateOnReconnect(b -> b)}
   *
   * @return a clustering service configuration builder
   */
  public ClusteringServiceConfigurationBuilder autoCreateOnReconnect(UnaryOperator<ServerSideConfigurationBuilder> serverSideConfig) {
    return new ClusteringServiceConfigurationBuilder(this.connectionSource, this.timeouts, ClientMode.AUTO_CREATE_ON_RECONNECT,
      serverSideConfig.apply(new ServerSideConfigurationBuilder()), properties);
  }

  /**
   * Only support connection to an existing entity.
   * <p>
   * An empty server-side configuration can be requested by performing no operations on the supplied builder:
   * {@code builder.expecting(b -> b)}
   *
   * @return a clustering service configuration builder
   */
  public ClusteringServiceConfigurationBuilder expecting(UnaryOperator<ServerSideConfigurationBuilder> serverSideConfig) {
    return new ClusteringServiceConfigurationBuilder(this.connectionSource, this.timeouts, ClientMode.EXPECTING,
      serverSideConfig.apply(new ServerSideConfigurationBuilder()), properties);
  }

  /**
   * Adds timeouts.
   * Read operations which time out return a result comparable to a cache miss.
   * Write operations which time out won't do anything.
   * Lifecycle operations which time out will fail with exception
   *
   * @param timeouts the amount of time permitted for all operations
   *
   * @return a clustering service configuration builder
   *
   * @throws NullPointerException if {@code timeouts} is {@code null}
   */
  public ClusteringServiceConfigurationBuilder timeouts(Timeouts timeouts) {
    return new ClusteringServiceConfigurationBuilder(connectionSource, timeouts, clientMode, serverSideConfiguration, properties);
  }

  /**
   * Adds timeouts.
   * Read operations which time out return a result comparable to a cache miss.
   * Write operations which time out won't do anything.
   * Lifecycle operations which time out will fail with exception
   *
   * @param timeoutsBuilder the builder for amount of time permitted for all operations
   *
   * @return a clustering service configuration builder
   *
   * @throws NullPointerException if {@code timeouts} is {@code null}
   */
  public ClusteringServiceConfigurationBuilder timeouts(Builder<? extends Timeouts> timeoutsBuilder) {
    return new ClusteringServiceConfigurationBuilder(connectionSource, timeoutsBuilder.build(), clientMode, serverSideConfiguration, properties);
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
   *
   * @deprecated Use {@link #timeouts(Timeouts)}. Note that calling this method will override any timeouts previously set
   * by setting the read operation timeout to the specified value and everything else to its default.
   */
  @Deprecated
  public ClusteringServiceConfigurationBuilder readOperationTimeout(long duration, TimeUnit unit) {
    Duration readTimeout = Duration.of(duration, toChronoUnit(unit));
    return timeouts(TimeoutsBuilder.timeouts().read(readTimeout).build());
  }

  @Override
  public ClusteringServiceConfiguration build() {
    if (serverSideConfiguration == null) {
      return build(null);
    } else {
      return build(serverSideConfiguration.buildServerSideConfiguration());
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
    return new ClusteringServiceConfiguration(connectionSource, timeouts, clientMode, serverSideConfiguration, properties);
  }

  private static ChronoUnit toChronoUnit(TimeUnit unit) {
    if(unit == null) {
      return null;
    }
    switch (unit) {
      case NANOSECONDS:  return ChronoUnit.NANOS;
      case MICROSECONDS: return ChronoUnit.MICROS;
      case MILLISECONDS: return ChronoUnit.MILLIS;
      case SECONDS:      return ChronoUnit.SECONDS;
      case MINUTES:      return ChronoUnit.MINUTES;
      case HOURS:        return ChronoUnit.HOURS;
      case DAYS:         return ChronoUnit.DAYS;
      default: throw new AssertionError("Unknown unit: " + unit);
    }
  }
}
