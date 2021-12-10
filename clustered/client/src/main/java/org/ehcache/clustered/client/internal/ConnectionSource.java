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
package org.ehcache.clustered.client.internal;

import org.terracotta.connection.ConnectionException;
import org.terracotta.lease.connection.LeasedConnection;
import org.terracotta.lease.connection.LeasedConnectionFactory;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

public abstract class ConnectionSource {

  public abstract String getClusterTierManager();

  public abstract LeasedConnection connect(Properties connectionProperties) throws ConnectionException;

  public abstract URI getClusterUri();

  public static class ClusterUri extends ConnectionSource {

    private final URI clusterUri;
    private final String clusterTierManager;

    public ClusterUri(URI clusterUri) {
      this.clusterUri = Objects.requireNonNull(clusterUri, "Cluster URI cannot be null");
      this.clusterTierManager = extractCacheManager(clusterUri);
    }

    @Override
    public String getClusterTierManager() {
      return clusterTierManager;
    }

    @Override
    public LeasedConnection connect(Properties connectionProperties) throws ConnectionException {
      return LeasedConnectionFactory.connect(extractClusterUri(clusterUri), connectionProperties);
    }

    @Override
    public URI getClusterUri() {
      return clusterUri;
    }

    @Override
    public String toString() {
      return "clusterUri: " + clusterUri;
    }

    private static String extractCacheManager(URI uri) {
      URI baseUri = extractClusterUri(uri);
      return baseUri.relativize(uri).getPath();
    }

    private static URI extractClusterUri(URI uri) {
      try {
        return new URI(uri.getScheme(), uri.getAuthority(), null, null, null);
      } catch (URISyntaxException e) {
        throw new AssertionError(e);
      }
    }
  }

  public static class ServerList extends ConnectionSource {

    private final Iterable<InetSocketAddress> servers;
    private final String clusterTierManager;

    public ServerList(Iterable<InetSocketAddress> servers, String clusterTierManager) {
      this.servers = cloneServers(Objects.requireNonNull(servers, "Servers cannot be null"));
      this.clusterTierManager = Objects.requireNonNull(clusterTierManager, "Cluster tier manager identifier cannot be null");
    }

    @Override
    public String getClusterTierManager() {
      return clusterTierManager;
    }

    @Override
    public LeasedConnection connect(Properties connectionProperties) throws ConnectionException {
      return LeasedConnectionFactory.connect(servers, connectionProperties);
    }

    @Override
    public URI getClusterUri() {
      throw new IllegalStateException("Cannot use getClusterUri() on ConnectionSource.ServerList. Use getServers() instead.");
    }

    public Iterable<InetSocketAddress> getServers() {
      return cloneServers(servers);
    }

    @Override
    public String toString() {
      return "servers: " + getServers() + " [cache-manager: " + getClusterTierManager() + "]";
    }

    private List<InetSocketAddress> cloneServers(Iterable<InetSocketAddress> servers) {
      List<InetSocketAddress> socketAddresses = new ArrayList<>();
      servers.forEach(socketAddresses::add);
      return socketAddresses;
    }
  }
}
