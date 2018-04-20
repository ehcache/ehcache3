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

public class ConnectionSource {
  private final URI clusterUri;
  private final Iterable<InetSocketAddress> servers;

  public ConnectionSource(URI clusterUri) {
    this.clusterUri = Objects.requireNonNull(clusterUri, "Cluster URI cannot be null");
    this.servers = null;
  }

  public ConnectionSource(Iterable<InetSocketAddress> servers) {
    this.servers = cloneServers(Objects.requireNonNull(servers, "Servers cannot be null"));
    this.clusterUri = null;
  }

  public URI getClusterUri() {
    return clusterUri;
  }

  public Iterable<InetSocketAddress> getServers() {
    if (servers == null) {
      return null;
    } else {
      return cloneServers(servers);
    }
  }

  public ConnectionSource copy() {
    if (clusterUri != null) {
      return new ConnectionSource(clusterUri);
    } else {
      return new ConnectionSource(cloneServers(servers));
    }
  }

  public LeasedConnection connect(Properties connectionProperties) throws ConnectionException {
    if (clusterUri != null) {
      return LeasedConnectionFactory.connect(extractClusterUri(clusterUri), connectionProperties);
    } else {
      return LeasedConnectionFactory.connect(servers, connectionProperties);
    }
  }

  @Override
  public String toString() {
    if (clusterUri != null) {
      return clusterUri.toString();
    } else {
      return servers.toString();
    }
  }

  private List<InetSocketAddress> cloneServers(Iterable<InetSocketAddress> servers) {
    List<InetSocketAddress> socketAddresses = new ArrayList<>();
    servers.forEach(socketAddresses::add);
    return socketAddresses;
  }

  private static URI extractClusterUri(URI uri) {
    try {
      return new URI(uri.getScheme(), uri.getAuthority(), null, null, null);
    } catch (URISyntaxException e) {
      throw new AssertionError(e);
    }
  }
}
