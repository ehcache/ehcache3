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
import org.terracotta.connection.entity.Entity;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.dynamic_config.api.model.Cluster;
import org.terracotta.dynamic_config.api.model.Node;
import org.terracotta.dynamic_config.api.model.UID;
import org.terracotta.dynamic_config.entity.topology.client.DynamicTopologyEntity;
import org.terracotta.dynamic_config.entity.topology.common.DynamicTopologyEntityConstants;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.exception.EntityNotProvidedException;
import org.terracotta.exception.EntityVersionMismatchException;
import org.terracotta.lease.connection.LeasedConnection;
import org.terracotta.lease.connection.LeasedConnectionFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

    private final CopyOnWriteArraySet<InetSocketAddress> servers;
    private final String clusterTierManager;

    public ServerList(Iterable<InetSocketAddress> servers, String clusterTierManager) {
      this.servers = createServerList(servers);
      this.clusterTierManager = Objects.requireNonNull(clusterTierManager, "Cluster tier manager identifier cannot be null");
    }

    private CopyOnWriteArraySet<InetSocketAddress> createServerList(Iterable<InetSocketAddress> servers) {
      Objects.requireNonNull(servers, "Servers cannot be null");
      CopyOnWriteArraySet<InetSocketAddress> serverList = new CopyOnWriteArraySet<>();
      servers.forEach(server -> serverList.add(server));
      return serverList;
    }

    @Override
    public String getClusterTierManager() {
      return clusterTierManager;
    }

    @Override
    public LeasedConnection connect(Properties connectionProperties) throws ConnectionException {
      LeasedConnection connection = LeasedConnectionFactory.connect(servers, connectionProperties);
      try {
        EntityRef<DynamicTopologyEntity, Object, DynamicTopologyEntity.Settings> ref = connection.getEntityRef(DynamicTopologyEntity.class, 1, DynamicTopologyEntityConstants.ENTITY_NAME);
        DynamicTopologyEntity dynamicTopologyEntity = ref.fetchEntity(null);
        dynamicTopologyEntity.setListener(new DynamicTopologyEntity.Listener() {
          @Override
          public void onNodeRemoval(Cluster cluster, UID stripeUID, Node removedNode) {
            servers.remove(removedNode.getInternalAddress());
            removedNode.getPublicAddress().ifPresent(servers::remove);
          }

          @Override
          public void onNodeAddition(Cluster cluster, UID addedNodeUID) {
            InetSocketAddress anAddress = servers.iterator().next(); // a random address from the user provided URI
            cluster.getEndpoints(anAddress).stream() // get the cluster node endpoints for this user address
              .filter(endpoint -> endpoint.getNodeUID().equals(addedNodeUID))
              .map(Node.Endpoint::getAddress)
              .forEach(servers::add);
          }
        });
        return new LeasedConnection() {
          @Override
          public <T extends Entity, C, U> EntityRef<T, C, U> getEntityRef(Class<T> cls, long version, String name) throws EntityNotProvidedException {
            return connection.getEntityRef(cls, version, name);
          }

          @Override
          public void close() throws IOException {
            Future<?> close = dynamicTopologyEntity.releaseEntity();
            try {
              close.get(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
              throw new IOException(e.getCause());
            } catch (TimeoutException e) {
            } finally {
              connection.close();
            }
          }
        };
      } catch (EntityNotProvidedException | EntityVersionMismatchException | EntityNotFoundException e) {
        throw new AssertionError(e);
      }
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
