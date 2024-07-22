/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.connection.ConnectionException;
import org.terracotta.connection.entity.Entity;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.dynamic_config.api.model.Cluster;
import org.terracotta.dynamic_config.api.model.EndpointType;
import org.terracotta.dynamic_config.api.model.Node;
import org.terracotta.dynamic_config.api.model.UID;
import org.terracotta.dynamic_config.entity.topology.client.DynamicTopologyEntity;
import org.terracotta.dynamic_config.entity.topology.common.DynamicTopologyEntityConstants;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.exception.EntityNotProvidedException;
import org.terracotta.exception.EntityVersionMismatchException;
import org.terracotta.inet.HostPort;
import org.terracotta.lease.connection.LeasedConnection;
import org.terracotta.lease.connection.LeasedConnectionFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;

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

  public static class ServerList extends AbstractConnectionSource {

    public ServerList(Iterable<InetSocketAddress> servers, String clusterTierManager) {
      super(servers, clusterTierManager);
    }

    @Override
    public URI getClusterUri() {
      throw new IllegalStateException("Cannot use getClusterUri() on ConnectionSource.ServerList. Use getServers() instead.");
    }

    @Override
    public String toString() {
      return "servers: " + getServers() + " [cache-manager: " + getClusterTierManager() + "]";
    }
  }

  private static abstract class AbstractConnectionSource extends ConnectionSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractConnectionSource.class);

    private final HostPortSet currentServers;

    private final String clusterTierManager;

    public AbstractConnectionSource(Iterable<InetSocketAddress> servers, String clusterTierManager) {
      this.currentServers = new HostPortSet(servers);
      this.clusterTierManager = Objects.requireNonNull(clusterTierManager, "Cluster tier manager identifier cannot be null");
    }

    @Override
    public final String getClusterTierManager() {
      return clusterTierManager;
    }

    @Override
    public final LeasedConnection connect(Properties connectionProperties) throws ConnectionException {
      LeasedConnection connection = LeasedConnectionFactory.connect(currentServers, connectionProperties);
      try {
        EntityRef<DynamicTopologyEntity, Object, DynamicTopologyEntity.Settings> ref = connection.getEntityRef(DynamicTopologyEntity.class, 1, DynamicTopologyEntityConstants.ENTITY_NAME);
        DynamicTopologyEntity dynamicTopologyEntity = ref.fetchEntity(null);
        try {
          currentServers.refresh(dynamicTopologyEntity.getUpcomingCluster());
        } catch (TimeoutException | InterruptedException e) {
          LOGGER.warn("Failed to populate connection with cluster topology - passive failover may fail", e);
        }
        dynamicTopologyEntity.setListener(new DynamicTopologyEntity.Listener() {
          @Override
          public void onNodeRemoval(Cluster cluster, UID stripeUID, Node removedNode) {
            currentServers.refresh(cluster);
          }

          @Override
          public void onNodeAddition(Cluster cluster, UID addedNodeUID) {
            currentServers.refresh(cluster);
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

          @Override
          public boolean isValid() {
            return connection.isValid();
          }


        };
      } catch (EntityNotProvidedException | EntityVersionMismatchException | EntityNotFoundException e) {
        throw new AssertionError(e);
      }
    }

    public final Iterable<InetSocketAddress> getServers() {
      return currentServers;
    }
  }

  private static class HostPortSet extends AbstractSet<InetSocketAddress> {

    private volatile EndpointType endpointType;
    private volatile Collection<HostPort> hostPorts;

    public HostPortSet(Iterable<InetSocketAddress> initial) {
      this.hostPorts = StreamSupport.stream(initial.spliterator(), false).map(HostPort::create).collect(toList());
      this.endpointType = null;
    }

    @Override
    public Iterator<InetSocketAddress> iterator() {
      return hostPorts.stream().map(HostPort::createInetSocketAddress).iterator();
    }

    @Override
    public int size() {
      return hostPorts.size();
    }

    public void refresh(Cluster cluster) {
      if (endpointType == null) {
        endpointType = cluster.determineEndpointType(hostPorts);
      }
      hostPorts = cluster.determineEndpoints(endpointType).stream().map(Node.Endpoint::getHostPort).collect(toList());
    }
  }
}
