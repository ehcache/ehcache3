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

package org.ehcache.clustered.client.internal.reconnect;

import org.ehcache.CachePersistenceException;
import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.client.config.Timeouts;
import org.ehcache.clustered.client.internal.ClusterTierManagerClientEntity;
import org.ehcache.clustered.client.internal.ClusterTierManagerClientEntityFactory;
import org.ehcache.clustered.client.internal.ClusterTierManagerCreationException;
import org.ehcache.clustered.client.internal.ClusterTierManagerValidationException;
import org.ehcache.clustered.client.internal.store.ClusterTierClientEntity;
import org.ehcache.clustered.client.service.EntityBusyException;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.exceptions.DestroyInProgressException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionException;
import org.terracotta.connection.ConnectionPropertyNames;
import org.terracotta.exception.EntityAlreadyExistsException;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.lease.connection.LeasedConnectionFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

public class ConnectionState {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionState.class);

  private static final String CONNECTION_PREFIX = "Ehcache:";

  private volatile Connection clusterConnection = null;
  private volatile ClusterTierManagerClientEntityFactory entityFactory = null;
  private volatile ClusterTierManagerClientEntity entity = null;
  private final Supplier<ReconnectHandle> handleSupplier;

  private final ConcurrentMap<String, ClusterTierClientEntity> clusterTierEntities = new ConcurrentHashMap<>();
  private final Timeouts timeouts;
  private final URI clusterUri;
  private final String entityIdentifier;

  public ConnectionState(URI clusterUri, Timeouts timeouts, String entityIdentifier, Supplier<ReconnectHandle> handleSupplier) {
    this.timeouts = timeouts;
    this.clusterUri = clusterUri;
    this.entityIdentifier = entityIdentifier;
    this.handleSupplier = handleSupplier;
  }

  public Connection getConnection() {
    return clusterConnection;
  }

  public ClusterTierClientEntity getClusterTierClientEntity(String cacheId) {
    return clusterTierEntities.get(cacheId);
  }

  public ClusterTierManagerClientEntityFactory getEntityFactory() {
    return entityFactory;
  }

  public ClusterTierClientEntity createClusterTierClientEntity(String cacheId,
                                                               ServerStoreConfiguration clientStoreConfiguration,
                                                               boolean isAutoCreate) throws CachePersistenceException {
    ClusterTierClientEntity storeClientEntity;
    try {
      storeClientEntity = entityFactory.fetchOrCreateClusteredStoreEntity(entityIdentifier, cacheId,
              clientStoreConfiguration, isAutoCreate);
      clusterTierEntities.put(cacheId, storeClientEntity);
    } catch (EntityNotFoundException e) {
      throw new CachePersistenceException("Cluster tier proxy '" + cacheId + "' for entity '" + entityIdentifier + "' does not exist.", e);
    }
    return storeClientEntity;
  }

  public void removeClusterTierClientEntity(String cacheId) {
    clusterTierEntities.remove(cacheId);
  }

  public void initClusterConnection(Properties properties) {
    try {
      properties.put(ConnectionPropertyNames.CONNECTION_NAME, CONNECTION_PREFIX + entityIdentifier);
      properties.put(ConnectionPropertyNames.CONNECTION_TIMEOUT, Long.toString(timeouts.getConnectionTimeout().toMillis()));
      clusterConnection = LeasedConnectionFactory.connect(clusterUri, properties);

    } catch (ConnectionException ex) {
      throw new RuntimeException(ex);
    }
  }

  public void createEntityFactory() {
    entityFactory = new ClusterTierManagerClientEntityFactory(clusterConnection, timeouts);
  }

  public void closeConnection() {
    Connection conn = clusterConnection;
    clusterConnection = null;
    if(conn != null) {
      try {
        conn.close();
      } catch (IOException e) {
        LOGGER.warn("Error closing cluster connection: " + e);
      }
    }
  }

  public void silentDestroy() {
    LOGGER.debug("Found a broken ClusterTierManager - trying to clean it up");
    try {
      // Random sleep to enable racing clients to have a window to do the cleanup
      Thread.sleep(new Random().nextInt(1000));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    try {
      entityFactory.destroy(entityIdentifier);
    } catch (EntityBusyException e) {
      // Ignore - we have a racy client
      LOGGER.debug("ClusterTierManager {} marked busy when trying to clean it up", entityIdentifier);
    }
  }

  public void acquireLeadership() {
    if (!entityFactory.acquireLeadership(entityIdentifier)) {
      entityFactory = null;
      closeConnection();
      throw new IllegalStateException("Couldn't acquire cluster-wide maintenance lease");
    }
  }

  public void initializeState(ClusteringServiceConfiguration configuration) {
    try {
      if (configuration.isAutoCreate()) {
        entity = autoCreateEntity(configuration);
      } else {
        try {
          entity = entityFactory.retrieve(entityIdentifier, configuration.getServerConfiguration());
        } catch (DestroyInProgressException | EntityNotFoundException e) {
          throw new IllegalStateException("The cluster tier manager '" + entityIdentifier + "' does not exist."
                  + " Please review your configuration.", e);
        } catch (TimeoutException e) {
          throw new RuntimeException("Could not connect to the cluster tier manager '" + entityIdentifier
                  + "'; retrieve operation timed out", e);
        }
      }
      entity.setReconnectHandle(() -> {
        ReconnectHandle reconnectHandle = this.handleSupplier.get();
        if (reconnectHandle != null) {
          ReconnectionThread reconnectionThread = new ReconnectionThread(reconnectHandle, clusterTierEntities.values());
          reconnectionThread.start();
        }
      });
    } catch (RuntimeException e) {
      entityFactory = null;
      closeConnection();
      throw e;
    }
  }

  public void destroyState() {
    entityFactory = null;

    clusterTierEntities.clear();
    entity = null;
  }

  public void destroyAll() throws CachePersistenceException {
    LOGGER.info("destroyAll called for cluster tiers on {}", clusterUri);

    try {
      entityFactory.destroy(entityIdentifier);
    } catch (EntityBusyException e) {
      throw new CachePersistenceException("Can not delete cluster tiers on " + clusterUri, e);
    }
  }

  public void destroy(String name, ClusteringServiceConfiguration configuration) throws CachePersistenceException {
    // will happen when in maintenance mode
    if(entity == null) {
      try {
        entity = entityFactory.retrieve(entityIdentifier, configuration.getServerConfiguration());
      } catch (EntityNotFoundException e) {
        // No entity on the server, so no need to destroy anything
      } catch (TimeoutException e) {
        throw new CachePersistenceException("Could not connect to the cluster tier manager '" + entityIdentifier
                + "'; retrieve operation timed out", e);
      } catch (DestroyInProgressException e) {
        silentDestroy();
        // Nothing left to do
        return;
      }
    }

    try {
      if (entity != null) {
        entityFactory.destroyClusteredStoreEntity(entityIdentifier, name);
      }
    } catch (EntityNotFoundException e) {
      // Ignore - does not exist, nothing to destroy
      LOGGER.debug("Destruction of cluster tier {} failed as it does not exist", name);
    }
  }

  private ClusterTierManagerClientEntity autoCreateEntity(ClusteringServiceConfiguration configuration) throws ClusterTierManagerValidationException, IllegalStateException {
    while (true) {
      try {
        entityFactory.create(entityIdentifier, configuration.getServerConfiguration());
      } catch (ClusterTierManagerCreationException e) {
        throw new IllegalStateException("Could not create the cluster tier manager '" + entityIdentifier + "'.", e);
      } catch (EntityAlreadyExistsException | EntityBusyException e) {
        //ignore - entity already exists - try to retrieve
      }
      try {
        return entityFactory.retrieve(entityIdentifier, configuration.getServerConfiguration());
      } catch (DestroyInProgressException e) {
        silentDestroy();
      } catch (EntityNotFoundException e) {
        //ignore - loop and try to create
      } catch (TimeoutException e) {
        throw new RuntimeException("Could not connect to the cluster tier manager '" + entityIdentifier
                + "'; retrieve operation timed out", e);
      }
    }
  }

}
