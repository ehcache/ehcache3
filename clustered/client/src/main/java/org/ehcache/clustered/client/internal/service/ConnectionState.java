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
package org.ehcache.clustered.client.internal.service;

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
import org.terracotta.exception.ConnectionClosedException;
import org.terracotta.exception.ConnectionShutdownException;
import org.terracotta.exception.EntityAlreadyExistsException;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.lease.connection.LeasedConnectionFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;

class ConnectionState {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionState.class);

  private static final String CONNECTION_PREFIX = "Ehcache:";

  private volatile Connection clusterConnection = null;
  private volatile ClusterTierManagerClientEntityFactory entityFactory = null;
  private volatile ClusterTierManagerClientEntity entity = null;

  private final ConcurrentMap<String, ClusterTierClientEntity> clusterTierEntities = new ConcurrentHashMap<>();
  private final Timeouts timeouts;
  private final URI clusterUri;
  private final String entityIdentifier;
  private final Properties connectionProperties;
  private final ClusteringServiceConfiguration serviceConfiguration;

  private Runnable connectionRecoveryListener = () -> {};

  ConnectionState(URI clusterUri, Timeouts timeouts, String entityIdentifier,
                         Properties connectionProperties, ClusteringServiceConfiguration serviceConfiguration) {
    this.timeouts = timeouts;
    this.clusterUri = clusterUri;
    this.entityIdentifier = entityIdentifier;
    this.connectionProperties = connectionProperties;
    this.serviceConfiguration = serviceConfiguration;
  }

  public void setConnectionRecoveryListener(Runnable connectionRecoveryListener) {
    this.connectionRecoveryListener = connectionRecoveryListener;
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
                                                               ServerStoreConfiguration clientStoreConfiguration, boolean isReconnect)
          throws CachePersistenceException {
    ClusterTierClientEntity storeClientEntity;
    while (true) {
      try {
        if (isReconnect) {
          storeClientEntity = entityFactory.getClusterTierClientEntity(entityIdentifier, cacheId);
        } else {
          storeClientEntity = entityFactory.fetchOrCreateClusteredStoreEntity(entityIdentifier, cacheId,
                  clientStoreConfiguration, serviceConfiguration.isAutoCreate());
        }
        clusterTierEntities.put(cacheId, storeClientEntity);
        break;
      } catch (EntityNotFoundException e) {
        throw new CachePersistenceException("Cluster tier proxy '" + cacheId + "' for entity '" + entityIdentifier + "' does not exist.", e);
      } catch (ConnectionClosedException | ConnectionShutdownException e) {
        LOGGER.info("Disconnected to the server", e);
        handleConnectionClosedException();
      }
    }

    return storeClientEntity;
  }

  public void removeClusterTierClientEntity(String cacheId) {
    clusterTierEntities.remove(cacheId);
  }

  public void initClusterConnection() {
    try {
      connectionProperties.put(ConnectionPropertyNames.CONNECTION_NAME, CONNECTION_PREFIX + entityIdentifier);
      connectionProperties.put(ConnectionPropertyNames.CONNECTION_TIMEOUT, Long.toString(timeouts.getConnectionTimeout().toMillis()));
      clusterConnection = LeasedConnectionFactory.connect(clusterUri, connectionProperties);
      entityFactory = new ClusterTierManagerClientEntityFactory(clusterConnection, timeouts);
    } catch (ConnectionException ex) {
      throw new RuntimeException(ex);
    }
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

  private void silentDestroy() {
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

  public void initializeState() {
    try {
      if (serviceConfiguration.isAutoCreate()) {
        autoCreateEntity();
      } else {
        retrieveEntity();
      }
    } catch (RuntimeException e) {
      entityFactory = null;
      closeConnection();
      throw e;
    }
  }

  private void retrieveEntity() {
    try {
      entity = entityFactory.retrieve(entityIdentifier, serviceConfiguration.getServerConfiguration());
    } catch (DestroyInProgressException | EntityNotFoundException e) {
      throw new IllegalStateException("The cluster tier manager '" + entityIdentifier + "' does not exist."
              + " Please review your configuration.", e);
    } catch (TimeoutException e) {
      throw new RuntimeException("Could not connect to the cluster tier manager '" + entityIdentifier
              + "'; retrieve operation timed out", e);
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

  public void destroy(String name) throws CachePersistenceException {
    // will happen when in maintenance mode
    if(entity == null) {
      try {
        entity = entityFactory.retrieve(entityIdentifier, serviceConfiguration.getServerConfiguration());
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

  private void autoCreateEntity() throws ClusterTierManagerValidationException, IllegalStateException {
    while (true) {
      try {
        entityFactory.create(entityIdentifier, serviceConfiguration.getServerConfiguration());
      } catch (ClusterTierManagerCreationException e) {
        throw new IllegalStateException("Could not create the cluster tier manager '" + entityIdentifier + "'.", e);
      } catch (EntityAlreadyExistsException | EntityBusyException e) {
        //ignore - entity already exists - try to retrieve
      } catch (ConnectionClosedException | ConnectionShutdownException e) {
        LOGGER.info("Disconnected to the server", e);
        initClusterConnection();
        continue;
      }

      try {
        entity = entityFactory.retrieve(entityIdentifier, serviceConfiguration.getServerConfiguration());
        break;
      } catch (DestroyInProgressException e) {
        silentDestroy();
      } catch (EntityNotFoundException e) {
        //ignore - loop and try to create
      } catch (TimeoutException e) {
        throw new RuntimeException("Could not connect to the cluster tier manager '" + entityIdentifier
                + "'; retrieve operation timed out", e);
      } catch (ConnectionClosedException | ConnectionShutdownException e) {
        LOGGER.info("Disconnected to the server", e);
        initClusterConnection();
      }
    }

  }

  private void handleConnectionClosedException() {
    try {
      destroyState();
      initClusterConnection();
      retrieveEntity();
      connectionRecoveryListener.run();
    } catch (ConnectionClosedException | ConnectionShutdownException e) {
      LOGGER.info("Disconnected to the server", e);
      handleConnectionClosedException();
    }
  }

}
