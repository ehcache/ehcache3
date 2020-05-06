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
import org.ehcache.clustered.client.config.ClusteringServiceConfiguration.ClientMode;
import org.ehcache.clustered.client.config.Timeouts;
import org.ehcache.clustered.client.internal.ClusterTierManagerClientEntity;
import org.ehcache.clustered.client.internal.ClusterTierManagerClientEntityFactory;
import org.ehcache.clustered.client.internal.ClusterTierManagerCreationException;
import org.ehcache.clustered.client.internal.ClusterTierManagerValidationException;
import org.ehcache.clustered.client.internal.ConnectionSource;
import org.ehcache.clustered.client.internal.PerpetualCachePersistenceException;
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

import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

class ConnectionState {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionState.class);

  private static final String CONNECTION_PREFIX = "Ehcache:";

  private volatile Executor asyncWorker;
  private volatile Connection clusterConnection = null;
  private volatile ClusterTierManagerClientEntityFactory entityFactory = null;
  private volatile ClusterTierManagerClientEntity entity = null;

  private final AtomicInteger reconnectCounter = new AtomicInteger();
  private final ConcurrentMap<String, ClusterTierClientEntity> clusterTierEntities = new ConcurrentHashMap<>();
  private final Timeouts timeouts;
  private final ConnectionSource connectionSource;
  private final String entityIdentifier;
  private final Properties connectionProperties;
  private final ClusteringServiceConfiguration serviceConfiguration;

  private Runnable connectionRecoveryListener = () -> {};

  ConnectionState(Timeouts timeouts, Properties connectionProperties, ClusteringServiceConfiguration serviceConfiguration) {
    this.timeouts = timeouts;
    this.connectionSource = serviceConfiguration.getConnectionSource();
    this.entityIdentifier = connectionSource.getClusterTierManager();
    this.connectionProperties = connectionProperties;
    connectionProperties.put(ConnectionPropertyNames.CONNECTION_NAME, CONNECTION_PREFIX + entityIdentifier);
    connectionProperties.put(ConnectionPropertyNames.CONNECTION_TIMEOUT, Long.toString(timeouts.getConnectionTimeout().toMillis()));
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
        storeClientEntity = entityFactory.fetchOrCreateClusteredStoreEntity(entityIdentifier, cacheId,
                clientStoreConfiguration, serviceConfiguration.getClientMode(), isReconnect);
        clusterTierEntities.put(cacheId, storeClientEntity);
        break;
      } catch (EntityNotFoundException e) {
        throw new PerpetualCachePersistenceException("Cluster tier proxy '" + cacheId + "' for entity '" + entityIdentifier + "' does not exist.", e);
      } catch (ConnectionClosedException | ConnectionShutdownException e) {
        LOGGER.info("Disconnected from the server", e);
        handleConnectionClosedException();
      }
    }

    return storeClientEntity;
  }

  public void removeClusterTierClientEntity(String cacheId) {
    clusterTierEntities.remove(cacheId);
  }

  public void initClusterConnection(Executor asyncWorker) {
    this.asyncWorker = requireNonNull(asyncWorker);
    try {
      connect();
    } catch (ConnectionException ex) {
      LOGGER.error("Initial connection failed due to", ex);
      throw new RuntimeException(ex);
    }
  }

  private void reconnect() {
    while (true) {
      try {
        connect();
        if (serviceConfiguration.getClientMode().equals(ClientMode.AUTO_CREATE_ON_RECONNECT)) {
          autoCreateEntity();
        }
        LOGGER.info("New connection to server is established, reconnect count is {}", reconnectCounter.incrementAndGet());
        break;
      } catch (ConnectionException e) {
        LOGGER.error("Re-connection to server failed, trying again", e);
      }
    }
  }

  private void connect() throws ConnectionException {
    clusterConnection = connectionSource.connect(connectionProperties);
    entityFactory = new ClusterTierManagerClientEntityFactory(clusterConnection, asyncWorker, timeouts);
  }

  public void closeConnection() {
    Connection conn = clusterConnection;
    clusterConnection = null;
    if(conn != null) {
      try {
        conn.close();
      } catch (IOException | ConnectionShutdownException e) {
        LOGGER.warn("Error closing cluster connection: " + e);
      }
    }
  }

  private boolean silentDestroyUtil() {
    try {
      silentDestroy();
      return true;
    } catch (ConnectionClosedException | ConnectionShutdownException e) {
      LOGGER.info("Disconnected from the server", e);
      reconnect();
      return false;
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
      switch (serviceConfiguration.getClientMode()) {
        case CONNECT:
        case EXPECTING:
          retrieveEntity();
          break;
        case AUTO_CREATE:
        case AUTO_CREATE_ON_RECONNECT:
          autoCreateEntity();
          break;
        default:
          throw new AssertionError(serviceConfiguration.getClientMode());
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

  public void destroyState(boolean healthyConnection) {
    if (entityFactory != null) {
      // proactively abandon any acquired read or write locks on a healthy connection
      entityFactory.abandonAllHolds(entityIdentifier, healthyConnection);
    }
    entityFactory = null;

    clusterTierEntities.clear();
    entity = null;
  }

  public void destroyAll() throws CachePersistenceException {
    LOGGER.info("destroyAll called for cluster tiers on {}", connectionSource);

    while (true) {
      try {
        entityFactory.destroy(entityIdentifier);
        break;
      } catch (EntityBusyException e) {
        throw new CachePersistenceException("Cannot delete cluster tiers on " + connectionSource, e);
      } catch (ConnectionClosedException | ConnectionShutdownException e) {
        handleConnectionClosedException();
      }
    }
  }

  public void destroy(String name) throws CachePersistenceException {
    // will happen when in maintenance mode
    while (true) {
      if (entity == null) {
        try {
          entity = entityFactory.retrieve(entityIdentifier, serviceConfiguration.getServerConfiguration());
        } catch (EntityNotFoundException e) {
          // No entity on the server, so no need to destroy anything
          break;
        } catch (TimeoutException e) {
          throw new CachePersistenceException("Could not connect to the cluster tier manager '" + entityIdentifier
                  + "'; retrieve operation timed out", e);
        } catch (DestroyInProgressException e) {
          if (silentDestroyUtil()) {
            // Nothing left to do
            break;
          }
        } catch (ConnectionClosedException | ConnectionShutdownException e) {
          reconnect();
        }
      }

      try {
        if (entity != null) {
          entityFactory.destroyClusteredStoreEntity(entityIdentifier, name);
          break;
        }
      } catch (EntityNotFoundException e) {
        // Ignore - does not exist, nothing to destroy
        LOGGER.debug("Destruction of cluster tier {} failed as it does not exist", name);
        break;
      } catch (ConnectionClosedException | ConnectionShutdownException e) {
        handleConnectionClosedException();
      }
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
        LOGGER.info("Disconnected from the server", e);
        reconnect();
        continue;
      }

      try {
        entity = entityFactory.retrieve(entityIdentifier, serviceConfiguration.getServerConfiguration());
        break;
      } catch (DestroyInProgressException e) {
        silentDestroyUtil();
      } catch (EntityNotFoundException e) {
        //ignore - loop and try to create
      } catch (TimeoutException e) {
        throw new RuntimeException("Could not connect to the cluster tier manager '" + entityIdentifier
                + "'; retrieve operation timed out", e);
      } catch (ConnectionClosedException | ConnectionShutdownException e) {
        LOGGER.info("Disconnected from the server", e);
        reconnect();
      }
    }

  }

  private void handleConnectionClosedException() {
    while (true) {
      try {
        destroyState(false);
        reconnect();
        connectionRecoveryListener.run();
        break;
      } catch (ConnectionClosedException | ConnectionShutdownException e) {
        LOGGER.info("Disconnected from the server", e);
      }
    }
  }

  //Only for test
  int getReconnectCount() {
    return reconnectCounter.get();
  }

}
