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

import org.ehcache.CachePersistenceException;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLock;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLock.Hold;
import org.ehcache.clustered.client.internal.store.ClusterTierClientEntity;
import org.ehcache.clustered.client.internal.store.InternalClusterTierClientEntity;
import org.ehcache.clustered.client.service.EntityBusyException;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ClusterTierManagerConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.exceptions.DestroyInProgressException;
import org.ehcache.clustered.common.internal.store.ClusterTierEntityConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.connection.Connection;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.exception.EntityAlreadyExistsException;
import org.terracotta.exception.EntityConfigurationException;
import org.terracotta.exception.EntityException;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.exception.EntityNotProvidedException;
import org.terracotta.exception.EntityVersionMismatchException;
import org.terracotta.exception.PermanentEntityException;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import static org.ehcache.clustered.common.EhcacheEntityVersion.ENTITY_VERSION;

public class ClusterTierManagerClientEntityFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTierManagerClientEntityFactory.class);

  private final Connection connection;
  private final Map<String, Hold> maintenanceHolds = new ConcurrentHashMap<String, Hold>();

  private final Timeouts entityTimeouts;

  public ClusterTierManagerClientEntityFactory(Connection connection) {
    this(connection, Timeouts.builder().build());
  }

  public ClusterTierManagerClientEntityFactory(Connection connection, Timeouts entityTimeouts) {
    this.connection = connection;
    this.entityTimeouts = entityTimeouts;
  }

  public boolean acquireLeadership(String entityIdentifier) {
    VoltronReadWriteLock lock = createAccessLockFor(entityIdentifier);

    Hold hold = lock.tryWriteLock();
    if (hold == null) {
      return false;
    } else {
      maintenanceHolds.put(entityIdentifier, hold);
      return true;
    }
  }

  public void abandonLeadership(String entityIdentifier) {
    Hold hold = maintenanceHolds.remove(entityIdentifier);
    if (hold == null) {
      throw new IllegalMonitorStateException("Leadership was never held");
    } else {
      hold.unlock();
    }
  }

  /**
   * Attempts to create and configure the {@code EhcacheActiveEntity} in the Ehcache clustered server.
   *
   * @param identifier the instance identifier for the {@code EhcacheActiveEntity}
   * @param config the {@code EhcacheActiveEntity} configuration to use for creation
   *
   * @throws EntityAlreadyExistsException if the {@code EhcacheActiveEntity} for {@code identifier} already exists
   * @throws ClusterTierManagerCreationException if an error preventing {@code EhcacheActiveEntity} creation was raised
   * @throws EntityBusyException if another client holding operational leadership prevented this client
   *        from becoming leader and creating the {@code EhcacheActiveEntity} instance
   * @throws TimeoutException if the creation and configuration of the {@code EhcacheActiveEntity} exceed the
   *        lifecycle operation timeout
   */
  public void create(final String identifier, final ServerSideConfiguration config)
    throws EntityAlreadyExistsException, ClusterTierManagerCreationException, EntityBusyException, TimeoutException {
    Hold existingMaintenance = maintenanceHolds.get(identifier);
    Hold localMaintenance = null;
    if (existingMaintenance == null) {
      localMaintenance = createAccessLockFor(identifier).tryWriteLock();
    }
    if (existingMaintenance == null && localMaintenance == null) {
      throw new EntityBusyException("Unable to create cluster tier manager for id "
                                    + identifier + ": another client owns the maintenance lease");
    }

    boolean finished = false;

    try {
      EntityRef<InternalClusterTierManagerClientEntity, ClusterTierManagerConfiguration, Void> ref = getEntityRef(identifier);
      try {
        while (true) {
          ref.create(new ClusterTierManagerConfiguration(identifier, config));
          try {
            InternalClusterTierManagerClientEntity entity = ref.fetchEntity(null);
            try {
              entity.setTimeouts(entityTimeouts);
              finished = true;
              return;
            } finally {
              if  (finished) {
                entity.close();
              } else {
                silentlyClose(entity, identifier);
              }
            }
          } catch (EntityNotFoundException e) {
            //continue;
          }
        }
      } catch (EntityConfigurationException e) {
        throw new ClusterTierManagerCreationException("Unable to configure cluster tier manager for id " + identifier, e);
      } catch (EntityNotProvidedException e) {
        LOGGER.error("Unable to create cluster tier manager for id {}", identifier, e);
        throw new AssertionError(e);
      } catch (EntityVersionMismatchException e) {
        LOGGER.error("Unable to create cluster tier manager for id {}", identifier, e);
        throw new AssertionError(e);
      }
    } finally {
      if (localMaintenance != null) {
        if (finished) {
          localMaintenance.unlock();
        } else {
          silentlyUnlock(localMaintenance, identifier);
        }
      }
    }
  }

  /**
   * Attempts to retrieve a reference to an existing {@code EhcacheActiveEntity} in an Ehcache clustered server.
   *
   * @param identifier the instance identifier for the {@code EhcacheActiveEntity}
   * @param config the {@code EhcacheActiveEntity} configuration to use for access checking
   *
   * @return an {@code ClusterTierManagerClientEntity} providing access to the {@code EhcacheActiveEntity} identified by
   *      {@code identifier}
   *
   * @throws EntityNotFoundException if the {@code EhcacheActiveEntity} identified as {@code identifier} does not exist
   * @throws IllegalArgumentException if {@code config} does not match the {@code EhcacheActiveEntity} configuration
   * @throws TimeoutException if the creation and configuration of the {@code EhcacheActiveEntity} exceed the
   *        lifecycle operation timeout
   */
  public ClusterTierManagerClientEntity retrieve(String identifier, ServerSideConfiguration config)
    throws DestroyInProgressException, EntityNotFoundException, ClusterTierManagerValidationException, TimeoutException {

    Hold fetchHold = createAccessLockFor(identifier).readLock();

    InternalClusterTierManagerClientEntity entity;
    try {
      entity = getEntityRef(identifier).fetchEntity(null);
    } catch (EntityVersionMismatchException e) {
      LOGGER.error("Unable to retrieve cluster tier manager for id {}", identifier, e);
      silentlyUnlock(fetchHold, identifier);
      throw new AssertionError(e);
    }

    entity.setTimeouts(entityTimeouts);
    boolean validated = false;
    try {
      entity.validate(config);
      validated = true;
      return entity;
    } catch (DestroyInProgressException e) {
      throw e;
    } catch (ClusterException e) {
      throw new ClusterTierManagerValidationException("Unable to validate cluster tier manager for id " + identifier, e);
    } finally {
      if (!validated) {
        silentlyClose(entity, identifier);
        silentlyUnlock(fetchHold, identifier);
      }
    }
  }

  public void destroy(final String identifier) throws ClusterTierManagerNotFoundException, EntityBusyException {
    Hold existingMaintenance = maintenanceHolds.get(identifier);
    Hold localMaintenance = null;

    if (existingMaintenance == null) {
      localMaintenance = createAccessLockFor(identifier).tryWriteLock();
    }

    if (existingMaintenance == null && localMaintenance == null) {
      throw new EntityBusyException("Destroy operation failed; " + identifier + " cluster tier's maintenance lease held");
    }

    boolean finished = false;

    try {
      EntityRef<InternalClusterTierManagerClientEntity, ClusterTierManagerConfiguration, Void> ref = getEntityRef(identifier);
      destroyAllClusterTiers(ref, identifier);
      try {
        if (!ref.destroy()) {
          throw new EntityBusyException("Destroy operation failed; " + identifier + " cluster tier in use by other clients");
        }
        finished = true;
      } catch (EntityNotProvidedException e) {
        LOGGER.error("Unable to delete cluster tier manager for id {}", identifier, e);
        throw new AssertionError(e);
      } catch (EntityNotFoundException e) {
        // Ignore - entity does not exist
      } catch (PermanentEntityException e) {
        LOGGER.error("Unable to destroy entity - server says it is permanent", e);
        throw new AssertionError(e);
      }
    } finally {
      if (localMaintenance != null) {
        if (finished) {
          localMaintenance.unlock();
        } else {
          silentlyUnlock(localMaintenance, identifier);
        }
      }
    }
  }

  private void destroyAllClusterTiers(EntityRef<InternalClusterTierManagerClientEntity, ClusterTierManagerConfiguration, Void> ref, String identifier) throws ClusterTierManagerNotFoundException {
    InternalClusterTierManagerClientEntity entity;
    try {
      entity = ref.fetchEntity(null);
      entity.setClientId(UUID.randomUUID());
    } catch (EntityNotFoundException e) {
      // Ignore - means entity does not exist
      return;
    } catch (EntityVersionMismatchException e) {
      throw new AssertionError(e);
    }
    Set<String> storeIdentifiers = entity.prepareForDestroy();
    LOGGER.warn("Preparing to destroy stores {}", storeIdentifiers);
    for (String storeIdentifier : storeIdentifiers) {
      try {
        destroyClusteredStoreEntity(identifier, storeIdentifier);
      } catch (EntityNotFoundException e) {
        // Ignore - assume it was deleted already
      } catch (CachePersistenceException e) {
        throw new AssertionError("Unable to destroy cluster tier - in use: " + e.getMessage());
      }
    }
    entity.close();
  }

  private void silentlyClose(ClusterTierManagerClientEntity entity, String identifier) {
    try {
      entity.close();
    } catch (Exception e) {
      LOGGER.error("Failed to close entity {}", identifier, e);
    }
  }

  private void silentlyUnlock(Hold localMaintenance, String identifier) {
    try {
      localMaintenance.unlock();
    } catch(Exception e) {
      LOGGER.error("Failed to unlock for id {}", identifier, e);
    }
  }

  private VoltronReadWriteLock createAccessLockFor(String entityIdentifier) {
    return new VoltronReadWriteLock(connection, "ClusterTierManagerClientEntityFactory-AccessLock-" + entityIdentifier);
  }

  private EntityRef<InternalClusterTierManagerClientEntity, ClusterTierManagerConfiguration, Void> getEntityRef(String identifier) {
    try {
      return connection.getEntityRef(InternalClusterTierManagerClientEntity.class, ENTITY_VERSION, identifier);
    } catch (EntityNotProvidedException e) {
      LOGGER.error("Unable to get cluster tier manager for id {}", identifier, e);
      throw new AssertionError(e);
    }
  }

  public ClusterTierClientEntity fetchOrCreateClusteredStoreEntity(UUID clientId, String clusterTierManagerIdentifier,
                                                                   String storeIdentifier, ServerStoreConfiguration clientStoreConfiguration,
                                                                   boolean autoCreate) throws EntityNotFoundException, CachePersistenceException {
    EntityRef<InternalClusterTierClientEntity, ClusterTierEntityConfiguration, Void> entityRef;
    try {
      entityRef = connection.getEntityRef(InternalClusterTierClientEntity.class, ENTITY_VERSION, entityName(clusterTierManagerIdentifier, storeIdentifier));
    } catch (EntityNotProvidedException e) {
      throw new AssertionError(e);
    }

    if (autoCreate) {
      while (true) {
        try {
          entityRef.create(new ClusterTierEntityConfiguration(clusterTierManagerIdentifier, storeIdentifier, clientStoreConfiguration));
        } catch (EntityAlreadyExistsException e) {
          // Ignore - entity exists
        } catch (EntityConfigurationException e) {
          throw new CachePersistenceException("Unable to create cluster tier", e);
        } catch (EntityException e) {
          throw new AssertionError(e);
        }
        try {
          InternalClusterTierClientEntity entity = entityRef.fetchEntity(null);
          entity.setClientId(clientId);
          entity.setStoreIdentifier(storeIdentifier);
          entity.setTimeouts(entityTimeouts);
          return entity;
        } catch (EntityNotFoundException e) {
          // Ignore - will try to create again
        } catch (EntityException e) {
          throw new AssertionError(e);
        }
      }
    } else {
      try {
        InternalClusterTierClientEntity entity = entityRef.fetchEntity(null);
        entity.setClientId(clientId);
        entity.setStoreIdentifier(storeIdentifier);
        entity.setTimeouts(entityTimeouts);
        return entity;
      } catch (EntityNotFoundException e) {
        throw e;
      } catch (EntityException e) {
        throw new AssertionError(e);
      }
    }
  }

  public void destroyClusteredStoreEntity(String clusterTierManagerIdentifier, String storeIdentifier) throws EntityNotFoundException, CachePersistenceException {
    EntityRef<InternalClusterTierClientEntity, ClusterTierEntityConfiguration, Void> entityRef;
    try {
      entityRef = connection.getEntityRef(InternalClusterTierClientEntity.class, ENTITY_VERSION, entityName(clusterTierManagerIdentifier, storeIdentifier));
      if (!entityRef.destroy()) {
        throw new CachePersistenceException("Cannot destroy cluster tier '" + storeIdentifier + "': in use by other client(s)");
      }
    } catch (EntityNotProvidedException e) {
      throw new AssertionError(e);
    } catch (PermanentEntityException e) {
      throw new AssertionError(e);
    }
  }

  private static String entityName(String clusterTierManagerIdentifier, String storeIdentifier) {
    return clusterTierManagerIdentifier + "$" + storeIdentifier;
  }
}
