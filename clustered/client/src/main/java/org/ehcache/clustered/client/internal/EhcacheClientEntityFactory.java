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

import org.ehcache.clustered.client.internal.service.ClusteredTierManagerConfigurationException;
import org.ehcache.clustered.client.internal.service.ClusteredTierManagerValidationException;
import org.ehcache.clustered.client.service.EntityBusyException;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLock;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLock.Hold;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.connection.Connection;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.exception.EntityAlreadyExistsException;
import org.terracotta.exception.EntityConfigurationException;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.exception.EntityNotProvidedException;
import org.terracotta.exception.EntityVersionMismatchException;
import org.terracotta.exception.PermanentEntityException;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

public class EhcacheClientEntityFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(EhcacheClientEntityFactory.class);

  private static final long ENTITY_VERSION = 1L;

  private final Connection connection;
  private final Map<String, Hold> maintenanceHolds = new ConcurrentHashMap<String, Hold>();

  private final EhcacheClientEntity.Timeouts entityTimeouts;

  public EhcacheClientEntityFactory(Connection connection) {
    this(connection, EhcacheClientEntity.Timeouts.builder().build());
  }

  public EhcacheClientEntityFactory(Connection connection, EhcacheClientEntity.Timeouts entityTimeouts) {
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
   * @throws EhcacheEntityCreationException if an error preventing {@code EhcacheActiveEntity} creation was raised
   * @throws EntityBusyException if another client holding operational leadership prevented this client
   *        from becoming leader and creating the {@code EhcacheActiveEntity} instance
   * @throws TimeoutException if the creation and configuration of the {@code EhcacheActiveEntity} exceed the
   *        lifecycle operation timeout
   */
  public void create(final String identifier, final ServerSideConfiguration config)
    throws EntityAlreadyExistsException, EhcacheEntityCreationException, EntityBusyException, TimeoutException {
    Hold existingMaintenance = maintenanceHolds.get(identifier);
    Hold localMaintenance = null;
    if (existingMaintenance == null) {
      localMaintenance = createAccessLockFor(identifier).tryWriteLock();
    }
    if (existingMaintenance == null && localMaintenance == null) {
      throw new EntityBusyException("Unable to create clustered tier manager for id "
                                    + identifier + ": another client owns the maintenance lease");
    }

    boolean finished = false;

    try {
      EntityRef<EhcacheClientEntity, UUID> ref = getEntityRef(identifier);
      try {
        while (true) {
          ref.create(UUID.randomUUID());
          try {
            EhcacheClientEntity entity = ref.fetchEntity();
            try {
              entity.setTimeouts(entityTimeouts);
              entity.configure(config);
              finished = true;
              return;
            } finally {
              if  (finished) {
                entity.close();
              } else {
                silentlyClose(entity, identifier);
              }
            }
          } catch (ClusteredTierManagerConfigurationException e) {
            try {
              ref.destroy();
            } catch (EntityNotFoundException f) {
              //ignore
            }
            throw new EhcacheEntityCreationException("Unable to configure clustered tier manager for id " + identifier, e);
          } catch (EntityNotFoundException e) {
            //continue;
          }
        }
      } catch (EntityNotProvidedException e) {
        LOGGER.error("Unable to create clustered tier manager for id {}", identifier, e);
        throw new AssertionError(e);
      } catch (EntityVersionMismatchException e) {
        LOGGER.error("Unable to create clustered tier manager for id {}", identifier, e);
        throw new AssertionError(e);
      } catch (PermanentEntityException e) {
        LOGGER.error("Unable to create entity - server indicates it is permanent", e);
        throw new AssertionError(e);
      } catch (EntityConfigurationException e) {
        LOGGER.error("Unable to create entity - configuration exception", e);
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
   * @return an {@code EhcacheClientEntity} providing access to the {@code EhcacheActiveEntity} identified by
   *      {@code identifier}
   *
   * @throws EntityNotFoundException if the {@code EhcacheActiveEntity} identified as {@code identifier} does not exist
   * @throws IllegalArgumentException if {@code config} does not match the {@code EhcacheActiveEntity} configuration
   * @throws TimeoutException if the creation and configuration of the {@code EhcacheActiveEntity} exceed the
   *        lifecycle operation timeout
   */
  public EhcacheClientEntity retrieve(String identifier, ServerSideConfiguration config)
    throws EntityNotFoundException, EhcacheEntityValidationException, TimeoutException {

    Hold fetchHold = createAccessLockFor(identifier).readLock();

    EhcacheClientEntity entity;
    try {
      entity = getEntityRef(identifier).fetchEntity();
    } catch (EntityVersionMismatchException e) {
      LOGGER.error("Unable to retrieve clustered tier manager for id {}", identifier, e);
      silentlyUnlock(fetchHold, identifier);
      throw new AssertionError(e);
    }

    /*
     * Currently entities are never closed as doing so can stall the client
     * when the server is dead.  Instead the connection is forcibly closed,
     * which suits our purposes since that will unlock the fetchHold too.
     */
    boolean validated = false;
    try {
      entity.setTimeouts(entityTimeouts);
      entity.validate(config);
      validated = true;
      return entity;
    } catch (ClusteredTierManagerValidationException e) {
      throw new EhcacheEntityValidationException("Unable to validate clustered tier manager for id " + identifier, e);
    } finally {
      if (!validated) {
        silentlyClose(entity, identifier);
        silentlyUnlock(fetchHold, identifier);
      }
    }
  }

  public void destroy(final String identifier) throws EhcacheEntityNotFoundException, EntityBusyException {
    Hold existingMaintenance = maintenanceHolds.get(identifier);
    Hold localMaintenance = null;

    if (existingMaintenance == null) {
      localMaintenance = createAccessLockFor(identifier).tryWriteLock();
    }

    if (existingMaintenance == null && localMaintenance == null) {
      throw new EntityBusyException("Destroy operation failed; " + identifier + " clustered tier's maintenance lease held");
    }

    boolean finished = false;

    try {
      EntityRef<EhcacheClientEntity, UUID> ref = getEntityRef(identifier);
      try {
        if (!ref.destroy()) {
          throw new EntityBusyException("Destroy operation failed; " + identifier + " clustered tier in use by other clients");
        }
        finished = true;
      } catch (EntityNotProvidedException e) {
        LOGGER.error("Unable to delete clustered tier manager for id {}", identifier, e);
        throw new AssertionError(e);
      } catch (EntityNotFoundException e) {
        throw new EhcacheEntityNotFoundException(e);
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

  private void silentlyClose(EhcacheClientEntity entity, String identifier) {
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
    return new VoltronReadWriteLock(connection, "EhcacheClientEntityFactory-AccessLock-" + entityIdentifier);
  }

  private EntityRef<EhcacheClientEntity, UUID> getEntityRef(String identifier) {
    try {
      return connection.getEntityRef(EhcacheClientEntity.class, ENTITY_VERSION, identifier);
    } catch (EntityNotProvidedException e) {
      LOGGER.error("Unable to get clustered tier manager for id {}", identifier, e);
      throw new AssertionError(e);
    }
  }

}
