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

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.connection.Connection;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.consensus.CoordinationService;
import org.terracotta.consensus.CoordinationService.ElectionTask;
import org.terracotta.exception.EntityAlreadyExistsException;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.exception.EntityNotProvidedException;
import org.terracotta.exception.EntityVersionMismatchException;

import static org.ehcache.clustered.common.Util.unwrapException;

public class EhcacheClientEntityFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(EhcacheClientEntityFactory.class);

  private static final long ENTITY_VERSION = 1L;

  private final Connection connection;
  private final CoordinationService coordinator;

  public EhcacheClientEntityFactory(Connection connection) {
    this(connection, new CoordinationService(connection));
  }

  EhcacheClientEntityFactory(Connection connection, CoordinationService coordinator) {
    this.connection = connection;
    this.coordinator = coordinator;
  }

  public void close() {
    if (coordinator != null) {
      coordinator.close();
    }
  }

  public boolean acquireLeadership(String entityIdentifier) {
    try {
      if (asLeaderOf(entityIdentifier, new ElectionTask<Boolean>() {
        @Override
        public Boolean call(boolean clean) throws Exception {
          return true;
        }
      }) == null) {
        return false;
      } else {
        return true;
      }
    } catch (ExecutionException ex) {
      LOGGER.error("Unable to acquire cluster leadership cluster id {}", entityIdentifier, ex);
      throw new AssertionError(ex.getCause());
    }
  }

  public void abandonLeadership(String entityIdentifier) {
    coordinator.delist(EhcacheClientEntity.class, entityIdentifier);
  }

  /**
   * Attempts to create and configure the {@code EhcacheActiveEntity} in the Ehcache clustered server.
   *
   * @param identifier the instance identifier for the {@code EhcacheActiveEntity}
   * @param config the {@code EhcacheActiveEntity} configuration
   *
   * @throws EntityAlreadyExistsException if the {@code EhcacheActiveEntity} for {@code identifier} already exists
   * @throws EhcacheEntityCreationException if an error preventing {@code EhcacheActiveEntity} creation was raised;
   *        this is generally resulting from another client holding operational leadership preventing this client
   *        from becoming leader and creating the {@code EhcacheActiveEntity} instance
   */
  public void create(final String identifier, final ServerSideConfiguration config)
      throws EntityAlreadyExistsException, EhcacheEntityCreationException {
    try {
      Boolean created = asLeaderOf(identifier, new ElectionTask<Boolean>() {
        @Override
        public Boolean call(boolean clean) throws EntityAlreadyExistsException {
          EntityRef<EhcacheClientEntity, UUID> ref = getEntityRef(identifier);
          try {
            while (true) {
              ref.create(UUID.randomUUID());
              EhcacheClientEntity entity = null;
              try {
                entity = ref.fetchEntity();
                configure(entity, config);
                return true;
              } catch (EntityNotFoundException e) {
                //continue;
              } finally {
                if (entity != null) {
                  entity.close();
                }
              }
            }
          } catch (EntityNotProvidedException e) {
            LOGGER.error("Unable to create entity for cluster id {}", identifier, e);
            throw new AssertionError(e);
          } catch (EntityVersionMismatchException e) {
            LOGGER.error("Unable to create entity for cluster id {}", identifier, e);
            throw new AssertionError(e);
          }
        }
      });
      if (created == null || !created) {
        String message = "Unable to create entity for cluster id " + identifier
            + ": unable to obtain cluster leadership";
        LOGGER.error(message);
        throw new EhcacheEntityCreationException(message);
      }
    } catch (ExecutionException ex) {
      throw unwrapException(ex, EntityAlreadyExistsException.class);
    }
  }

  public EhcacheClientEntity retrieve(String identifier, ServerSideConfiguration config) throws EntityNotFoundException, IllegalArgumentException {
    try {
      EhcacheClientEntity entity = getEntityRef(identifier).fetchEntity();
      boolean validated = false;
      try {
        validate(entity, config);
        validated = true;
        return entity;
      } finally {
        if (!validated) {
          entity.close();
        }
      }
    } catch (EntityVersionMismatchException e) {
      LOGGER.error("Unable to retrieve entity for cluster id {}", identifier, e);
      throw new AssertionError(e);
    }
  }

  public void destroy(final String identifier) throws EhcacheEntityNotFoundException, EhcacheEntityBusyException {
    try {
      while (true) {
        Boolean success = asLeaderOf(identifier, new ElectionTask<Boolean>() {
          @Override
          public Boolean call(boolean clean) throws EntityNotFoundException, EhcacheEntityBusyException {
            EntityRef<EhcacheClientEntity, UUID> ref = getEntityRef(identifier);
            try {
              if (ref.tryDestroy()) {
                return Boolean.TRUE;
              } else {
                throw new EhcacheEntityBusyException("Destroy operation failed; " + identifier + " caches in use by other clients");
              }
            } catch (EntityNotProvidedException e) {
              LOGGER.error("Unable to delete entity for cluster id {}", identifier, e);
              throw new AssertionError(e);
            }
          }
        });
        if (Boolean.TRUE.equals(success)) {
          return;
        }
      }
    } catch (ExecutionException ex) {
      Throwable cause = (ex.getCause() == null ? ex : ex.getCause());
      if (cause instanceof Error) {
        throw new Error(cause);
      } else if (cause instanceof EntityNotFoundException) {
        throw new EhcacheEntityNotFoundException(cause);
      } else if (cause instanceof EhcacheEntityBusyException) {
        throw new EhcacheEntityBusyException(cause);
      } else if (cause instanceof IllegalArgumentException) {
        throw new IllegalArgumentException(cause);
      } else if (cause instanceof IllegalStateException) {
        throw new IllegalStateException(cause);
      } else {
        throw new RuntimeException(cause);
      }
    }
  }

  private <T> T asLeaderOf(String identifier, final ElectionTask<T> task) throws ExecutionException {
    return coordinator.executeIfLeader(EhcacheClientEntity.class, identifier, task);
  }

  private EntityRef<EhcacheClientEntity, UUID> getEntityRef(String identifier) {
    try {
      return connection.getEntityRef(EhcacheClientEntity.class, ENTITY_VERSION, identifier);
    } catch (EntityNotProvidedException e) {
      LOGGER.error("Unable to get entity for cluster id {}", identifier, e);
      throw new AssertionError(e);
    }
  }

  private void validate(EhcacheClientEntity entity, ServerSideConfiguration config) {
    entity.validate(config);
  }

  private void configure(EhcacheClientEntity entity, ServerSideConfiguration config) {
    entity.configure(config);
  }
}