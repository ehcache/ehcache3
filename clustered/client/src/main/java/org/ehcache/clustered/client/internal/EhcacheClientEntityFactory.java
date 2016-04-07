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
      throw new AssertionError(ex.getCause());
    }
  }

  public void abandonLeadership(String entityIdentifier) {
    coordinator.delist(EhcacheClientEntity.class, entityIdentifier);
  }

  public EhcacheClientEntity create(final String identifier, final ServerSideConfiguration config) throws EntityAlreadyExistsException {
    try {
      EhcacheClientEntity created = asLeaderOf(identifier, new ElectionTask<EhcacheClientEntity>() {
        @Override
        public EhcacheClientEntity call(boolean clean) throws EntityAlreadyExistsException {
          EntityRef<EhcacheClientEntity, UUID> ref = getEntityRef(identifier);
          try {
            while (true) {
              ref.create(UUID.randomUUID());
              try {
                return configure(ref.fetchEntity(), config);
              } catch (EntityNotFoundException e) {
                //continue;
              }
            }
          } catch (EntityNotProvidedException e) {
            throw new AssertionError(e);
          } catch (EntityVersionMismatchException e) {
            throw new AssertionError(e);
          }
        }
      });
      if (created == null) {
        throw new AssertionError("Not the leader");
      } else {
        return created;
      }
    } catch (ExecutionException ex) {
      throw unwrapException(ex, EntityAlreadyExistsException.class);
    }
  }

  public EhcacheClientEntity createOrRetrieve(String identifier, ServerSideConfiguration config) {
    try {
      return retrieve(identifier, config);
    } catch (EntityNotFoundException e) {
      try {
        return create(identifier, config);
      } catch (EntityAlreadyExistsException f) {
        throw new AssertionError("Somebody else doing leader work!");
      }
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
      throw new AssertionError(e);
    }
  }

  public void destroy(final String identifier) throws EntityNotFoundException {
    throw new UnsupportedOperationException("Destroy implementation waiting on fix for Terracotta-OSS/terracotta-apis#27");
  }

  private <T> T asLeaderOf(String identifier, final ElectionTask<T> task) throws ExecutionException {
    return coordinator.executeIfLeader(EhcacheClientEntity.class, identifier, task);
  }

  private EntityRef<EhcacheClientEntity, UUID> getEntityRef(String identifier) {
    try {
      return connection.getEntityRef(EhcacheClientEntity.class, ENTITY_VERSION, identifier);
    } catch (EntityNotProvidedException e) {
      throw new AssertionError(e);
    }
  }

  private void validate(EhcacheClientEntity entity, ServerSideConfiguration config) {
    entity.validate(config);
  }

  private EhcacheClientEntity configure(EhcacheClientEntity entity, ServerSideConfiguration config) {
    entity.configure(config);
    return entity;
  }
}