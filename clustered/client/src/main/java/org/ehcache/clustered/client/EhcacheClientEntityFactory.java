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
package org.ehcache.clustered.client;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.terracotta.connection.Connection;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.consensus.CoordinationService;
import org.terracotta.consensus.CoordinationService.ElectionTask;
import org.terracotta.exception.EntityAlreadyExistsException;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.exception.EntityNotProvidedException;
import org.terracotta.exception.EntityVersionMismatchException;

public class EhcacheClientEntityFactory {
  
  private final Connection connection;
  private final CoordinationService coordinator;
  
  public EhcacheClientEntityFactory(Connection connection) {
    this(connection, new CoordinationService(connection));
  }

  EhcacheClientEntityFactory(Connection connection, CoordinationService coordinator) {
    this.connection = connection;
    this.coordinator = coordinator;
  }
  
  public EhcacheClientEntity create(final String identifier, final Object config) throws EntityAlreadyExistsException {
    try {
      while (true) {
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
        if (created != null) {
          return created;
        }
      }
    } catch (ExecutionException ex) {
      throw unwrapException(ex, EntityAlreadyExistsException.class);
    }
  }
  
  public EhcacheClientEntity createOrRetrieve(String identifier, Object config) {
    while (true) {
      try {
        return retrieve(identifier, config);
      } catch (EntityNotFoundException e) {
        try {
          return create(identifier, config);
        } catch (EntityAlreadyExistsException f) {
          //Entity got created by another leader - try to retrieve
          //continue;
        }
      }
    }
  }

  public EhcacheClientEntity retrieve(String identifier, Object config) throws EntityNotFoundException {
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
    try {
      while (true) {
        Boolean success = asLeaderOf(identifier, new ElectionTask<Boolean>() {
          @Override
          public Boolean call(boolean clean) throws EntityNotFoundException {
            EntityRef<EhcacheClientEntity, UUID> ref = getEntityRef(identifier);
            try {
              ref.destroy();
              return Boolean.TRUE;
            } catch (EntityNotProvidedException e) {
              throw new AssertionError(e);
            }
          }
        });
        if (Boolean.TRUE.equals(success)) {
          return;
        }
      }
    } catch (ExecutionException ex) {
      throw unwrapException(ex, EntityNotFoundException.class);
    }
  }

  private <T> T asLeaderOf(String identifier, final ElectionTask<T> task) throws ExecutionException {
    try {
      return coordinator.executeIfLeader(EhcacheClientEntity.class, identifier, task);
    } finally {
      coordinator.delist(EhcacheClientEntity.class, identifier);
    }
  }

  private EntityRef<EhcacheClientEntity, UUID> getEntityRef(String identifier) {
    try {
      return connection.getEntityRef(EhcacheClientEntity.class, 0, identifier);
    } catch (EntityNotProvidedException e) {
      throw new AssertionError(e);
    }
  }

  private void validate(EhcacheClientEntity entity, Object config) {
    //no-op
  }

  private EhcacheClientEntity configure(EhcacheClientEntity entity, Object config) {
    return entity;
  }

  private <T extends Exception> T unwrapException(Throwable t, Class<T> aClass) {
    Throwable cause = t.getCause();
    if (cause != null) {
      t = cause;
    }
    
    if (aClass.isInstance(t)) {
      return aClass.cast(t);
    } else if (t instanceof RuntimeException) {
      throw (RuntimeException) t;
    } else if (t instanceof Error) {
      throw (Error) t;
    } else {
      throw new RuntimeException(t);
    }
  }
}