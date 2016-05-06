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

import org.ehcache.CachePersistenceException;
import org.ehcache.clustered.common.ServerStoreConfiguration;
import org.ehcache.clustered.common.ClusteredEhcacheIdentity;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.messages.EhcacheEntityResponse.Failure;
import org.ehcache.clustered.common.messages.EhcacheEntityResponse.Type;
import org.terracotta.connection.entity.Entity;
import org.terracotta.entity.EntityClientEndpoint;
import org.terracotta.entity.InvokeFuture;

import static org.ehcache.clustered.common.Util.unwrapException;

/**
 *
 * @author cdennis
 */
public class EhcacheClientEntity implements Entity {

  private final EntityClientEndpoint<EhcacheEntityMessage, EhcacheEntityResponse> endpoint;

  public EhcacheClientEntity(EntityClientEndpoint<EhcacheEntityMessage, EhcacheEntityResponse> endpoint) {
    this.endpoint = endpoint;
  }

  public UUID identity() {
    return ClusteredEhcacheIdentity.deserialize(endpoint.getEntityConfiguration());
  }

  @Override
  public void close() {
    endpoint.close();
  }

  public void validate(ServerSideConfiguration config) throws IllegalArgumentException {
    try {
      invoke(EhcacheEntityMessage.validate(config));
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  public void configure(ServerSideConfiguration config) throws IllegalStateException {
    try {
      invoke(EhcacheEntityMessage.configure(config));
    } catch (IllegalStateException e) {
      throw e;
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  public void createCache(String name, ServerStoreConfiguration serverStoreConfiguration) throws CachePersistenceException {
    try {
      invoke(EhcacheEntityMessage.createServerStore(name, serverStoreConfiguration));
    } catch (Exception e) {
      throw unwrapException(e, CachePersistenceException.class);
    }
  }

  public void validateCache(String name, ServerStoreConfiguration serverStoreConfiguration) throws CachePersistenceException {
    try {
      invoke(EhcacheEntityMessage.validateServerStore(name , serverStoreConfiguration));
    } catch (Exception e) {
      throw unwrapException(e, CachePersistenceException.class);
    }
  }

  public void releaseCache(String name) throws CachePersistenceException {
    try {
      invoke(EhcacheEntityMessage.releaseServerStore(name));
    } catch (Exception e) {
      throw unwrapException(e, CachePersistenceException.class);
    }
  }

  public void destroyCache(String name) throws CachePersistenceException {
    try {
      invoke(EhcacheEntityMessage.destroyServerStore(name));
    } catch (Exception e) {
      throw unwrapException(e, CachePersistenceException.class);
    }
  }

  private EhcacheEntityResponse invoke(EhcacheEntityMessage message) throws Exception {
    InvokeFuture<EhcacheEntityResponse> result = endpoint.beginInvoke().message(message).invoke();
    boolean interrupted = false;
    try {
      while (true) {
        try {
          EhcacheEntityResponse response = result.get();
          if (Type.FAILURE.equals(response.getType())) {
            throw ((Failure) response).getCause();
          } else {
            return response;
          }
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
