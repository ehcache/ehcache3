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
import org.ehcache.clustered.ClusteredEhcacheIdentity;
import org.ehcache.clustered.ServerSideConfiguration;
import org.ehcache.clustered.messages.EhcacheCodec;
import org.ehcache.clustered.messages.EhcacheEntityMessage;
import org.ehcache.clustered.messages.EhcacheEntityResponse;
import org.terracotta.connection.entity.Entity;
import org.terracotta.entity.EntityClientEndpoint;
import org.terracotta.entity.InvokeFuture;
import org.terracotta.exception.EntityException;

import static org.ehcache.clustered.Util.unwrapException;

/**
 *
 * @author cdennis
 */
public class EhcacheClientEntity implements Entity {

  private final EntityClientEndpoint endpoint;
  
  public EhcacheClientEntity(EntityClientEndpoint endpoint) {
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
    } catch (EntityException e) {
      throw unwrapException(e, IllegalArgumentException.class);
    }
  }

  public void configure(ServerSideConfiguration config) throws IllegalStateException {
    try {
      invoke(EhcacheEntityMessage.configure(config));
    } catch (EntityException e) {
      throw unwrapException(e, IllegalStateException.class);
    }
  }
  
  private EhcacheEntityResponse invoke(EhcacheEntityMessage message) throws EntityException {
    InvokeFuture<byte[]> result = endpoint.beginInvoke().payload(EhcacheCodec.serializeMessage(message)).invoke();
    boolean interrupted = false;
    try {
      while (true) {
        try {
          return EhcacheCodec.deserializeResponse(result.get());
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
