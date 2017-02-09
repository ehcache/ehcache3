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
package org.ehcache.clustered.lock.server;

import java.util.Collections;
import java.util.Set;
import org.ehcache.clustered.common.internal.lock.LockMessaging;
import org.ehcache.clustered.common.internal.lock.LockMessaging.LockOperation;
import org.ehcache.clustered.common.internal.lock.LockMessaging.LockTransition;

import org.ehcache.clustered.lock.server.messages.LockSyncMessaging;
import org.terracotta.entity.ActiveServerEntity;
import org.terracotta.entity.ClientCommunicator;
import org.terracotta.entity.ConcurrencyStrategy;
import org.terracotta.entity.EntityServerService;
import org.terracotta.entity.MessageCodec;
import org.terracotta.entity.PassiveServerEntity;
import org.terracotta.entity.ServiceConfiguration;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.entity.SyncMessageCodec;

/**
 *
 * @author cdennis
 */
public class VoltronReadWriteLockServerEntityService implements EntityServerService<LockOperation, LockTransition> {

  @Override
  public long getVersion() {
    return 1L;
  }

  @Override
  public boolean handlesEntityType(String typeName) {
    return "org.ehcache.clustered.client.internal.lock.VoltronReadWriteLockClient".equals(typeName);
  }

  @Override
  public ActiveServerEntity<LockOperation, LockTransition> createActiveEntity(ServiceRegistry registry, byte[] config) {
    ClientCommunicator communicator = registry.getService(config(ClientCommunicator.class));
    return new VoltronReadWriteLockActiveEntity(communicator);
  }

  @Override
  public PassiveServerEntity<LockOperation, LockTransition> createPassiveEntity(ServiceRegistry registry, byte[] config) {
    return VoltronReadWriteLockPassiveEntity.INSTANCE;
  }

  @Override
  public ConcurrencyStrategy<LockOperation> getConcurrencyStrategy(byte[] config) {
    return new ConcurrencyStrategy<LockOperation>() {
      @Override
      public int concurrencyKey(LockOperation message) {
        return MANAGEMENT_KEY;
      }

      @Override
      public Set<Integer> getKeysForSynchronization() {
        return Collections.emptySet();
      }
    };
  }

  @Override
  public MessageCodec<LockOperation, LockTransition> getMessageCodec() {
    return LockMessaging.codec();
  }

  @Override
  public SyncMessageCodec<LockOperation> getSyncMessageCodec() {
    return LockSyncMessaging.syncCodec();
  }

  private static final <T> ServiceConfiguration<T> config(final Class<T> klazz) {
    return new ServiceConfiguration<T>() {
      @Override
      public Class<T> getServiceType() {
        return klazz;
      }
    };
  }
}
