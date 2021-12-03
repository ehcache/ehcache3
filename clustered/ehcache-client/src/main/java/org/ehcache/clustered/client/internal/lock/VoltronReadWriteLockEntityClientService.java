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

package org.ehcache.clustered.client.internal.lock;

import org.ehcache.clustered.common.internal.lock.LockMessaging;
import org.ehcache.clustered.common.internal.lock.LockMessaging.LockOperation;
import org.ehcache.clustered.common.internal.lock.LockMessaging.LockTransition;

import org.terracotta.entity.EntityClientEndpoint;
import org.terracotta.entity.EntityClientService;
import org.terracotta.entity.MessageCodec;

public class VoltronReadWriteLockEntityClientService implements EntityClientService<VoltronReadWriteLockClient, Void, LockOperation, LockTransition, Void> {

  @Override
  public boolean handlesEntityType(Class<VoltronReadWriteLockClient> cls) {
    return VoltronReadWriteLockClient.class.isAssignableFrom(cls);
  }

  @Override
  public byte[] serializeConfiguration(Void nothing) {
    return new byte[0];
  }

  @Override
  public Void deserializeConfiguration(byte[] bytes) {
    return null;
  }

  @Override
  public VoltronReadWriteLockClient create(EntityClientEndpoint<LockOperation, LockTransition> endpoint, Void userData) {
    return new VoltronReadWriteLockClient(endpoint);
  }

  @Override
  public MessageCodec<LockOperation, LockTransition> getMessageCodec() {
    return LockMessaging.codec();
  }
}
