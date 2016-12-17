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

import org.ehcache.clustered.common.internal.ClusteredEhcacheIdentity;
import org.ehcache.clustered.common.internal.messages.CommonConfigCodec;
import org.ehcache.clustered.common.internal.messages.EhcacheCodec;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;

import org.ehcache.clustered.common.internal.messages.LifeCycleMessageCodec;
import org.ehcache.clustered.common.internal.messages.ResponseCodec;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpCodec;
import org.ehcache.clustered.common.internal.messages.StateRepositoryOpCodec;
import org.terracotta.entity.EntityClientEndpoint;
import org.terracotta.entity.EntityClientService;
import org.terracotta.entity.MessageCodec;

public class EhcacheClientEntityService implements EntityClientService<EhcacheClientEntity, UUID, EhcacheEntityMessage, EhcacheEntityResponse> {

  @Override
  public boolean handlesEntityType(Class<EhcacheClientEntity> cls) {
    return EhcacheClientEntity.class.isAssignableFrom(cls);
  }

  @Override
  public byte[] serializeConfiguration(UUID configuration) {
    return ClusteredEhcacheIdentity.serialize(configuration);
  }

  @Override
  public UUID deserializeConfiguration(byte[] configuration) {
    return ClusteredEhcacheIdentity.deserialize(configuration);
  }

  @Override
  public EhcacheClientEntity create(EntityClientEndpoint<EhcacheEntityMessage, EhcacheEntityResponse> endpoint) {
    return new EhcacheClientEntity(endpoint);
  }

  @Override
  public MessageCodec<EhcacheEntityMessage, EhcacheEntityResponse> getMessageCodec() {
    return new EhcacheCodec(new ServerStoreOpCodec(), new LifeCycleMessageCodec(new CommonConfigCodec()),
      new StateRepositoryOpCodec(), new ResponseCodec());
  }
}
