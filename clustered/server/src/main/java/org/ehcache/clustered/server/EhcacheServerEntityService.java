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
package org.ehcache.clustered.server;

import org.ehcache.clustered.common.messages.EhcacheCodec;
import org.ehcache.clustered.common.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.messages.EhcacheEntityResponse;
import org.terracotta.entity.ConcurrencyStrategy;
import org.terracotta.entity.MessageCodec;
import org.terracotta.entity.PassiveServerEntity;
import org.terracotta.entity.ServerEntityService;
import org.terracotta.entity.ServiceRegistry;

import static org.ehcache.clustered.server.ConcurrencyStrategies.noConcurrency;
import org.terracotta.entity.SyncMessageCodec;

public class EhcacheServerEntityService implements ServerEntityService<EhcacheEntityMessage, EhcacheEntityResponse> {

  private static final long ENTITY_VERSION = 1L;
  @Override
  public long getVersion() {
    return ENTITY_VERSION;
  }

  @Override
  public boolean handlesEntityType(String typeName) {
    return "org.ehcache.clustered.client.internal.EhcacheClientEntity".equals(typeName);
  }

  @Override
  public EhcacheActiveEntity createActiveEntity(ServiceRegistry registry, byte[] configuration) {
    return new EhcacheActiveEntity(configuration);
  }

  @Override
  public PassiveServerEntity<EhcacheEntityMessage, EhcacheEntityResponse> createPassiveEntity(ServiceRegistry registry, byte[] configuration) {
    throw new UnsupportedOperationException("Active/passive is not supported yet");
  }

  @Override
  public ConcurrencyStrategy<EhcacheEntityMessage> getConcurrencyStrategy(byte[] config) {
    return noConcurrency();
  }

  @Override
  public MessageCodec<EhcacheEntityMessage, EhcacheEntityResponse> getMessageCodec() {
    return EhcacheCodec.messageCodec();
  }

  @Override
  public SyncMessageCodec<EhcacheEntityMessage> getSyncMessageCodec() {
    return null;
  }
}
