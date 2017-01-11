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

import org.ehcache.clustered.common.internal.messages.CommonConfigCodec;
import org.ehcache.clustered.common.internal.messages.ConfigCodec;
import org.ehcache.clustered.common.internal.messages.EhcacheCodec;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.LifeCycleMessageCodec;
import org.ehcache.clustered.common.internal.messages.ResponseCodec;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpCodec;
import org.ehcache.clustered.common.internal.messages.StateRepositoryOpCodec;
import org.ehcache.clustered.server.internal.messages.EhcacheServerCodec;
import org.ehcache.clustered.server.internal.messages.EhcacheSyncMessageCodec;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessageCodec;
import org.terracotta.entity.CommonServerEntity;
import org.terracotta.entity.ConcurrencyStrategy;
import org.terracotta.entity.EntityServerService;
import org.terracotta.entity.ExecutionStrategy;
import org.terracotta.entity.MessageCodec;
import org.terracotta.entity.ServiceRegistry;

import static org.ehcache.clustered.server.ConcurrencyStrategies.defaultConcurrency;
import org.terracotta.entity.SyncMessageCodec;
import org.terracotta.logging.LoggingSupplement;

public class EhcacheServerEntityService implements EntityServerService<EhcacheEntityMessage, EhcacheEntityResponse> {

  private static final long ENTITY_VERSION = 1L;
  private static final int DEFAULT_CONCURRENCY = 16;
  private static final KeySegmentMapper DEFAULT_MAPPER = new KeySegmentMapper(DEFAULT_CONCURRENCY);
  private static final ConfigCodec CONFIG_CODEC = new CommonConfigCodec();

  static {
    try {
      LoggingSupplement.logNamespaceToServer("org.ehcache");
    } catch (NoClassDefFoundError e) {
      // Ignore - execution outside of server ends up requiring log4j
    }
  }

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
    return new EhcacheActiveEntity(registry, configuration, DEFAULT_MAPPER);
  }

  @Override
  public EhcachePassiveEntity createPassiveEntity(ServiceRegistry registry, byte[] configuration) {
    return new EhcachePassiveEntity(registry, configuration, DEFAULT_MAPPER);
  }

  @Override
  public ConcurrencyStrategy<EhcacheEntityMessage> getConcurrencyStrategy(byte[] config) {
    return defaultConcurrency(DEFAULT_MAPPER);
  }

  @Override
  public MessageCodec<EhcacheEntityMessage, EhcacheEntityResponse> getMessageCodec() {
    EhcacheCodec ehcacheCodec = new EhcacheCodec(new ServerStoreOpCodec(),
      new LifeCycleMessageCodec(CONFIG_CODEC), new StateRepositoryOpCodec(), new ResponseCodec());
    return new EhcacheServerCodec(ehcacheCodec, new PassiveReplicationMessageCodec(CONFIG_CODEC));
  }

  @Override
  public SyncMessageCodec<EhcacheEntityMessage> getSyncMessageCodec() {
    return new EhcacheSyncMessageCodec(CONFIG_CODEC);
  }

  @Override
  public <AP extends CommonServerEntity<EhcacheEntityMessage, EhcacheEntityResponse>> AP reconfigureEntity(ServiceRegistry registry, AP oldEntity, byte[] configuration) {
    throw new UnsupportedOperationException("Reconfigure not supported in Ehcache");
  }

  @Override
  public ExecutionStrategy<EhcacheEntityMessage> getExecutionStrategy(byte[] configuration) {
    return new EhcacheExecutionStrategy();
  }
}
