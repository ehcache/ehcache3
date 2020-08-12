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

import org.ehcache.clustered.common.EhcacheEntityVersion;
import org.ehcache.clustered.common.internal.ClusterTierManagerConfiguration;
import org.ehcache.clustered.common.internal.messages.CommonConfigCodec;
import org.ehcache.clustered.common.internal.messages.ConfigCodec;
import org.ehcache.clustered.common.internal.messages.EhcacheCodec;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EntityConfigurationCodec;
import org.ehcache.clustered.common.internal.messages.LifeCycleMessageCodec;
import org.ehcache.clustered.common.internal.messages.ResponseCodec;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpCodec;
import org.ehcache.clustered.common.internal.messages.StateRepositoryOpCodec;
import org.ehcache.clustered.server.internal.messages.EhcacheServerCodec;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessageCodec;
import org.ehcache.clustered.server.management.Management;
import org.ehcache.clustered.server.state.EhcacheStateService;
import org.ehcache.clustered.server.state.config.EhcacheStateServiceConfig;
import org.terracotta.entity.CommonServerEntity;
import org.terracotta.entity.ConcurrencyStrategy;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.EntityServerService;
import org.terracotta.entity.ExecutionStrategy;
import org.terracotta.entity.MessageCodec;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.entity.ServiceException;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.entity.SyncMessageCodec;

import static org.ehcache.clustered.server.ConcurrencyStrategies.clusterTierManagerConcurrency;

public class ClusterTierManagerServerEntityService implements EntityServerService<EhcacheEntityMessage, EhcacheEntityResponse> {

  private static final int DEFAULT_CONCURRENCY = 16;
  private static final KeySegmentMapper DEFAULT_MAPPER = new KeySegmentMapper(DEFAULT_CONCURRENCY);
  private static final ConfigCodec CONFIG_CODEC = new CommonConfigCodec();

  private final EntityConfigurationCodec configCodec = new EntityConfigurationCodec(CONFIG_CODEC);

  @Override
  public long getVersion() {
    return EhcacheEntityVersion.ENTITY_VERSION;
  }

  @Override
  public boolean handlesEntityType(String typeName) {
    return "org.ehcache.clustered.client.internal.ClusterTierManagerClientEntity".equals(typeName);
  }

  @Override
  public ClusterTierManagerActiveEntity createActiveEntity(ServiceRegistry registry, byte[] configuration) throws ConfigurationException {
    ClusterTierManagerConfiguration clusterTierManagerConfiguration =
      configCodec.decodeClusterTierManagerConfiguration(configuration);
    EhcacheStateService ehcacheStateService;
    try {
      ehcacheStateService = registry.getService(new EhcacheStateServiceConfig(clusterTierManagerConfiguration, registry, DEFAULT_MAPPER));
    } catch (ServiceException e) {
      throw new ConfigurationException("Unable to retrieve EhcacheStateService: " + e.getMessage());
    }
    Management management = new Management(registry, ehcacheStateService, true, clusterTierManagerConfiguration.getIdentifier());
    return new ClusterTierManagerActiveEntity(clusterTierManagerConfiguration, ehcacheStateService, management);
  }

  @Override
  public ClusterTierManagerPassiveEntity createPassiveEntity(ServiceRegistry registry, byte[] configuration) throws ConfigurationException {
    ClusterTierManagerConfiguration clusterTierManagerConfiguration = configCodec.decodeClusterTierManagerConfiguration(configuration);
    EhcacheStateService ehcacheStateService;
    try {
      ehcacheStateService = registry.getService(new EhcacheStateServiceConfig(clusterTierManagerConfiguration, registry, DEFAULT_MAPPER));
    } catch (ServiceException e) {
      throw new ConfigurationException("Unable to retrieve EhcacheStateService: " + e.getMessage());
    }
    Management management = new Management(registry, ehcacheStateService, false, clusterTierManagerConfiguration.getIdentifier());
    return new ClusterTierManagerPassiveEntity(clusterTierManagerConfiguration, ehcacheStateService, management);
  }

  @Override
  public ConcurrencyStrategy<EhcacheEntityMessage> getConcurrencyStrategy(byte[] config) {
    return clusterTierManagerConcurrency();
  }

  @Override
  public MessageCodec<EhcacheEntityMessage, EhcacheEntityResponse> getMessageCodec() {
    EhcacheCodec ehcacheCodec = new EhcacheCodec(new ServerStoreOpCodec(),
      new LifeCycleMessageCodec(CONFIG_CODEC), new StateRepositoryOpCodec(), new ResponseCodec());
    return new EhcacheServerCodec(ehcacheCodec, new PassiveReplicationMessageCodec());
  }

  @Override
  public SyncMessageCodec<EhcacheEntityMessage> getSyncMessageCodec() {
    return new SyncMessageCodec<EhcacheEntityMessage>() {
      @Override
      public byte[] encode(int concurrencyKey, EhcacheEntityMessage response) {
        throw new UnsupportedOperationException("This entity does not have sync messages");
      }

      @Override
      public EhcacheEntityMessage decode(int concurrencyKey, byte[] payload) {
        throw new UnsupportedOperationException("This entity does not have sync messages");
      }
    };
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
