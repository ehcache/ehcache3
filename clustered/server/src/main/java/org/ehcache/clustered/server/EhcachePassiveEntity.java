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

import org.ehcache.clustered.common.PoolAllocation;
import org.ehcache.clustered.common.internal.ClusteredEhcacheIdentity;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.exceptions.IllegalMessageException;
import org.ehcache.clustered.common.internal.exceptions.LifecycleException;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.LifecycleMessage;
import org.ehcache.clustered.common.internal.messages.LifecycleMessage.ConfigureStoreManager;
import org.ehcache.clustered.common.internal.messages.LifecycleMessage.CreateServerStore;
import org.ehcache.clustered.common.internal.messages.LifecycleMessage.DestroyServerStore;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage;
import org.ehcache.clustered.common.internal.messages.StateRepositoryOpMessage;
import org.ehcache.clustered.server.state.EhcacheStateService;
import org.ehcache.clustered.server.state.config.EhcacheStateServiceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.PassiveServerEntity;
import org.terracotta.entity.ServiceRegistry;

import java.util.UUID;

class EhcachePassiveEntity implements PassiveServerEntity<EhcacheEntityMessage, EhcacheEntityResponse> {

  private static final Logger LOGGER = LoggerFactory.getLogger(EhcachePassiveEntity.class);

  private final UUID identity;
  private final EhcacheStateService ehcacheStateService;

  @Override
  public void invoke(EhcacheEntityMessage message) {

    try {
      switch (message.getType()) {
        case LIFECYCLE_OP:
          invokeLifeCycleOperation((LifecycleMessage) message);
          break;
        case SERVER_STORE_OP:
          invokeServerStoreOperation((ServerStoreOpMessage)message);
          break;
        case STATE_REPO_OP:
          ehcacheStateService.getStateRepositoryManager().invoke((StateRepositoryOpMessage)message);
          break;
        default:
          throw new IllegalMessageException("Unknown message : " + message);
      }
    } catch (Exception e) {
      LOGGER.error("Unexpected exception raised during operation: " + message, e);
    }

  }

  EhcachePassiveEntity(ServiceRegistry services, byte[] config) {
    this.identity = ClusteredEhcacheIdentity.deserialize(config);
    ehcacheStateService = services.getService(new EhcacheStateServiceConfig(services));
    if (ehcacheStateService == null) {
      throw new AssertionError("Server failed to retrieve EhcacheStateService.");
    }
  }

  private void invokeServerStoreOperation(ServerStoreOpMessage message) throws ClusterException {
    ServerStoreImpl cacheStore = ehcacheStateService.getStore(message.getCacheId());
    if (cacheStore == null) {
      // An operation on a non-existent store should never get out of the client
      throw new LifecycleException("Clustered tier does not exist : '" + message.getCacheId() + "'");
    }

    switch (message.operation()) {
      //TODO: check if append and getandappend can be combined
      case APPEND: {
        ServerStoreOpMessage.AppendMessage appendMessage = (ServerStoreOpMessage.AppendMessage)message;
        cacheStore.append(appendMessage.getKey(), appendMessage.getPayload());
        break;
      }
      case GET_AND_APPEND: {
        ServerStoreOpMessage.GetAndAppendMessage getAndAppendMessage = (ServerStoreOpMessage.GetAndAppendMessage)message;
        cacheStore.getAndAppend(getAndAppendMessage.getKey(), getAndAppendMessage.getPayload());
        break;
      }
      case REPLACE: {
        ServerStoreOpMessage.ReplaceAtHeadMessage replaceAtHeadMessage = (ServerStoreOpMessage.ReplaceAtHeadMessage) message;
        cacheStore.replaceAtHead(replaceAtHeadMessage.getKey(), replaceAtHeadMessage.getExpect(), replaceAtHeadMessage.getUpdate());
        break;
      }
      case CLEAR: {
        cacheStore.clear();
        break;
      }
      default:
        throw new IllegalMessageException("Unknown ServerStore operation : " + message);
    }
  }

  private void invokeLifeCycleOperation(LifecycleMessage message) throws ClusterException{
    switch (message.operation()) {
      case CONFIGURE:
        ehcacheStateService.configure((ConfigureStoreManager) message);
        break;
      case CREATE_SERVER_STORE:
        createServerStore((CreateServerStore) message);
        break;
      case DESTROY_SERVER_STORE:
        destroyServerStore((DestroyServerStore) message);
        break;
      default:
        throw new IllegalMessageException("Unknown LifeCycle operation " + message);
    }
  }

  private void createServerStore(CreateServerStore createServerStore) throws ClusterException {
    if (!ehcacheStateService.isConfigured()) {
      throw new LifecycleException("Clustered Tier Manager is not configured");
    }
    if(createServerStore.getStoreConfiguration().getPoolAllocation() instanceof PoolAllocation.Unknown) {
      throw new LifecycleException("Clustered tier can't be created with an Unknown resource pool");
    }

    final String name = createServerStore.getName();    // client cache identifier/name

    LOGGER.info("Creating new clustered tier '{}'", name);

    ServerStoreConfiguration storeConfiguration = createServerStore.getStoreConfiguration();
    ehcacheStateService.createStore(name, storeConfiguration);

  }

  private void destroyServerStore(DestroyServerStore destroyServerStore) throws ClusterException {
    if (!ehcacheStateService.isConfigured()) {
      throw new LifecycleException("Clustered Tier Manager is not configured");
    }

    String name = destroyServerStore.getName();

    LOGGER.info("Destroying clustered tier '{}'", name);
    ehcacheStateService.destroyServerStore(name);
  }

  @Override
  public void startSyncEntity() {

  }

  @Override
  public void endSyncEntity() {

  }

  @Override
  public void startSyncConcurrencyKey(int concurrencyKey) {

  }

  @Override
  public void endSyncConcurrencyKey(int concurrencyKey) {

  }

  @Override
  public void createNew() {

  }

  @Override
  public void destroy() {
    ehcacheStateService.destroy();
  }
}
