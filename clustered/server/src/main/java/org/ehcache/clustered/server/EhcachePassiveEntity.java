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
import org.ehcache.clustered.common.internal.exceptions.ServerMisconfigurationException;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.LifecycleMessage;
import org.ehcache.clustered.common.internal.messages.LifecycleMessage.ConfigureStoreManager;
import org.ehcache.clustered.common.internal.messages.LifecycleMessage.CreateServerStore;
import org.ehcache.clustered.common.internal.messages.LifecycleMessage.DestroyServerStore;
import org.ehcache.clustered.common.internal.messages.LifecycleMessage.ValidateStoreManager;
import org.ehcache.clustered.common.internal.messages.RetirementMessage;
import org.ehcache.clustered.common.internal.messages.RetirementMessage.ServerStoreRetirementMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage;
import org.ehcache.clustered.common.internal.messages.StateRepositoryOpMessage;
import org.ehcache.clustered.server.state.ClientMessageTracker;
import org.ehcache.clustered.server.state.EhcacheStateService;
import org.ehcache.clustered.server.state.config.EhcacheStateServiceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.BasicServiceConfiguration;
import org.terracotta.entity.PassiveServerEntity;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.offheapresource.OffHeapResources;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;

class EhcachePassiveEntity implements PassiveServerEntity<EhcacheEntityMessage, EhcacheEntityResponse> {

  private static final Logger LOGGER = LoggerFactory.getLogger(EhcachePassiveEntity.class);

  private final UUID identity;
  private final Set<String> offHeapResourceIdentifiers;
  private final EhcacheStateService ehcacheStateService;

  @Override
  public void invoke(EhcacheEntityMessage message) {

    try {
      if (this.offHeapResourceIdentifiers.isEmpty()) {
        throw new ServerMisconfigurationException("Server started without any offheap resources defined." +
                                                  " Check your server configuration and define at least one offheap resource.");
      }

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
        case RETIREMENT_OP:
          invokeRetirementMessages((RetirementMessage)message);
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
    OffHeapResources offHeapResources = services.getService(new BasicServiceConfiguration<OffHeapResources>(OffHeapResources.class));
    if (offHeapResources == null) {
      this.offHeapResourceIdentifiers = Collections.emptySet();
    } else {
      this.offHeapResourceIdentifiers = offHeapResources.getAllIdentifiers();
    }
    ehcacheStateService = services.getService(new EhcacheStateServiceConfig(services, this.offHeapResourceIdentifiers));
    if (ehcacheStateService == null) {
      throw new AssertionError("Server failed to retrieve EhcacheStateService.");
    }
  }

  private void invokeRetirementMessages(RetirementMessage message) throws ClusterException {

    switch (message.operation()) {
      case SERVERSTORE_RETIRE:
        ServerStoreRetirementMessage retirementMessage = (ServerStoreRetirementMessage)message;
        ServerStoreImpl cacheStore = ehcacheStateService.getStore(retirementMessage.getCacheId());
        if (cacheStore == null) {
          // An operation on a non-existent store should never get out of the client
          throw new LifecycleException("Clustered tier does not exist : '" + retirementMessage.getCacheId() + "'");
        }
        cacheStore.put(retirementMessage.getKey(), retirementMessage.getChain());
        ehcacheStateService.getClientMessageTracker().applied(message.getId(), message.getClientId());
        break;
      case RETIRE:
        ehcacheStateService.getClientMessageTracker().add(message.getClientId());
        break;
      default:
        throw new IllegalMessageException("Unknown Retirement Message : " + message);
    }
  }

  private void invokeServerStoreOperation(ServerStoreOpMessage message) throws ClusterException {
    ServerStoreImpl cacheStore = ehcacheStateService.getStore(message.getCacheId());
    if (cacheStore == null) {
      // An operation on a non-existent store should never get out of the client
      throw new LifecycleException("Clustered tier does not exist : '" + message.getCacheId() + "'");
    }

    switch (message.operation()) {
      case APPEND:
      case GET_AND_APPEND: {
        ehcacheStateService.getClientMessageTracker().track(message.getId(), message.getClientId());
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

  private void invokeLifeCycleOperation(LifecycleMessage message) throws ClusterException {
    switch (message.operation()) {
      case CONFIGURE:
        configure((ConfigureStoreManager) message);
        break;
      case VALIDATE:
        trackAndApplyMessage(message);
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

  private void configure(ConfigureStoreManager message) throws ClusterException {
    ehcacheStateService.configure(message);
    ehcacheStateService.getClientMessageTracker().setEntityConfiguredStamp(message.getClientId(), message.getId());
  }

  private void trackAndApplyMessage(LifecycleMessage message) {
    ClientMessageTracker clientMessageTracker = ehcacheStateService.getClientMessageTracker();
    if (!clientMessageTracker.isAdded(message.getClientId())) {
      throw new IllegalStateException("Untracked client id " + message.getClientId());
    }
    clientMessageTracker.track(message.getId(), message.getClientId());
    clientMessageTracker.applied(message.getId(), message.getClientId());
  }

  private void createServerStore(CreateServerStore createServerStore) throws ClusterException {
    if (!ehcacheStateService.isConfigured()) {
      throw new LifecycleException("Clustered Tier Manager is not configured");
    }
    if(createServerStore.getStoreConfiguration().getPoolAllocation() instanceof PoolAllocation.Unknown) {
      throw new LifecycleException("Clustered tier can't be created with an Unknown resource pool");
    }

    trackAndApplyMessage(createServerStore);

    final String name = createServerStore.getName();    // client cache identifier/name

    LOGGER.info("Creating new clustered tier '{}'", name);

    ServerStoreConfiguration storeConfiguration = createServerStore.getStoreConfiguration();
    ehcacheStateService.createStore(name, storeConfiguration);

  }

  private void destroyServerStore(DestroyServerStore destroyServerStore) throws ClusterException {
    if (!ehcacheStateService.isConfigured()) {
      throw new LifecycleException("Clustered Tier Manager is not configured");
    }

    trackAndApplyMessage(destroyServerStore);

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
  public void loadExisting() {

  }

  @Override
  public void destroy() {
    ehcacheStateService.destroy();
  }
}
