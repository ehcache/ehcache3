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

package org.ehcache.clustered.server.store;

import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.exceptions.LifecycleException;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EhcacheMessageType;
import org.ehcache.clustered.common.internal.messages.EhcacheOperationMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage;
import org.ehcache.clustered.common.internal.messages.StateRepositoryOpMessage;
import org.ehcache.clustered.common.internal.store.ClusteredTierEntityConfiguration;
import org.ehcache.clustered.server.KeySegmentMapper;
import org.ehcache.clustered.server.ServerSideServerStore;
import org.ehcache.clustered.server.internal.messages.EhcacheDataSyncMessage;
import org.ehcache.clustered.server.internal.messages.EhcacheSyncMessage;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage.ChainReplicationMessage;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage.ClearInvalidationCompleteMessage;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage.InvalidationCompleteMessage;
import org.ehcache.clustered.server.management.ClusterTierManagement;
import org.ehcache.clustered.server.state.ClientMessageTracker;
import org.ehcache.clustered.server.state.EhcacheStateService;
import org.ehcache.clustered.server.state.InvalidationTracker;
import org.ehcache.clustered.server.state.config.EhcacheStoreStateServiceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.PassiveServerEntity;
import org.terracotta.entity.ServiceRegistry;

import java.util.concurrent.TimeoutException;

import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.isPassiveReplicationMessage;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.isStateRepoOperationMessage;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.isStoreOperationMessage;

/**
 * ClusteredTierPassiveEntity
 */
public class ClusteredTierPassiveEntity implements PassiveServerEntity<EhcacheEntityMessage, EhcacheEntityResponse> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusteredTierPassiveEntity.class);

  private final EhcacheStateService stateService;
  private final String storeIdentifier;
  private final ClusterTierManagement management;

  public ClusteredTierPassiveEntity(ServiceRegistry registry, ClusteredTierEntityConfiguration config, KeySegmentMapper defaultMapper) throws ConfigurationException {
    if (config == null) {
      throw new ConfigurationException("ClusteredTierManagerConfiguration cannot be null");
    }
    storeIdentifier = config.getStoreIdentifier();
    stateService = registry.getService(new EhcacheStoreStateServiceConfig(config.getManagerIdentifier(), defaultMapper));
    if (stateService == null) {
      throw new AssertionError("Server failed to retrieve EhcacheStateService.");
    }
    try {
      stateService.createStore(config.getStoreIdentifier(), config.getConfiguration());
      if(config.getConfiguration().getConsistency() == Consistency.EVENTUAL) {
        stateService.addInvalidationtracker(storeIdentifier);
      }
    } catch (ClusterException e) {
      // TODO move the method above to throw ConfigurationException directly
      throw new ConfigurationException("ClusteredTier creation failed: " + e.getMessage(), e);
    }
    management = new ClusterTierManagement(registry, stateService, false, storeIdentifier);
  }

  @Override
  public void invoke(EhcacheEntityMessage message) {
    try {
      if (message instanceof EhcacheOperationMessage) {
        EhcacheOperationMessage operationMessage = (EhcacheOperationMessage) message;
        EhcacheMessageType messageType = operationMessage.getMessageType();
        if (isStoreOperationMessage(messageType)) {
          try {
            invokeServerStoreOperation((ServerStoreOpMessage)message);
          } catch (ClusterException e) {
            // Store operation should not be critical enough to fail a passive
            LOGGER.error("Unexpected exception raised during operation: " + message, e);
          }
        } else if (isStateRepoOperationMessage(messageType)) {
          try {
            stateService.getStateRepositoryManager().invoke((StateRepositoryOpMessage)message);
          } catch (ClusterException e) {
            // State repository operations should not be critical enough to fail a passive
            LOGGER.error("Unexpected exception raised during operation: " + message, e);
          }
        } else if (isPassiveReplicationMessage(messageType)) {
          try {
            invokeRetirementMessages((PassiveReplicationMessage)message);
          } catch (ClusterException e) {
            LOGGER.error("Unexpected exception raised during operation: " + message, e);
          }
        } else {
          throw new AssertionError("Unsupported EhcacheOperationMessage: " + operationMessage.getMessageType());
        }
      } else if (message instanceof EhcacheSyncMessage) {
        invokeSyncOperation((EhcacheSyncMessage) message);
      } else {
        throw new AssertionError("Unsupported EhcacheEntityMessage: " + message.getClass());
      }
    } catch (ClusterException e) {
      // Reaching here means a lifecycle or sync operation failed
      throw new IllegalStateException("A lifecycle or sync operation failed", e);
    }
  }

  private void invokeSyncOperation(EhcacheSyncMessage message) throws ClusterException {
    switch (message.getMessageType()) {
      case DATA:
        EhcacheDataSyncMessage dataSyncMessage = (EhcacheDataSyncMessage) message;
        ServerSideServerStore store = stateService.getStore(storeIdentifier);
        dataSyncMessage.getChainMap().entrySet().forEach(entry -> {
          store.put(entry.getKey(), entry.getValue());

        });
        break;
      default:
        throw new AssertionError("Unsupported Sync operation " + message.getMessageType());
    }
  }

  private void invokeRetirementMessages(PassiveReplicationMessage message) throws ClusterException {

    switch (message.getMessageType()) {
      case CHAIN_REPLICATION_OP:
        LOGGER.debug("Chain Replication message for msgId {} & client Id {}", message.getId(), message.getClientId());
        ChainReplicationMessage retirementMessage = (ChainReplicationMessage) message;
        ServerSideServerStore cacheStore = stateService.getStore(storeIdentifier);
        if (cacheStore == null) {
          // An operation on a non-existent store should never get out of the client
          throw new LifecycleException("Clustered tier does not exist : '" + storeIdentifier + "'");
        }
        cacheStore.put(retirementMessage.getKey(), retirementMessage.getChain());
        applyMessage(message);
        trackHashInvalidationForEventualCache(retirementMessage);
        break;
      case INVALIDATION_COMPLETE:
        untrackHashInvalidationForEventualCache((InvalidationCompleteMessage)message);
        break;
      case CLEAR_INVALIDATION_COMPLETE:
        stateService.getInvalidationTracker(storeIdentifier).setClearInProgress(false);
        break;
      case CLIENT_ID_TRACK_OP:
        stateService.getClientMessageTracker().remove(message.getClientId());
        break;
      default:
        throw new AssertionError("Unsupported Retirement Message : " + message);
    }
  }

  private void invokeServerStoreOperation(ServerStoreOpMessage message) throws ClusterException {
    ServerSideServerStore cacheStore = stateService.getStore(storeIdentifier);
    if (cacheStore == null) {
      // An operation on a non-existent store should never get out of the client
      throw new LifecycleException("Clustered tier does not exist : '" + storeIdentifier + "'");
    }

    switch (message.getMessageType()) {
      case REPLACE: {
        ServerStoreOpMessage.ReplaceAtHeadMessage replaceAtHeadMessage = (ServerStoreOpMessage.ReplaceAtHeadMessage)message;
        cacheStore.replaceAtHead(replaceAtHeadMessage.getKey(), replaceAtHeadMessage.getExpect(), replaceAtHeadMessage.getUpdate());
        break;
      }
      case CLEAR: {
        LOGGER.info("Clearing cluster tier {}", storeIdentifier);
        try {
          cacheStore.clear();
        } catch (TimeoutException e) {
          throw new AssertionError("Server side store is not expected to throw timeout exception");
        }
        InvalidationTracker invalidationTracker = stateService.getInvalidationTracker(storeIdentifier);
        if (invalidationTracker != null) {
          invalidationTracker.setClearInProgress(true);
        }
        break;
      }
      default:
        throw new AssertionError("Unsupported ServerStore operation : " + message.getMessageType());
    }
  }

  private void untrackHashInvalidationForEventualCache(InvalidationCompleteMessage message) {
    stateService.getInvalidationTracker(storeIdentifier).getInvalidationMap().computeIfPresent(message.getKey(), (key, count) -> {
      if (count == 1) {
        return null;
      }
      return count - 1;
    });
  }

  private void trackHashInvalidationForEventualCache(ChainReplicationMessage retirementMessage) {
    InvalidationTracker invalidationTracker = stateService.getInvalidationTracker(storeIdentifier);
    if (invalidationTracker != null) {
      invalidationTracker.getInvalidationMap().compute(retirementMessage.getKey(), (key, count) -> {
        if (count == null) {
          return 1;
        } else {
          return count + 1;
        }
      });
    }
  }

  private void applyMessage(EhcacheOperationMessage message) {
    ClientMessageTracker clientMessageTracker = stateService.getClientMessageTracker();
    clientMessageTracker.applied(message.getId(), message.getClientId());
  }

  @Override
  public void startSyncEntity() {
    LOGGER.info("Sync started.");
  }

  @Override
  public void endSyncEntity() {
    LOGGER.info("Sync completed.");
  }

  @Override
  public void startSyncConcurrencyKey(int concurrencyKey) {
    LOGGER.info("Sync started for concurrency key {}.", concurrencyKey);
  }

  @Override
  public void endSyncConcurrencyKey(int concurrencyKey) {
    LOGGER.info("Sync complete for concurrency key {}.", concurrencyKey);
  }

  @Override
  public void createNew() {
    management.init();
  }

  @Override
  public void destroy() {
    LOGGER.info("Destroying clustered tier '{}'", storeIdentifier);
    try {
      stateService.destroyServerStore(storeIdentifier);
      stateService.removeInvalidationtracker(storeIdentifier);
    } catch (ClusterException e) {
      throw new AssertionError(e);
    }
  }
}
