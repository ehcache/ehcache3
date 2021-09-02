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
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.exceptions.LifecycleException;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EhcacheMessageType;
import org.ehcache.clustered.common.internal.messages.EhcacheOperationMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage;
import org.ehcache.clustered.common.internal.messages.StateRepositoryOpMessage;
import org.ehcache.clustered.common.internal.store.ClusterTierEntityConfiguration;
import org.ehcache.clustered.server.KeySegmentMapper;
import org.ehcache.clustered.server.ServerSideServerStore;
import org.ehcache.clustered.server.internal.messages.EhcacheDataSyncMessage;
import org.ehcache.clustered.server.internal.messages.EhcacheMessageTrackerMessage;
import org.ehcache.clustered.server.internal.messages.EhcacheStateRepoSyncMessage;
import org.ehcache.clustered.server.internal.messages.EhcacheSyncMessage;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage.InvalidationCompleteMessage;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage.ChainReplicationMessage;
import org.ehcache.clustered.server.management.ClusterTierManagement;
import org.ehcache.clustered.server.state.EhcacheStateContext;
import org.ehcache.clustered.server.state.EhcacheStateService;
import org.ehcache.clustered.server.state.config.EhcacheStoreStateServiceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.client.message.tracker.OOOMessageHandler;
import org.terracotta.client.message.tracker.OOOMessageHandlerConfiguration;
import org.terracotta.entity.ClientSourceId;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.EntityUserException;
import org.terracotta.entity.InvokeContext;
import org.terracotta.entity.PassiveServerEntity;
import org.terracotta.entity.ServiceException;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.entity.StateDumpCollector;
import org.terracotta.offheapstore.exceptions.OversizeMappingException;

import java.util.concurrent.TimeoutException;

import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.getResponse;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.success;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.isPassiveReplicationMessage;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.isStateRepoOperationMessage;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.isStoreOperationMessage;
import static org.ehcache.clustered.server.ConcurrencyStrategies.clusterTierConcurrency;

/**
 * ClusterTierPassiveEntity
 */
public class ClusterTierPassiveEntity implements PassiveServerEntity<EhcacheEntityMessage, EhcacheEntityResponse> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTierPassiveEntity.class);

  private final EhcacheStateService stateService;
  private final OOOMessageHandler<EhcacheEntityMessage, EhcacheEntityResponse> messageHandler;
  private final String storeIdentifier;
  private final ClusterTierManagement management;
  private final ServerStoreConfiguration configuration;
  private final String managerIdentifier;

  public ClusterTierPassiveEntity(ServiceRegistry registry, ClusterTierEntityConfiguration config, KeySegmentMapper defaultMapper) throws ConfigurationException {
    if (config == null) {
      throw new ConfigurationException("ClusterTierManagerConfiguration cannot be null");
    }
    storeIdentifier = config.getStoreIdentifier();
    configuration = config.getConfiguration();
    managerIdentifier = config.getManagerIdentifier();
    try {
      stateService = registry.getService(new EhcacheStoreStateServiceConfig(config.getManagerIdentifier(), defaultMapper));
      messageHandler = registry.getService(new OOOMessageHandlerConfiguration<>(managerIdentifier + "###" + storeIdentifier,
        ClusterTierActiveEntity::isTrackedMessage, defaultMapper.getSegments() + 1, new MessageToTrackerSegmentFunction(clusterTierConcurrency(defaultMapper))));
    } catch (ServiceException e) {
      throw new ConfigurationException("Unable to retrieve service: " + e.getMessage());
    }
    if (stateService == null) {
      throw new AssertionError("Server failed to retrieve EhcacheStateService.");
    }
    management = new ClusterTierManagement(registry, stateService, false, storeIdentifier, config.getManagerIdentifier());
  }

  protected EhcacheStateService getStateService() {
    return stateService;
  }

  protected String getStoreIdentifier() {
    return storeIdentifier;
  }

  @Override
  public void addStateTo(StateDumpCollector dump) {
    ClusterTierDump.dump(dump, managerIdentifier, storeIdentifier, configuration);
  }

  @Override
  public void createNew() throws ConfigurationException {
    stateService.createStore(storeIdentifier, configuration, false);
    management.entityCreated();
  }

  private boolean isEventual() {
    return configuration.getConsistency() == Consistency.EVENTUAL;
  }

  @Override
  public void invokePassive(InvokeContext context, EhcacheEntityMessage message) throws EntityUserException {
    InvokeContext realContext = context;
    // For ChainReplicationMessage, we need to recreate the real client context from the one stored in the message. Because the current
    // context comes from the active message. That's not what we want. So instead we recreate a new context using the original client
    // id and transaction id stored in the message
    if (message instanceof ChainReplicationMessage) {
      realContext = new InvokeContext() {
        @Override
        public ClientSourceId getClientSource() {
          return context.makeClientSourceId(((ChainReplicationMessage) message).getClientId());
        }

        @Override
        public long getCurrentTransactionId() {
          return ((ChainReplicationMessage) message).getTransactionId();
        }

        @Override
        public long getOldestTransactionId() {
          return ((ChainReplicationMessage)message).getOldestTransactionId();
        }

        @Override
        public boolean isValidClientInformation() {
          return true;
        }

        @Override
        public ClientSourceId makeClientSourceId(long l) {
          return context.makeClientSourceId(l);
        }

        @Override
        public int getConcurrencyKey() {
          return context.getConcurrencyKey();
        }
      };
    }
    messageHandler.invoke(realContext, message, this::invokePassiveInternal);
  }

  @SuppressWarnings("try")
  private EhcacheEntityResponse invokePassiveInternal(InvokeContext context, EhcacheEntityMessage message) {
    if (message instanceof EhcacheOperationMessage) {
      EhcacheOperationMessage operationMessage = (EhcacheOperationMessage) message;
      try (EhcacheStateContext ignored = stateService.beginProcessing(operationMessage, storeIdentifier)) {
        EhcacheMessageType messageType = operationMessage.getMessageType();
        if (isStoreOperationMessage(messageType)) {
          invokeServerStoreOperation((ServerStoreOpMessage) message);
        } else if (isStateRepoOperationMessage(messageType)) {
          return stateService.getStateRepositoryManager().invoke((StateRepositoryOpMessage) message);
        } else if (isPassiveReplicationMessage(messageType)) {
          return invokeRetirementMessages((PassiveReplicationMessage) message);
        } else {
          throw new AssertionError("Unsupported EhcacheOperationMessage: " + operationMessage.getMessageType());
        }
      } catch (ClusterException | OversizeMappingException e) {
        // The above operations are not critical enough to fail a passive, so just log the exception
        LOGGER.error("Unexpected exception raised during operation: " + message, e);
      }
    } else if (message instanceof EhcacheSyncMessage) {
      invokeSyncOperation(context, (EhcacheSyncMessage) message);
    } else {
      throw new AssertionError("Unsupported EhcacheEntityMessage: " + message.getClass());
    }

    // Default response for messages not returning anything specific
    return success();
  }

  private void invokeSyncOperation(InvokeContext context, EhcacheSyncMessage message) {
    switch (message.getMessageType()) {
      case DATA:
        EhcacheDataSyncMessage dataSyncMessage = (EhcacheDataSyncMessage) message;
        ServerSideServerStore store = stateService.getStore(storeIdentifier);
        dataSyncMessage.getChainMap().forEach(store::put);
        break;
      case STATE_REPO:
        EhcacheStateRepoSyncMessage stateRepoSyncMessage = (EhcacheStateRepoSyncMessage) message;
        stateService.getStateRepositoryManager().processSyncMessage(stateRepoSyncMessage);
        break;
      case MESSAGE_TRACKER:
        EhcacheMessageTrackerMessage messageTrackerMessage = (EhcacheMessageTrackerMessage) message;
        messageTrackerMessage.getTrackedMessages().forEach((key, value) ->
          messageHandler.loadTrackedResponsesForSegment(messageTrackerMessage.getSegmentId(), context.makeClientSourceId(key), value));
        break;
      default:
        throw new AssertionError("Unsupported Sync operation " + message.getMessageType());
    }
  }

  private EhcacheEntityResponse invokeRetirementMessages(PassiveReplicationMessage message) throws ClusterException {

    switch (message.getMessageType()) {
      case CHAIN_REPLICATION_OP:
        ChainReplicationMessage retirementMessage = (ChainReplicationMessage) message;
        LOGGER.debug("Chain Replication message for transactionId {} & clientId {}", retirementMessage.getTransactionId(), retirementMessage.getClientId());
        ServerSideServerStore cacheStore = stateService.getStore(storeIdentifier);
        if (cacheStore == null) {
          // An operation on a non-existent store should never get out of the client
          throw new LifecycleException("cluster tier does not exist : '" + storeIdentifier + "'");
        }
        if (isEventual()) {
          stateService.getInvalidationTracker(storeIdentifier).trackHashInvalidation(retirementMessage.getKey());
        }
        cacheStore.put(retirementMessage.getKey(), retirementMessage.getChain());
        // Returns the real original result of the operation. We consider that it's always a GET_AND_APPEND since APPEND
        // is unused right now. Other types of messages are not tracked so we don't care that they return the right result
        return getResponse(retirementMessage.getResult());
      case INVALIDATION_COMPLETE:
        if (isEventual()) {
          InvalidationCompleteMessage invalidationCompleteMessage = (InvalidationCompleteMessage) message;
          stateService.getInvalidationTracker(storeIdentifier).untrackHashInvalidation(invalidationCompleteMessage.getKey());
        }
        break;
      case CLEAR_INVALIDATION_COMPLETE:
        if (isEventual()) {
          stateService.getInvalidationTracker(storeIdentifier).setClearInProgress(false);
        }
        break;
      default:
        throw new AssertionError("Unsupported Retirement Message : " + message);
    }
    return null;
  }

  private void invokeServerStoreOperation(ServerStoreOpMessage message) throws ClusterException {
    ServerSideServerStore cacheStore = stateService.getStore(storeIdentifier);
    if (cacheStore == null) {
      // An operation on a non-existent store should never get out of the client
      throw new LifecycleException("cluster tier does not exist : '" + storeIdentifier + "'");
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
        if (isEventual()) {
          stateService.getInvalidationTracker(storeIdentifier).setClearInProgress(true);
        }
        break;
      }
      default:
        throw new AssertionError("Unsupported ServerStore operation : " + message.getMessageType());
    }
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
  public void destroy() {
    LOGGER.info("Destroying cluster tier '{}'", storeIdentifier);
    try {
      stateService.destroyServerStore(storeIdentifier);
    } catch (ClusterException e) {
      throw new AssertionError(e);
    }
    management.close();
    messageHandler.destroy();
  }

  @Override
  public void notifyDestroyed(ClientSourceId sourceId) {
    messageHandler.untrackClient(sourceId);
  }
}
