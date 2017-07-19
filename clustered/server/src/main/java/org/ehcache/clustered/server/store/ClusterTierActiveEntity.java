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
import org.ehcache.clustered.common.internal.exceptions.InvalidOperationException;
import org.ehcache.clustered.common.internal.exceptions.InvalidStoreException;
import org.ehcache.clustered.common.internal.exceptions.LifecycleException;
import org.ehcache.clustered.common.internal.messages.ClusterTierReconnectMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponseFactory;
import org.ehcache.clustered.common.internal.messages.EhcacheMessageType;
import org.ehcache.clustered.common.internal.messages.EhcacheOperationMessage;
import org.ehcache.clustered.common.internal.messages.LifecycleMessage;
import org.ehcache.clustered.common.internal.messages.LifecycleMessage.ValidateServerStore;
import org.ehcache.clustered.common.internal.messages.ReconnectMessageCodec;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.KeyBasedServerStoreOpMessage;
import org.ehcache.clustered.common.internal.messages.StateRepositoryOpMessage;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.ClusterTierEntityConfiguration;
import org.ehcache.clustered.common.internal.store.Element;
import org.ehcache.clustered.server.CommunicatorServiceConfiguration;
import org.ehcache.clustered.server.KeySegmentMapper;
import org.ehcache.clustered.server.ServerSideServerStore;
import org.ehcache.clustered.server.ServerStoreCompatibility;
import org.ehcache.clustered.server.internal.messages.EhcacheDataSyncMessage;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage.ChainReplicationMessage;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage.ClearInvalidationCompleteMessage;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage.InvalidationCompleteMessage;
import org.ehcache.clustered.server.management.ClusterTierClientState;
import org.ehcache.clustered.server.management.ClusterTierManagement;
import org.ehcache.clustered.server.state.ClientMessageTracker;
import org.ehcache.clustered.server.state.EhcacheStateService;
import org.ehcache.clustered.server.state.InvalidationTracker;
import org.ehcache.clustered.server.state.config.EhcacheStoreStateServiceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.ActiveServerEntity;
import org.terracotta.entity.BasicServiceConfiguration;
import org.terracotta.entity.ClientCommunicator;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.IEntityMessenger;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.entity.PassiveSynchronizationChannel;
import org.terracotta.entity.ServiceRegistry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toSet;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.allInvalidationDone;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.clientInvalidateAll;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.clientInvalidateHash;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.hashInvalidationDone;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.serverInvalidateHash;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.isLifecycleMessage;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.isStateRepoOperationMessage;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.isStoreOperationMessage;
import static org.ehcache.clustered.server.ConcurrencyStrategies.DEFAULT_KEY;

/**
 * ClusterTierActiveEntity
 */
public class ClusterTierActiveEntity implements ActiveServerEntity<EhcacheEntityMessage, EhcacheEntityResponse> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTierActiveEntity.class);
  static final String SYNC_DATA_SIZE_PROP = "ehcache.sync.data.size.threshold";
  private static final long DEFAULT_SYNC_DATA_SIZE_THRESHOLD = 4 * 1024 * 1024;

  private final String storeIdentifier;
  private final ServerStoreConfiguration configuration;
  private final EhcacheEntityResponseFactory responseFactory;
  private final ClientCommunicator clientCommunicator;
  private final EhcacheStateService stateService;
  private final IEntityMessenger entityMessenger;
  private final ServerStoreCompatibility storeCompatibility = new ServerStoreCompatibility();
  private final ConcurrentMap<ClientDescriptor, ClusterTierClientState> connectedClients = new ConcurrentHashMap<>();
  private final AtomicBoolean reconnectComplete = new AtomicBoolean(true);
  private final AtomicInteger invalidationIdGenerator = new AtomicInteger();
  private final ConcurrentMap<Integer, InvalidationHolder> clientsWaitingForInvalidation = new ConcurrentHashMap<>();
  private final ReconnectMessageCodec reconnectMessageCodec = new ReconnectMessageCodec();
  private final ClusterTierManagement management;

  private final Object inflightInvalidationsMutex = new Object();
  private volatile List<InvalidationTuple> inflightInvalidations;

  public ClusterTierActiveEntity(ServiceRegistry registry, ClusterTierEntityConfiguration entityConfiguration, KeySegmentMapper defaultMapper) throws ConfigurationException {
    if (entityConfiguration == null) {
      throw new ConfigurationException("ClusteredStoreEntityConfiguration cannot be null");
    }
    storeIdentifier = entityConfiguration.getStoreIdentifier();
    configuration = entityConfiguration.getConfiguration();
    responseFactory = new EhcacheEntityResponseFactory();
    clientCommunicator = registry.getService(new CommunicatorServiceConfiguration());
    stateService = registry.getService(new EhcacheStoreStateServiceConfig(entityConfiguration.getManagerIdentifier(), defaultMapper));
    entityMessenger = registry.getService(new BasicServiceConfiguration<>(IEntityMessenger.class));
    if (entityMessenger == null) {
      throw new AssertionError("Server failed to retrieve IEntityMessenger service.");
    }
    management = new ClusterTierManagement(registry, stateService, true, storeIdentifier, entityConfiguration.getManagerIdentifier());
  }

  @Override
  public void createNew() throws ConfigurationException {
    ServerSideServerStore store = stateService.createStore(storeIdentifier, configuration, true);
    store.setEvictionListener(this::invalidateHashAfterEviction);
    management.init();
  }

  List<InvalidationTuple> getInflightInvalidations() {
    return this.inflightInvalidations;
  }

  @Override
  public void loadExisting() {
    inflightInvalidations = new ArrayList<>();
    if (!isStrong()) {
      LOGGER.debug("Preparing for handling inflight invalidations");
      addInflightInvalidationsForEventualCaches();
    }
    stateService.loadStore(storeIdentifier, configuration).setEvictionListener(this::invalidateHashAfterEviction);
    reconnectComplete.set(false);
    management.init();
  }

  private void invalidateHashAfterEviction(long key) {
    Set<ClientDescriptor> clientsToInvalidate = Collections.newSetFromMap(new ConcurrentHashMap<ClientDescriptor, Boolean>());
    clientsToInvalidate.addAll(getAttachedClients());

    for (ClientDescriptor clientDescriptorThatHasToInvalidate : clientsToInvalidate) {
      LOGGER.debug("SERVER: eviction happened; asking client {} to invalidate hash {} from cache {}", clientDescriptorThatHasToInvalidate, key, storeIdentifier);
      try {
        clientCommunicator.sendNoResponse(clientDescriptorThatHasToInvalidate, serverInvalidateHash(key));
      } catch (MessageCodecException mce) {
        throw new AssertionError("Codec error", mce);
      }
    }
  }

  @Override
  public void connected(ClientDescriptor clientDescriptor) {
    ClusterTierClientState clientState = new ClusterTierClientState(storeIdentifier, false);
    if (connectedClients.putIfAbsent(clientDescriptor, clientState) == null) {
      management.clientConnected(clientDescriptor, clientState);
    }
  }

  @Override
  public void disconnected(ClientDescriptor clientDescriptor) {
    // cleanup all invalidation requests waiting for a ack from this client
    Set<Integer> invalidationIds = clientsWaitingForInvalidation.keySet();
    for (Integer invalidationId : invalidationIds) {
      clientInvalidated(clientDescriptor, invalidationId);
    }

    // cleanup all invalidation request this client was blocking on
    Iterator<Map.Entry<Integer, InvalidationHolder>> it = clientsWaitingForInvalidation.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<Integer, InvalidationHolder> next = it.next();
      ClientDescriptor clientDescriptorWaitingForInvalidation = next.getValue().clientDescriptorWaitingForInvalidation;
      if (clientDescriptorWaitingForInvalidation != null && clientDescriptorWaitingForInvalidation.equals(clientDescriptor)) {
        it.remove();
      }
    }

    ClusterTierClientState clientState = connectedClients.remove(clientDescriptor);
    if (clientState != null) {
      management.clientDisconnected(clientDescriptor, clientState);

      UUID clientId = clientState.getClientIdentifier();
      if (clientId != null) {
        try {
          entityMessenger.messageSelf(new PassiveReplicationMessage.ClientIDTrackerMessage(clientId));
        } catch (MessageCodecException mce) {
          throw new AssertionError("Codec error", mce);
        }
        ClientMessageTracker messageTracker = stateService.getClientMessageTracker(storeIdentifier);
        if (messageTracker != null) {
          messageTracker.remove(clientId);
        }
      }
    }
  }

  @Override
  public EhcacheEntityResponse invoke(ClientDescriptor clientDescriptor, EhcacheEntityMessage message) {
    clearClientTrackedAtReconnectComplete();

    try {
      if (message instanceof EhcacheOperationMessage) {
        EhcacheOperationMessage operationMessage = (EhcacheOperationMessage) message;
        EhcacheMessageType messageType = operationMessage.getMessageType();
        if (isStoreOperationMessage(messageType)) {
          return invokeServerStoreOperation(clientDescriptor, (ServerStoreOpMessage) message);
        } else if (isLifecycleMessage(messageType)) {
          return invokeLifeCycleOperation(clientDescriptor, (LifecycleMessage) message);
        } else if (isStateRepoOperationMessage(messageType)) {
          return invokeStateRepositoryOperation((StateRepositoryOpMessage) message);
        }
      }
      throw new AssertionError("Unsupported message : " + message.getClass());
    } catch (ClusterException e) {
      return responseFactory.failure(e);
    } catch (Exception e) {
      LOGGER.error("Unexpected exception raised during operation: " + message, e);
      return responseFactory.failure(new InvalidOperationException(e));
    }
  }

  private EhcacheEntityResponse invokeStateRepositoryOperation(StateRepositoryOpMessage message) throws ClusterException {
    EhcacheEntityResponse response = stateService.getStateRepositoryManager().invoke(message);
    trackMessage(message);
    return response;
  }

  private EhcacheEntityResponse invokeLifeCycleOperation(ClientDescriptor clientDescriptor, LifecycleMessage message) throws ClusterException {
    switch (message.getMessageType()) {
      case VALIDATE_SERVER_STORE:
        validateServerStore(clientDescriptor, (ValidateServerStore) message);
        break;
      default:
        throw new AssertionError("Unsupported LifeCycle operation " + message);
    }
    return responseFactory.success();
  }

  private void validateServerStore(ClientDescriptor clientDescriptor, ValidateServerStore validateServerStore) throws ClusterException {
    ClusterTierClientState clientState = connectedClients.get(clientDescriptor);
    if (clientState == null) {
      throw new AssertionError("Client "+ clientDescriptor +" trying to reconnect is not connected to entity");
    }
    if (clientState.isAttached()) {
      throw new LifecycleException("Client : " + clientDescriptor + " is already being tracked with Client ID : " + clientState.getClientIdentifier());
    }

    ServerStoreConfiguration clientConfiguration = validateServerStore.getStoreConfiguration();
    LOGGER.info("Client {} validating cluster tier '{}'", clientDescriptor, storeIdentifier);
    ServerSideServerStore store = stateService.getStore(storeIdentifier);
    if (store != null) {
      storeCompatibility.verify(store.getStoreConfiguration(), clientConfiguration);
      attachStore(clientDescriptor, validateServerStore.getClientId());
      management.clientValidated(clientDescriptor, connectedClients.get(clientDescriptor));
    } else {
      throw new InvalidStoreException("cluster tier '" + storeIdentifier + "' does not exist");
    }
  }

  private void attachStore(ClientDescriptor clientDescriptor, UUID clientId) {
    ClusterTierClientState clientState = new ClusterTierClientState(storeIdentifier, true, clientId);
    connectedClients.replace(clientDescriptor, clientState);
    LOGGER.info("Client: {} with client ID: {} attached to cluster tier '{}'", clientDescriptor, clientId, storeIdentifier);
  }

  private EhcacheEntityResponse invokeServerStoreOperation(ClientDescriptor clientDescriptor, ServerStoreOpMessage message) throws ClusterException {
    ServerSideServerStore cacheStore = stateService.getStore(storeIdentifier);
    if (cacheStore == null) {
      // An operation on a non-existent store should never get out of the client
      throw new LifecycleException("cluster tier does not exist : '" + storeIdentifier + "'");
    }

    ClusterTierClientState clientState = connectedClients.get(clientDescriptor);
    if (clientState == null || !clientState.isAttached()) {
      // An operation on a store should never happen from client not attached onto this store
      throw new LifecycleException("Client not attached to cluster tier '" + storeIdentifier + "'");
    }

    if (inflightInvalidations != null) {
      synchronized (inflightInvalidationsMutex) {
        // This logic totally counts on the fact that invokes will only happen
        // after all handleReconnects are done, else this is flawed.
        if (inflightInvalidations != null) {
          List<InvalidationTuple> tmpInflightInvalidations = this.inflightInvalidations;
          this.inflightInvalidations = null;
          LOGGER.debug("Stalling all operations for cluster tier {} for firing inflight invalidations again.", storeIdentifier);
          tmpInflightInvalidations.forEach(invalidationState -> {
            if (invalidationState.isClearInProgress()) {
              invalidateAll(invalidationState.getClientDescriptor());
            }
            invalidationState.getInvalidationsInProgress()
                .forEach(hashInvalidationToBeResent -> invalidateHashForClient(invalidationState.getClientDescriptor(), hashInvalidationToBeResent));
          });
        }
      }
    }

    switch (message.getMessageType()) {
      case GET_STORE: {
        ServerStoreOpMessage.GetMessage getMessage = (ServerStoreOpMessage.GetMessage) message;
        try {
          return responseFactory.response(cacheStore.get(getMessage.getKey()));
        } catch (TimeoutException e) {
          throw new AssertionError("Server side store is not expected to throw timeout exception");
        }
      }
      case APPEND: {
        if (!isMessageDuplicate(message)) {
          ServerStoreOpMessage.AppendMessage appendMessage = (ServerStoreOpMessage.AppendMessage)message;

          trackMessage(message);
          InvalidationTracker invalidationTracker = stateService.getInvalidationTracker(storeIdentifier);
          if (invalidationTracker != null) {
            invalidationTracker.trackHashInvalidation(appendMessage.getKey());
          }

          final Chain newChain;
          try {
            cacheStore.append(appendMessage.getKey(), appendMessage.getPayload());
            newChain = cacheStore.get(appendMessage.getKey());
          } catch (TimeoutException e) {
            throw new AssertionError("Server side store is not expected to throw timeout exception");
          }
          sendMessageToSelfAndDeferRetirement(appendMessage, newChain);
          invalidateHashForClient(clientDescriptor, appendMessage.getKey());
        }
        return responseFactory.success();
      }
      case GET_AND_APPEND: {
        ServerStoreOpMessage.GetAndAppendMessage getAndAppendMessage = (ServerStoreOpMessage.GetAndAppendMessage)message;
        LOGGER.trace("Message {} : GET_AND_APPEND on key {} from client {}", message, getAndAppendMessage.getKey(), getAndAppendMessage.getClientId());
        if (!isMessageDuplicate(message)) {
          LOGGER.trace("Message {} : is not duplicate", message);

          trackMessage(message);
          InvalidationTracker invalidationTracker = stateService.getInvalidationTracker(storeIdentifier);
          if (invalidationTracker != null) {
            invalidationTracker.trackHashInvalidation(getAndAppendMessage.getKey());
          }

          final Chain result;
          final Chain newChain;
          try {
            result = cacheStore.getAndAppend(getAndAppendMessage.getKey(), getAndAppendMessage.getPayload());
            newChain = cacheStore.get(getAndAppendMessage.getKey());
          } catch (TimeoutException e) {
            throw new AssertionError("Server side store is not expected to throw timeout exception");
          }
          sendMessageToSelfAndDeferRetirement(getAndAppendMessage, newChain);
          LOGGER.debug("Send invalidations for key {}", getAndAppendMessage.getKey());
          invalidateHashForClient(clientDescriptor, getAndAppendMessage.getKey());
          return responseFactory.response(result);
        }
        try {
          return responseFactory.response(cacheStore.get(getAndAppendMessage.getKey()));
        } catch (TimeoutException e) {
          throw new AssertionError("Server side store is not expected to throw timeout exception");
        }
      }
      case REPLACE: {
        ServerStoreOpMessage.ReplaceAtHeadMessage replaceAtHeadMessage = (ServerStoreOpMessage.ReplaceAtHeadMessage) message;
        cacheStore.replaceAtHead(replaceAtHeadMessage.getKey(), replaceAtHeadMessage.getExpect(), replaceAtHeadMessage.getUpdate());
        return responseFactory.success();
      }
      case CLIENT_INVALIDATION_ACK: {
        ServerStoreOpMessage.ClientInvalidationAck clientInvalidationAck = (ServerStoreOpMessage.ClientInvalidationAck) message;
        int invalidationId = clientInvalidationAck.getInvalidationId();
        LOGGER.debug("SERVER: got notification of invalidation ack in cache {} from {} (ID {})", storeIdentifier, clientDescriptor, invalidationId);
        clientInvalidated(clientDescriptor, invalidationId);
        return responseFactory.success();
      }
      case CLIENT_INVALIDATION_ALL_ACK: {
        ServerStoreOpMessage.ClientInvalidationAllAck clientInvalidationAllAck = (ServerStoreOpMessage.ClientInvalidationAllAck) message;
        int invalidationId = clientInvalidationAllAck.getInvalidationId();
        LOGGER.debug("SERVER: got notification of invalidation ack in cache {} from {} (ID {})", storeIdentifier, clientDescriptor, invalidationId);
        clientInvalidated(clientDescriptor, invalidationId);
        return responseFactory.success();
      }
      case CLEAR: {
        if (!isMessageDuplicate(message)) {
          LOGGER.info("Clearing cluster tier {}", storeIdentifier);
          try {
            cacheStore.clear();
          } catch (TimeoutException e) {
            throw new AssertionError("Server side store is not expected to throw timeout exception");
          }
          trackMessage(message);

          InvalidationTracker invalidationTracker = stateService.getInvalidationTracker(storeIdentifier);
          if (invalidationTracker != null) {
            invalidationTracker.setClearInProgress(true);
          }
          invalidateAll(clientDescriptor);
        }
        return responseFactory.success();
      }
      default:
        throw new AssertionError("Unsupported ServerStore operation : " + message);
    }
  }

  private void invalidateAll(ClientDescriptor originatingClientDescriptor) {
    int invalidationId = invalidationIdGenerator.getAndIncrement();
    Set<ClientDescriptor> clientsToInvalidate = Collections.newSetFromMap(new ConcurrentHashMap<ClientDescriptor, Boolean>());
    clientsToInvalidate.addAll(getAttachedClients());
    if (originatingClientDescriptor != null) {
      clientsToInvalidate.remove(originatingClientDescriptor);
    }

    InvalidationHolder invalidationHolder = new InvalidationHolder(originatingClientDescriptor, clientsToInvalidate);
    clientsWaitingForInvalidation.put(invalidationId, invalidationHolder);

    LOGGER.debug("SERVER: requesting {} client(s) invalidation of all in cache {} (ID {})", clientsToInvalidate.size(), storeIdentifier, invalidationId);
    for (ClientDescriptor clientDescriptorThatHasToInvalidate : clientsToInvalidate) {
      LOGGER.debug("SERVER: asking client {} to invalidate all from cache {} (ID {})", clientDescriptorThatHasToInvalidate, storeIdentifier, invalidationId);
      try {
        clientCommunicator.sendNoResponse(clientDescriptorThatHasToInvalidate, clientInvalidateAll(invalidationId));
      } catch (MessageCodecException mce) {
        throw new AssertionError("Codec error", mce);
      }
    }

    if (clientsToInvalidate.isEmpty()) {
      clientInvalidated(invalidationHolder.clientDescriptorWaitingForInvalidation, invalidationId);
    }
  }

  private void clientInvalidated(ClientDescriptor clientDescriptor, int invalidationId) {
    InvalidationHolder invalidationHolder = clientsWaitingForInvalidation.get(invalidationId);

    if (invalidationHolder == null) { // Happens when client is re-sending/sending invalidations for which server has lost track since fail-over happened.
      LOGGER.debug("Ignoring invalidation from client {} " + clientDescriptor);
      return;
    }

    invalidationHolder.clientsHavingToInvalidate.remove(clientDescriptor);
    if (invalidationHolder.clientsHavingToInvalidate.isEmpty()) {
      if (clientsWaitingForInvalidation.remove(invalidationId) != null) {
        try {
          Long key = invalidationHolder.key;
          if (key == null) {
            if (isStrong()) {
              clientCommunicator.sendNoResponse(invalidationHolder.clientDescriptorWaitingForInvalidation, allInvalidationDone());
              LOGGER.debug("SERVER: notifying originating client that all other clients invalidated all in cache {} from {} (ID {})", storeIdentifier, clientDescriptor, invalidationId);
            } else {
              entityMessenger.messageSelf(new ClearInvalidationCompleteMessage());

              InvalidationTracker invalidationTracker = stateService.getInvalidationTracker(storeIdentifier);
              if (invalidationTracker != null) {
                invalidationTracker.setClearInProgress(false);
              }

            }
          } else {
            if (isStrong()) {
              clientCommunicator.sendNoResponse(invalidationHolder.clientDescriptorWaitingForInvalidation, hashInvalidationDone(key));
              LOGGER.debug("SERVER: notifying originating client that all other clients invalidated key {} in cache {} from {} (ID {})", key, storeIdentifier, clientDescriptor, invalidationId);
            } else {
              entityMessenger.messageSelf(new InvalidationCompleteMessage(key));

              InvalidationTracker invalidationTracker = stateService.getInvalidationTracker(storeIdentifier);
              if (invalidationTracker != null) {
                invalidationTracker.untrackHashInvalidation(key);
              }
            }
          }
        } catch (MessageCodecException mce) {
          throw new AssertionError("Codec error", mce);
        }
      }
    }
  }

  private void invalidateHashForClient(ClientDescriptor originatingClientDescriptor, long key) {
    int invalidationId = invalidationIdGenerator.getAndIncrement();
    Set<ClientDescriptor> clientsToInvalidate = Collections.newSetFromMap(new ConcurrentHashMap<ClientDescriptor, Boolean>());
    clientsToInvalidate.addAll(getAttachedClients());
    if (originatingClientDescriptor != null) {
      clientsToInvalidate.remove(originatingClientDescriptor);
    }

    InvalidationHolder invalidationHolder = new InvalidationHolder(originatingClientDescriptor, clientsToInvalidate, key);
    clientsWaitingForInvalidation.put(invalidationId, invalidationHolder);

    LOGGER.debug("SERVER: requesting {} client(s) invalidation of hash {} in cache {} (ID {})", clientsToInvalidate.size(), key, storeIdentifier, invalidationId);
    for (ClientDescriptor clientDescriptorThatHasToInvalidate : clientsToInvalidate) {
      LOGGER.debug("SERVER: asking client {} to invalidate hash {} from cache {} (ID {})", clientDescriptorThatHasToInvalidate, key, storeIdentifier, invalidationId);
      try {
        clientCommunicator.sendNoResponse(clientDescriptorThatHasToInvalidate, clientInvalidateHash(key, invalidationId));
      } catch (MessageCodecException mce) {
        throw new AssertionError("Codec error", mce);
      }
    }

    if (clientsToInvalidate.isEmpty()) {
      clientInvalidated(invalidationHolder.clientDescriptorWaitingForInvalidation, invalidationId);
    }
  }

  private void trackMessage(EhcacheOperationMessage message) {
    ClientMessageTracker clientMessageTracker = stateService.getClientMessageTracker(storeIdentifier);
    if (clientMessageTracker != null) {
      clientMessageTracker.applied(message.getId(), message.getClientId());
    }
  }

  private boolean isMessageDuplicate(EhcacheEntityMessage message) {
    ClientMessageTracker clientMessageTracker = stateService.getClientMessageTracker(storeIdentifier);
    if (clientMessageTracker != null) {
      return clientMessageTracker.isDuplicate(message.getId(), message.getClientId());
    }
    return false;
  }

  private void sendMessageToSelfAndDeferRetirement(KeyBasedServerStoreOpMessage message, Chain result) {
    try {
      entityMessenger.messageSelfAndDeferRetirement(message, new ChainReplicationMessage(message.getKey(), result, message.getId(), message.getClientId()));
    } catch (MessageCodecException e) {
      throw new AssertionError("Codec error", e);
    }
  }

  private void addInflightInvalidationsForEventualCaches() {
    InvalidationTracker invalidationTracker = stateService.getInvalidationTracker(storeIdentifier);
    if (invalidationTracker != null) {
      inflightInvalidations.add(new InvalidationTuple(null, invalidationTracker.getTrackedKeys(), invalidationTracker.isClearInProgress()));
      invalidationTracker.clear();
    }
  }

  @Override
  public void handleReconnect(ClientDescriptor clientDescriptor, byte[] extendedReconnectData) {
    if (inflightInvalidations == null) {
      throw new AssertionError("Load existing was not invoked before handleReconnect");
    }

    if (connectedClients.get(clientDescriptor) == null) {
      throw new AssertionError("Client "+ clientDescriptor +" trying to reconnect is not connected to entity");
    }
    ClusterTierReconnectMessage reconnectMessage = reconnectMessageCodec.decode(extendedReconnectData);
    ServerSideServerStore serverStore = stateService.getStore(storeIdentifier);
    addInflightInvalidationsForStrongCache(clientDescriptor, reconnectMessage, serverStore);

    attachStore(clientDescriptor, reconnectMessage.getClientId());
    LOGGER.info("Client '{}' successfully reconnected to newly promoted ACTIVE after failover.", clientDescriptor);

    management.clientReconnected(clientDescriptor, connectedClients.get(clientDescriptor));
  }

  private void clearClientTrackedAtReconnectComplete() {
    if (!reconnectComplete.get()) {
      if (reconnectComplete.compareAndSet(false, true)) {
        ClientMessageTracker clientMessageTracker = stateService.getClientMessageTracker(storeIdentifier);
        if (clientMessageTracker != null) {
          clientMessageTracker.reconcileTrackedClients(getTrackedClients().collect(toSet()));
        }
      }
    }
  }

  private Stream<UUID> getTrackedClients() {
    return connectedClients.entrySet().stream()
      .filter(entry -> entry.getValue().isAttached())
      .map(entry -> entry.getValue().getClientIdentifier());
  }

  private void addInflightInvalidationsForStrongCache(ClientDescriptor clientDescriptor, ClusterTierReconnectMessage reconnectMessage, ServerSideServerStore serverStore) {
    if (serverStore.getStoreConfiguration().getConsistency().equals(Consistency.STRONG)) {
      Set<Long> invalidationsInProgress = reconnectMessage.getInvalidationsInProgress();
      LOGGER.debug("Number of Inflight Invalidations from client ID {} for cache {} is {}.", reconnectMessage.getClientId(), storeIdentifier, invalidationsInProgress
          .size());
      inflightInvalidations.add(new InvalidationTuple(clientDescriptor, invalidationsInProgress, reconnectMessage.isClearInProgress()));
    }
  }

  @Override
  public void synchronizeKeyToPassive(PassiveSynchronizationChannel<EhcacheEntityMessage> syncChannel, int concurrencyKey) {
    LOGGER.info("Sync started for concurrency key {}.", concurrencyKey);
    if (concurrencyKey == DEFAULT_KEY) {
      stateService.getStateRepositoryManager().syncMessageFor(storeIdentifier).forEach(syncChannel::synchronizeToPassive);
    } else {
      int segmentId = concurrencyKey - DEFAULT_KEY - 1;
      Long dataSizeThreshold = Long.getLong(SYNC_DATA_SIZE_PROP, DEFAULT_SYNC_DATA_SIZE_THRESHOLD);
      AtomicLong size = new AtomicLong(0);
      ServerSideServerStore store = stateService.getStore(storeIdentifier);
      final AtomicReference<Map<Long, Chain>> mappingsToSend = new AtomicReference<>(new HashMap<>());
      store.getSegmentKeySets().get(segmentId)
        .forEach(key -> {
          final Chain chain;
          try {
            chain = store.get(key);
          } catch (TimeoutException e) {
            throw new AssertionError("Server side store is not expected to throw timeout exception");
          }
          for (Element element : chain) {
            size.addAndGet(element.getPayload().remaining());
          }
          mappingsToSend.get().put(key, chain);
          if (size.get() > dataSizeThreshold) {
            syncChannel.synchronizeToPassive(new EhcacheDataSyncMessage(mappingsToSend.get()));
            mappingsToSend.set(new HashMap<>());
            size.set(0);
          }
        });
      if (!mappingsToSend.get().isEmpty()) {
        syncChannel.synchronizeToPassive(new EhcacheDataSyncMessage(mappingsToSend.get()));
        mappingsToSend.set(new HashMap<>());
        size.set(0);
      }
    }
    LOGGER.info("Sync complete for concurrency key {}.", concurrencyKey);
  }

  @Override
  public void destroy() {
    LOGGER.info("Destroying cluster tier '{}'", storeIdentifier);
    try {
      stateService.destroyServerStore(storeIdentifier);
    } catch (ClusterException e) {
      LOGGER.error("Failed to destroy server store - does not exist", e);
    }
  }

  Set<ClientDescriptor> getConnectedClients() {
    return unmodifiableSet(connectedClients.keySet());
  }

  Set<ClientDescriptor> getAttachedClients() {
    return unmodifiableSet(connectedClients.entrySet().stream()
      .filter(entry -> entry.getValue().isAttached())
      .map(Map.Entry::getKey)
      .collect(toSet()));
  }

  ConcurrentMap<Integer, InvalidationHolder> getClientsWaitingForInvalidation() {
    return clientsWaitingForInvalidation;
  }

  private boolean isStrong() {
    return this.configuration.getConsistency() == Consistency.STRONG;
  }

  static class InvalidationHolder {
    final ClientDescriptor clientDescriptorWaitingForInvalidation;
    final Set<ClientDescriptor> clientsHavingToInvalidate;
    final Long key;

    InvalidationHolder(ClientDescriptor clientDescriptorWaitingForInvalidation, Set<ClientDescriptor> clientsHavingToInvalidate, Long key) {
      this.clientDescriptorWaitingForInvalidation = clientDescriptorWaitingForInvalidation;
      this.clientsHavingToInvalidate = clientsHavingToInvalidate;
      this.key = key;
    }

    InvalidationHolder(ClientDescriptor clientDescriptorWaitingForInvalidation, Set<ClientDescriptor> clientsHavingToInvalidate) {
      this(clientDescriptorWaitingForInvalidation, clientsHavingToInvalidate, null);
    }
  }

  private static class InvalidationTuple {
    private final ClientDescriptor clientDescriptor;
    private final Set<Long> invalidationsInProgress;
    private final boolean isClearInProgress;

    InvalidationTuple(ClientDescriptor clientDescriptor, Set<Long> invalidationsInProgress, boolean isClearInProgress) {
      this.clientDescriptor = clientDescriptor;
      this.invalidationsInProgress = invalidationsInProgress;
      this.isClearInProgress = isClearInProgress;
    }

    ClientDescriptor getClientDescriptor() {
      return clientDescriptor;
    }

    Set<Long> getInvalidationsInProgress() {
      return invalidationsInProgress;
    }

    boolean isClearInProgress() {
      return isClearInProgress;
    }
  }
}
