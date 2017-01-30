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
import org.ehcache.clustered.common.internal.exceptions.InvalidServerStoreConfigurationException;
import org.ehcache.clustered.common.internal.exceptions.InvalidStoreException;
import org.ehcache.clustered.common.internal.exceptions.LifecycleException;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponseFactory;
import org.ehcache.clustered.common.internal.messages.EhcacheMessageType;
import org.ehcache.clustered.common.internal.messages.EhcacheOperationMessage;
import org.ehcache.clustered.common.internal.messages.LifecycleMessage;
import org.ehcache.clustered.common.internal.messages.LifecycleMessage.ValidateServerStore;
import org.ehcache.clustered.common.internal.messages.ReconnectMessage;
import org.ehcache.clustered.common.internal.messages.ReconnectMessageCodec;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.KeyBasedServerStoreOpMessage;
import org.ehcache.clustered.common.internal.messages.StateRepositoryOpMessage;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.ClusteredTierEntityConfiguration;
import org.ehcache.clustered.common.internal.store.Element;
import org.ehcache.clustered.server.CommunicatorServiceConfiguration;
import org.ehcache.clustered.server.KeySegmentMapper;
import org.ehcache.clustered.server.ServerSideServerStore;
import org.ehcache.clustered.server.ServerStoreCompatibility;
import org.ehcache.clustered.server.internal.messages.EhcacheDataSyncMessage;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage.ChainReplicationMessage;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage.ClearInvalidationCompleteMessage;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage.InvalidationCompleteMessage;
import org.ehcache.clustered.server.management.ClusterTierManagement;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Collections.synchronizedList;
import static java.util.Collections.unmodifiableSet;
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
 * ClusteredTierActiveEntity
 */
public class ClusteredTierActiveEntity implements ActiveServerEntity<EhcacheEntityMessage, EhcacheEntityResponse> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusteredTierActiveEntity.class);
  static final String SYNC_DATA_SIZE_PROP = "ehcache.sync.data.size.threshold";
  private static final long DEFAULT_SYNC_DATA_SIZE_THRESHOLD = 4 * 1024 * 1024;

  private final String storeIdentifier;
  private final ServerStoreConfiguration configuration;
  private final EhcacheEntityResponseFactory responseFactory;
  private final ClientCommunicator clientCommunicator;
  private final EhcacheStateService stateService;
  private final IEntityMessenger entityMessenger;
  private final ServerStoreCompatibility storeCompatibility = new ServerStoreCompatibility();
  private final ConcurrentMap<ClientDescriptor, Boolean> connectedClients = new ConcurrentHashMap<>();
  private final AtomicBoolean reconnectComplete = new AtomicBoolean(true);
  private final AtomicInteger invalidationIdGenerator = new AtomicInteger();
  private final ConcurrentMap<Integer, InvalidationHolder> clientsWaitingForInvalidation = new ConcurrentHashMap<>();
  private final InvalidStoreException creationException;
  private final ReconnectMessageCodec reconnectMessageCodec = new ReconnectMessageCodec();
  private final ClusterTierManagement management;

  private volatile List<InvalidationTuple> inflightInvalidations;

  public ClusteredTierActiveEntity(ServiceRegistry registry, ClusteredTierEntityConfiguration entityConfiguration, KeySegmentMapper defaultMapper) throws ConfigurationException {
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
    InvalidStoreException exception = null;
    ServerSideServerStore store;
    try {
      store = stateService.createStore(entityConfiguration.getStoreIdentifier(), entityConfiguration
        .getConfiguration());
    } catch (InvalidStoreException e) {
      exception = e;
      LOGGER.debug("Got exception during creation - most likely failover is happening", e);
      store = stateService.getStore(entityConfiguration.getStoreIdentifier());
    }
    creationException = exception;
    store.setEvictionListener(this::invalidateHashAfterEviction);
    management = new ClusterTierManagement(registry, stateService, true, storeIdentifier);
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
    if (connectedClients.putIfAbsent(clientDescriptor, false) == null) {
      management.clientConnected(clientDescriptor);
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

    if (connectedClients.remove(clientDescriptor) != null) {
      management.clientDisconnected(clientDescriptor);
    }
  }

  @Override
  public EhcacheEntityResponse invoke(ClientDescriptor clientDescriptor, EhcacheEntityMessage message) {
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
    return stateService.getStateRepositoryManager().invoke(message);
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
    ServerStoreConfiguration clientConfiguration = validateServerStore.getStoreConfiguration();

    LOGGER.info("Client {} validating clustered tier '{}'", clientDescriptor, storeIdentifier);
    ServerSideServerStore store = stateService.getStore(storeIdentifier);
    if (store != null) {
      storeCompatibility.verify(store.getStoreConfiguration(), clientConfiguration);
      attachStore(clientDescriptor, storeIdentifier);
    } else {
      throw new InvalidStoreException("Clustered tier '" + storeIdentifier + "' does not exist");
    }
  }

  private void attachStore(ClientDescriptor clientDescriptor, String storeId) {
    connectedClients.replace(clientDescriptor, false, true);

    LOGGER.info("Client {} attached to clustered tier '{}'", clientDescriptor, storeId);

    management.clientValidated(clientDescriptor);
  }

  private EhcacheEntityResponse invokeServerStoreOperation(ClientDescriptor clientDescriptor, ServerStoreOpMessage message) throws ClusterException {
    ServerSideServerStore cacheStore = stateService.getStore(storeIdentifier);
    if (cacheStore == null) {
      // An operation on a non-existent store should never get out of the client
      throw new LifecycleException("Clustered tier does not exist : '" + storeIdentifier + "'");
    }

    Boolean connectedClient = connectedClients.get(clientDescriptor);
    if (connectedClient == null || !connectedClient) {
      // An operation on a store should never happen from client no attached onto that store
      throw new LifecycleException("Client not attached to clustered tier '" + storeIdentifier + "'");
    }

    // This logic totally counts on the fact that invokes will only happen
    // after all handleReconnects are done, else this is flawed.
    if (inflightInvalidations != null) {
      LOGGER.debug("Stalling all operations for cache {} for firing inflight invalidations again.", storeIdentifier);
      inflightInvalidations.forEach(invalidationState -> {
        if (invalidationState.isClearInProgress()) {
          invalidateAll(invalidationState.getClientDescriptor());
        }
        invalidationState.getInvalidationsInProgress()
            .forEach(hashInvalidationToBeResent -> invalidateHashForClient(invalidationState.getClientDescriptor(), hashInvalidationToBeResent));
      });
      inflightInvalidations = null;
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
          final Chain result;
          final Chain newChain;
          try {
            result = cacheStore.getAndAppend(getAndAppendMessage.getKey(), getAndAppendMessage.getPayload());
            newChain = cacheStore.get(getAndAppendMessage.getKey());
          } catch (TimeoutException e) {
            throw new AssertionError("Server side store is not expected to throw timeout exception");
          }
          sendMessageToSelfAndDeferRetirement(getAndAppendMessage, newChain);
          EhcacheEntityResponse response = responseFactory.response(result);
          LOGGER.debug("Send invalidations for key {}", getAndAppendMessage.getKey());
          invalidateHashForClient(clientDescriptor, getAndAppendMessage.getKey());
          return response;
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

    InvalidationHolder invalidationHolder = new InvalidationHolder(originatingClientDescriptor, clientsToInvalidate, storeIdentifier);
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
          boolean isStrong = stateService.getStore(invalidationHolder.cacheId).getStoreConfiguration().getConsistency() == Consistency.STRONG;
          if (key == null) {
            if (isStrong) {
              clientCommunicator.sendNoResponse(invalidationHolder.clientDescriptorWaitingForInvalidation, allInvalidationDone());
              LOGGER.debug("SERVER: notifying originating client that all other clients invalidated all in cache {} from {} (ID {})", invalidationHolder.cacheId, clientDescriptor, invalidationId);
            } else {
              entityMessenger.messageSelf(new ClearInvalidationCompleteMessage());
            }
          } else {
            if (isStrong) {
              clientCommunicator.sendNoResponse(invalidationHolder.clientDescriptorWaitingForInvalidation, hashInvalidationDone(key));
              LOGGER.debug("SERVER: notifying originating client that all other clients invalidated key {} in cache {} from {} (ID {})", key, invalidationHolder.cacheId, clientDescriptor, invalidationId);
            } else {
              entityMessenger.messageSelf(new InvalidationCompleteMessage(key));
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

    InvalidationHolder invalidationHolder = new InvalidationHolder(originatingClientDescriptor, clientsToInvalidate, storeIdentifier, key);
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

  private boolean isMessageDuplicate(EhcacheEntityMessage message) {
    return stateService.getClientMessageTracker().isDuplicate(message.getId(), message.getClientId());
  }

  private void sendMessageToSelfAndDeferRetirement(KeyBasedServerStoreOpMessage message, Chain result) {
    try {
      entityMessenger.messageSelfAndDeferRetirement(message, new ChainReplicationMessage(message.getKey(), result, message.getId(), message.getClientId()));
    } catch (MessageCodecException e) {
      throw new AssertionError("Codec error", e);
    }
  }

  @Override
  public void loadExisting() {
    ServerSideServerStore store = stateService.getStore(storeIdentifier);
    if (store != null) {
      try {
        storeCompatibility.verify(store.getStoreConfiguration(), configuration);
      } catch (InvalidServerStoreConfigurationException e) {
        throw new AssertionError("Configuration mismatch", e);
      }
    } else {
      throw new AssertionError("Clustered tier '" + storeIdentifier + "' does not exist");
    }
    LOGGER.debug("Preparing for handling Inflight Invalidations and independent Passive Evictions in loadExisting");
    inflightInvalidations = synchronizedList(new ArrayList<>());
    addInflightInvalidationsForEventualCaches();
    reconnectComplete.set(false);
    management.init();
  }

  private void addInflightInvalidationsForEventualCaches() {
    InvalidationTracker invalidationTracker = stateService.removeInvalidationtracker(storeIdentifier);
    if (invalidationTracker != null) {
      inflightInvalidations.add(new InvalidationTuple(null, invalidationTracker.getInvalidationMap()
          .keySet(), invalidationTracker.isClearInProgress()));
      invalidationTracker.getInvalidationMap().clear();
    }
  }

  @Override
  public void handleReconnect(ClientDescriptor clientDescriptor, byte[] extendedReconnectData) {
    if (inflightInvalidations == null) {
      throw new AssertionError("Load existing was not invoked before handleReconnect");
    }
    if (!connectedClients.replace(clientDescriptor, false, true)) {
      throw new AssertionError("Client "+ clientDescriptor +" trying to reconnect is not connected to entity");
    }
    ReconnectMessage reconnectMessage = reconnectMessageCodec.decode(extendedReconnectData);
    ServerSideServerStore serverStore = stateService.getStore(storeIdentifier);
    addInflightInvalidationsForStrongCache(clientDescriptor, reconnectMessage, serverStore);

    serverStore.setEvictionListener(this::invalidateHashAfterEviction);
    attachStore(clientDescriptor, storeIdentifier);
    LOGGER.info("Client '{}' successfully reconnected to newly promoted ACTIVE after failover.", clientDescriptor);

    management.clientReconnected(clientDescriptor);
  }

  private void addInflightInvalidationsForStrongCache(ClientDescriptor clientDescriptor, ReconnectMessage reconnectMessage, ServerSideServerStore serverStore) {
    if (serverStore.getStoreConfiguration().getConsistency().equals(Consistency.STRONG)) {
      Set<Long> invalidationsInProgress = reconnectMessage.getInvalidationsInProgress(storeIdentifier);
      LOGGER.debug("Number of Inflight Invalidations from client ID {} for cache {} is {}.", reconnectMessage.getClientId(), storeIdentifier, invalidationsInProgress
          .size());
      inflightInvalidations.add(new InvalidationTuple(clientDescriptor, invalidationsInProgress, reconnectMessage.isClearInProgress(storeIdentifier)));
    }
  }

  @Override
  public void synchronizeKeyToPassive(PassiveSynchronizationChannel<EhcacheEntityMessage> syncChannel, int concurrencyKey) {
    LOGGER.info("Sync started for concurrency key {}.", concurrencyKey);
    Long dataSizeThreshold = Long.getLong(SYNC_DATA_SIZE_PROP, DEFAULT_SYNC_DATA_SIZE_THRESHOLD);
    AtomicLong size = new AtomicLong(0);
    ServerSideServerStore store = stateService.getStore(storeIdentifier);
    final AtomicReference<Map<Long, Chain>> mappingsToSend = new AtomicReference<>(new HashMap<>());
    store.getSegmentKeySets().get(concurrencyKey - DEFAULT_KEY)
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
    LOGGER.info("Sync complete for concurrency key {}.", concurrencyKey);
  }

  @Override
  public void createNew() {
    if (creationException != null) {
      throw new AssertionError("Store conflict", creationException);
    }
    management.init();
  }

  @Override
  public void destroy() {
    LOGGER.info("Destroying clustered tier '{}'", storeIdentifier);
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
      .filter(Map.Entry::getValue)
      .map(Map.Entry::getKey)
      .collect(Collectors.toSet()));
  }

  ConcurrentMap<Integer, InvalidationHolder> getClientsWaitingForInvalidation() {
    return clientsWaitingForInvalidation;
  }

  static class InvalidationHolder {
    final ClientDescriptor clientDescriptorWaitingForInvalidation;
    final Set<ClientDescriptor> clientsHavingToInvalidate;
    final String cacheId;
    final Long key;

    InvalidationHolder(ClientDescriptor clientDescriptorWaitingForInvalidation, Set<ClientDescriptor> clientsHavingToInvalidate, String cacheId, Long key) {
      this.clientDescriptorWaitingForInvalidation = clientDescriptorWaitingForInvalidation;
      this.clientsHavingToInvalidate = clientsHavingToInvalidate;
      this.cacheId = cacheId;
      this.key = key;
    }

    InvalidationHolder(ClientDescriptor clientDescriptorWaitingForInvalidation, Set<ClientDescriptor> clientsHavingToInvalidate, String cacheId) {
      this.clientDescriptorWaitingForInvalidation = clientDescriptorWaitingForInvalidation;
      this.clientsHavingToInvalidate = clientsHavingToInvalidate;
      this.cacheId = cacheId;
      this.key = null;
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
