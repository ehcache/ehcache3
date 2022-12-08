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
import org.ehcache.clustered.common.internal.messages.EhcacheMessageType;
import org.ehcache.clustered.server.internal.messages.EhcacheMessageTrackerCatchup;
import org.ehcache.clustered.common.internal.messages.EhcacheOperationMessage;
import org.ehcache.clustered.common.internal.messages.LifecycleMessage;
import org.ehcache.clustered.common.internal.messages.LifecycleMessage.ValidateServerStore;
import org.ehcache.clustered.common.internal.messages.ReconnectMessageCodec;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.AppendMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.ClientInvalidationAck;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.ClientInvalidationAllAck;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.EnableEventListenerMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.GetMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.IteratorAdvanceMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.IteratorCloseMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.IteratorOpenMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.KeyBasedServerStoreOpMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.LockMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.ReplaceAtHeadMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.UnlockMessage;
import org.ehcache.clustered.common.internal.messages.StateRepositoryOpMessage;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.ClusterTierEntityConfiguration;
import org.ehcache.clustered.common.internal.store.Element;
import org.ehcache.clustered.server.CommunicatorServiceConfiguration;
import org.ehcache.clustered.server.KeySegmentMapper;
import org.ehcache.clustered.server.ServerSideServerStore;
import org.ehcache.clustered.server.ServerStoreCompatibility;
import org.ehcache.clustered.server.ServerStoreEventListener;
import org.ehcache.clustered.server.internal.messages.EhcacheDataSyncMessage;
import org.ehcache.clustered.server.internal.messages.EhcacheMessageTrackerMessage;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage.ClearInvalidationCompleteMessage;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage.InvalidationCompleteMessage;
import org.ehcache.clustered.server.management.ClusterTierManagement;
import org.ehcache.clustered.server.offheap.InternalChain;
import org.ehcache.clustered.server.state.EhcacheStateContext;
import org.ehcache.clustered.server.state.EhcacheStateService;
import org.ehcache.clustered.server.state.InvalidationTracker;
import org.ehcache.clustered.server.state.config.EhcacheStoreStateServiceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.client.message.tracker.OOOMessageHandler;
import org.terracotta.client.message.tracker.OOOMessageHandlerConfiguration;
import org.terracotta.entity.ActiveInvokeContext;
import org.terracotta.entity.ActiveServerEntity;
import org.terracotta.entity.BasicServiceConfiguration;
import org.terracotta.entity.ClientCommunicator;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.ClientSourceId;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.EntityUserException;
import org.terracotta.entity.IEntityMessenger;
import org.terracotta.entity.InvokeContext;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.entity.PassiveSynchronizationChannel;
import org.terracotta.entity.ServiceException;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.entity.StateDumpCollector;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.allInvalidationDone;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.clientInvalidateAll;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.clientInvalidateHash;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.failure;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.getResponse;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.hashInvalidationDone;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.iteratorBatchResponse;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.lockFailure;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.lockSuccess;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.resolveRequest;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.serverAppend;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.serverInvalidateHash;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.success;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.isLifecycleMessage;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.isStateRepoOperationMessage;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.isStoreOperationMessage;
import static org.ehcache.clustered.server.ConcurrencyStrategies.DEFAULT_KEY;
import static org.ehcache.clustered.server.ConcurrencyStrategies.DefaultConcurrencyStrategy.TRACKER_SYNC_KEY;

/**
 * ClusterTierActiveEntity
 */
public class ClusterTierActiveEntity implements ActiveServerEntity<EhcacheEntityMessage, EhcacheEntityResponse> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTierActiveEntity.class);
  static final String SYNC_DATA_SIZE_PROP = "ehcache.sync.data.size.threshold";
  private static final long DEFAULT_SYNC_DATA_SIZE_THRESHOLD = 2 * 1024 * 1024;
  static final String SYNC_DATA_GETS_PROP = "ehcache.sync.data.gets.threshold";

  // threshold for max number of chains per data sync message.
  // typically hits this threshold first before data size threshold for caches having small sized values.
  private static final int DEFAULT_SYNC_DATA_GETS_THRESHOLD = 8 * 1024;

  static final String CHAIN_COMPACTION_THRESHOLD_PROP = "ehcache.server.chain.compaction.threshold";
  private static final int DEFAULT_CHAIN_COMPACTION_THRESHOLD = 8;

  private static final int MAX_SYNC_CONCURRENCY = 1;
  private static final Executor SYNC_GETS_EXECUTOR = new ThreadPoolExecutor(0, MAX_SYNC_CONCURRENCY,
    20, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

  private final String storeIdentifier;
  private final ServerStoreConfiguration configuration;
  private final ClientCommunicator clientCommunicator;
  private final EhcacheStateService stateService;
  private final OOOMessageHandler<EhcacheEntityMessage, EhcacheEntityResponse> messageHandler;
  private final IEntityMessenger<EhcacheEntityMessage, EhcacheEntityResponse> entityMessenger;
  private final ServerStoreCompatibility storeCompatibility = new ServerStoreCompatibility();
  private final AtomicBoolean reconnectComplete = new AtomicBoolean(true);
  private final AtomicInteger invalidationIdGenerator = new AtomicInteger();
  private final ConcurrentMap<Integer, InvalidationHolder> clientsWaitingForInvalidation = new ConcurrentHashMap<>();
  private final ReconnectMessageCodec reconnectMessageCodec = new ReconnectMessageCodec();
  private final ClusterTierManagement management;
  private final String managerIdentifier;
  private final Object inflightInvalidationsMutex = new Object();
  private volatile List<InvalidationTuple> inflightInvalidations;
  private final Set<ClientDescriptor> eventListeners = new HashSet<>(); // accesses are synchronized on eventListeners itself
  private final Map<ClientDescriptor, Boolean> connectedClients = new ConcurrentHashMap<>();
  private final Map<ClientDescriptor, Map<UUID, Iterator<Chain>>> liveIterators = new ConcurrentHashMap<>();
  private final int chainCompactionLimit;
  private final ServerLockManager lockManager;

  private final long dataSizeThreshold = Long.getLong(SYNC_DATA_SIZE_PROP, DEFAULT_SYNC_DATA_SIZE_THRESHOLD);
  private final int dataGetsThreshold = Integer.getInteger(SYNC_DATA_GETS_PROP, DEFAULT_SYNC_DATA_GETS_THRESHOLD);
  private volatile Integer dataMapInitialCapacity = null;

  @SuppressWarnings("unchecked")
  public ClusterTierActiveEntity(ServiceRegistry registry, ClusterTierEntityConfiguration entityConfiguration, KeySegmentMapper defaultMapper) throws ConfigurationException {
    if (entityConfiguration == null) {
      throw new ConfigurationException("ClusteredStoreEntityConfiguration cannot be null");
    }
    storeIdentifier = entityConfiguration.getStoreIdentifier();
    configuration = entityConfiguration.getConfiguration();
    managerIdentifier = entityConfiguration.getManagerIdentifier();
    try {
      clientCommunicator = registry.getService(new CommunicatorServiceConfiguration());
      stateService = registry.getService(new EhcacheStoreStateServiceConfig(entityConfiguration.getManagerIdentifier(), defaultMapper));
      entityMessenger = registry.getService(new BasicServiceConfiguration<>(IEntityMessenger.class));
      messageHandler = registry.getService(new OOOMessageHandlerConfiguration<>(managerIdentifier + "###" + storeIdentifier,
        ClusterTierActiveEntity::isTrackedMessage));
    } catch (ServiceException e) {
      throw new ConfigurationException("Unable to retrieve service: " + e.getMessage());
    }
    if (entityMessenger == null) {
      throw new AssertionError("Server failed to retrieve IEntityMessenger service.");
    }
    management = new ClusterTierManagement(registry, stateService, true, storeIdentifier, entityConfiguration.getManagerIdentifier());
    chainCompactionLimit = Integer.getInteger(CHAIN_COMPACTION_THRESHOLD_PROP, DEFAULT_CHAIN_COMPACTION_THRESHOLD);
    if (configuration.isLoaderWriterConfigured()) {
      lockManager = new LockManagerImpl();
    } else {
      lockManager = new NoopLockManager();
    }
  }

  static boolean isTrackedMessage(EhcacheEntityMessage msg) {
    if (msg instanceof EhcacheOperationMessage) {
      return EhcacheMessageType.isTrackedOperationMessage(((EhcacheOperationMessage) msg).getMessageType());
    } else {
      return false;
    }
  }

  @Override
  public void addStateTo(StateDumpCollector dump) {
    ClusterTierDump.dump(dump, managerIdentifier, storeIdentifier, configuration);
    Set<ClientDescriptor> clients = new HashSet<>(getConnectedClients());

    List<Map<String, String>> allClients = new ArrayList<>(clients.size());
    for (ClientDescriptor entry : clients) {
      Map<String,String> clientMap = new HashMap<>(1);
      clientMap.put("clientDescriptor", entry.toString());
      allClients.add(clientMap);
    }
    dump.addState("clientCount", String.valueOf(clients.size()));
    dump.addState("clients", allClients);
  }

  @Override
  public void createNew() throws ConfigurationException {
    ServerSideServerStore store = stateService.createStore(storeIdentifier, configuration, true);
    store.setEventListener(new Listener());
    management.entityCreated();
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
    stateService.loadStore(storeIdentifier, configuration).setEventListener(new Listener());
    reconnectComplete.set(false);
    management.entityPromotionCompleted();
  }

  private class Listener implements ServerStoreEventListener {
    @Override
    public void onAppend(Chain beforeAppend, ByteBuffer appended) {
      Set<ClientDescriptor> clients = new HashSet<>(getValidatedClients());
      for (ClientDescriptor clientDescriptor : clients) {
        LOGGER.debug("SERVER: append happened in cache {}; notifying client {} ", storeIdentifier, clientDescriptor);
        try {
          clientCommunicator.sendNoResponse(clientDescriptor, serverAppend(appended.duplicate(), beforeAppend));
        } catch (MessageCodecException mce) {
          throw new AssertionError("Codec error", mce);
        }
      }
    }
    @Override
    public void onEviction(long key, InternalChain evictedChain) {
      Set<ClientDescriptor> clientsToInvalidate = new HashSet<>(getValidatedClients());
      if (!clientsToInvalidate.isEmpty()) {
        Chain detachedChain = evictedChain.detach();
        for (ClientDescriptor clientDescriptorThatHasToInvalidate : clientsToInvalidate) {
          LOGGER.debug("SERVER: eviction happened; asking client {} to invalidate hash {} from cache {}", clientDescriptorThatHasToInvalidate, key, storeIdentifier);
          try {
            boolean eventsEnabledForClient = isEventsEnabledFor(clientDescriptorThatHasToInvalidate);
            clientCommunicator.sendNoResponse(clientDescriptorThatHasToInvalidate, serverInvalidateHash(key, eventsEnabledForClient ? detachedChain : null));
          } catch (MessageCodecException mce) {
            throw new AssertionError("Codec error", mce);
          }
        }
      }
    }
  }

  @Override
  public void connected(ClientDescriptor clientDescriptor) {
    connectedClients.put(clientDescriptor, Boolean.FALSE);
  }

  @Override
  public void disconnected(ClientDescriptor clientDescriptor) {
    // cleanup all invalidation requests waiting for a ack from this client
    Set<Integer> invalidationIds = clientsWaitingForInvalidation.keySet();
    for (Integer invalidationId : invalidationIds) {
      clientInvalidated(clientDescriptor, invalidationId);
    }

    // cleanup all invalidation request this client was blocking on
    for(Iterator<Map.Entry<Integer, InvalidationHolder>> it = clientsWaitingForInvalidation.entrySet().iterator(); it.hasNext();) {
      Map.Entry<Integer, InvalidationHolder> next = it.next();
      ClientDescriptor clientDescriptorWaitingForInvalidation = next.getValue().clientDescriptorWaitingForInvalidation;
      if (clientDescriptorWaitingForInvalidation != null && clientDescriptorWaitingForInvalidation.equals(clientDescriptor)) {
        it.remove();
      }
    }

    lockManager.sweepLocksForClient(clientDescriptor,
                                    configuration.isWriteBehindConfigured() ? null : heldKeys -> heldKeys.forEach(stateService.getStore(storeIdentifier)::remove));

    liveIterators.remove(clientDescriptor);

    removeEventListener(clientDescriptor, stateService.getStore(storeIdentifier));

    connectedClients.remove(clientDescriptor);
  }

  @Override
  public EhcacheEntityResponse invokeActive(ActiveInvokeContext<EhcacheEntityResponse> context, EhcacheEntityMessage message)  throws EntityUserException {
    return messageHandler.invoke(context, message, this::invokeActiveInternal);
  }

  @SuppressWarnings("try")
  private EhcacheEntityResponse invokeActiveInternal(InvokeContext context, EhcacheEntityMessage message) {

    try {
      if (message instanceof EhcacheOperationMessage) {
        EhcacheOperationMessage operationMessage = (EhcacheOperationMessage) message;
        try (EhcacheStateContext ignored = stateService.beginProcessing(operationMessage, storeIdentifier)) {
          EhcacheMessageType messageType = operationMessage.getMessageType();
          if (isStoreOperationMessage(messageType)) {
            return invokeServerStoreOperation(context, (ServerStoreOpMessage) message);
          } else if (isLifecycleMessage(messageType)) {
            return invokeLifeCycleOperation(context, (LifecycleMessage) message);
          } else if (isStateRepoOperationMessage(messageType)) {
            return invokeStateRepositoryOperation((StateRepositoryOpMessage) message);
          }
        }
      }
      throw new AssertionError("Unsupported message : " + message.getClass());
    } catch (ClusterException e) {
      return failure(e);
    } catch (Exception e) {
      LOGGER.error("Unexpected exception raised during operation: " + message, e);
      return failure(new InvalidOperationException(e));
    }
  }

  private EhcacheEntityResponse invokeStateRepositoryOperation(StateRepositoryOpMessage message) {
    return stateService.getStateRepositoryManager().invoke(message);
  }

  private EhcacheEntityResponse invokeLifeCycleOperation(InvokeContext context, LifecycleMessage message) throws ClusterException {
    @SuppressWarnings("unchecked")
    ActiveInvokeContext<EhcacheEntityResponse> activeInvokeContext = (ActiveInvokeContext<EhcacheEntityResponse>) context;
    switch (message.getMessageType()) {
      case VALIDATE_SERVER_STORE:
        validateServerStore(activeInvokeContext.getClientDescriptor(), (ValidateServerStore) message);
        break;
      default:
        throw new AssertionError("Unsupported LifeCycle operation " + message);
    }
    return success();
  }

  private void validateServerStore(ClientDescriptor clientDescriptor, ValidateServerStore validateServerStore) throws ClusterException {
    ServerStoreConfiguration clientConfiguration = validateServerStore.getStoreConfiguration();
    LOGGER.info("Client {} validating cluster tier '{}'", clientDescriptor, storeIdentifier);
    ServerSideServerStore store = stateService.getStore(storeIdentifier);
    if (store != null) {
      storeCompatibility.verify(store.getStoreConfiguration(), clientConfiguration);
      connectedClients.put(clientDescriptor, Boolean.TRUE);
    } else {
      throw new InvalidStoreException("cluster tier '" + storeIdentifier + "' does not exist");
    }
  }

  private EhcacheEntityResponse invokeServerStoreOperation(InvokeContext context, ServerStoreOpMessage message) throws ClusterException {
    @SuppressWarnings("unchecked")
    ActiveInvokeContext<EhcacheEntityResponse> activeInvokeContext = (ActiveInvokeContext<EhcacheEntityResponse>) context;
    ClientDescriptor clientDescriptor = activeInvokeContext.getClientDescriptor();

    ServerSideServerStore cacheStore = stateService.getStore(storeIdentifier);
    if (cacheStore == null) {
      // An operation on a non-existent store should never get out of the client
      throw new LifecycleException("cluster tier does not exist : '" + storeIdentifier + "'");
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
        GetMessage getMessage = (GetMessage) message;
        try {
          return getResponse(cacheStore.get(getMessage.getKey()));
        } catch (TimeoutException e) {
          throw new AssertionError("Server side store is not expected to throw timeout exception", e);
        }
      }
      case APPEND: {
        AppendMessage appendMessage = (AppendMessage)message;

        long key = appendMessage.getKey();
        InvalidationTracker invalidationTracker = stateService.getInvalidationTracker(storeIdentifier);
        if (invalidationTracker != null) {
          invalidationTracker.trackHashInvalidation(key);
        }

        final Chain newChain;
        try {
          cacheStore.append(key, appendMessage.getPayload());
          newChain = cacheStore.get(key);
        } catch (TimeoutException e) {
          throw new AssertionError("Server side store is not expected to throw timeout exception", e);
        }
        sendMessageToSelfAndDeferRetirement(activeInvokeContext, appendMessage, newChain);
        invalidateHashForClient(clientDescriptor, key);
        if (newChain.length() > chainCompactionLimit) {
          requestChainResolution(clientDescriptor, key, newChain);
        }
        if (!configuration.isWriteBehindConfigured()) {
          lockManager.unlock(key);
        }
        return success();
      }
      case GET_AND_APPEND: {
        ServerStoreOpMessage.GetAndAppendMessage getAndAppendMessage = (ServerStoreOpMessage.GetAndAppendMessage)message;
        LOGGER.trace("Message {} : GET_AND_APPEND on key {} from client {}", message, getAndAppendMessage.getKey(), context.getClientSource().toLong());

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
          throw new AssertionError("Server side store is not expected to throw timeout exception", e);
        }
        sendMessageToSelfAndDeferRetirement(activeInvokeContext, getAndAppendMessage, newChain);
        LOGGER.debug("Send invalidations for key {}", getAndAppendMessage.getKey());
        invalidateHashForClient(clientDescriptor, getAndAppendMessage.getKey());
        return getResponse(result);
      }
      case REPLACE: {
        ReplaceAtHeadMessage replaceAtHeadMessage = (ReplaceAtHeadMessage) message;
        cacheStore.replaceAtHead(replaceAtHeadMessage.getKey(), replaceAtHeadMessage.getExpect(), replaceAtHeadMessage.getUpdate());
        return success();
      }
      case CLIENT_INVALIDATION_ACK: {
        ClientInvalidationAck clientInvalidationAck = (ClientInvalidationAck) message;
        int invalidationId = clientInvalidationAck.getInvalidationId();
        LOGGER.debug("SERVER: got notification of invalidation ack in cache {} from {} (ID {})", storeIdentifier, clientDescriptor, invalidationId);
        clientInvalidated(clientDescriptor, invalidationId);
        return success();
      }
      case CLIENT_INVALIDATION_ALL_ACK: {
        ClientInvalidationAllAck clientInvalidationAllAck = (ClientInvalidationAllAck) message;
        int invalidationId = clientInvalidationAllAck.getInvalidationId();
        LOGGER.debug("SERVER: got notification of invalidation ack in cache {} from {} (ID {})", storeIdentifier, clientDescriptor, invalidationId);
        clientInvalidated(clientDescriptor, invalidationId);
        return success();
      }
      case CLEAR: {
        LOGGER.info("Clearing cluster tier {}", storeIdentifier);
        try {
          cacheStore.clear();
        } catch (TimeoutException e) {
          throw new AssertionError("Server side store is not expected to throw timeout exception", e);
        }

        InvalidationTracker invalidationTracker = stateService.getInvalidationTracker(storeIdentifier);
        if (invalidationTracker != null) {
          invalidationTracker.setClearInProgress(true);
        }
        invalidateAll(clientDescriptor);
        return success();
      }
      case LOCK: {
        LockMessage lockMessage = (LockMessage) message;
        if (lockManager.lock(lockMessage.getHash(), activeInvokeContext.getClientDescriptor())) {
          try {
            Chain chain = cacheStore.get(lockMessage.getHash());
            return lockSuccess(chain);
          } catch (TimeoutException e) {
            throw new AssertionError("Server side store is not expected to throw timeout exception", e);
          }
        } else {
          return lockFailure();
        }
      }
      case UNLOCK: {
        UnlockMessage unlockMessage = (UnlockMessage) message;
        lockManager.unlock(unlockMessage.getHash());
        return success();
      }
      case ITERATOR_OPEN: {
        IteratorOpenMessage iteratorOpenMessage = (IteratorOpenMessage) message;
        try {
          Iterator<Chain> iterator = cacheStore.iterator();
          List<Chain> batch = iteratorBatch(iterator, iteratorOpenMessage.getBatchSize());

          if (iterator.hasNext()) {
            Map<UUID, Iterator<Chain>> liveIterators = this.liveIterators.computeIfAbsent(clientDescriptor, client -> new ConcurrentHashMap<>());
            UUID id;
            do {
              id = UUID.randomUUID();
            } while (liveIterators.putIfAbsent(id, iterator) != null);
            return iteratorBatchResponse(id, batch, false);
          } else {
            return iteratorBatchResponse(UUID.randomUUID(), batch, true);
          }
        } catch (TimeoutException e) {
          throw new AssertionError("Server side store is not expected to throw timeout exception", e);
        }
      }
      case ITERATOR_CLOSE: {
        IteratorCloseMessage iteratorCloseMessage = (IteratorCloseMessage) message;
        liveIterators.computeIfPresent(clientDescriptor, (client, iterators) -> {
          iterators.remove(iteratorCloseMessage.getIdentity());
          if (iterators.isEmpty()) {
            return null;
          } else {
            return iterators;
          }
        });
        return success();
      }
      case ITERATOR_ADVANCE: {
        IteratorAdvanceMessage iteratorAdvanceMessage = (IteratorAdvanceMessage) message;
        UUID id = iteratorAdvanceMessage.getIdentity();

        Iterator<Chain> iterator = liveIterators.getOrDefault(clientDescriptor, emptyMap()).get(id);
        if (iterator == null) {
          return failure(new InvalidOperationException("Referenced iterator is already closed (or never existed)"));
        } else {
          List<Chain> batch = iteratorBatch(iterator, iteratorAdvanceMessage.getBatchSize());
          if (iterator.hasNext()) {
            return iteratorBatchResponse(id, batch, false);
          } else {
            liveIterators.computeIfPresent(clientDescriptor, (client, iterators) -> {
              iterators.remove(id);
              return iterators.isEmpty() ? null : iterators;
            });
            return iteratorBatchResponse(id, batch, true);
          }
        }
      }
      case ENABLE_EVENT_LISTENER: {
        // we need to keep a count of how many clients have registered as events listeners
        // as we want to disable events from the store once all listeners are gone
        // so we need to keep all requesting client descriptors in a set so that duplicate
        // registrations from a single client are ignored
        EnableEventListenerMessage enableEventListenerMessage = (EnableEventListenerMessage) message;
        if (enableEventListenerMessage.isEnable()) {
          addEventListener(clientDescriptor, cacheStore);
        } else {
          removeEventListener(clientDescriptor, cacheStore);
        }
        return success();
      }
      default:
        throw new AssertionError("Unsupported ServerStore operation : " + message);
    }
  }

  private void addEventListener(ClientDescriptor clientDescriptor, ServerSideServerStore cacheStore) {
    synchronized (eventListeners) {
      if (eventListeners.add(clientDescriptor)) {
        cacheStore.enableEvents(true);
      }
    }
  }

  private void removeEventListener(ClientDescriptor clientDescriptor, ServerSideServerStore cacheStore) {
    synchronized (eventListeners) {
      if (eventListeners.remove(clientDescriptor) && eventListeners.isEmpty()) {
        cacheStore.enableEvents(false);
      }
    }
  }

  private boolean isEventsEnabledFor(ClientDescriptor clientDescriptor) {
    synchronized (eventListeners) {
      return eventListeners.contains(clientDescriptor);
    }
  }

  // for testing
  Set<ClientDescriptor> getEventListeners() {
    synchronized (eventListeners) {
      return new HashSet<>(eventListeners);
    }
  }

  private List<Chain> iteratorBatch(Iterator<Chain> iterator, int batchSize) {
    List<Chain> chains = new ArrayList<>();
    int size = 0;
    while (iterator.hasNext() && size < batchSize && size >= 0) {
      Chain nextChain = iterator.next();
      chains.add(nextChain);
      for (Element e: nextChain) {
        size += e.getPayload().remaining();
      }
    }
    return chains;
  }

  private void invalidateAll(ClientDescriptor originatingClientDescriptor) {
    int invalidationId = invalidationIdGenerator.getAndIncrement();
    Set<ClientDescriptor> clientsToInvalidate = new HashSet<>(getValidatedClients());
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
    Set<ClientDescriptor> validatedClients = getValidatedClients();
    Set<ClientDescriptor> clientsToInvalidate = ConcurrentHashMap.newKeySet(validatedClients.size());
    clientsToInvalidate.addAll(validatedClients);
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

  private void requestChainResolution(ClientDescriptor clientDescriptor, long key, Chain chain) {
    try {
      clientCommunicator.sendNoResponse(clientDescriptor, resolveRequest(key, chain));
    } catch (MessageCodecException e) {
      throw new AssertionError("Codec error", e);
    }
  }

  /**
   * Send a {@link PassiveReplicationMessage} to the passive, reuse the same transaction id and client id as the original message since this
   * original message won't ever be sent to the passive and these ids will be used to prevent duplication if the active goes down and the
   * client resends the original message to the passive (now our new active).
   *
   * @param context context of the message
   * @param message message to be forwarded
   * @param newChain resulting chain to send
   */
  private void sendMessageToSelfAndDeferRetirement(ActiveInvokeContext<EhcacheEntityResponse> context, KeyBasedServerStoreOpMessage message, Chain newChain) {
    try {
      long clientId = context.getClientSource().toLong();
      entityMessenger.messageSelfAndDeferRetirement(message, new PassiveReplicationMessage.ChainReplicationMessage(message.getKey(), newChain,
        context.getCurrentTransactionId(), context.getOldestTransactionId(), clientId));
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
  public void notifyDestroyed(ClientSourceId sourceId) {
    messageHandler.untrackClient(sourceId);
  }

  @Override
  public ReconnectHandler startReconnect() {
    try {
      this.entityMessenger.messageSelf(new EhcacheMessageTrackerCatchup(this.messageHandler.getRecordedMessages().filter(m->m.getRequest() != null).collect(Collectors.toList())));
    } catch (MessageCodecException mce) {
      throw new AssertionError("Codec error", mce);
    }
    return (clientDescriptor, bytes) -> {
      if (inflightInvalidations == null) {
        throw new AssertionError("Load existing was not invoked before handleReconnect");
      }

      ClusterTierReconnectMessage reconnectMessage = reconnectMessageCodec.decode(bytes);
      ServerSideServerStore serverStore = stateService.getStore(storeIdentifier);
      addInflightInvalidationsForStrongCache(clientDescriptor, reconnectMessage, serverStore);
      lockManager.createLockStateAfterFailover(clientDescriptor, reconnectMessage.getLocksHeld());
      if (reconnectMessage.isEventsEnabled()) {
        addEventListener(clientDescriptor, serverStore);
      }

      LOGGER.info("Client '{}' successfully reconnected to newly promoted ACTIVE after failover.", clientDescriptor);

      connectedClients.put(clientDescriptor, Boolean.TRUE);
    };
  }

  private void addInflightInvalidationsForStrongCache(ClientDescriptor clientDescriptor, ClusterTierReconnectMessage reconnectMessage, ServerSideServerStore serverStore) {
    if (serverStore.getStoreConfiguration().getConsistency().equals(Consistency.STRONG)) {
      Set<Long> invalidationsInProgress = reconnectMessage.getInvalidationsInProgress();
      LOGGER.debug("Number of Inflight Invalidations from client ID {} for cache {} is {}.", clientDescriptor.getSourceId().toLong(), storeIdentifier, invalidationsInProgress
          .size());
      inflightInvalidations.add(new InvalidationTuple(clientDescriptor, invalidationsInProgress, reconnectMessage.isClearInProgress()));
    }
  }

  @Override
  public void synchronizeKeyToPassive(PassiveSynchronizationChannel<EhcacheEntityMessage> syncChannel, int concurrencyKey) {
    LOGGER.info("Sync started for concurrency key {}.", concurrencyKey);
    if (concurrencyKey == DEFAULT_KEY) {
      stateService.getStateRepositoryManager().syncMessageFor(storeIdentifier).forEach(syncChannel::synchronizeToPassive);
    } else if (concurrencyKey == TRACKER_SYNC_KEY) {
      sendMessageTrackerReplication(syncChannel);
    } else {
      boolean interrupted = false;
      BlockingQueue<DataSyncMessageHandler> messageQ = new SynchronousQueue<>();
      int segmentId = concurrencyKey - DEFAULT_KEY - 1;
      Thread thisThread = Thread.currentThread();
      CompletableFuture<Void> asyncGet = CompletableFuture.runAsync(
        () -> doGetsForSync(segmentId, messageQ, syncChannel, thisThread), SYNC_GETS_EXECUTOR);
      try {
        try {
          while (messageQ.take().execute()) ;
        } catch (InterruptedException e) {
          interrupted = true;
        }
        if (interrupted) {
          // here we may have been interrupted due to a genuine exception on the async get thread
          // let us try and not loose that exception as it takes precedence over the interrupt
          asyncGet.get(10, TimeUnit.SECONDS);
          // we received a genuine interrupt
          throw new InterruptedException();
        } else {
          asyncGet.get();
        }
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        throw new RuntimeException(e);
      }
    }
    LOGGER.info("Sync complete for concurrency key {}.", concurrencyKey);
  }

  private void doGetsForSync(int segmentId, BlockingQueue<DataSyncMessageHandler> messageQ,
                             PassiveSynchronizationChannel<EhcacheEntityMessage> syncChannel, Thread waitingThread) {
    int numKeyGets = 0;
    long dataSize = 0;
    try {
      ServerSideServerStore store = stateService.getStore(storeIdentifier);
      Set<Long> keys = store.getSegmentKeySets().get(segmentId);
      int remainingKeys = keys.size();
      Map<Long, Chain> mappingsToSend = new HashMap<>(computeInitialMapCapacity(remainingKeys));
      boolean capacityAdjusted = false;
      for (Long key : keys) {
        final Chain chain;
        try {
          chain = store.get(key);
          if (chain.isEmpty()) {
            // evicted just continue with next
            remainingKeys--;
            continue;
          }
          numKeyGets++;
        } catch (TimeoutException e) {
          throw new AssertionError("Server side store is not expected to throw timeout exception");
        }
        for (Element element : chain) {
          dataSize += element.getPayload().remaining();
        }
        mappingsToSend.put(key, chain);
        if (dataSize > dataSizeThreshold || numKeyGets >= dataGetsThreshold) {
          putMessage(messageQ, syncChannel, mappingsToSend);
          if (!capacityAdjusted && segmentId == 0) {
            capacityAdjusted = true;
            adjustInitialCapacity(numKeyGets);
          }
          remainingKeys -= numKeyGets;
          mappingsToSend = new HashMap<>(computeMapCapacity(remainingKeys, numKeyGets));
          dataSize = 0;
          numKeyGets = 0;
        }
      }
      if (!mappingsToSend.isEmpty()) {
        putMessage(messageQ, syncChannel, mappingsToSend);
      }
      // put the last message indicator into the queue
      putMessage(messageQ, null, null);
    } catch (Throwable e) {
      // ensure waiting peer thread is interrupted, if we run into trouble
      waitingThread.interrupt();
      throw e;
    }
  }

  private void putMessage(BlockingQueue<DataSyncMessageHandler> messageQ,
                          PassiveSynchronizationChannel<EhcacheEntityMessage> syncChannel,
                          Map<Long, Chain> mappingsToSend) {
    try {
      if (syncChannel != null) {
        final EhcacheDataSyncMessage msg = new EhcacheDataSyncMessage(mappingsToSend);
        messageQ.put(() -> {
          syncChannel.synchronizeToPassive(msg);
          return true;
        });
      } else {
        // we are done
        messageQ.put(() -> false);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Compute map capacity based on {@code remainingSize} and {@code expectedGets}. Both varies depending on the size of
   * the chains and number of keys in the map.
   * <p>
   * NOTE: if expected gets dips below 32, keep at 32 as it indicates a large segment with possibly smaller number of keys
   * which means the next iteration may show more keys in the map.
   *
   * @param remainingSize is the number of keys left in the segment yet to be send
   * @param expectedGets is the max expected number of keys that could be put in the map before the map gets full
   * @return required capacity for the map.
   */
  private int computeMapCapacity(int remainingSize, int expectedGets) {
    if (remainingSize < 16) {
      return 16;
    } else if (expectedGets < 32) {
      return 32;
    } else if (remainingSize < expectedGets) {
      return (int) ((float) remainingSize / 0.75f + 1.0f);
    } else {
      return (int) ((float) expectedGets / 0.75f + 1.0f);
    }
  }

  /**
   * Adjust {@link this#dataMapInitialCapacity} based on what we learned about the cache during iteration of segment 0.
   *
   * NOTE: The required capacity calculation and the initial capacity adjustment assumes some sort of symmetry across
   * multiple segments, but it is possible that in a given segment, some keys has chain with LARGE data and some keys
   * has small chain with smaller data sizes. But on a larger sweep that should even out. Even if it does not even out,
   * this should perform better as the initial size is reset back to a minimum of 32 and not 16 when a cache is large
   * and when a cache is very small it starts with initial size of 16 as the {@link this#computeInitialMapCapacity(int)}
   * will take the total number of keys in the segment into account.
   *
   * @param actualKeyGets the actual number of keys we got when the map got full
   */
  private void adjustInitialCapacity(int actualKeyGets) {
    // even when there are larger data chains with less keys..let us keep the lower bound at 32.
    dataMapInitialCapacity =  (actualKeyGets < 32) ? 32 : (int) ((float) actualKeyGets / 0.75f + 1.0f);
  }

  /**
   * Starts with an initial size of configured {@link this#dataGetsThreshold} or adjusted initial size, unless the
   * segment of the cache is smaller than the initial expected size.
   *
   * @param totalKeys is the total number of keys in this segment
   */
  private int computeInitialMapCapacity(int totalKeys) {
    if (dataMapInitialCapacity == null) {
      dataMapInitialCapacity = (int) ((float) dataGetsThreshold / 0.75f + 1.0f);
    }
    if (totalKeys < 16) {
      return 16;
    } else if (totalKeys < dataMapInitialCapacity) {
      return (int) ((float) totalKeys / 0.75f + 1.0f);
    } else {
      return dataMapInitialCapacity;
    }
  }

  /**
   * Executes message sending asynchronously to preparation of message.
   */
  @FunctionalInterface
  private interface DataSyncMessageHandler {
    boolean execute();
  }

  private void sendMessageTrackerReplication(PassiveSynchronizationChannel<EhcacheEntityMessage> syncChannel) {
    Map<Long, Map<Long, EhcacheEntityResponse>> clientSourceIdTrackingMap = messageHandler.getTrackedClients()
      .collect(toMap(ClientSourceId::toLong, clientSourceId -> messageHandler.getRecordedMessages().filter(r->r.getClientSourceId().toLong() == clientSourceId.toLong()).collect(Collectors.toMap(rm->rm.getTransactionId(), rm->rm.getResponse()))));
    if (!clientSourceIdTrackingMap.isEmpty()) {
      syncChannel.synchronizeToPassive(new EhcacheMessageTrackerMessage(clientSourceIdTrackingMap));
    }
  }

  @Override
  public void destroy() {
    LOGGER.info("Destroying cluster tier '{}'", storeIdentifier);
    try {
      stateService.destroyServerStore(storeIdentifier);
    } catch (ClusterException e) {
      LOGGER.error("Failed to destroy server store - does not exist", e);
    }
    messageHandler.destroy();
    management.close();
  }

  protected Set<ClientDescriptor> getConnectedClients() {
    return connectedClients.keySet();
  }

  protected Set<ClientDescriptor> getValidatedClients() {
    return connectedClients.entrySet().stream().filter(Map.Entry::getValue).map(Map.Entry::getKey).collect(toSet());
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
