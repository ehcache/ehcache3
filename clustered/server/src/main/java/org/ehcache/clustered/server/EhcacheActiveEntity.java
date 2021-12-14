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

import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ClusteredTierManagerConfiguration;
import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.exceptions.InvalidClientIdException;
import org.ehcache.clustered.common.internal.exceptions.InvalidOperationException;
import org.ehcache.clustered.common.internal.exceptions.LifecycleException;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponseFactory;
import org.ehcache.clustered.common.internal.messages.EhcacheMessageType;
import org.ehcache.clustered.common.internal.messages.EhcacheOperationMessage;
import org.ehcache.clustered.common.internal.messages.LifecycleMessage;
import org.ehcache.clustered.common.internal.messages.ReconnectMessage;
import org.ehcache.clustered.common.internal.messages.ReconnectMessageCodec;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage.ClientIDTrackerMessage;
import org.ehcache.clustered.server.management.Management;
import org.ehcache.clustered.server.state.EhcacheStateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.ActiveServerEntity;
import org.terracotta.entity.BasicServiceConfiguration;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.IEntityMessenger;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.entity.PassiveSynchronizationChannel;
import org.terracotta.entity.ServiceRegistry;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.isLifecycleMessage;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.isPassiveReplicationMessage;
import static org.ehcache.clustered.common.internal.messages.LifecycleMessage.ValidateStoreManager;

public class EhcacheActiveEntity implements ActiveServerEntity<EhcacheEntityMessage, EhcacheEntityResponse> {

  private static final Logger LOGGER = LoggerFactory.getLogger(EhcacheActiveEntity.class);
  static final String SYNC_DATA_SIZE_PROP = "ehcache.sync.data.size.threshold";

  /**
   * Tracks the state of a connected client.  An entry is added to this map when the
   * {@link #connected(ClientDescriptor)} method is invoked for a client and removed when the
   * {@link #disconnected(ClientDescriptor)} method is invoked for the client.
   */
  private final Map<ClientDescriptor, ClientState> clientStateMap = new ConcurrentHashMap<>();

  private final ConcurrentHashMap<String, Set<ClientDescriptor>> storeClientMap =
      new ConcurrentHashMap<>();

  private final ConcurrentHashMap<ClientDescriptor, UUID> clientIdMap = new ConcurrentHashMap<>();
  private final Set<UUID> trackedClients = Collections.newSetFromMap(new ConcurrentHashMap<>());
  private final ReconnectMessageCodec reconnectMessageCodec = new ReconnectMessageCodec();
  private final EhcacheEntityResponseFactory responseFactory;
  private final EhcacheStateService ehcacheStateService;
  private final IEntityMessenger entityMessenger;
  private final Management management;
  private final AtomicBoolean reconnectComplete = new AtomicBoolean(true);
  private final ServerSideConfiguration configuration;

  public EhcacheActiveEntity(ServiceRegistry services, ClusteredTierManagerConfiguration config,
                             EhcacheStateService ehcacheStateService, Management management) throws ConfigurationException {
    if (config == null) {
      throw new ConfigurationException("ClusteredTierManagerConfiguration cannot be null");
    }
    this.configuration = config.getConfiguration();
    this.responseFactory = new EhcacheEntityResponseFactory();
    this.ehcacheStateService = ehcacheStateService;
    if (ehcacheStateService == null) {
      throw new AssertionError("Server failed to retrieve EhcacheStateService.");
    }
    entityMessenger = services.getService(new BasicServiceConfiguration<>(IEntityMessenger.class));
    if (entityMessenger == null) {
      throw new AssertionError("Server failed to retrieve IEntityMessenger service.");
    }
    try {
      ehcacheStateService.configure();
      this.management = management;
    } catch (ConfigurationException e) {
      ehcacheStateService.destroy();
      throw e;
    }
  }

  /**
   * Gets the map of connected clients along with the server stores each is using.
   * If the client is using no stores, the set of stores will be empty for that client.
   *
   * @return an unmodifiable copy of the connected client map
   */
  // This method is intended for unit test use; modifications are likely needed for other (monitoring) purposes
  Set<ClientDescriptor> getConnectedClients() {
    final Set<ClientDescriptor> clients = new HashSet<>();
    for (Entry<ClientDescriptor, ClientState> entry : clientStateMap.entrySet()) {
      clients.add(entry.getKey());
    }
    return Collections.unmodifiableSet(clients);
  }

  @Override
  public void connected(ClientDescriptor clientDescriptor) {
    if (!clientStateMap.containsKey(clientDescriptor)) {
      LOGGER.info("Connecting {}", clientDescriptor);
      ClientState clientState = new ClientState();
      clientStateMap.put(clientDescriptor, clientState);
      management.clientConnected(clientDescriptor, clientState);
    } else {
      // This is logically an AssertionError
      LOGGER.error("Client {} already registered as connected", clientDescriptor);
    }
  }

  @Override
  public void disconnected(ClientDescriptor clientDescriptor) {
    ClientState clientState = clientStateMap.remove(clientDescriptor);
    if (clientState == null) {
      // This is logically an AssertionError
      LOGGER.error("Client {} not registered as connected", clientDescriptor);
    } else {
      LOGGER.info("Disconnecting {}", clientDescriptor);
      management.clientDisconnected(clientDescriptor, clientState);
    }
    UUID clientId = clientIdMap.remove(clientDescriptor);
    if (clientId != null) {
      try {
        entityMessenger.messageSelf(new ClientIDTrackerMessage(clientId));
      } catch (MessageCodecException mce) {
        throw new AssertionError("Codec error", mce);
      }
      trackedClients.remove(clientId);
      ehcacheStateService.getClientMessageTracker().remove(clientId);
    }
  }

  @Override
  public EhcacheEntityResponse invoke(ClientDescriptor clientDescriptor, EhcacheEntityMessage message) {
    try {
      clearClientTrackedAtReconnectComplete();

      if (message instanceof EhcacheOperationMessage) {
        EhcacheOperationMessage operationMessage = (EhcacheOperationMessage) message;
        EhcacheMessageType messageType = operationMessage.getMessageType();
        if (isLifecycleMessage(messageType)) {
          return invokeLifeCycleOperation(clientDescriptor, (LifecycleMessage) message);
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

  private void clearClientTrackedAtReconnectComplete() {

    if (!reconnectComplete.get()) {
      if (reconnectComplete.compareAndSet(false, true)) {
        ehcacheStateService.getClientMessageTracker().reconcileTrackedClients(trackedClients);
      }
    }

  }

  @Override
  public void handleReconnect(ClientDescriptor clientDescriptor, byte[] extendedReconnectData) {
    ClientState clientState = this.clientStateMap.get(clientDescriptor);
    if (clientState == null) {
      throw new AssertionError("Client "+ clientDescriptor +" trying to reconnect is not connected to entity");
    }
    clientState.attach();
    ReconnectMessage reconnectMessage = reconnectMessageCodec.decode(extendedReconnectData);
    addClientId(clientDescriptor, reconnectMessage.getClientId());
    LOGGER.info("Client '{}' successfully reconnected to newly promoted ACTIVE after failover.", clientDescriptor);

    management.clientReconnected(clientDescriptor, clientState);
  }

  @Override
  public void synchronizeKeyToPassive(PassiveSynchronizationChannel<EhcacheEntityMessage> syncChannel, int concurrencyKey) {
    // Nothing to sync
  }

  @Override
  public void createNew() {
    management.init();
    management.sharedPoolsConfigured();
  }

  @Override
  public void loadExisting() {
    ehcacheStateService.loadExisting(configuration);
    LOGGER.debug("Preparing for handling Inflight Invalidations and independent Passive Evictions in loadExisting");
    reconnectComplete.set(false);

    management.init();
    management.sharedPoolsConfigured();
  }

  private void validateClientConnected(ClientDescriptor clientDescriptor) throws ClusterException {
    ClientState clientState = this.clientStateMap.get(clientDescriptor);
    validateClientConnected(clientDescriptor, clientState);
  }

  private static void validateClientConnected(ClientDescriptor clientDescriptor, ClientState clientState) throws LifecycleException {
    if (clientState == null) {
      throw new LifecycleException("Client " + clientDescriptor + " is not connected to the Clustered Tier Manager");
    }
  }

  private EhcacheEntityResponse invokeLifeCycleOperation(ClientDescriptor clientDescriptor, LifecycleMessage message) throws ClusterException {
    switch (message.getMessageType()) {
      case VALIDATE:
        validate(clientDescriptor, (ValidateStoreManager) message);
        break;
      case PREPARE_FOR_DESTROY:
        return prepareForDestroy();
      default:
        throw new AssertionError("Unsupported LifeCycle operation " + message);
    }
    return responseFactory.success();
  }

  private EhcacheEntityResponse prepareForDestroy() {
    // TODO toggle a destroy in progress flag
    return new EhcacheEntityResponse.PrepareForDestroy(ehcacheStateService.getStores());
  }

  /**
   * {@inheritDoc}
   * <p>
   * This method is invoked in response to a call to a {@code com.tc.objectserver.api.ServerEntityRequest}
   * message for a {@code ServerEntityAction.DESTROY_ENTITY} request which is sent via a call to the
   * {@code ClusteringService.destroyAll} method.  This method is expected to be called only when no
   * clients actively using this entity.
   */
  @Override
  public void destroy() {
    /*
     * Ensure the allocated stores are closed out.
     */
    for (String store : ehcacheStateService.getStores()) {
      final Set<ClientDescriptor> attachedClients = storeClientMap.get(store);
      if (attachedClients != null && !attachedClients.isEmpty()) {
        // This is logically an AssertionError; logging and continuing destroy
        LOGGER.error("Clustered tier '{}' has {} clients attached during clustered tier manager destroy", store);
      }

      LOGGER.info("Destroying clustered tier '{}' for clustered tier manager destroy", store);
      storeClientMap.remove(store);
    }

    ehcacheStateService.destroy();

  }

  /**
   * Handles the {@link ValidateStoreManager ValidateStoreManager} message.  This message is used by a client to
   * connect to an established {@code EhcacheActiveEntity}.  This method validates the client-provided configuration
   * against the existing configuration to ensure compatibility.
   *
   * @param clientDescriptor the client identifier requesting attachment to a configured store manager
   * @param message the {@code ValidateStoreManager} message carrying the client expected resource pool configuration
   */
  private void validate(ClientDescriptor clientDescriptor, ValidateStoreManager message) throws ClusterException {
    validateClientConnected(clientDescriptor);
    if (clientIdMap.get(clientDescriptor) != null) {
      throw new LifecycleException("Client : " + clientDescriptor + " is already being tracked with Client Id : " + clientIdMap.get(clientDescriptor));
    } else if (trackedClients.contains(message.getClientId())) {
      throw new InvalidClientIdException("Client ID : " + message.getClientId() + " is already being tracked by Active paired with Client : " + clientDescriptor);
    }

    addClientId(clientDescriptor, message.getClientId());
    ehcacheStateService.validate(message.getConfiguration());
    this.clientStateMap.get(clientDescriptor).attach();
    management.clientValidated(clientDescriptor, this.clientStateMap.get(clientDescriptor));
  }

  private void addClientId(ClientDescriptor clientDescriptor, UUID clientId) {
    LOGGER.info("Adding Client {} with client ID : {} ", clientDescriptor, clientId);
    clientIdMap.put(clientDescriptor, clientId);
    trackedClients.add(clientId);
  }

}
