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
import org.ehcache.clustered.common.internal.ClusterTierManagerConfiguration;
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
import org.ehcache.clustered.common.internal.messages.ClusterTierManagerReconnectMessage;
import org.ehcache.clustered.common.internal.messages.ReconnectMessageCodec;
import org.ehcache.clustered.server.management.Management;
import org.ehcache.clustered.server.state.EhcacheStateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.ActiveInvokeContext;
import org.terracotta.entity.ActiveServerEntity;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.PassiveSynchronizationChannel;
import org.terracotta.entity.StateDumpCollector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.isLifecycleMessage;
import static org.ehcache.clustered.common.internal.messages.LifecycleMessage.ValidateStoreManager;

public class ClusterTierManagerActiveEntity implements ActiveServerEntity<EhcacheEntityMessage, EhcacheEntityResponse> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTierManagerActiveEntity.class);

  /**
   * Tracks the state of a connected client.  An entry is added to this map when the
   * {@link #connected(ClientDescriptor)} method is invoked for a client and removed when the
   * {@link #disconnected(ClientDescriptor)} method is invoked for the client.
   */
  private final Map<ClientDescriptor, ClientState> clientStateMap = new ConcurrentHashMap<>();

  private final ReconnectMessageCodec reconnectMessageCodec = new ReconnectMessageCodec();
  private final EhcacheEntityResponseFactory responseFactory;
  private final EhcacheStateService ehcacheStateService;
  private final Management management;
  private final AtomicBoolean reconnectComplete = new AtomicBoolean(true);
  private final ServerSideConfiguration configuration;
  private final ClusterTierManagerConfiguration clusterTierManagerConfig;

  public ClusterTierManagerActiveEntity(ClusterTierManagerConfiguration config,
                                        EhcacheStateService ehcacheStateService, Management management) throws ConfigurationException {
    clusterTierManagerConfig = config;
    if (config == null) {
      throw new ConfigurationException("ClusterTierManagerConfiguration cannot be null");
    }
    this.configuration = config.getConfiguration();
    this.responseFactory = new EhcacheEntityResponseFactory();
    this.ehcacheStateService = ehcacheStateService;
    if (ehcacheStateService == null) {
      throw new AssertionError("Server failed to retrieve EhcacheStateService.");
    }
    try {
      ehcacheStateService.configure();
      this.management = management;
    } catch (ConfigurationException e) {
      ehcacheStateService.destroy();
      throw e;
    }
  }

  @Override
  public void addStateTo(StateDumpCollector dump) {
    ClusterTierManagerDump.dump(dump, clusterTierManagerConfig);
    {
      Map<ClientDescriptor, ClientState> clients = new HashMap<>(clientStateMap);

      List<Map> allClients = new ArrayList<>();
      for (Map.Entry<ClientDescriptor, ClientState> entry : clients.entrySet()) {
        Map<String,String> clientMap = new HashMap<>(3);
        clientMap.put("clientDescriptor", entry.getKey().toString());
        clientMap.put("clientIdentifier", String.valueOf(entry.getValue().getClientIdentifier()));
        clientMap.put("attached", String.valueOf(entry.getValue().isAttached()));
        allClients.add(clientMap);
      }
      dump.addState("clientCount", String.valueOf(allClients.size()));
      dump.addState("clients", allClients);
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
  }

  @Override
  public EhcacheEntityResponse invokeActive(ActiveInvokeContext invokeContext, EhcacheEntityMessage message) {
    try {
      if (message instanceof EhcacheOperationMessage) {
        EhcacheOperationMessage operationMessage = (EhcacheOperationMessage) message;
        EhcacheMessageType messageType = operationMessage.getMessageType();
        if (isLifecycleMessage(messageType)) {
          return invokeLifeCycleOperation(invokeContext.getClientDescriptor(), (LifecycleMessage) message);
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

  @Override
  public void handleReconnect(ClientDescriptor clientDescriptor, byte[] extendedReconnectData) {
    ClientState clientState = this.clientStateMap.get(clientDescriptor);
    if (clientState == null) {
      throw new AssertionError("Client "+ clientDescriptor +" trying to reconnect is not connected to entity");
    }
    ClusterTierManagerReconnectMessage reconnectMessage = reconnectMessageCodec.decodeReconnectMessage(extendedReconnectData);
    clientState.attach(reconnectMessage.getClientId());
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
    if (clientState == null) {
      throw new LifecycleException("Client " + clientDescriptor + " is not connected to the cluster tier manager");
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
    EhcacheEntityResponse.PrepareForDestroy response = new EhcacheEntityResponse.PrepareForDestroy(ehcacheStateService
      .getStores());
    ehcacheStateService.prepareForDestroy();
    return response;
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
    ehcacheStateService.destroy();
    management.close();
  }

  /**
   * Handles the {@link ValidateStoreManager ValidateStoreManager} message.  This message is used by a client to
   * connect to an established {@code ClusterTierManagerActiveEntity}.  This method validates the client-provided configuration
   * against the existing configuration to ensure compatibility.
   *
   * @param clientDescriptor the client identifier requesting attachment to a configured store manager
   * @param message the {@code ValidateStoreManager} message carrying the client expected resource pool configuration
   */
  private void validate(ClientDescriptor clientDescriptor, ValidateStoreManager message) throws ClusterException {
    validateClientConnected(clientDescriptor);
    ClientState clientState = clientStateMap.get(clientDescriptor);
    UUID clientId = clientState.getClientIdentifier();
    if (clientId != null) {
      throw new LifecycleException("Client : " + clientDescriptor + " is already being tracked with Client Id : " + clientId);
    }
    if (getTrackedClients().contains(message.getClientId())) {
      throw new InvalidClientIdException("Client ID : " + message.getClientId() + " is already being tracked.");
    }

    ehcacheStateService.validate(message.getConfiguration());
    clientState.attach(message.getClientId());
    management.clientValidated(clientDescriptor, clientState);
  }

  private Set<UUID> getTrackedClients() {
    return clientStateMap.entrySet().stream()
      .filter(entry -> entry.getValue().isAttached())
      .map(entry -> entry.getValue().getClientIdentifier())
      .collect(Collectors.toSet());
  }

}
