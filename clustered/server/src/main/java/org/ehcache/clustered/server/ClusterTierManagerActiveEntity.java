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
import org.ehcache.clustered.common.internal.exceptions.InvalidOperationException;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EhcacheMessageType;
import org.ehcache.clustered.common.internal.messages.EhcacheOperationMessage;
import org.ehcache.clustered.common.internal.messages.LifecycleMessage;
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

import java.util.concurrent.atomic.AtomicBoolean;

import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.failure;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.success;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.isLifecycleMessage;
import static org.ehcache.clustered.common.internal.messages.LifecycleMessage.ValidateStoreManager;

public class ClusterTierManagerActiveEntity implements ActiveServerEntity<EhcacheEntityMessage, EhcacheEntityResponse> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTierManagerActiveEntity.class);

  private final EhcacheStateService ehcacheStateService;
  private final Management management;
  private final AtomicBoolean reconnectComplete = new AtomicBoolean(true);
  private final ServerSideConfiguration configuration;
  private final ClusterTierManagerConfiguration clusterTierManagerConfig;

  /**
   * Only used for subclassing when testing
   */
  protected ClusterTierManagerActiveEntity() {
    ehcacheStateService = null;
    management = null;
    configuration = null;
    clusterTierManagerConfig = null;
  }

  public ClusterTierManagerActiveEntity(ClusterTierManagerConfiguration config,
                                        EhcacheStateService ehcacheStateService, Management management) throws ConfigurationException {
    clusterTierManagerConfig = config;
    if (config == null) {
      throw new ConfigurationException("ClusterTierManagerConfiguration cannot be null");
    }
    this.configuration = config.getConfiguration();
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
  }

  @Override
  public void connected(ClientDescriptor clientDescriptor) {
    LOGGER.info("Connecting {}", clientDescriptor);
  }

  @Override
  public void disconnected(ClientDescriptor clientDescriptor) {
    LOGGER.info("Disconnecting {}", clientDescriptor);
  }

  @Override
  public EhcacheEntityResponse invokeActive(ActiveInvokeContext<EhcacheEntityResponse> invokeContext, EhcacheEntityMessage message) {
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
      return failure(e);
    } catch (Exception e) {
      LOGGER.error("Unexpected exception raised during operation: " + message, e);
      return failure(new InvalidOperationException(e));
    }
  }

  @Override
  public ReconnectHandler startReconnect() {
    return (clientDescriptor, bytes) -> LOGGER.info("Client '{}' successfully reconnected to newly promoted ACTIVE after failover.", clientDescriptor);
  }

  @Override
  public void synchronizeKeyToPassive(PassiveSynchronizationChannel<EhcacheEntityMessage> syncChannel, int concurrencyKey) {
    // Nothing to sync
  }

  @Override
  public void createNew() {
    management.entityCreated();
    management.sharedPoolsConfigured();
  }

  @Override
  public void loadExisting() {
    ehcacheStateService.loadExisting(configuration);
    LOGGER.debug("Preparing for handling Inflight Invalidations and independent Passive Evictions in loadExisting");
    reconnectComplete.set(false);

    management.entityPromotionCompleted();
    management.sharedPoolsConfigured();
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
    return success();
  }

  private EhcacheEntityResponse prepareForDestroy() {
    EhcacheEntityResponse.PrepareForDestroy response = EhcacheEntityResponse.prepareForDestroy(ehcacheStateService.getStores());
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
    ehcacheStateService.validate(message.getConfiguration());
  }
}
