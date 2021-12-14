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

import org.ehcache.clustered.common.internal.ClusteredTierManagerConfiguration;
import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EhcacheMessageType;
import org.ehcache.clustered.common.internal.messages.EhcacheOperationMessage;
import org.ehcache.clustered.common.internal.messages.LifecycleMessage;
import org.ehcache.clustered.server.internal.messages.PassiveReplicationMessage;
import org.ehcache.clustered.server.management.Management;
import org.ehcache.clustered.server.state.ClientMessageTracker;
import org.ehcache.clustered.server.state.EhcacheStateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.PassiveServerEntity;

import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.isLifecycleMessage;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.isPassiveReplicationMessage;

public class EhcachePassiveEntity implements PassiveServerEntity<EhcacheEntityMessage, EhcacheEntityResponse> {

  private static final Logger LOGGER = LoggerFactory.getLogger(EhcachePassiveEntity.class);

  private final EhcacheStateService ehcacheStateService;
  private final Management management;

  @Override
  public void invoke(EhcacheEntityMessage message) {

    try {
      if (message instanceof EhcacheOperationMessage) {
        EhcacheOperationMessage operationMessage = (EhcacheOperationMessage) message;
        EhcacheMessageType messageType = operationMessage.getMessageType();
        if (isLifecycleMessage(messageType)) {
          invokeLifeCycleOperation((LifecycleMessage) message);
        } else if (isPassiveReplicationMessage(messageType)) {
          try {
            invokeRetirementMessages((PassiveReplicationMessage)message);
          } catch (ClusterException e) {
            LOGGER.error("Unexpected exception raised during operation: " + message, e);
          }
        } else {
          throw new AssertionError("Unsupported EhcacheOperationMessage: " + operationMessage.getMessageType());
        }
      } else {
        throw new AssertionError("Unsupported EhcacheEntityMessage: " + message.getClass());
      }
    } catch (ClusterException e) {
      // Reaching here means a lifecycle or sync operation failed
      throw new IllegalStateException("A lifecycle or sync operation failed", e);
    }
  }

  public EhcachePassiveEntity(ClusteredTierManagerConfiguration config,
                              EhcacheStateService ehcacheStateService, Management management) throws ConfigurationException {
    if (config == null) {
      throw new ConfigurationException("ClusteredTierManagerConfiguration cannot be null");
    }
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

  private void invokeRetirementMessages(PassiveReplicationMessage message) throws ClusterException {

    switch (message.getMessageType()) {
      case CLIENT_ID_TRACK_OP:
        ehcacheStateService.getClientMessageTracker().remove(message.getClientId());
        break;
      default:
        throw new AssertionError("Unsupported Retirement Message : " + message);
    }
  }

  private void invokeLifeCycleOperation(LifecycleMessage message) throws ClusterException {
    switch (message.getMessageType()) {
      case VALIDATE:
        applyMessage(message);
        break;
      default:
        throw new AssertionError("Unsupported LifeCycle operation " + message.getMessageType());
    }
  }

  private void applyMessage(EhcacheOperationMessage message) {
    ClientMessageTracker clientMessageTracker = ehcacheStateService.getClientMessageTracker();
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
    management.sharedPoolsConfigured();
  }

  @Override
  public void destroy() {
    ehcacheStateService.destroy();
  }
}
