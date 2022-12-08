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

import org.ehcache.clustered.common.internal.ClusterTierManagerConfiguration;
import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EhcacheMessageType;
import org.ehcache.clustered.common.internal.messages.EhcacheOperationMessage;
import org.ehcache.clustered.common.internal.messages.LifecycleMessage;
import org.ehcache.clustered.server.management.Management;
import org.ehcache.clustered.server.state.EhcacheStateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.InvokeContext;
import org.terracotta.entity.PassiveServerEntity;
import org.terracotta.entity.StateDumpCollector;

import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.isLifecycleMessage;

public class ClusterTierManagerPassiveEntity implements PassiveServerEntity<EhcacheEntityMessage, EhcacheEntityResponse> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTierManagerPassiveEntity.class);

  private final EhcacheStateService ehcacheStateService;
  private final Management management;
  private final ClusterTierManagerConfiguration configuration;

  public ClusterTierManagerPassiveEntity(ClusterTierManagerConfiguration config,
                                         EhcacheStateService ehcacheStateService, Management management) throws ConfigurationException {
    if (config == null) {
      throw new ConfigurationException("ClusterTierManagerConfiguration cannot be null");
    }
    this.ehcacheStateService = ehcacheStateService;
    this.configuration = config;
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
    ClusterTierManagerDump.dump(dump, configuration);
  }

  @Override
  public void invokePassive(InvokeContext context, EhcacheEntityMessage message) {
    if (message instanceof EhcacheOperationMessage) {
      EhcacheOperationMessage operationMessage = (EhcacheOperationMessage) message;
      EhcacheMessageType messageType = operationMessage.getMessageType();
      if (isLifecycleMessage(messageType)) {
        invokeLifeCycleOperation((LifecycleMessage) message);
      } else {
        throw new AssertionError("Unsupported EhcacheOperationMessage: " + operationMessage.getMessageType());
      }
    } else {
      throw new AssertionError("Unsupported EhcacheEntityMessage: " + message.getClass());
    }
  }

  private void invokeLifeCycleOperation(LifecycleMessage message) {
    switch (message.getMessageType()) {
      case PREPARE_FOR_DESTROY:
        ehcacheStateService.prepareForDestroy();
        break;
      default:
        throw new AssertionError("Unsupported LifeCycle operation " + message.getMessageType());
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
  public void createNew() {
    management.entityCreated();
    management.sharedPoolsConfigured();
  }

  @Override
  public void destroy() {
    ehcacheStateService.destroy();
    management.close();
  }
}
