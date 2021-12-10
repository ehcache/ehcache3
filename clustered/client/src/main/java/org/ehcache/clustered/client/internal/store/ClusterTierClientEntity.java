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

package org.ehcache.clustered.client.internal.store;

import org.ehcache.clustered.client.internal.service.ClusterTierException;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.messages.ClusterTierReconnectMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage;
import org.ehcache.clustered.common.internal.messages.StateRepositoryOpMessage;
import org.terracotta.connection.entity.Entity;
import org.terracotta.entity.MessageCodecException;

import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * ClusterTierClientEntity
 */
public interface ClusterTierClientEntity extends Entity {

  UUID getClientId();

  boolean isConnected();

  void validate(ServerStoreConfiguration clientStoreConfiguration) throws ClusterTierException, TimeoutException;

  EhcacheEntityResponse invokeServerStoreOperation(ServerStoreOpMessage message, boolean track) throws ClusterException, TimeoutException;

  void invokeServerStoreOperationAsync(ServerStoreOpMessage message, boolean track) throws MessageCodecException;

  EhcacheEntityResponse invokeStateRepositoryOperation(StateRepositoryOpMessage message, boolean track) throws ClusterException, TimeoutException;

  <T extends EhcacheEntityResponse> void addResponseListener(Class<T> responseType, ResponseListener<T> responseListener);

  void setDisconnectionListener(DisconnectionListener disconnectionListener);

  void setReconnectListener(ReconnectListener reconnectListener);

  interface ResponseListener<T extends EhcacheEntityResponse> {
    void onResponse(T response);
  }

  interface DisconnectionListener {
    void onDisconnection();
  }

  interface ReconnectListener {
    void onHandleReconnect(ClusterTierReconnectMessage reconnectMessage);
  }
}
