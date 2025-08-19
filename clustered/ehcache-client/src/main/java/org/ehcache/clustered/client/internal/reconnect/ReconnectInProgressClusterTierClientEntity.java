/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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
package org.ehcache.clustered.client.internal.reconnect;

import org.ehcache.clustered.client.config.Timeouts;
import org.ehcache.clustered.client.internal.service.ClusterTierException;
import org.ehcache.clustered.client.internal.store.ClusterTierClientEntity;
import org.ehcache.clustered.client.internal.store.ReconnectInProgressException;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EhcacheOperationMessage;
import org.ehcache.clustered.common.internal.messages.StateRepositoryOpMessage;

import java.util.concurrent.TimeoutException;

public class ReconnectInProgressClusterTierClientEntity implements ClusterTierClientEntity {

  private volatile boolean isMarkedClose = false;

  @Override
  public Timeouts getTimeouts() {
    throw new ReconnectInProgressException();
  }

  @Override
  public boolean isConnected() {
    throw new ReconnectInProgressException();
  }

  @Override
  public void validate(ServerStoreConfiguration clientStoreConfiguration) throws ClusterTierException, TimeoutException {
    throw new ReconnectInProgressException();
  }

  @Override
  public void invokeAndWaitForSend(EhcacheOperationMessage message, boolean track) throws ClusterException, TimeoutException {
    throw new ReconnectInProgressException();
  }

  @Override
  public void invokeAndWaitForReceive(EhcacheOperationMessage message, boolean track) throws ClusterException, TimeoutException {
    throw new ReconnectInProgressException();
  }

  @Override
  public EhcacheEntityResponse invokeAndWaitForComplete(EhcacheOperationMessage message, boolean track) throws ClusterException, TimeoutException {
    throw new ReconnectInProgressException();
  }

  @Override
  public EhcacheEntityResponse invokeAndWaitForRetired(EhcacheOperationMessage message, boolean track) throws ClusterException, TimeoutException {
    throw new ReconnectInProgressException();
  }

  @Override
  public EhcacheEntityResponse invokeStateRepositoryOperation(StateRepositoryOpMessage message, boolean track) throws ClusterException, TimeoutException {
    throw new ReconnectInProgressException();
  }

  @Override
  public <T extends EhcacheEntityResponse> void addResponseListener(Class<T> responseType, ResponseListener<T> responseListener) {
    throw new ReconnectInProgressException();
  }

  @Override
  public void addDisconnectionListener(DisconnectionListener disconnectionListener) {
    throw new ReconnectInProgressException();
  }

  @Override
  public void addReconnectListener(ReconnectListener reconnectListener) {
    throw new ReconnectInProgressException();
  }

  @Override
  public void enableEvents(boolean enable) throws ClusterException, TimeoutException {
    throw new ReconnectInProgressException();
  }

  @Override
  public void close() {
    isMarkedClose = true;
  }

  public boolean checkIfClosed() {
    return isMarkedClose;
  }
}
