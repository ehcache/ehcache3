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

import org.ehcache.clustered.client.config.Timeouts;
import org.ehcache.clustered.client.internal.service.ClusterTierException;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EhcacheOperationMessage;
import org.ehcache.clustered.common.internal.messages.StateRepositoryOpMessage;
import org.terracotta.lease.LeaseMaintainer;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class LeaseCheckingClusterTierEntity implements InternalClusterTierClientEntity {

  private final InternalClusterTierClientEntity delegate;
  private final LeaseMaintainer leaseMaintainer;
  private volatile ServerStoreProxy.ServerCallback callback;
  private final AtomicBoolean leaseExpired = new AtomicBoolean();

  public LeaseCheckingClusterTierEntity(InternalClusterTierClientEntity clusterTierClientEntity, LeaseMaintainer leaseMaintainer) {
    this.delegate = clusterTierClientEntity;
    this.leaseMaintainer = leaseMaintainer;
  }

  @Override
  public void setTimeouts(Timeouts timeouts) {
    delegate.setTimeouts(timeouts);
  }

  @Override
  public void setStoreIdentifier(String storeIdentifier) {
    delegate.setStoreIdentifier(storeIdentifier);
  }

  @Override
  public Timeouts getTimeouts() {
    return delegate.getTimeouts();
  }

  //TODO: check this, Should not be used anymore
  @Override
  public boolean isConnected() {
    return delegate.isConnected();
  }

  @Override
  public void validate(ServerStoreConfiguration clientStoreConfiguration) throws ClusterTierException, TimeoutException {
    delegate.validate(clientStoreConfiguration);
  }

  @Override
  public void invokeAndWaitForSend(EhcacheOperationMessage message, boolean track) throws ClusterException, TimeoutException {
    delegate.invokeAndWaitForSend(message, track);
  }

  @Override
  public void invokeAndWaitForReceive(EhcacheOperationMessage message, boolean track) throws ClusterException, TimeoutException {
    delegate.invokeAndWaitForReceive(message, track);
  }

  @Override
  public EhcacheEntityResponse invokeAndWaitForComplete(EhcacheOperationMessage message, boolean track) throws ClusterException, TimeoutException {
    return delegate.invokeAndWaitForComplete(message, track);
  }

  @Override
  public EhcacheEntityResponse invokeAndWaitForRetired(EhcacheOperationMessage message, boolean track) throws ClusterException, TimeoutException {
    return delegate.invokeAndWaitForRetired(message, track);
  }

  @Override
  public EhcacheEntityResponse invokeStateRepositoryOperation(StateRepositoryOpMessage message, boolean track) throws ClusterException, TimeoutException {
    return delegate.invokeStateRepositoryOperation(message, track);
  }

  @Override
  public <T extends EhcacheEntityResponse> void addResponseListener(Class<T> responseType, ResponseListener<T> responseListener) {
    delegate.addResponseListener(responseType, responseListener);
  }

  @Override
  public void setDisconnectionListener(DisconnectionListener disconnectionListener) {
    delegate.setDisconnectionListener(disconnectionListener);
  }

  @Override
  public void setReconnectListener(ReconnectListener reconnectListener) {
    delegate.setReconnectListener(reconnectListener);
  }

  @Override
  public void setLeaseExpiredCallback(ServerStoreProxy.ServerCallback callback) {
    this.callback = callback;
  }

  @Override
  public void close() {
    delegate.close();
  }
}
