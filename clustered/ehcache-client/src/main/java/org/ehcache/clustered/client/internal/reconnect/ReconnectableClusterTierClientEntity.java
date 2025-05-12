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
import org.ehcache.clustered.client.internal.service.ClusterTierValidationException;
import org.ehcache.clustered.client.internal.store.ClusterTierClientEntity;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EhcacheOperationMessage;
import org.ehcache.clustered.common.internal.messages.StateRepositoryOpMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.connection.Connection;
import org.terracotta.exception.ConnectionClosedException;
import org.terracotta.exception.ConnectionShutdownException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.ehcache.core.util.ExceptionUtil.containsCause;

public class ReconnectableClusterTierClientEntity implements ClusterTierClientEntity {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReconnectableClusterTierClientEntity.class);

  private final AtomicReference<ClusterTierClientEntity> delegateRef = new AtomicReference<>();
  private final Runnable reconnectTask;

  private final Map<Class<? extends EhcacheEntityResponse>,
    List<ResponseListener<? extends EhcacheEntityResponse>>> responseListeners = new ConcurrentHashMap<>();
  private final List<DisconnectionListener> disconnectionListeners = new CopyOnWriteArrayList<>();
  private final List<ReconnectListener> reconnectListeners = new CopyOnWriteArrayList<>();
  private final ExecutorService asyncExecutor;
  private volatile boolean enableEventing = false;

  public ReconnectableClusterTierClientEntity(ClusterTierClientEntity clientEntity, Runnable reconnectTask,
    ExecutorService asyncExecutor) {
    delegateRef.set(clientEntity);
    this.reconnectTask = reconnectTask;
    this.asyncExecutor = asyncExecutor;
  }

  public ClusterTierClientEntity delegate() {
    return delegateRef.get();
  }

  public void setDelegateRef(ClusterTierClientEntity clientEntity) {
    delegateRef.set(clientEntity);
  }

  @SuppressWarnings("unchecked")
  public void setUpReconnect(ClusterTierClientEntity newClientEntity, ServerStoreConfiguration serverStoreConfiguration,
    Connection connection) throws ClusterException, TimeoutException, ClusterTierException {
    ReconnectInProgressClusterTierClientEntity reconnectInProgressClusterTierClientEntity =
      (ReconnectInProgressClusterTierClientEntity) delegate();
    if (reconnectInProgressClusterTierClientEntity.checkIfClosed()) {
      newClientEntity.close();
      try {
        connection.close();
      } catch (IOException e) {
        LOGGER.debug("Exception closing cluster connection", e);
      }
    } else {
      newClientEntity.enableEvents(enableEventing);
      newClientEntity.validate(serverStoreConfiguration);

      responseListeners.forEach((k, v) -> {
        v.forEach(resp -> newClientEntity.addResponseListener(
          k, (ClusterTierClientEntity.ResponseListener) resp));
      });
      disconnectionListeners.forEach(newClientEntity::addDisconnectionListener);
      reconnectListeners.forEach(newClientEntity::addReconnectListener);
    }
    setDelegateRef(newClientEntity);
  }

  @Override
  public Timeouts getTimeouts() {
    return delegate().getTimeouts();
  }

  @Override
  public boolean isConnected() {
    return delegate().isConnected();
  }

  @Override
  public void validate(ServerStoreConfiguration clientStoreConfiguration) throws ClusterTierException, TimeoutException {
    try {
      onReconnectVariant(clientEntity -> {
        try {
          clientEntity.validate(clientStoreConfiguration);
        } catch (ClusterTierException e) {
          throw new InternalClusterTierValidationException(e);
        }
        return null;
      });
    } catch (InternalClusterTierValidationException e) {
      throw new ClusterTierValidationException(e);
    }
  }

  @Override
  public void invokeAndWaitForSend(EhcacheOperationMessage message,
    boolean track) throws ClusterException, TimeoutException {
    onReconnect(clientEntity -> {
      clientEntity.invokeAndWaitForSend(message, track);
      return null;
    });
  }

  @Override
  public void invokeAndWaitForReceive(EhcacheOperationMessage message, boolean track) throws ClusterException,
    TimeoutException {
    onReconnect(clientEntity -> {
      clientEntity.invokeAndWaitForReceive(message, track);
      return null;
    });
  }

  @Override
  public EhcacheEntityResponse invokeAndWaitForComplete(EhcacheOperationMessage message, boolean track)
    throws ClusterException, TimeoutException {
    return onReconnect(clientEntity -> clientEntity.invokeAndWaitForComplete(message, track));
  }

  @Override
  public EhcacheEntityResponse invokeAndWaitForRetired(EhcacheOperationMessage message, boolean track) throws
    ClusterException, TimeoutException {
    return onReconnect(clientEntity -> clientEntity.invokeAndWaitForRetired(message, track));
  }

  @Override
  public EhcacheEntityResponse invokeStateRepositoryOperation(StateRepositoryOpMessage message, boolean track) throws
    ClusterException, TimeoutException {
    return onReconnect(clientEntity -> clientEntity.invokeStateRepositoryOperation(message, track));
  }

  @Override
  public <T extends EhcacheEntityResponse> void addResponseListener(Class<T> responseType,
    ResponseListener<T> responseListener) {
    delegate().addResponseListener(responseType, responseListener);

    responseListeners.compute(responseType, (k, v) -> {
      if (v == null) {
        v = new CopyOnWriteArrayList<>();
      }
      v.add(responseListener);
      return v;
    });
  }

  @Override
  public void addDisconnectionListener(DisconnectionListener disconnectionListener) {
    delegate().addDisconnectionListener(disconnectionListener);
    disconnectionListeners.add(disconnectionListener);
  }

  @Override
  public void addReconnectListener(ReconnectListener reconnectListener) {
    delegate().addReconnectListener(reconnectListener);
    reconnectListeners.add(reconnectListener);
  }

  @Override
  public void enableEvents(boolean enable) throws ClusterException, TimeoutException {
    onReconnect(clientEntity -> {
      clientEntity.enableEvents(enable);
      return null;
    });
    enableEventing = enable;
  }

  @Override
  public void close() {
    try {
      delegate().close();
    } catch (Throwable t) {
      if (containsCause(t, ConnectionClosedException.class) || containsCause(t, ConnectionShutdownException.class)) {
        LOGGER.debug("Store was already closed, since connection was closed");
      } else {
        throw t;
      }
    }
  }

  private <T> T onReconnect(TimeoutAndClusterExceptionFunction<ClusterTierClientEntity, T> function) throws
    TimeoutException, ClusterException {
    ClusterTierClientEntity cl = delegate();
    try {
      return function.apply(cl);
    } catch (Exception sspe) {
      if (containsCause(sspe, ConnectionClosedException.class)) {
        if (delegateRef.compareAndSet(cl, new ReconnectInProgressClusterTierClientEntity())) {
          CompletableFuture.runAsync(reconnectTask, asyncExecutor);
        }
        return onReconnect(function);
      } else {
        throw sspe;
      }
    }
  }

  private <T> T onReconnectVariant(TimeoutExceptionFunction<ClusterTierClientEntity, T> function) throws
    TimeoutException {
    ClusterTierClientEntity cl = delegate();
    try {
      return function.apply(cl);
    } catch (Exception sspe) {
      if (containsCause(sspe, ConnectionClosedException.class)) {
        if (delegateRef.compareAndSet(cl, new ReconnectInProgressClusterTierClientEntity())) {
          CompletableFuture.runAsync(reconnectTask, asyncExecutor);
        }
        return onReconnectVariant(function);
      } else {
        throw sspe;
      }
    }
  }

  @FunctionalInterface
  private interface TimeoutAndClusterExceptionFunction<U, V> {
    V apply(U u) throws TimeoutException, ClusterException;
  }

  @FunctionalInterface
  private interface TimeoutExceptionFunction<U, V> {
    V apply(U u) throws TimeoutException;
  }

  private static class InternalClusterTierValidationException extends RuntimeException {
    private static final long serialVersionUID = -8059994461118117667L;

    InternalClusterTierValidationException(Throwable cause) {
      super(cause);
    }
  }
}
