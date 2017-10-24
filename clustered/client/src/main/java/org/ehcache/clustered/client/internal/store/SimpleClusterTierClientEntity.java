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

import org.ehcache.clustered.client.config.TimeoutDuration;
import org.ehcache.clustered.client.internal.Timeouts;
import org.ehcache.clustered.client.internal.service.ClusterTierException;
import org.ehcache.clustered.client.internal.service.ClusterTierValidationException;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.messages.ClusterTierReconnectMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.Failure;
import org.ehcache.clustered.common.internal.messages.EhcacheMessageType;
import org.ehcache.clustered.common.internal.messages.EhcacheOperationMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheResponseType;
import org.ehcache.clustered.common.internal.messages.LifeCycleMessageFactory;
import org.ehcache.clustered.common.internal.messages.ReconnectMessageCodec;
import org.ehcache.clustered.common.internal.messages.StateRepositoryOpMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.EndpointDelegate;
import org.terracotta.entity.EntityClientEndpoint;
import org.terracotta.entity.EntityResponse;
import org.terracotta.entity.InvocationBuilder;
import org.terracotta.entity.InvokeFuture;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.exception.EntityException;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * ClusterTierClientEntity
 */
public class SimpleClusterTierClientEntity implements InternalClusterTierClientEntity {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleClusterTierClientEntity.class);
  private static final Set<EhcacheMessageType> GET_STORE_OPS = EnumSet.of(EhcacheMessageType.GET_STORE);

  private final EntityClientEndpoint<EhcacheEntityMessage, EhcacheEntityResponse> endpoint;
  private final LifeCycleMessageFactory messageFactory;
  private final Object lock = new Object();
  private final ReconnectMessageCodec reconnectMessageCodec = new ReconnectMessageCodec();
  private final Map<Class<? extends EhcacheEntityResponse>, List<ResponseListener<? extends EhcacheEntityResponse>>> responseListeners =
    new ConcurrentHashMap<>();

  private ReconnectListener reconnectListener = reconnectMessage -> {
    // No op
  };
  private DisconnectionListener disconnectionListener = () -> {
    // No op
  };
  private Timeouts timeouts = Timeouts.builder().build();
  private String storeIdentifier;
  private volatile boolean connected = true;


  public SimpleClusterTierClientEntity(EntityClientEndpoint<EhcacheEntityMessage, EhcacheEntityResponse> endpoint) {
    this.endpoint = endpoint;
    this.messageFactory = new LifeCycleMessageFactory();
    endpoint.setDelegate(new EndpointDelegate<EhcacheEntityResponse>() {
      @Override
      public void handleMessage(EhcacheEntityResponse messageFromServer) {
        LOGGER.trace("Entity response received from server: {}", messageFromServer);
        fireResponseEvent(messageFromServer);
      }

      @Override
      public byte[] createExtendedReconnectData() {
        synchronized (lock) {
          ClusterTierReconnectMessage reconnectMessage = new ClusterTierReconnectMessage();
          reconnectListener.onHandleReconnect(reconnectMessage);
          return reconnectMessageCodec.encode(reconnectMessage);
        }
      }

      @Override
      public void didDisconnectUnexpectedly() {
        fireDisconnectionEvent();
      }
    });
  }

  @Override
  public void setTimeouts(Timeouts timeouts) {
    this.timeouts = timeouts;
  }

  void fireDisconnectionEvent() {
    connected = false;
    disconnectionListener.onDisconnection();
  }

  private <T extends EhcacheEntityResponse> void fireResponseEvent(T response) {
    @SuppressWarnings("unchecked")
    List<ResponseListener<T>> responseListeners = (List) this.responseListeners.get(response.getClass());
    if (responseListeners == null) {
      LOGGER.warn("Ignoring the response {} as no registered response listener could be found.", response);
      return;
    }
    LOGGER.debug("{} registered response listener(s) for {}", responseListeners.size(), response.getClass());
    for (ResponseListener<T> responseListener : responseListeners) {
      responseListener.onResponse(response);
    }
  }

  @Override
  public void close() {
    reconnectListener = null;
    disconnectionListener = null;
    endpoint.close();
  }

  @Override
  public Timeouts getTimeouts() {
    return timeouts;
  }

  @Override
  public void setReconnectListener(ReconnectListener reconnectListener) {
    this.reconnectListener = reconnectListener;
  }

  @Override
  public void setDisconnectionListener(DisconnectionListener disconnectionListener) {
    this.disconnectionListener = disconnectionListener;
  }

  @Override
  public boolean isConnected() {
    return connected;
  }

  @Override
  public <T extends EhcacheEntityResponse> void addResponseListener(Class<T> responseType, ResponseListener<T> responseListener) {
    List<ResponseListener<? extends EhcacheEntityResponse>> responseListeners = this.responseListeners.get(responseType);
    if (responseListeners == null) {
      responseListeners = new CopyOnWriteArrayList<>();
      this.responseListeners.put(responseType, responseListeners);
    }
    responseListeners.add(responseListener);
  }

  @Override
  public void validate(ServerStoreConfiguration clientStoreConfiguration) throws ClusterTierException, TimeoutException {
    try {
      invokeInternalAndWait(endpoint.beginInvoke(), timeouts.getLifecycleOperationTimeout(), messageFactory.validateServerStore(storeIdentifier , clientStoreConfiguration), false);
    } catch (ClusterException e) {
      throw new ClusterTierValidationException("Error validating cluster tier '" + storeIdentifier + "'", e);
    }
  }

  @Override
  public void setStoreIdentifier(String storeIdentifier) {
    this.storeIdentifier = storeIdentifier;
  }

  void setConnected(boolean connected) {
    this.connected = connected;
  }

  @Override
  public EhcacheEntityResponse invokeStateRepositoryOperation(StateRepositoryOpMessage message, boolean track) throws ClusterException, TimeoutException {
    return invokeAndWaitForRetired(message, track);
  }

  @Override
  public void invokeAndWaitForSend(EhcacheOperationMessage message, boolean track) throws ClusterException, TimeoutException {
    invokeInternal(endpoint.beginInvoke().ackSent(), getTimeoutDuration(message).toNanos(), message, track);
  }

  @Override
  public void invokeAndWaitForReceive(EhcacheOperationMessage message, boolean track)
    throws ClusterException, TimeoutException {
    invokeInternal(endpoint.beginInvoke().ackReceived(), getTimeoutDuration(message).toNanos(), message, track);
  }

  @Override
  public EhcacheEntityResponse invokeAndWaitForComplete(EhcacheOperationMessage message, boolean track)
    throws ClusterException, TimeoutException {
    return invokeInternalAndWait(endpoint.beginInvoke().blockGetOnRetire(false), message, track);
  }

  @Override
  public EhcacheEntityResponse invokeAndWaitForRetired(EhcacheOperationMessage message, boolean track)
    throws ClusterException, TimeoutException {
    return invokeInternalAndWait(endpoint.beginInvoke().blockGetOnRetire(true), message, track);
  }

  private EhcacheEntityResponse invokeInternalAndWait(InvocationBuilder<EhcacheEntityMessage, EhcacheEntityResponse> invocationBuilder, EhcacheOperationMessage message, boolean track)
      throws ClusterException, TimeoutException {
    return invokeInternalAndWait(invocationBuilder, getTimeoutDuration(message), message, track);
  }

  private EhcacheEntityResponse invokeInternalAndWait(InvocationBuilder<EhcacheEntityMessage, EhcacheEntityResponse> invocationBuilder, TimeoutDuration timeLimit, EhcacheEntityMessage message, boolean track)
      throws ClusterException, TimeoutException {
    try {
      long end = System.nanoTime() + timeLimit.toNanos();
      InvokeFuture<EhcacheEntityResponse> future = invokeInternal(invocationBuilder, end - System.nanoTime(), message, track);
      EhcacheEntityResponse response = waitFor(end - System.nanoTime(), future);
      if (EhcacheResponseType.FAILURE.equals(response.getResponseType())) {
        throw ((Failure)response).getCause();
      } else {
        return response;
      }
    } catch (EntityException e) {
      throw new RuntimeException(message + " error: " + e.toString(), e);
    } catch (TimeoutException e) {
      String msg = "Timeout exceeded for " + message + " message; " + timeLimit;
      TimeoutException timeoutException = new TimeoutException(msg);
      timeoutException.initCause(e);
      LOGGER.info(msg, timeoutException);
      throw timeoutException;
    }
  }

  private InvokeFuture<EhcacheEntityResponse> invokeInternal(InvocationBuilder<EhcacheEntityMessage, EhcacheEntityResponse> invocationBuilder, long timeout, EhcacheEntityMessage message, boolean track) throws TimeoutException {
    boolean interrupted = Thread.interrupted();
    try {
      long end = System.nanoTime() + timeout;
      while (true) {
        try {
          long nanos = end - System.nanoTime();
          if (nanos > 0) {
            return invocationBuilder.message(message).invokeWithTimeout(nanos, TimeUnit.NANOSECONDS);
          } else {
            throw new TimeoutException();
          }
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } catch (MessageCodecException e) {
      throw new RuntimeException(message + " error: " + e.toString(), e);
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private TimeoutDuration getTimeoutDuration(EhcacheOperationMessage message) {
    TimeoutDuration timeLimit = timeouts.getMutativeOperationTimeout();
    if (GET_STORE_OPS.contains(message.getMessageType())) {
      timeLimit = timeouts.getReadOperationTimeout();
    }
    return timeLimit;
  }

  private static <T extends EntityResponse> T waitFor(long nanos, InvokeFuture<T> future)
      throws EntityException, TimeoutException {
    boolean interrupted = false;
    long deadlineTimeout = System.nanoTime() + nanos;
    try {
      while (true) {
        try {
          long timeRemaining = deadlineTimeout - System.nanoTime();
          return future.getWithTimeout(timeRemaining, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
