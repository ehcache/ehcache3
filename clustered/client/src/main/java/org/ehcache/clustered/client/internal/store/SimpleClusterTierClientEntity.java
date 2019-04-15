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

import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.ehcache.clustered.client.config.Timeouts.nanosStartingFromNow;

/**
 * ClusterTierClientEntity
 */
public class SimpleClusterTierClientEntity implements InternalClusterTierClientEntity {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleClusterTierClientEntity.class);
  private static final Set<EhcacheMessageType> GET_STORE_OPS = EnumSet.of(
    EhcacheMessageType.GET_STORE,
    EhcacheMessageType.ITERATOR_ADVANCE,
    EhcacheMessageType.ITERATOR_OPEN,
    EhcacheMessageType.ITERATOR_CLOSE);

  private final EntityClientEndpoint<EhcacheEntityMessage, EhcacheEntityResponse> endpoint;
  private final LifeCycleMessageFactory messageFactory;
  private final Object lock = new Object();
  private final ReconnectMessageCodec reconnectMessageCodec = new ReconnectMessageCodec();
  private final Map<Class<? extends EhcacheEntityResponse>, List<ResponseListener<? extends EhcacheEntityResponse>>> responseListeners =
    new ConcurrentHashMap<>();
  private final List<DisconnectionListener> disconnectionListeners = new CopyOnWriteArrayList<>();
  private final Timeouts timeouts;
  private final String storeIdentifier;

  private final List<ReconnectListener> reconnectListeners = new CopyOnWriteArrayList<>();

  private volatile boolean connected = true;

  public SimpleClusterTierClientEntity(EntityClientEndpoint<EhcacheEntityMessage, EhcacheEntityResponse> endpoint,
                                       Timeouts timeouts, String storeIdentifier) {
    this.endpoint = endpoint;
    this.timeouts = timeouts;
    this.storeIdentifier = storeIdentifier;
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
          reconnectListeners.forEach(reconnectListener -> reconnectListener.onHandleReconnect(reconnectMessage));
          return reconnectMessageCodec.encode(reconnectMessage);
        }
      }

      @Override
      public void didDisconnectUnexpectedly() {
        LOGGER.info("Cluster tier for cache {} disconnected", storeIdentifier);
        fireDisconnectionEvent();
      }
    });
  }

  void fireDisconnectionEvent() {
    connected = false;
    disconnectionListeners.forEach(DisconnectionListener::onDisconnection);
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
    endpoint.close();
    reconnectListeners.clear();
    disconnectionListeners.clear();
  }

  @Override
  public Timeouts getTimeouts() {
    return timeouts;
  }

  @Override
  public void addReconnectListener(ReconnectListener reconnectListener) {
    this.reconnectListeners.add(reconnectListener);
  }

  @Override
  public void addDisconnectionListener(DisconnectionListener disconnectionListener) {
    this.disconnectionListeners.add(disconnectionListener);
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
      invokeInternalAndWait(endpoint.beginInvoke(), timeouts.getConnectionTimeout(), messageFactory.validateServerStore(storeIdentifier , clientStoreConfiguration), false);
    } catch (ClusterException e) {
      throw new ClusterTierValidationException("Error validating cluster tier '" + storeIdentifier + "'", e);
    }
  }

  @Override
  public EhcacheEntityResponse invokeStateRepositoryOperation(StateRepositoryOpMessage message, boolean track) throws ClusterException, TimeoutException {
    return invokeAndWaitForRetired(message, track);
  }

  @Override
  public void invokeAndWaitForSend(EhcacheOperationMessage message, boolean track) throws TimeoutException {
    invokeInternal(endpoint.beginInvoke().ackSent(), getTimeoutDuration(message), message, track);
  }

  @Override
  public void invokeAndWaitForReceive(EhcacheOperationMessage message, boolean track)
          throws ClusterException, TimeoutException  {
    invokeInternalAndWait(endpoint.beginInvoke().ackReceived(), message, track);
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

  private EhcacheEntityResponse invokeInternalAndWait(InvocationBuilder<EhcacheEntityMessage, EhcacheEntityResponse> invocationBuilder, Duration timeLimit, EhcacheEntityMessage message, boolean track)
      throws ClusterException, TimeoutException {
    try {
      LongSupplier nanosRemaining = nanosStartingFromNow(timeLimit);
      InvokeFuture<EhcacheEntityResponse> future = invokeInternal(invocationBuilder, Duration.ofNanos(nanosRemaining.getAsLong()), message, track);
      EhcacheEntityResponse response = waitFor(nanosRemaining.getAsLong(), future);
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

  private InvokeFuture<EhcacheEntityResponse> invokeInternal(InvocationBuilder<EhcacheEntityMessage, EhcacheEntityResponse> invocationBuilder, Duration timeout, EhcacheEntityMessage message, boolean track) throws TimeoutException {
    boolean interrupted = Thread.interrupted();
    try {
      LongSupplier nanosRemaining = nanosStartingFromNow(timeout);
      while (true) {
        try {
          long nanos = nanosRemaining.getAsLong();
          if (nanos > 0) {
            return invocationBuilder.message(message).invokeWithTimeout(nanos, NANOSECONDS);
          } else {
            throw new TimeoutException("Timed out waiting for server response to message: " + message);
          }
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } catch (MessageCodecException e) {
      throw new RuntimeException(message + " error: " + e.getMessage(), e);
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private Duration getTimeoutDuration(EhcacheOperationMessage message) {
    if (GET_STORE_OPS.contains(message.getMessageType())) {
      return timeouts.getReadOperationTimeout();
    } else {
      return timeouts.getWriteOperationTimeout();
    }
  }

  private static <T extends EntityResponse> T waitFor(long nanos, InvokeFuture<T> future)
      throws EntityException, TimeoutException {
    boolean interrupted = false;
    long deadlineTimeout = System.nanoTime() + nanos;
    try {
      while (true) {
        try {
          long timeRemaining = deadlineTimeout - System.nanoTime();
          return future.getWithTimeout(timeRemaining, NANOSECONDS);
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
