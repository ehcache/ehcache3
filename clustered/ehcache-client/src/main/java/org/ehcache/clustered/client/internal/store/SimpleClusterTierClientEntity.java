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

package org.ehcache.clustered.client.internal.store;

import org.ehcache.clustered.client.config.Timeouts;
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
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage;
import org.ehcache.clustered.common.internal.messages.StateRepositoryOpMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.EndpointDelegate;
import org.terracotta.entity.EntityClientEndpoint;
import org.terracotta.entity.EntityResponse;
import org.terracotta.entity.InvocationCallback;

import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.terracotta.entity.InvocationCallback.Types.COMPLETE;
import static org.terracotta.entity.InvocationCallback.Types.RECEIVED;
import static org.terracotta.entity.InvocationCallback.Types.RETIRED;
import static org.terracotta.entity.InvocationCallback.Types.SENT;

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
  private volatile boolean eventsEnabled;

  private final Executor asyncWorker;

  public SimpleClusterTierClientEntity(EntityClientEndpoint<EhcacheEntityMessage, EhcacheEntityResponse> endpoint,
                                       Timeouts timeouts, String storeIdentifier, Executor asyncWorker) {
    this.endpoint = endpoint;
    this.timeouts = timeouts;
    this.storeIdentifier = storeIdentifier;
    this.asyncWorker = requireNonNull(asyncWorker);
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
          ClusterTierReconnectMessage reconnectMessage = new ClusterTierReconnectMessage(eventsEnabled);
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
      Runnable responseProcessing = () -> {
        try {
          responseListener.onResponse(response);
        } catch (TimeoutException e) {
          LOGGER.debug("Timeout exception processing: {} - resubmitting", response, e);
          fireResponseEvent(response);
        } catch (Exception e) {
          LOGGER.warn("Unhandled failure processing: {}", response, e);
        }
      };
      try {
        asyncWorker.execute(responseProcessing);
      } catch (RejectedExecutionException f) {
        LOGGER.warn("Response task execution rejected using inline execution: {}", response, f);
        responseProcessing.run();
      }
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
  public void enableEvents(boolean enable) throws ClusterException, TimeoutException {
    if (enable == this.eventsEnabled) {
      return;
    }
    // make sure the server received and processed the message before returning
    this.invokeAndWaitForComplete(new ServerStoreOpMessage.EnableEventListenerMessage(enable), true);
    this.eventsEnabled = enable;
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
  public void validate(ServerStoreConfiguration clientStoreConfiguration) throws ClusterTierValidationException, TimeoutException {
    try {
      invokeInternalAndWaitFor(COMPLETE, timeouts.getConnectionTimeout(), messageFactory.validateServerStore(storeIdentifier , clientStoreConfiguration));
    } catch (ClusterException e) {
      throw new ClusterTierValidationException("Error validating cluster tier '" + storeIdentifier + "'", e);
    }
  }

  @Override
  public EhcacheEntityResponse invokeStateRepositoryOperation(StateRepositoryOpMessage message, boolean track) throws ClusterException, TimeoutException {
    return invokeAndWaitForRetired(message, track);
  }

  @Override
  public void invokeAndWaitForSend(EhcacheOperationMessage message, boolean track) throws TimeoutException, ClusterException {
    invokeInternalAndWaitFor(SENT, getTimeoutDuration(message), message);
  }

  @Override
  public void invokeAndWaitForReceive(EhcacheOperationMessage message, boolean track)
          throws ClusterException, TimeoutException  {
    invokeInternalAndWaitFor(RECEIVED, message);
  }

  @Override
  public EhcacheEntityResponse invokeAndWaitForComplete(EhcacheOperationMessage message, boolean track)
    throws ClusterException, TimeoutException {
    return invokeInternalAndWaitFor(COMPLETE, message);
  }

  @Override
  public EhcacheEntityResponse invokeAndWaitForRetired(EhcacheOperationMessage message, boolean track)
    throws ClusterException, TimeoutException {
    return invokeInternalAndWaitFor(RETIRED, message);
  }

  private EhcacheEntityResponse invokeInternalAndWaitFor(InvocationCallback.Types type, EhcacheOperationMessage message)
      throws ClusterException, TimeoutException {
    return invokeInternalAndWaitFor(type, getTimeoutDuration(message), message);
  }

  private EhcacheEntityResponse invokeInternalAndWaitFor(InvocationCallback.Types type, Duration timeLimit, EhcacheEntityMessage message)
      throws ClusterException, TimeoutException {
    try {
      Future<EhcacheEntityResponse> future = endpoint.message(message).invokeAnd(type);
      EhcacheEntityResponse response = waitFor(timeLimit.toNanos(), future);
      if (response != null && EhcacheResponseType.FAILURE.equals(response.getResponseType())) {
        throw ((Failure)response).getCause();
      } else {
        return response;
      }
    } catch (ExecutionException e) {
      throw new RuntimeException(message + " error: " + e.getCause(), e.getCause());
    } catch (TimeoutException e) {
      String msg = "Timeout exceeded for " + message + " message; " + timeLimit;
      TimeoutException timeoutException = new TimeoutException(msg);
      timeoutException.initCause(e);
      LOGGER.info(msg, timeoutException);
      throw timeoutException;
    }
  }

  private Duration getTimeoutDuration(EhcacheOperationMessage message) {
    if (GET_STORE_OPS.contains(message.getMessageType())) {
      return timeouts.getReadOperationTimeout();
    } else {
      return timeouts.getWriteOperationTimeout();
    }
  }

  private static <T extends EntityResponse> T waitFor(long nanos, Future<T> future)
    throws ExecutionException, TimeoutException {
    boolean interrupted = Thread.interrupted();
    long deadlineTimeout = System.nanoTime() + nanos;
    try {
      while (true) {
        try {
          long timeRemaining = deadlineTimeout - System.nanoTime();
          return future.get(timeRemaining, NANOSECONDS);
        } catch (TimeoutException e) {
          future.cancel(true);
          throw e;
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
