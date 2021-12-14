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
import org.ehcache.clustered.client.internal.service.ClusteredTierException;
import org.ehcache.clustered.client.internal.service.ClusteredTierValidationException;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.Failure;
import org.ehcache.clustered.common.internal.messages.EhcacheMessageType;
import org.ehcache.clustered.common.internal.messages.EhcacheOperationMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheResponseType;
import org.ehcache.clustered.common.internal.messages.LifeCycleMessageFactory;
import org.ehcache.clustered.common.internal.messages.ReconnectMessage;
import org.ehcache.clustered.common.internal.messages.ReconnectMessageCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.EndpointDelegate;
import org.terracotta.entity.EntityClientEndpoint;
import org.terracotta.entity.EntityResponse;
import org.terracotta.entity.InvokeFuture;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.exception.EntityException;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ClusteredTierClientEntity
 */
public class SimpleClusteredTierClientEntity implements InternalClusterTierClientEntity {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleClusteredTierClientEntity.class);
  private static final Set<EhcacheMessageType> GET_STORE_OPS = EnumSet.of(EhcacheMessageType.GET_STORE);

  private final EntityClientEndpoint<EhcacheEntityMessage, EhcacheEntityResponse> endpoint;
  private final LifeCycleMessageFactory messageFactory;
  private final AtomicLong sequenceGenerator = new AtomicLong(0L);
  private final Object lock = new Object();
  private final ReconnectMessageCodec reconnectMessageCodec = new ReconnectMessageCodec();
  private final Map<Class<? extends EhcacheEntityResponse>, List<ResponseListener<? extends EhcacheEntityResponse>>> responseListeners =
    new ConcurrentHashMap<Class<? extends EhcacheEntityResponse>, List<ResponseListener<? extends EhcacheEntityResponse>>>();

  private UUID clientId;
  private ReconnectListener reconnectListener = new ReconnectListener() {
    @Override
    public void onHandleReconnect(ReconnectMessage reconnectMessage) {
      // No op
    }
  };
  private DisconnectionListener disconnectionListener = new DisconnectionListener() {
    @Override
    public void onDisconnection() {
      // No op
    }
  };
  private Timeouts timeouts = Timeouts.builder().build();
  private String storeIdentifier;
  private volatile boolean connected = true;


  public SimpleClusteredTierClientEntity(EntityClientEndpoint<EhcacheEntityMessage, EhcacheEntityResponse> endpoint) {
    this.endpoint = endpoint;
    this.messageFactory = new LifeCycleMessageFactory();
    endpoint.setDelegate(new EndpointDelegate() {
      @Override
      public void handleMessage(EntityResponse messageFromServer) {
        LOGGER.trace("Entity response received from server: {}", messageFromServer);
        if (messageFromServer instanceof EhcacheEntityResponse) {
          fireResponseEvent((EhcacheEntityResponse) messageFromServer);
        }
      }

      @Override
      public byte[] createExtendedReconnectData() {
        synchronized (lock) {
          ReconnectMessage reconnectMessage = new ReconnectMessage(clientId, Collections.<String>emptySet());
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
  public void setClientId(UUID clientId) {
    this.clientId = clientId;
    messageFactory.setClientId(clientId);
  }

  @Override
  public UUID getClientId() {
    if (clientId == null) {
      throw new IllegalStateException("Client Id cannot be null");
    }
    return this.clientId;
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
      responseListeners = new CopyOnWriteArrayList<ResponseListener<? extends EhcacheEntityResponse>>();
      this.responseListeners.put(responseType, responseListeners);
    }
    responseListeners.add(responseListener);
  }

  @Override
  public void validate(ServerStoreConfiguration clientStoreConfiguration) throws ClusteredTierException, TimeoutException {
    try {
      invokeInternal(timeouts.getLifecycleOperationTimeout(), messageFactory.validateServerStore(storeIdentifier , clientStoreConfiguration), false);
    } catch (ClusterException e) {
      throw new ClusteredTierValidationException("Error validating clustered tier '" + storeIdentifier + "'", e);
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
  public EhcacheEntityResponse invoke(EhcacheEntityMessage message, boolean replicate)
      throws ClusterException, TimeoutException {
    TimeoutDuration timeLimit = timeouts.getMutativeOperationTimeout();
    if (message instanceof EhcacheOperationMessage) {
      if (GET_STORE_OPS.contains(((EhcacheOperationMessage) message).getMessageType())) {
        timeLimit = timeouts.getReadOperationTimeout();
      }
    }
    return invokeInternal(timeLimit, message, replicate);
  }

  private EhcacheEntityResponse invokeInternal(TimeoutDuration timeLimit, EhcacheEntityMessage message, boolean replicate)
      throws ClusterException, TimeoutException {

    try {
      EhcacheEntityResponse response = waitFor(timeLimit, invokeAsync(message, replicate));
      if (EhcacheResponseType.FAILURE.equals(response.getResponseType())) {
        throw ((Failure)response).getCause();
      } else {
        return response;
      }
    } catch (EntityException e) {
      throw new RuntimeException(message + " error: " + e.toString(), e);
    } catch (MessageCodecException e) {
      throw new RuntimeException(message + " error: " + e.toString(), e);
    } catch (TimeoutException e) {
      String msg = "Timeout exceeded for " + message + " message; " + timeLimit;
      TimeoutException timeoutException = new TimeoutException(msg);
      timeoutException.initCause(e);
      LOGGER.info(msg, timeoutException);
      throw timeoutException;
    }
  }

  @Override
  public InvokeFuture<EhcacheEntityResponse> invokeAsync(EhcacheEntityMessage message, boolean replicate)
      throws MessageCodecException {
    getClientId();
    if (replicate) {
      message.setId(sequenceGenerator.getAndIncrement());
    }
    return endpoint.beginInvoke().message(message).replicate(replicate).invoke();
  }

  private static <T> T waitFor(TimeoutDuration timeLimit, InvokeFuture<T> future)
      throws EntityException, TimeoutException {
    boolean interrupted = false;
    long deadlineTimeout = System.nanoTime() + timeLimit.toNanos();
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
