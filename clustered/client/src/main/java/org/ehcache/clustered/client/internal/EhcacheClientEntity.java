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

package org.ehcache.clustered.client.internal;

import org.ehcache.clustered.client.config.TimeoutDuration;
import org.ehcache.clustered.client.internal.service.ClusteredTierDestructionException;
import org.ehcache.clustered.client.internal.service.ClusteredTierManagerConfigurationException;
import org.ehcache.clustered.client.internal.service.ClusteredTierCreationException;
import org.ehcache.clustered.client.internal.service.ClusteredTierManagerValidationException;
import org.ehcache.clustered.client.internal.service.ClusteredTierReleaseException;
import org.ehcache.clustered.client.internal.service.ClusteredTierValidationException;
import org.ehcache.clustered.common.internal.ClusteredEhcacheIdentity;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.exceptions.InvalidClientIdException;
import org.ehcache.clustered.common.internal.exceptions.ResourceBusyException;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.Failure;
import org.ehcache.clustered.common.internal.messages.EhcacheResponseType;
import org.ehcache.clustered.common.internal.messages.EhcacheMessageType;
import org.ehcache.clustered.common.internal.messages.EhcacheOperationMessage;
import org.ehcache.clustered.common.internal.messages.LifeCycleMessageFactory;
import org.ehcache.clustered.common.internal.messages.ReconnectMessage;
import org.ehcache.clustered.common.internal.messages.ReconnectMessageCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.connection.entity.Entity;
import org.terracotta.entity.EndpointDelegate;
import org.terracotta.entity.EntityClientEndpoint;
import org.terracotta.entity.EntityResponse;
import org.terracotta.entity.InvokeFuture;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.exception.EntityException;

import java.util.ArrayList;
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
 * The client-side {@link Entity} through which clustered cache operations are performed.
 * An instance of this class is created by the {@link EhcacheClientEntityService}.
 * The server-side partner is the {@code EhcacheActiveEntity}.
 */
public class EhcacheClientEntity implements Entity {

  private static final Logger LOGGER = LoggerFactory.getLogger(EhcacheClientEntity.class);

  public interface ResponseListener<T extends EhcacheEntityResponse> {
    void onResponse(T response);
  }

  public interface DisconnectionListener {
    void onDisconnection();
  }

  public interface ReconnectListener {
    void onHandleReconnect(ReconnectMessage reconnectMessage);
  }

  private final AtomicLong sequenceGenerator = new AtomicLong(0L);

  private final EntityClientEndpoint<EhcacheEntityMessage, EhcacheEntityResponse> endpoint;
  private final LifeCycleMessageFactory messageFactory;
  private final Map<Class<? extends EhcacheEntityResponse>, List<ResponseListener<? extends EhcacheEntityResponse>>> responseListeners = new ConcurrentHashMap<Class<? extends EhcacheEntityResponse>, List<ResponseListener<? extends EhcacheEntityResponse>>>();
  private final List<DisconnectionListener> disconnectionListeners = new CopyOnWriteArrayList<DisconnectionListener>();
  private final List<ReconnectListener> reconnectListeners = new ArrayList<ReconnectListener>();
  private final ReconnectMessageCodec reconnectMessageCodec = new ReconnectMessageCodec();
  private volatile boolean connected = true;
  private final Set<String> caches = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
  private final Object lock = new Object();
  private volatile UUID clientId;

  private Timeouts timeouts = Timeouts.builder().build();

  public EhcacheClientEntity(EntityClientEndpoint<EhcacheEntityMessage, EhcacheEntityResponse> endpoint) {
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
          ReconnectMessage reconnectMessage = new ReconnectMessage(clientId, caches);
          for (ReconnectListener reconnectListener : reconnectListeners) {
            reconnectListener.onHandleReconnect(reconnectMessage);
          }
          return reconnectMessageCodec.encode(reconnectMessage);
        }
      }

      @Override
      public void didDisconnectUnexpectedly() {
        fireDisconnectionEvent();
      }
    });
  }

  void fireDisconnectionEvent() {
    connected = false;
    for (DisconnectionListener listener : disconnectionListeners) {
      listener.onDisconnection();
    }
  }

  void setConnected(boolean connected) {
    this.connected = connected;
  }

  void setTimeouts(Timeouts timeouts) {
    this.timeouts = timeouts;
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

  public UUID getClientId() {
    if (clientId == null) {
      throw new IllegalStateException("Client Id cannot be null");
    }
    return this.clientId;
  }

  public boolean isConnected() {
    return connected;
  }

  public void addDisconnectionListener(DisconnectionListener listener) {
    disconnectionListeners.add(listener);
  }

  public void removeDisconnectionListener(DisconnectionListener listener) {
    disconnectionListeners.remove(listener);
  }

  public List<DisconnectionListener> getDisconnectionListeners() {
    return Collections.unmodifiableList(disconnectionListeners);
  }

  public void addReconnectListener(ReconnectListener listener) {
    synchronized (lock) {
      reconnectListeners.add(listener);
    }
  }

  public void removeReconnectListener(ReconnectListener listener) {
    synchronized (lock) {
      reconnectListeners.remove(listener);
    }
  }

  public List<ReconnectListener> getReconnectListeners() {
    return Collections.unmodifiableList(reconnectListeners);
  }

  public <T extends EhcacheEntityResponse> void addResponseListener(Class<T> responseType, ResponseListener<T> responseListener) {
    List<ResponseListener<? extends EhcacheEntityResponse>> responseListeners = this.responseListeners.get(responseType);
    if (responseListeners == null) {
      responseListeners = new CopyOnWriteArrayList<ResponseListener<? extends EhcacheEntityResponse>>();
      this.responseListeners.put(responseType, responseListeners);
    }
    responseListeners.add(responseListener);
  }

  public <T extends EhcacheEntityResponse> void removeResponseListener(Class<T> responseType, ResponseListener<T> responseListener) {
    List<ResponseListener<? extends EhcacheEntityResponse>> responseListeners = this.responseListeners.get(responseType);
    if (responseListeners != null) {
      responseListeners.remove(responseListener);
    }
  }

  public UUID identity() {
    return ClusteredEhcacheIdentity.deserialize(endpoint.getEntityConfiguration());
  }

  @Override
  public void close() {
    endpoint.close();
    this.responseListeners.clear();
    this.disconnectionListeners.clear();
    this.reconnectListeners.clear();
  }

  public void validate(ServerSideConfiguration config) throws ClusteredTierManagerValidationException, TimeoutException {
    try {
      while (true) {
        try {
          clientId = UUID.randomUUID();
          this.messageFactory.setClientId(clientId);
          invokeInternal(timeouts.getLifecycleOperationTimeout(), messageFactory.validateStoreManager(config), false);
          break;
        } catch (InvalidClientIdException e) {
          //nothing to do - loop again since the earlier generated UUID is being already tracked by the server
        }
      }
    } catch (ClusterException e) {
      throw new ClusteredTierManagerValidationException("Error validating server clustered tier manager", e);
    }
  }

  public void configure(ServerSideConfiguration config) throws ClusteredTierManagerConfigurationException, TimeoutException {
    try {
      clientId = UUID.randomUUID();
      this.messageFactory.setClientId(clientId);
      invokeInternal(timeouts.getLifecycleOperationTimeout(), messageFactory.configureStoreManager(config), true);
    } catch (ClusterException e) {
      throw new ClusteredTierManagerConfigurationException("Error configuring clustered tier manager", e);
    }
  }

  public void createCache(String name, ServerStoreConfiguration serverStoreConfiguration)
      throws ClusteredTierCreationException, TimeoutException {
    try {
      invokeInternal(timeouts.getLifecycleOperationTimeout(), messageFactory.createServerStore(name, serverStoreConfiguration), true);
      caches.add(name);
    } catch (ClusterException e) {
      throw new ClusteredTierCreationException("Error creating clustered tier '" + name + "'", e);
    }
  }

  public void validateCache(String name, ServerStoreConfiguration serverStoreConfiguration)
      throws ClusteredTierValidationException, TimeoutException {
    try {
      invokeInternal(timeouts.getLifecycleOperationTimeout(), messageFactory.validateServerStore(name , serverStoreConfiguration), false);
      caches.add(name);
    } catch (ClusterException e) {
      throw new ClusteredTierValidationException("Error validating clustered tier '" + name + "'", e);
    }
  }

  public void releaseCache(String name) throws ClusteredTierReleaseException, TimeoutException {
    try {
      invokeInternal(timeouts.getLifecycleOperationTimeout(), messageFactory.releaseServerStore(name), false);
      caches.remove(name);
    } catch (ClusterException e) {
      throw new ClusteredTierReleaseException("Error releasing clustered tier '" + name + "'", e);
    }
  }

  public void destroyCache(String name) throws ClusteredTierDestructionException, TimeoutException {
    try {
      invokeInternal(timeouts.getLifecycleOperationTimeout(), messageFactory.destroyServerStore(name), true);
    } catch (ResourceBusyException e) {
      throw new ClusteredTierDestructionException(e.getMessage(), e);
    } catch (ClusterException e) {
      throw new ClusteredTierDestructionException("Error destroying clustered tier '" + name + "'", e);
    }
  }

  /**
   * Sends a message to the {@code EhcacheActiveEntity} associated with this {@code EhcacheClientEntity} and
   * awaits a response.
   *
   * @param message the {@code EhcacheEntityMessage} to send
   * @param replicate if {@code true}, indicates that the message should be replicated to passive servers
   *
   * @return an {@code EhcacheEntityResponse} holding a successful response from the server for {@code message}
   *
   * @throws ClusterException thrown to reflect a server-side operation fault
   * @throws TimeoutException if the server interactions take longer than the timeout configured for the operation
   */
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

  private static final Set<EhcacheMessageType> GET_STORE_OPS = EnumSet.of(EhcacheMessageType.GET_STORE);

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

  /**
   * Describes the timeouts for {@link EhcacheClientEntity} operations.  Use
   * {@link #builder()} to construct an instance.
   */
  public static final class Timeouts {

    public static final TimeoutDuration DEFAULT_READ_OPERATION_TIMEOUT = TimeoutDuration.of(20, TimeUnit.SECONDS);

    private final TimeoutDuration readOperationTimeout;
    private final TimeoutDuration mutativeOperationTimeout;
    private final TimeoutDuration lifecycleOperationTimeout;

    private Timeouts(TimeoutDuration readOperationTimeout, TimeoutDuration mutativeOperationTimeout, TimeoutDuration lifecycleOperationTimeout) {
      this.readOperationTimeout = readOperationTimeout;
      this.mutativeOperationTimeout = mutativeOperationTimeout;
      this.lifecycleOperationTimeout = lifecycleOperationTimeout;
    }

    public TimeoutDuration getReadOperationTimeout() {
      return readOperationTimeout;
    }

    public TimeoutDuration getMutativeOperationTimeout() {
      return mutativeOperationTimeout;
    }

    public TimeoutDuration getLifecycleOperationTimeout() {
      return lifecycleOperationTimeout;
    }

    public static Builder builder() {
      return new Builder();
    }

    @Override
    public String toString() {
      return "Timeouts{" +
          "readOperationTimeout=" + readOperationTimeout +
          ", mutativeOperationTimeout=" + mutativeOperationTimeout +
          ", lifecycleOperationTimeout=" + lifecycleOperationTimeout +
          '}';
    }

    /**
     * Constructs instances of {@link Timeouts}.  When obtained from
     * {@link Timeouts#builder()}, the default values are pre-set.
     */
    public static final class Builder {
      private TimeoutDuration readOperationTimeout = DEFAULT_READ_OPERATION_TIMEOUT;
      private TimeoutDuration mutativeOperationTimeout = TimeoutDuration.of(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
      private TimeoutDuration lifecycleOperationTimeout = TimeoutDuration.of(20, TimeUnit.SECONDS);

      /**
       * Sets the timeout for read operations.  The default value for this timeout is
       * 5 seconds.
       *
       * @param readOperationTimeout the {@code TimeoutDuration} to use for the read operation timeout
       *
       * @return this {@code Builder}
       */
      public Builder setReadOperationTimeout(TimeoutDuration readOperationTimeout) {
        if (readOperationTimeout == null) {
          throw new NullPointerException("readOperationTimeout");
        }
        this.readOperationTimeout = readOperationTimeout;
        return this;
      }

      /**
       * Sets the timeout for mutative operations like {@code put} and {@code remove}.  The default value
       * for this timeout is {@link TimeoutDuration#NONE}.
       *
       * @param mutativeOperationTimeout the {@code TimeoutDuration} to use for a mutative operation timeout
       *
       * @return this {@code Builder}
       */
      public Builder setMutativeOperationTimeout(TimeoutDuration mutativeOperationTimeout) {
        if (mutativeOperationTimeout == null) {
          throw new NullPointerException("mutativeOperationTimeout");
        }
        this.mutativeOperationTimeout = mutativeOperationTimeout;
        return this;
      }

      /**
       * Sets the timeout for server store manager lifecycle operations like {@code validate} and {@code validateCache}.
       *
       * @param lifecycleOperationTimeout the {@code TimeoutDuration} to use for a store manager lifecycle operation timeout
       *
       * @return this {@code Builder}
       */
      public Builder setLifecycleOperationTimeout(TimeoutDuration lifecycleOperationTimeout) {
        if (lifecycleOperationTimeout == null) {
          throw new NullPointerException("lifecycleOperationTimeout");
        }
        this.lifecycleOperationTimeout = lifecycleOperationTimeout;
        return this;
      }

      /**
       * Gets a new {@link Timeouts} instance using the current timeout duration settings.
       *
       * @return a new {@code Timeouts} instance
       */
      public Timeouts build() {
        return new Timeouts(readOperationTimeout, mutativeOperationTimeout, lifecycleOperationTimeout);
      }
    }
  }
}
