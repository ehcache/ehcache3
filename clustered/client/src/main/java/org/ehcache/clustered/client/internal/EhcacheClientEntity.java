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
import org.ehcache.clustered.common.internal.exceptions.ResourceBusyException;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.Failure;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.Type;
import org.ehcache.clustered.common.internal.messages.LifeCycleMessageFactory;
import org.ehcache.clustered.common.internal.messages.LifecycleMessage;
import org.ehcache.clustered.common.internal.messages.ReconnectDataCodec;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.ServerStoreOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.connection.entity.Entity;
import org.terracotta.entity.EndpointDelegate;
import org.terracotta.entity.EntityClientEndpoint;
import org.terracotta.entity.EntityResponse;
import org.terracotta.entity.InvokeFuture;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.exception.EntityException;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.ServerStoreOp.GET;
import static org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.ServerStoreOp.getServerStoreOp;

/**
 * The client-side {@link Entity} through which clustered cache operations are performed.
 * An instance of this class is created by the {@link EhcacheClientEntityService}.
 * The server-side partner is the {@code EhcacheActiveEntity}.
 */
public class EhcacheClientEntity implements Entity {

  private static final Logger LOGGER = LoggerFactory.getLogger(EhcacheClientEntity.class);

  private Set<String> reconnectData = new HashSet<String>();
  private int reconnectDatalen = 0;

  public interface ResponseListener<T extends EhcacheEntityResponse> {
    void onResponse(T response);
  }

  public interface DisconnectionListener {
    void onDisconnection();
  }

  private final EntityClientEndpoint<EhcacheEntityMessage, EhcacheEntityResponse> endpoint;
  private final LifeCycleMessageFactory messageFactory;
  private final Map<Class<? extends EhcacheEntityResponse>, List<ResponseListener<? extends EhcacheEntityResponse>>> responseListeners = new ConcurrentHashMap<Class<? extends EhcacheEntityResponse>, List<ResponseListener<? extends EhcacheEntityResponse>>>();
  private final List<DisconnectionListener> disconnectionListeners = new CopyOnWriteArrayList<DisconnectionListener>();
  private final ReconnectDataCodec reconnectDataCodec = new ReconnectDataCodec();
  private volatile boolean connected = true;

  private Timeouts timeouts = Timeouts.builder().build();

  public EhcacheClientEntity(EntityClientEndpoint<EhcacheEntityMessage, EhcacheEntityResponse> endpoint) {
    this.endpoint = endpoint;
    this.messageFactory = new LifeCycleMessageFactory();
    endpoint.setDelegate(new EndpointDelegate() {
      @Override
      public void handleMessage(EntityResponse messageFromServer) {
        if (messageFromServer instanceof EhcacheEntityResponse) {
          fireResponseEvent((EhcacheEntityResponse) messageFromServer);
        }
      }

      @Override
      public byte[] createExtendedReconnectData() {
        return reconnectDataCodec.encode(reconnectData, reconnectDatalen);
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

  private void fireResponseEvent(EhcacheEntityResponse response) {
    List<ResponseListener<? extends EhcacheEntityResponse>> responseListeners = this.responseListeners.get(response.getClass());
    if (responseListeners == null) {
      return;
    }
    LOGGER.debug("{} registered response listener(s) for {}", responseListeners.size(), response.getClass());
    for (ResponseListener responseListener : responseListeners) {
      responseListener.onResponse(response);
    }
  }

  public boolean isConnected() {
    return connected;
  }

  public void addDisconnectionListener(DisconnectionListener listener) {
    disconnectionListeners.add(listener);
  }

  public <T extends EhcacheEntityResponse> void addResponseListener(Class<T> responseType, ResponseListener<T> responseListener) {
    List<ResponseListener<? extends EhcacheEntityResponse>> responseListeners = this.responseListeners.get(responseType);
    if (responseListeners == null) {
      responseListeners = new CopyOnWriteArrayList<ResponseListener<? extends EhcacheEntityResponse>>();
      this.responseListeners.put(responseType, responseListeners);
    }
    responseListeners.add(responseListener);
  }

  public UUID identity() {
    return ClusteredEhcacheIdentity.deserialize(endpoint.getEntityConfiguration());
  }

  @Override
  public void close() {
    endpoint.close();
  }

  public void validate(ServerSideConfiguration config) throws ClusteredTierManagerValidationException, TimeoutException {
    try {
      invokeInternal(timeouts.getLifecycleOperationTimeout(), messageFactory.validateStoreManager(config), false);
    } catch (ClusterException e) {
      throw new ClusteredTierManagerValidationException("Error validating server clustered tier manager", e);
    }
  }

  public void configure(ServerSideConfiguration config) throws ClusteredTierManagerConfigurationException, TimeoutException {
    try {
      invokeInternal(timeouts.getLifecycleOperationTimeout(), messageFactory.configureStoreManager(config), true);
    } catch (ClusterException e) {
      throw new ClusteredTierManagerConfigurationException("Error configuring clustered tier manager", e);
    }
  }

  public void createCache(String name, ServerStoreConfiguration serverStoreConfiguration)
      throws ClusteredTierCreationException, TimeoutException {
    try {
      invokeInternal(timeouts.getLifecycleOperationTimeout(), messageFactory.createServerStore(name, serverStoreConfiguration), true);
      addReconnectData(name);
    } catch (ClusterException e) {
      throw new ClusteredTierCreationException("Error creating clustered tier '" + name + "'", e);
    }
  }

  public void validateCache(String name, ServerStoreConfiguration serverStoreConfiguration)
      throws ClusteredTierValidationException, TimeoutException {
    try {
      invokeInternal(timeouts.getLifecycleOperationTimeout(), messageFactory.validateServerStore(name , serverStoreConfiguration), false);
      addReconnectData(name);
    } catch (ClusterException e) {
      throw new ClusteredTierValidationException("Error validating clustered tier '" + name + "'", e);
    }
  }

  public void releaseCache(String name) throws ClusteredTierReleaseException, TimeoutException {
    try {
      invokeInternal(timeouts.getLifecycleOperationTimeout(), messageFactory.releaseServerStore(name), false);
      removeReconnectData(name);
    } catch (ClusterException e) {
      throw new ClusteredTierReleaseException("Error releasing clustered tier '" + name + "'", e);
    }
  }

  public void destroyCache(String name) throws ClusteredTierDestructionException, TimeoutException {
    try {
      invokeInternal(timeouts.getLifecycleOperationTimeout(), messageFactory.destroyServerStore(name), true);
      removeReconnectData(name);
    } catch (ResourceBusyException e) {
      throw new ClusteredTierDestructionException(e.getMessage(), e);
    } catch (ClusterException e) {
      throw new ClusteredTierDestructionException("Error destroying clustered tier '" + name + "'", e);
    }
  }

  private void addReconnectData(String name) {
    reconnectData.add(name);
    reconnectDatalen += name.length();
  }

  private void removeReconnectData(String name) {
    if (!reconnectData.contains(name)) {
      reconnectData.remove(name);
      reconnectDatalen -= name.length();
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
    TimeoutDuration timeLimit;
    if (message.getType() == EhcacheEntityMessage.Type.SERVER_STORE_OP
        && GET_STORE_OPS.contains(getServerStoreOp(message.getOpCode()))) {
      timeLimit = timeouts.getReadOperationTimeout();
    } else {
      timeLimit = timeouts.getMutativeOperationTimeout();
    }
    return invokeInternal(timeLimit, message, replicate);
  }

  private static final Set<ServerStoreOp> GET_STORE_OPS = EnumSet.of(GET);

  private EhcacheEntityResponse invokeInternal(TimeoutDuration timeLimit, EhcacheEntityMessage message, boolean replicate)
      throws ClusterException, TimeoutException {

    try {
      EhcacheEntityResponse response = waitFor(timeLimit, invokeAsync(message, replicate));
      if (Type.FAILURE.equals(response.getType())) {
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
    if (replicate) {
      return endpoint.beginInvoke().message(message).replicate(true).invoke(); //TODO: remove replicate call once
      //https://github.com/Terracotta-OSS/terracotta-apis/issues/139 is fixed
    } else {
      return endpoint.beginInvoke().message(message).replicate(false).invoke();
    }
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

    public static final TimeoutDuration DEFAULT_READ_OPERATION_TIMEOUT = TimeoutDuration.of(5, TimeUnit.SECONDS);

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
