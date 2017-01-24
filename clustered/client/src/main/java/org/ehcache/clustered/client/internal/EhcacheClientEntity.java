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
import org.ehcache.clustered.client.internal.service.ClusteredTierCreationException;
import org.ehcache.clustered.client.internal.service.ClusteredTierManagerValidationException;
import org.ehcache.clustered.client.internal.service.ClusteredTierReleaseException;
import org.ehcache.clustered.client.internal.service.ClusteredTierValidationException;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.exceptions.InvalidClientIdException;
import org.ehcache.clustered.common.internal.exceptions.ResourceBusyException;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.Failure;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.PrepareForDestroy;
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

  private final AtomicLong sequenceGenerator = new AtomicLong(0L);

  private final ReconnectMessageCodec reconnectMessageCodec = new ReconnectMessageCodec();
  private final EntityClientEndpoint<EhcacheEntityMessage, EhcacheEntityResponse> endpoint;
  private final LifeCycleMessageFactory messageFactory;
  private volatile UUID clientId;

  private Timeouts timeouts = Timeouts.builder().build();

  public EhcacheClientEntity(EntityClientEndpoint<EhcacheEntityMessage, EhcacheEntityResponse> endpoint) {
    this.endpoint = endpoint;
    this.messageFactory = new LifeCycleMessageFactory();
    endpoint.setDelegate(new EndpointDelegate() {
      @Override
      public void handleMessage(EntityResponse messageFromServer) {
        // Nothing to do
      }

      @Override
      public byte[] createExtendedReconnectData() {
        ReconnectMessage reconnectMessage = new ReconnectMessage(clientId);
        return reconnectMessageCodec.encode(reconnectMessage);
      }

      @Override
      public void didDisconnectUnexpectedly() {
        // Nothing to do
      }
    });
  }

  void setTimeouts(Timeouts timeouts) {
    this.timeouts = timeouts;
  }

  public UUID getClientId() {
    if (clientId == null) {
      throw new IllegalStateException("Client Id cannot be null");
    }
    return this.clientId;
  }

  @Override
  public void close() {
    endpoint.close();
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

  public Set<String> prepareForDestroy() {
    try {
      PrepareForDestroy response = (PrepareForDestroy) invokeInternal(timeouts.getLifecycleOperationTimeout(), messageFactory
        .prepareForDestroy(), true);
      return response.getStores();
    } catch (ClusterException e) {
      // TODO handle this
    } catch (TimeoutException e) {
      // TODO handle this
    }
    return null;
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
