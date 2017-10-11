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
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.exceptions.InvalidClientIdException;
import org.ehcache.clustered.common.internal.messages.ClusterTierManagerReconnectMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.Failure;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.PrepareForDestroy;
import org.ehcache.clustered.common.internal.messages.EhcacheResponseType;
import org.ehcache.clustered.common.internal.messages.LifeCycleMessageFactory;
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

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The client-side {@link Entity} through which clustered cache operations are performed.
 * An instance of this class is created by the {@link ClusterTierManagerClientEntityService}.
 * The server-side partner is the {@code EhcacheActiveEntity}.
 */
public class SimpleClusterTierManagerClientEntity implements InternalClusterTierManagerClientEntity {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleClusterTierManagerClientEntity.class);

  private final AtomicLong sequenceGenerator = new AtomicLong(0L);

  private final ReconnectMessageCodec reconnectMessageCodec = new ReconnectMessageCodec();
  private final EntityClientEndpoint<EhcacheEntityMessage, EhcacheEntityResponse> endpoint;
  private final LifeCycleMessageFactory messageFactory;
  private volatile UUID clientId;

  private Timeouts timeouts = Timeouts.builder().build();

  public SimpleClusterTierManagerClientEntity(EntityClientEndpoint<EhcacheEntityMessage, EhcacheEntityResponse> endpoint) {
    this.endpoint = endpoint;
    this.messageFactory = new LifeCycleMessageFactory();
    endpoint.setDelegate(new EndpointDelegate<EhcacheEntityResponse>() {
      @Override
      public void handleMessage(EhcacheEntityResponse messageFromServer) {
        // Nothing to do
      }

      @Override
      public byte[] createExtendedReconnectData() {
        ClusterTierManagerReconnectMessage reconnectMessage = new ClusterTierManagerReconnectMessage(clientId);
        return reconnectMessageCodec.encode(reconnectMessage);
      }

      @Override
      public void didDisconnectUnexpectedly() {
        // Nothing to do
      }
    });
  }

  @Override
  public void setTimeouts(Timeouts timeouts) {
    this.timeouts = timeouts;
  }

  @Override
  public UUID getClientId() {
    if (clientId == null) {
      throw new IllegalStateException("Client Id cannot be null");
    }
    return this.clientId;
  }

  @Override
  public void setClientId(UUID clientId) {
    this.clientId = clientId;
  }

  @Override
  public void close() {
    endpoint.close();
  }

  @Override
  public void validate(ServerSideConfiguration config) throws ClusterException, TimeoutException {
    boolean clientIdGenerated = false;
    while (true) {
      try {
        if (clientIdGenerated || clientId == null) {
          clientId = UUID.randomUUID();
          clientIdGenerated = true;
        }
        this.messageFactory.setClientId(clientId);
        invokeInternal(timeouts.getLifecycleOperationTimeout(), messageFactory.validateStoreManager(config), false);
        return;
      } catch (InvalidClientIdException e) {
        if (!clientIdGenerated) {
          throw new AssertionError("Injected ClientID refused by server - " + clientId);
        }
      }
    }
  }

  @Override
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

  private InvokeFuture<EhcacheEntityResponse> invokeAsync(EhcacheEntityMessage message, boolean replicate)
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
