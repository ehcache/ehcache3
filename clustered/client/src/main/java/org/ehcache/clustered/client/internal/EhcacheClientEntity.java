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

import org.ehcache.clustered.client.internal.service.ClusteredStoreDestructionException;
import org.ehcache.clustered.client.internal.service.ClusteredStoreManagerConfigurationException;
import org.ehcache.clustered.client.internal.service.ClusteredStoreCreationException;
import org.ehcache.clustered.client.internal.service.ClusteredStoreManagerValidationException;
import org.ehcache.clustered.client.internal.service.ClusteredStoreReleaseException;
import org.ehcache.clustered.client.internal.service.ClusteredStoreValidationException;
import org.ehcache.clustered.common.internal.ClusteredEhcacheIdentity;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.exceptions.ClusteredEhcacheException;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.Failure;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.Type;
import org.ehcache.clustered.common.internal.messages.LifeCycleMessageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.connection.entity.Entity;
import org.terracotta.entity.EndpointDelegate;
import org.terracotta.entity.EntityClientEndpoint;
import org.terracotta.entity.EntityResponse;
import org.terracotta.entity.InvokeFuture;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.exception.EntityException;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 *
 * @author cdennis
 */
public class EhcacheClientEntity implements Entity {

  private static final Logger LOGGER = LoggerFactory.getLogger(EhcacheClientEntity.class);

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
  private volatile boolean connected = true;

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
        return new byte[0];
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

  public void validate(ServerSideConfiguration config) throws ClusteredStoreManagerValidationException {
    try {
      invokeInternal(messageFactory.validateStoreManager(config), false);
    } catch (Exception e) {
      throw new ClusteredStoreManagerValidationException("Error validating server store manager", e);
    }
  }

  public void configure(ServerSideConfiguration config) throws ClusteredStoreManagerConfigurationException {
    try {
      invokeInternal(messageFactory.configureStoreManager(config), true);
    } catch (Exception e) {
      throw new ClusteredStoreManagerConfigurationException("Error configuring server store manager", e);
    }
  }

  public void createCache(String name, ServerStoreConfiguration serverStoreConfiguration) throws ClusteredStoreCreationException {
    try {
      invokeInternal(messageFactory.createServerStore(name, serverStoreConfiguration), true);
    } catch (Exception e) {
      throw new ClusteredStoreCreationException("Error creating server store '" + name + "'", e);
    }
  }

  public void validateCache(String name, ServerStoreConfiguration serverStoreConfiguration) throws ClusteredStoreValidationException {
    try {
      invokeInternal(messageFactory.validateServerStore(name , serverStoreConfiguration), false);
    } catch (Exception e) {
      throw new ClusteredStoreValidationException("Error validating server store '" + name + "'", e);
    }
  }

  public void releaseCache(String name) throws ClusteredStoreReleaseException {
    try {
      invokeInternal(messageFactory.releaseServerStore(name), false);
    } catch (Exception e) {
      throw new ClusteredStoreReleaseException("Error releasing server store '" + name + "'", e);
    }
  }

  public void destroyCache(String name) throws ClusteredStoreDestructionException {
    try {
      invokeInternal(messageFactory.destroyServerStore(name), true);
    } catch (Exception e) {
      throw new ClusteredStoreDestructionException("Error destroying server store '" + name + "'", e);
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
   * @throws ClusteredEhcacheException thrown to reflect a server-side operation fault
   */
  public EhcacheEntityResponse invoke(EhcacheEntityMessage message, boolean replicate) throws ClusteredEhcacheException {
      return invokeInternal(message, replicate);
  }

  private EhcacheEntityResponse invokeInternal(EhcacheEntityMessage message, boolean replicate)
      throws ClusteredEhcacheException {

    try {
      EhcacheEntityResponse response = waitFor(invokeAsync(message, replicate));
      if (Type.FAILURE.equals(response.getType())) {
        Exception cause = ((Failure)response).getCause();
        if (cause instanceof ClusteredEhcacheException) {
          throw (ClusteredEhcacheException) cause;
        }
        throw new RuntimeException(message + " error: " + cause.toString(), cause);
      } else {
        return response;
      }
    } catch (EntityException e) {
      throw new RuntimeException(message + " error: " + e.toString(), e);
    } catch (MessageCodecException e) {
      throw new RuntimeException(message + " error: " + e.toString(), e);
    }
  }

  public InvokeFuture<EhcacheEntityResponse> invokeAsync(EhcacheEntityMessage message, boolean replicate)
      throws MessageCodecException {
    if (replicate) {
      return endpoint.beginInvoke().message(message).replicate(true).ackCompleted().invoke();
    } else {
      return endpoint.beginInvoke().message(message).invoke();
    }
  }

  private static <T> T waitFor(InvokeFuture<T> future) throws EntityException {
    boolean interrupted = false;
    try {
      while (true) {
        try {
          return future.get();
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
