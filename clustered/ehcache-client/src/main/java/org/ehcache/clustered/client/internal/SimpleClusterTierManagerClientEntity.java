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

package org.ehcache.clustered.client.internal;

import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.Failure;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.PrepareForDestroy;
import org.ehcache.clustered.common.internal.messages.EhcacheResponseType;
import org.ehcache.clustered.common.internal.messages.LifeCycleMessageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.connection.entity.Entity;
import org.terracotta.entity.EndpointDelegate;
import org.terracotta.entity.EntityClientEndpoint;
import org.terracotta.entity.EntityResponse;
import org.terracotta.entity.MessageCodecException;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * The client-side {@link Entity} through which clustered cache operations are performed.
 * An instance of this class is created by the {@link ClusterTierManagerClientEntityService}.
 * The server-side partner is the {@code EhcacheActiveEntity}.
 */
public class SimpleClusterTierManagerClientEntity implements ClusterTierManagerClientEntity {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleClusterTierManagerClientEntity.class);

  private final EntityClientEndpoint<EhcacheEntityMessage, EhcacheEntityResponse> endpoint;
  private final LifeCycleMessageFactory messageFactory;

  public SimpleClusterTierManagerClientEntity(EntityClientEndpoint<EhcacheEntityMessage, EhcacheEntityResponse> endpoint) {
    this.endpoint = endpoint;
    this.messageFactory = new LifeCycleMessageFactory();
    endpoint.setDelegate(new EndpointDelegate<EhcacheEntityResponse>() {
      @Override
      public void handleMessage(EhcacheEntityResponse messageFromServer) {

      }

      @Override
      public byte[] createExtendedReconnectData() {
        return new byte[0];
      }

      @Override
      public void didDisconnectUnexpectedly() {
        LOGGER.info("CacheManager got disconnected from server");
      }
    });
  }

  @Override
  public void close() {
    endpoint.close();
  }

  @Override
  public void validate(ServerSideConfiguration config) throws ClusterException {
    invokeInternal(messageFactory.validateStoreManager(config));
  }

  @Override
  public Set<String> prepareForDestroy() {
    try {
      PrepareForDestroy response = (PrepareForDestroy) invokeInternal(messageFactory.prepareForDestroy());
      return response.getStores();
    } catch (ClusterException e) {
      // TODO handle this
    }
    return null;
  }

  private EhcacheEntityResponse invokeInternal(EhcacheEntityMessage message)
      throws ClusterException {

    try {
      EhcacheEntityResponse response = waitFor(invokeAsync(message));
      if (EhcacheResponseType.FAILURE.equals(response.getResponseType())) {
        throw ((Failure)response).getCause();
      } else {
        return response;
      }
    } catch (ExecutionException | MessageCodecException e) {
      throw new RuntimeException(message + " error: " + e, e);
    }
  }

  private Future<EhcacheEntityResponse> invokeAsync(EhcacheEntityMessage message)
      throws MessageCodecException {
    return endpoint.message(message).invoke();
  }

  /**
   * Will wait forever on {@code Future.get()}. In case of interruption, it will take note and resume waiting on get. The
   * interruption flag is then set if needed before returning the get value (or if an exception occurred).
   *
   * @param future Future we want to get
   * @param <T> type of the response
   * @return the result of the get
   */
  private static <T extends EntityResponse> T waitFor(Future<T> future)
    throws ExecutionException {
    boolean interrupted = Thread.interrupted();
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
