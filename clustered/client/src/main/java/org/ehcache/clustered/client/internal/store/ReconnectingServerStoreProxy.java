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

import org.ehcache.clustered.common.internal.store.Chain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.exception.ConnectionClosedException;
import org.terracotta.exception.ConnectionShutdownException;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public class ReconnectingServerStoreProxy implements ServerStoreProxy {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReconnectingServerStoreProxy.class);

  private final AtomicReference<ServerStoreProxy> delegateRef;
  private final Runnable onReconnect;

  public ReconnectingServerStoreProxy(ServerStoreProxy serverStoreProxy, Runnable onReconnect) {
    this.delegateRef = new AtomicReference<>(serverStoreProxy);
    this.onReconnect = onReconnect;
  }

  @Override
  public String getCacheId() {
    return proxy().getCacheId();
  }

  @Override
  public void close() {
    try {
      proxy().close();
    } catch (ConnectionClosedException | ConnectionShutdownException e) {
      LOGGER.debug("Store was already closed, since connection was closed");
    }
  }

  @Override
  public Chain get(long key) throws TimeoutException {
    return onStoreProxy(serverStoreProxy -> serverStoreProxy.get(key));
  }

  @Override
  public void append(long key, ByteBuffer payLoad) throws TimeoutException {
    onStoreProxy((TimeoutExceptionFunction<ServerStoreProxy, Void>) serverStoreProxy -> {
      serverStoreProxy.append(key, payLoad);
      return null;
    });
  }

  @Override
  public Chain getAndAppend(long key, ByteBuffer payLoad) throws TimeoutException {
    return onStoreProxy(serverStoreProxy -> serverStoreProxy.getAndAppend(key, payLoad));
  }

  @Override
  public void replaceAtHead(long key, Chain expect, Chain update) {
    try {
      onStoreProxy((TimeoutExceptionFunction<ServerStoreProxy, Void>)serverStoreProxy -> {
        serverStoreProxy.replaceAtHead(key, expect, update);
        return null;
      });
    } catch (TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void clear() throws TimeoutException {
    onStoreProxy((TimeoutExceptionFunction<ServerStoreProxy, Void>) serverStoreProxy -> {
      serverStoreProxy.clear();
      return null;
    });
  }

  private ServerStoreProxy proxy() {
    return delegateRef.get();
  }

  private <T> T onStoreProxy(TimeoutExceptionFunction<ServerStoreProxy, T> function) throws TimeoutException {
    ServerStoreProxy storeProxy = proxy();
    try {
      return function.apply(storeProxy);
    } catch (ServerStoreProxyException sspe) {
      if (sspe.getCause() instanceof ConnectionClosedException) {
        if (delegateRef.compareAndSet(storeProxy, new ReconnectInProgressProxy(storeProxy.getCacheId()))) {
          onReconnect.run();
        }
        return onStoreProxy(function);
      } else {
        throw sspe;
      }
    }
  }

  @FunctionalInterface
  private interface TimeoutExceptionFunction<U, V> {
    V apply(U u) throws TimeoutException;
  }

  private static class ReconnectInProgressProxy implements ServerStoreProxy {

    private final String cacheId;

    ReconnectInProgressProxy(String cacheId) {
      this.cacheId = cacheId;
    }

    @Override
    public String getCacheId() {
      return this.cacheId;
    }

    @Override
    public void close() {
      throw new ReconnectInProgressException();
    }

    @Override
    public Chain get(long key) {
      throw new ReconnectInProgressException();
    }

    @Override
    public void append(long key, ByteBuffer payLoad) {
      throw new ReconnectInProgressException();
    }

    @Override
    public Chain getAndAppend(long key, ByteBuffer payLoad) {
      throw new ReconnectInProgressException();
    }

    @Override
    public void replaceAtHead(long key, Chain expect, Chain update) {
      throw new ReconnectInProgressException();
    }

    @Override
    public void clear() {
      throw new ReconnectInProgressException();
    }
  }
}
