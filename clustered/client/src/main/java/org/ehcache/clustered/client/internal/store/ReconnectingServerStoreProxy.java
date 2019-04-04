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

import org.ehcache.clustered.client.internal.store.lock.LockingServerStoreProxy;
import org.ehcache.clustered.client.internal.store.lock.LockingServerStoreProxyImpl;
import org.ehcache.clustered.common.internal.store.Chain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.exception.ConnectionClosedException;
import org.terracotta.exception.ConnectionShutdownException;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public class ReconnectingServerStoreProxy implements LockingServerStoreProxy {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReconnectingServerStoreProxy.class);

  private final AtomicReference<LockingServerStoreProxy> delegateRef;
  private final Runnable onReconnect;

  public ReconnectingServerStoreProxy(ServerStoreProxy serverStoreProxy, Runnable onReconnect) {
    if (serverStoreProxy instanceof LockingServerStoreProxy) {
      this.delegateRef = new AtomicReference<>((LockingServerStoreProxy) serverStoreProxy);
    } else {
      this.delegateRef = new AtomicReference<>(unsupportedLocking(serverStoreProxy));
    }
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
  public ChainEntry get(long key) throws TimeoutException {
    return onStoreProxy(serverStoreProxy -> serverStoreProxy.get(key));
  }

  @Override
  public void append(long key, ByteBuffer payLoad) throws TimeoutException {
    onStoreProxy(serverStoreProxy -> {
      serverStoreProxy.append(key, payLoad);
      return null;
    });
  }

  @Override
  public ChainEntry getAndAppend(long key, ByteBuffer payLoad) throws TimeoutException {
    return onStoreProxy(serverStoreProxy -> serverStoreProxy.getAndAppend(key, payLoad));
  }

  @Override
  public void replaceAtHead(long key, Chain expect, Chain update) {
    try {
      onStoreProxy(serverStoreProxy -> {
        serverStoreProxy.replaceAtHead(key, expect, update);
        return null;
      });
    } catch (TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void clear() throws TimeoutException {
    onStoreProxy(serverStoreProxy -> {
      serverStoreProxy.clear();
      return null;
    });
  }

  @Override
  public Iterator<Chain> iterator() throws TimeoutException {
    return onStoreProxy(LockingServerStoreProxy::iterator);
  }

  private LockingServerStoreProxy proxy() {
    return delegateRef.get();
  }

  private <T> T onStoreProxy(TimeoutExceptionFunction<LockingServerStoreProxy, T> function) throws TimeoutException {
    LockingServerStoreProxy storeProxy = proxy();
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

  @Override
  public ChainEntry lock(long key) throws TimeoutException {
    return onStoreProxy(lockingServerStoreProxy -> lockingServerStoreProxy.lock(key));
  }

  @Override
  public void unlock(long key, boolean localonly) throws TimeoutException {
    onStoreProxy(lockingServerStoreProxy -> {
      lockingServerStoreProxy.unlock(key, localonly);
      return null;
    });
  }

  @FunctionalInterface
  private interface TimeoutExceptionFunction<U, V> {
    V apply(U u) throws TimeoutException;
  }

  private static class ReconnectInProgressProxy extends LockingServerStoreProxyImpl {

    private final String cacheId;

    ReconnectInProgressProxy(String cacheId) {
      super(null, null);
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
    public ChainEntry get(long key) {
      throw new ReconnectInProgressException();
    }

    @Override
    public void append(long key, ByteBuffer payLoad) {
      throw new ReconnectInProgressException();
    }

    @Override
    public ChainEntry getAndAppend(long key, ByteBuffer payLoad) {
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

    @Override
    public Iterator<Chain> iterator() {
      throw new ReconnectInProgressException();
    }

    @Override
    public ChainEntry lock(long key) {
      throw new ReconnectInProgressException();
    }

    @Override
    public void unlock(long key, boolean localonly) {
      throw new ReconnectInProgressException();
    }
  }

  private LockingServerStoreProxy unsupportedLocking(ServerStoreProxy serverStoreProxy) {
    return new LockingServerStoreProxy() {
      @Override
      public ChainEntry lock(long hash) throws TimeoutException {
        throw new UnsupportedOperationException("Lock ops are not supported");
      }

      @Override
      public void unlock(long hash, boolean localonly) throws TimeoutException {
        throw new UnsupportedOperationException("Lock ops are not supported");
      }

      @Override
      public ChainEntry get(long key) throws TimeoutException {
        return serverStoreProxy.get(key);
      }

      @Override
      public ChainEntry getAndAppend(long key, ByteBuffer payLoad) throws TimeoutException {
        return serverStoreProxy.getAndAppend(key, payLoad);
      }

      @Override
      public String getCacheId() {
        return serverStoreProxy.getCacheId();
      }

      @Override
      public void close() {
        serverStoreProxy.close();
      }

      @Override
      public void append(long key, ByteBuffer payLoad) throws TimeoutException {
        serverStoreProxy.append(key, payLoad);
      }

      @Override
      public void replaceAtHead(long key, Chain expect, Chain update) {
        serverStoreProxy.replaceAtHead(key, expect, update);
      }

      @Override
      public void clear() throws TimeoutException {
        serverStoreProxy.clear();
      }

      @Override
      public Iterator<Chain> iterator() throws TimeoutException {
        return serverStoreProxy.iterator();
      }
    };
  }
}
