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
import org.terracotta.exception.ConnectionClosedException;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public class ReconnectingServerStoreProxy implements ServerStoreProxy {

  private final ServerStoreProxy swap = new SwappableProxy();
  private final ServerStoreProxy delegate;
  private final AtomicReference<ServerStoreProxy> delegateRef;
  private final Runnable onReconnect;

  public ReconnectingServerStoreProxy(ServerStoreProxy serverStoreProxy, Runnable onReconnect) {
    this.delegate = serverStoreProxy;
    this.delegateRef = new AtomicReference<>(serverStoreProxy);
    this.onReconnect = onReconnect;
  }

  @Override
  public String getCacheId() {
    return proxy().getCacheId();
  }

  @Override
  public void close() {
    proxy().close();
  }

  @Override
  public Chain get(long key) throws TimeoutException {
    try {
      return proxy().get(key);
    } catch (ServerStoreProxyException sspe) {
      handleException(sspe);
    }
    return null;
  }

  @Override
  public void append(long key, ByteBuffer payLoad) throws TimeoutException {
    try {
      proxy().append(key, payLoad);
    } catch (ServerStoreProxyException sspe) {
      handleException(sspe);
    }
  }

  @Override
  public Chain getAndAppend(long key, ByteBuffer payLoad) throws TimeoutException {
    try {
      return proxy().getAndAppend(key, payLoad);
    } catch (ServerStoreProxyException sspe) {
      handleException(sspe);
    }
    return null;
  }

  @Override
  public void replaceAtHead(long key, Chain expect, Chain update) {
    try {
      proxy().replaceAtHead(key, expect, update);
    } catch (ServerStoreProxyException sspe) {
      handleException(sspe);
    }
  }

  @Override
  public void clear() throws TimeoutException {
    try {
      proxy().clear();
    } catch (ServerStoreProxyException sspe) {
      handleException(sspe);
    }
  }

  private ServerStoreProxy proxy() {
    return delegateRef.get();
  }

  private void handleException(ServerStoreProxyException sspe) {
    if (sspe.getCause() instanceof ConnectionClosedException) {
      if (delegateRef.compareAndSet(delegate, swap)) {
        onReconnect.run();
      }
      throw new ReconnectInProgressException();
    } else {
      throw sspe;
    }
  }

  private static class SwappableProxy implements ServerStoreProxy {

    @Override
    public String getCacheId() {
      throw new ReconnectInProgressException();
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
