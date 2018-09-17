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
package org.ehcache.clustered.client.internal.loaderwriter;

import org.ehcache.clustered.client.internal.store.ClusteredStore;
import org.ehcache.clustered.client.internal.store.ClusteredValueHolder;
import org.ehcache.clustered.client.internal.store.ResolvedChain;
import org.ehcache.clustered.client.internal.store.ServerStoreProxy;
import org.ehcache.clustered.client.internal.store.lock.LockManager;
import org.ehcache.clustered.client.internal.store.lock.LockingServerStoreProxy;
import org.ehcache.clustered.client.internal.store.operations.ChainResolver;
import org.ehcache.clustered.client.internal.store.operations.ConditionalRemoveOperation;
import org.ehcache.clustered.client.internal.store.operations.ConditionalReplaceOperation;
import org.ehcache.clustered.client.internal.store.operations.EternalChainResolver;
import org.ehcache.clustered.client.internal.store.operations.Operation;
import org.ehcache.clustered.client.internal.store.operations.PutIfAbsentOperation;
import org.ehcache.clustered.client.internal.store.operations.PutOperation;
import org.ehcache.clustered.client.internal.store.operations.RemoveOperation;
import org.ehcache.clustered.client.internal.store.operations.ReplaceOperation;
import org.ehcache.clustered.client.internal.store.operations.Result;
import org.ehcache.clustered.client.internal.store.operations.codecs.OperationsCodec;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.core.exceptions.StorePassThroughException;
import org.ehcache.core.spi.store.tiering.AuthoritativeTier;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.resilience.StoreAccessException;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static org.ehcache.core.exceptions.ExceptionFactory.newCacheLoadingException;
import static org.ehcache.core.exceptions.ExceptionFactory.newCacheWritingException;
import static org.ehcache.core.exceptions.StorePassThroughException.handleException;

public class ClusteredLoaderWriterStore<K, V> extends ClusteredStore<K, V> implements AuthoritativeTier<K, V> {

  private final CacheLoaderWriter<? super K, V> cacheLoaderWriter;
  private final boolean useLoaderInAtomics;

  public ClusteredLoaderWriterStore(Configuration<K, V> config, OperationsCodec<K, V> codec, ChainResolver<K, V> resolver, TimeSource timeSource,
                                    CacheLoaderWriter<? super K, V> loaderWriter, boolean useLoaderInAtomics) {
    super(config, codec, resolver, timeSource);
    this.cacheLoaderWriter = loaderWriter;
    this.useLoaderInAtomics = useLoaderInAtomics;
  }

  /**
   * For Tests
   */
  ClusteredLoaderWriterStore(Configuration<K, V> config, OperationsCodec<K, V> codec, EternalChainResolver<K, V> resolver,
                             ServerStoreProxy proxy, TimeSource timeSource, CacheLoaderWriter<? super K, V> loaderWriter) {
    super(config, codec, resolver, proxy, timeSource);
    this.cacheLoaderWriter = loaderWriter;
    this.useLoaderInAtomics = true;
  }

  private LockManager getProxy() {
    return (LockManager) storeProxy;
  }

  @Override
  protected ValueHolder<V> getInternal(K key) throws StoreAccessException, TimeoutException {
    ValueHolder<V> holder = super.getInternal(key);
    try {
      if (holder == null) {
        long hash = extractLongKey(key);
        boolean unlocked = false;
        getProxy().lock(hash);
        try {
          V value = null;
          try {
            value = cacheLoaderWriter.load(key);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          if (value == null) {
            return null;
          }
          append(key, value);
          unlocked = true;
          return new ClusteredValueHolder<>(value);
        } finally {
          if (!unlocked) {
            getProxy().unlock(hash);
          }
        }
      }
    } catch (RuntimeException re) {
      throw handleException(re);
    }
    return holder;
  }

  private void append(K key, V value) throws TimeoutException {
    PutOperation<K, V> operation = new PutOperation<>(key, value, timeSource.getTimeMillis());
    ByteBuffer payload = codec.encode(operation);
    long extractedKey = extractLongKey(key);
    storeProxy.append(extractedKey, payload);
  }

  @Override
  protected PutStatus silentPut(K key, V value) throws StoreAccessException {
    try {
      long hash = extractLongKey(key);
      boolean unlocked = false;
      getProxy().lock(hash);
      try {
        cacheLoaderWriter.write(key, value);
        append(key, value);
        unlocked = true;
      } finally {
        if (!unlocked) {
          getProxy().unlock(hash);
        }
      }
      return PutStatus.PUT;
    } catch (Exception e) {
      throw handleException(e);
    }
  }

  @Override
  protected boolean silentRemove(K key) throws StoreAccessException {
    try {
      long hash = extractLongKey(key);
      boolean unlocked = false;
      RemoveOperation<K, V> operation = new RemoveOperation<>(key, timeSource.getTimeMillis());
      ByteBuffer payLoad = codec.encode(operation);
      Chain chain = getProxy().lock(hash);
      try {
        cacheLoaderWriter.delete(key);
        storeProxy.append(hash, payLoad);
        unlocked = true;
        ResolvedChain<K, V> resolvedChain = resolver.resolve(chain, key, timeSource.getTimeMillis());
        if (resolvedChain.getResolvedResult(key) != null) {
          return true;
        } else {
          return false;
        }
      } finally {
        if (!unlocked) {
          getProxy().unlock(hash);
        }
      }
    } catch (Exception e) {
      throw handleException(e);
    }
  }

  @Override
  protected V silentPutIfAbsent(K key, V value) throws StoreAccessException {
    try {
      long hash = extractLongKey(key);
      boolean unlocked = false;
      Chain existing = getProxy().lock(hash);
      try {
        ResolvedChain<K, V> resolvedChain = resolver.resolve(existing, key, timeSource.getTimeMillis());
        Result<K, V> result = resolvedChain.getResolvedResult(key);
        V existingVal = result == null ? null : result.getValue();
        if (existingVal != null) {
          return existingVal;
        } else {
          existingVal = loadFromLoaderWriter(key);
          if (existingVal == null) {
            cacheLoaderWriter.write(key, value);
          }
          PutIfAbsentOperation<K, V> operation = new PutIfAbsentOperation<>(key, value, timeSource.getTimeMillis());
          ByteBuffer payload = codec.encode(operation);
          storeProxy.append(hash, payload);
          unlocked = true;
          return existingVal;
        }
      } finally {
        if (!unlocked) {
          getProxy().unlock(hash);
        }
      }
    } catch (Exception e) {
      throw handleException(e);
    }
  }

  @Override
  protected V silentReplace(K key, V value) throws StoreAccessException {
    try {
      long hash = extractLongKey(key);
      boolean unlocked = false;
      Chain existing = getProxy().lock(hash);
      try {
        ResolvedChain<K, V> resolvedChain = resolver.resolve(existing, key, timeSource.getTimeMillis());
        Result<K, V> result = resolvedChain.getResolvedResult(key);
        V existingVal = result == null ? null : result.getValue();
        if (existingVal != null) {
          cacheLoaderWriter.write(key, value);
          ReplaceOperation<K, V> operation = new ReplaceOperation<>(key, value, timeSource.getTimeMillis());
          ByteBuffer payload = codec.encode(operation);
          storeProxy.append(hash, payload);
          unlocked = true;
          return existingVal;
        } else {
          V inCache = loadFromLoaderWriter(key);
          if (inCache != null) {
            cacheLoaderWriter.write(key, value);
            ReplaceOperation<K, V> operation = new ReplaceOperation<>(key, value, timeSource.getTimeMillis());
            ByteBuffer payload = codec.encode(operation);
            storeProxy.append(hash, payload);
            unlocked = true;
            return inCache;
          } else {
            return null;
          }
        }
      } finally {
        if (!unlocked) {
          getProxy().unlock(hash);
        }
      }
    } catch (Exception e) {
      throw handleException(e);
    }
  }

  @Override
  protected V silentRemove(K key, V value) throws StoreAccessException {
    try {
      long hash = extractLongKey(key);
      boolean unlocked = false;
      Chain existing = getProxy().lock(hash);
      try {
        ResolvedChain<K, V> resolvedChain = resolver.resolve(existing, key, timeSource.getTimeMillis());
        Result<K, V> result = resolvedChain.getResolvedResult(key);
        V existingVal = result == null ? null : result.getValue();
        if (existingVal == null) {
          existingVal = loadFromLoaderWriter(key);
        }
        if (value.equals(existingVal)) {
          cacheLoaderWriter.delete(key);
          ConditionalRemoveOperation<K, V> operation = new ConditionalRemoveOperation<>(key, value, timeSource.getTimeMillis());
          ByteBuffer payLoad = codec.encode(operation);
          storeProxy.append(hash, payLoad);
          unlocked = true;
        }
        return existingVal;
      } finally {
        if (!unlocked) {
          getProxy().unlock(hash);
        }
      }
    } catch (Exception e) {
      throw handleException(e);
    }
  }

  @Override
  protected V silentReplace(K key, V oldValue, V newValue) throws StoreAccessException {
    try {
      long hash = extractLongKey(key);
      boolean unlocked = false;
      Chain existing = getProxy().lock(hash);
      try {
        ResolvedChain<K, V> resolvedChain = resolver.resolve(existing, key, timeSource.getTimeMillis());
        Result<K, V> result = resolvedChain.getResolvedResult(key);
        V existingVal = result == null ? null : result.getValue();
        if (existingVal == null) {
          existingVal = loadFromLoaderWriter(key);
        }
        if (oldValue.equals(existingVal)) {
          cacheLoaderWriter.write(key, newValue);
          ConditionalReplaceOperation<K, V> operation = new ConditionalReplaceOperation<>(key, oldValue, newValue, timeSource.getTimeMillis());
          ByteBuffer payLoad = codec.encode(operation);
          storeProxy.append(hash, payLoad);
          unlocked = true;
        }
        return existingVal;
      } finally {
        if (!unlocked) {
          getProxy().unlock(hash);
        }
      }
    } catch (Exception e) {
      throw handleException(e);
    }
  }

  private V loadFromLoaderWriter(K key) {
    if (useLoaderInAtomics) {
      try {
        return cacheLoaderWriter.load(key);
      } catch (Exception e) {
        throw new StorePassThroughException(newCacheLoadingException(e));
      }
    }
    return null;
  }

}
