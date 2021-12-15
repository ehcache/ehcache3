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
import org.ehcache.clustered.client.internal.store.ServerStoreProxy;
import org.ehcache.clustered.client.internal.store.lock.LockingServerStoreProxy;
import org.ehcache.clustered.client.internal.store.operations.ChainResolver;
import org.ehcache.clustered.client.internal.store.operations.EternalChainResolver;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.clustered.common.internal.store.operations.ConditionalRemoveOperation;
import org.ehcache.clustered.common.internal.store.operations.ConditionalReplaceOperation;
import org.ehcache.clustered.common.internal.store.operations.PutIfAbsentOperation;
import org.ehcache.clustered.common.internal.store.operations.PutOperation;
import org.ehcache.clustered.common.internal.store.operations.RemoveOperation;
import org.ehcache.clustered.common.internal.store.operations.ReplaceOperation;
import org.ehcache.clustered.common.internal.store.operations.codecs.OperationsCodec;
import org.ehcache.config.ResourceType;
import org.ehcache.core.events.StoreEventDispatcher;
import org.ehcache.core.exceptions.StorePassThroughException;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.core.spi.store.tiering.AuthoritativeTier;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.spi.time.TimeSourceService;
import org.ehcache.impl.store.DefaultStoreEventDispatcher;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterConfiguration;
import org.ehcache.spi.loaderwriter.CacheLoadingException;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.ehcache.core.exceptions.ExceptionFactory.newCacheLoadingException;
import static org.ehcache.core.exceptions.StorePassThroughException.handleException;

public class ClusteredLoaderWriterStore<K, V> extends ClusteredStore<K, V> implements AuthoritativeTier<K, V> {

  private final CacheLoaderWriter<? super K, V> cacheLoaderWriter;
  private final boolean useLoaderInAtomics;

  public ClusteredLoaderWriterStore(Configuration<K, V> config, OperationsCodec<K, V> codec, ChainResolver<K, V> resolver, TimeSource timeSource,
                                    CacheLoaderWriter<? super K, V> loaderWriter, boolean useLoaderInAtomics, StoreEventDispatcher<K, V> storeEventDispatcher, StatisticsService statisticsService) {
    super(config, codec, resolver, timeSource, storeEventDispatcher, statisticsService);
    this.cacheLoaderWriter = loaderWriter;
    this.useLoaderInAtomics = useLoaderInAtomics;
  }

  /**
   * For Tests
   */
  ClusteredLoaderWriterStore(Configuration<K, V> config, OperationsCodec<K, V> codec, EternalChainResolver<K, V> resolver,
                             ServerStoreProxy proxy, TimeSource timeSource, CacheLoaderWriter<? super K, V> loaderWriter, StatisticsService statisticsService) {
    super(config, codec, resolver, proxy, timeSource, null, statisticsService);
    this.cacheLoaderWriter = loaderWriter;
    this.useLoaderInAtomics = true;
  }

  private LockingServerStoreProxy getProxy() {
    return (LockingServerStoreProxy) storeProxy;
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
          V value;
          try {
            value = cacheLoaderWriter.load(key);
          } catch (Exception e) {
            throw new StorePassThroughException(new CacheLoadingException(e));
          }
          if (value == null) {
            return null;
          }
          append(key, value);
          unlocked = true;
          return new ClusteredValueHolder<>(value);
        } finally {
          getProxy().unlock(hash, unlocked);
        }
      }
    } catch (RuntimeException re) {
      throw handleException(re);
    }
    return holder;
  }

  @Override
  public boolean containsKey(K key) throws StoreAccessException {
    try {
      return super.getInternal(key) != null;
    } catch (TimeoutException e) {
      return false;
    }
  }

  private void append(K key, V value) throws TimeoutException {
    PutOperation<K, V> operation = new PutOperation<>(key, value, timeSource.getTimeMillis());
    ByteBuffer payload = codec.encode(operation);
    long extractedKey = extractLongKey(key);
    storeProxy.append(extractedKey, payload);
  }

  @Override
  protected void silentPut(K key, V value) throws StoreAccessException {
    try {
      long hash = extractLongKey(key);
      boolean unlocked = false;
      getProxy().lock(hash);
      try {
        cacheLoaderWriter.write(key, value);
        append(key, value);
        unlocked = true;
      } finally {
        getProxy().unlock(hash, unlocked);
      }
    } catch (Exception e) {
      throw handleException(e);
    }
  }

  @Override
  protected ValueHolder<V> silentGetAndPut(K key, V value) throws StoreAccessException {
    try {
      long hash = extractLongKey(key);
      boolean unlocked = false;
      final ServerStoreProxy.ChainEntry chain = getProxy().lock(hash);
      try {
        cacheLoaderWriter.write(key, value);
        append(key, value);
        unlocked = true;
        return resolver.resolve(chain, key, timeSource.getTimeMillis());
      } finally {
        getProxy().unlock(hash, unlocked);
      }
    } catch (Exception e) {
      throw handleException(e);
    }
  }

  @Override
  protected ValueHolder<V> silentRemove(K key) throws StoreAccessException {
    try {
      long hash = extractLongKey(key);
      boolean unlocked = false;
      RemoveOperation<K, V> operation = new RemoveOperation<>(key, timeSource.getTimeMillis());
      ByteBuffer payLoad = codec.encode(operation);
      ServerStoreProxy.ChainEntry chain = getProxy().lock(hash);
      try {
        cacheLoaderWriter.delete(key);
        storeProxy.append(hash, payLoad);
        unlocked = true;
        return resolver.resolve(chain, key, timeSource.getTimeMillis());
      } finally {
        getProxy().unlock(hash, unlocked);
      }
    } catch (Exception e) {
      throw handleException(e);
    }
  }

  @Override
  protected ValueHolder<V> silentPutIfAbsent(K key, V value) throws StoreAccessException {
    try {
      long hash = extractLongKey(key);
      boolean unlocked = false;
      ServerStoreProxy.ChainEntry existing = getProxy().lock(hash);
      try {
        ValueHolder<V> existingVal = resolver.resolve(existing, key, timeSource.getTimeMillis());
        if (existingVal != null) {
          return existingVal;
        } else {
          existingVal = loadFromLoaderWriter(key);
          if (existingVal == null) {
            cacheLoaderWriter.write(key, value);
            PutIfAbsentOperation<K, V> operation = new PutIfAbsentOperation<>(key, value, timeSource.getTimeMillis());
            ByteBuffer payload = codec.encode(operation);
            storeProxy.append(hash, payload);
            unlocked = true;
          }
          return existingVal;
        }
      } finally {
        getProxy().unlock(hash, unlocked);
      }
    } catch (Exception e) {
      throw handleException(e);
    }
  }

  @Override
  protected ValueHolder<V> silentReplace(K key, V value) throws StoreAccessException {
    try {
      long hash = extractLongKey(key);
      boolean unlocked = false;
      ServerStoreProxy.ChainEntry existing = getProxy().lock(hash);
      try {
        ValueHolder<V> existingVal = resolver.resolve(existing, key, timeSource.getTimeMillis());
        if (existingVal != null) {
          cacheLoaderWriter.write(key, value);
          ReplaceOperation<K, V> operation = new ReplaceOperation<>(key, value, timeSource.getTimeMillis());
          ByteBuffer payload = codec.encode(operation);
          storeProxy.append(hash, payload);
          unlocked = true;
          return existingVal;
        } else {
          ValueHolder<V> inCache = loadFromLoaderWriter(key);
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
        getProxy().unlock(hash, unlocked);
      }
    } catch (Exception e) {
      throw handleException(e);
    }
  }

  @Override
  protected ValueHolder<V> silentRemove(K key, V value) throws StoreAccessException {
    try {
      long hash = extractLongKey(key);
      boolean unlocked = false;
      ServerStoreProxy.ChainEntry existing = getProxy().lock(hash);
      try {
        ValueHolder<V> existingVal = resolver.resolve(existing, key, timeSource.getTimeMillis());
        if (existingVal == null) {
          existingVal = loadFromLoaderWriter(key);
        }
        if (existingVal != null && value.equals(existingVal.get())) {
          cacheLoaderWriter.delete(key);
          ConditionalRemoveOperation<K, V> operation = new ConditionalRemoveOperation<>(key, value, timeSource.getTimeMillis());
          ByteBuffer payLoad = codec.encode(operation);
          storeProxy.append(hash, payLoad);
          unlocked = true;
        }
        return existingVal;
      } finally {
        getProxy().unlock(hash, unlocked);
      }
    } catch (Exception e) {
      throw handleException(e);
    }
  }

  @Override
  protected ValueHolder<V> silentReplace(K key, V oldValue, V newValue) throws StoreAccessException {
    try {
      long hash = extractLongKey(key);
      boolean unlocked = false;
      ServerStoreProxy.ChainEntry existing = getProxy().lock(hash);
      try {
        ValueHolder<V> existingVal = resolver.resolve(existing, key, timeSource.getTimeMillis());
        if (existingVal == null) {
          existingVal = loadFromLoaderWriter(key);
        }
        if (existingVal != null && oldValue.equals(existingVal.get())) {
          cacheLoaderWriter.write(key, newValue);
          ConditionalReplaceOperation<K, V> operation = new ConditionalReplaceOperation<>(key, oldValue, newValue, timeSource.getTimeMillis());
          ByteBuffer payLoad = codec.encode(operation);
          storeProxy.append(hash, payLoad);
          unlocked = true;
        }
        return existingVal;
      } finally {
        getProxy().unlock(hash, unlocked);
      }
    } catch (Exception e) {
      throw handleException(e);
    }
  }

  private ValueHolder<V> loadFromLoaderWriter(K key) {
    if (useLoaderInAtomics) {
      try {
        V loaded = cacheLoaderWriter.load(key);
        if (loaded == null) {
          return null;
        } else {
          return new ClusteredValueHolder<>(loaded);
        }
      } catch (Exception e) {
        throw new StorePassThroughException(newCacheLoadingException(e));
      }
    }
    return null;
  }

  /**
   * Provider of {@link ClusteredLoaderWriterStore} instances.
   */
  @ServiceDependencies({ TimeSourceService.class, ClusteringService.class})
  public static class Provider extends ClusteredStore.Provider {
    @Override
    protected <K, V> ClusteredStore<K, V> createStore(Configuration<K, V> storeConfig,
                                                      OperationsCodec<K, V> codec,
                                                      ChainResolver<K, V> resolver,
                                                      TimeSource timeSource,
                                                      boolean useLoaderInAtomics,
                                                      Object[] serviceConfigs) {
      StoreEventDispatcher<K, V> storeEventDispatcher = new DefaultStoreEventDispatcher<>(storeConfig.getDispatcherConcurrency());
      return new ClusteredLoaderWriterStore<>(storeConfig, codec, resolver, timeSource,
                                              storeConfig.getCacheLoaderWriter(), useLoaderInAtomics, storeEventDispatcher, getServiceProvider().getService(StatisticsService.class));
    }

    @Override
    public int rank(Set<ResourceType<?>> resourceTypes, Collection<ServiceConfiguration<?, ?>> serviceConfigs) {
      int parentRank = super.rank(resourceTypes, serviceConfigs);
      if (parentRank == 0 || serviceConfigs.stream().noneMatch(CacheLoaderWriterConfiguration.class::isInstance)) {
        return 0;
      }
      return parentRank + 1;
    }

    @Override
    public int rankAuthority(ResourceType<?> authorityResource, Collection<ServiceConfiguration<?, ?>> serviceConfigs) {
      int parentRank = super.rankAuthority(authorityResource, serviceConfigs);
      if (parentRank == 0 || serviceConfigs.stream().noneMatch(CacheLoaderWriterConfiguration.class::isInstance)) {
        return 0;
      }
      return parentRank + 1;
    }
  }

}
