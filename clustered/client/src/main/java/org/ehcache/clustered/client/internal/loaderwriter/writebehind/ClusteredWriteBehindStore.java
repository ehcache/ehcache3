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
package org.ehcache.clustered.client.internal.loaderwriter.writebehind;

import org.ehcache.clustered.client.internal.loaderwriter.ClusteredLoaderWriterStore;
import org.ehcache.clustered.client.internal.store.ClusteredStore;
import org.ehcache.clustered.client.internal.store.ClusteredValueHolder;
import org.ehcache.clustered.client.internal.store.ServerStoreProxy;
import org.ehcache.clustered.client.internal.store.lock.LockingServerStoreProxy;
import org.ehcache.clustered.client.internal.store.operations.ChainResolver;
import org.ehcache.clustered.common.internal.store.operations.ConditionalRemoveOperation;
import org.ehcache.clustered.common.internal.store.operations.ConditionalReplaceOperation;
import org.ehcache.clustered.common.internal.store.operations.PutIfAbsentOperation;
import org.ehcache.clustered.common.internal.store.operations.PutOperation;
import org.ehcache.clustered.common.internal.store.operations.PutWithWriterOperation;
import org.ehcache.clustered.common.internal.store.operations.RemoveOperation;
import org.ehcache.clustered.common.internal.store.operations.ReplaceOperation;
import org.ehcache.clustered.common.internal.store.operations.codecs.OperationsCodec;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.config.ResourceType;
import org.ehcache.core.spi.store.tiering.AuthoritativeTier;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.spi.time.TimeSourceService;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.WriteBehindConfiguration;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;

import static org.ehcache.core.exceptions.StorePassThroughException.handleException;
import static org.ehcache.core.spi.service.ServiceUtils.findSingletonAmongst;

public class ClusteredWriteBehindStore<K, V> extends ClusteredStore<K, V> implements AuthoritativeTier<K, V> {

  private final CacheLoaderWriter<? super K, V> cacheLoaderWriter;
  private final ClusteredWriteBehind<K, V> clusteredWriteBehind;

  private ClusteredWriteBehindStore(Configuration<K, V> config,
                                    OperationsCodec<K, V> codec,
                                    ChainResolver<K, V> resolver,
                                    TimeSource timeSource,
                                    CacheLoaderWriter<? super K, V> loaderWriter,
                                    ExecutorService executorService) {
    super(config, codec, resolver, timeSource);
    this.cacheLoaderWriter = loaderWriter;
    this.clusteredWriteBehind = new ClusteredWriteBehind<>(this, executorService,
                                                         resolver,
                                                         this.cacheLoaderWriter,
                                                         codec);
  }


  ServerStoreProxy.ChainEntry lock(long hash) throws TimeoutException {
    return ((LockingServerStoreProxy) storeProxy).lock(hash);
  }

  void unlock(long hash, boolean localOnly) throws TimeoutException {
    ((LockingServerStoreProxy) storeProxy).unlock(hash, localOnly);
  }

  void replaceAtHead(long key, Chain expected, Chain replacement) {
    storeProxy.replaceAtHead(key, expected, replacement);
  }

  @Override
  protected ValueHolder<V> getInternal(K key) throws StoreAccessException, TimeoutException {
    try {
      ServerStoreProxy.ChainEntry chain = storeProxy.get(extractLongKey(key));
      /*
       * XXX : This condition is wrong... it should be "are there any entries for this key in the chain"
       * Most sensible fix I can think of right now would be to push the cacheLoaderWriter access in to the chain
       * resolver.
       */
      if (!chain.isEmpty()) {
        return resolver.resolve(chain, key, timeSource.getTimeMillis(), Integer.MAX_VALUE);
      } else {
        long hash = extractLongKey(key);
        lock(hash);
        try {
          V value;
          try {
            value = cacheLoaderWriter.load(key);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          if (value == null) {
            return null;
          }
          append(key, value);
          return new ClusteredValueHolder<>(value);
        } finally {
          unlock(hash, false);
        }
      }
    } catch (RuntimeException re) {
      throw handleException(re);
    }
  }

  private void append(K key, V value) throws TimeoutException {
    PutOperation<K, V> operation = new PutOperation<>(key, value, timeSource.getTimeMillis());
    ByteBuffer payload = codec.encode(operation);
    long extractedKey = extractLongKey(key);
    storeProxy.append(extractedKey, payload);
  }

  @Override
  protected PutStatus silentPut(final K key, final V value) throws StoreAccessException {
    try {
      PutWithWriterOperation<K, V> operation = new PutWithWriterOperation<>(key, value, timeSource.getTimeMillis());
      ByteBuffer payload = codec.encode(operation);
      long extractedKey = extractLongKey(key);
      storeProxy.append(extractedKey, payload);
      return PutStatus.PUT;
    } catch (Exception re) {
      throw handleException(re);
    }
  }

  @Override
  protected ValueHolder<V> silentPutIfAbsent(K key, V value) throws StoreAccessException {
    try {
      PutIfAbsentOperation<K, V> operation = new PutIfAbsentOperation<>(key, value, timeSource.getTimeMillis());
      ByteBuffer payload = codec.encode(operation);
      long extractedKey = extractLongKey(key);
      ServerStoreProxy.ChainEntry chain = storeProxy.getAndAppend(extractedKey, payload);
      return resolver.resolve(chain, key, timeSource.getTimeMillis(), Integer.MAX_VALUE);
    } catch (Exception re) {
      throw handleException(re);
    }
  }

  @Override
  protected boolean silentRemove(K key) throws StoreAccessException {
    try {
      RemoveOperation<K, V> operation = new RemoveOperation<>(key, timeSource.getTimeMillis());
      ByteBuffer payload = codec.encode(operation);
      long extractedKey = extractLongKey(key);
      ServerStoreProxy.ChainEntry chain = storeProxy.getAndAppend(extractedKey, payload);
      return resolver.resolve(chain, key, timeSource.getTimeMillis(), Integer.MAX_VALUE) != null;
    } catch (Exception re) {
      throw handleException(re);
    }
  }

  @Override
  protected ValueHolder<V> silentRemove(K key, V value) throws StoreAccessException {
    try {
      ConditionalRemoveOperation<K, V> operation = new ConditionalRemoveOperation<>(key, value, timeSource.getTimeMillis());
      ByteBuffer payload = codec.encode(operation);
      long extractedKey = extractLongKey(key);
      ServerStoreProxy.ChainEntry chain = storeProxy.getAndAppend(extractedKey, payload);
      return resolver.resolve(chain, key, timeSource.getTimeMillis(), Integer.MAX_VALUE);
    } catch (Exception re) {
      throw handleException(re);
    }
  }

  @Override
  protected ValueHolder<V> silentReplace(K key, V value) throws StoreAccessException {
    try {
      ReplaceOperation<K, V> operation = new ReplaceOperation<>(key, value, timeSource.getTimeMillis());
      ByteBuffer payload = codec.encode(operation);
      long extractedKey = extractLongKey(key);
      ServerStoreProxy.ChainEntry chain = storeProxy.getAndAppend(extractedKey, payload);
      return resolver.resolve(chain, key, timeSource.getTimeMillis(), Integer.MAX_VALUE);
    } catch (Exception re) {
      throw handleException(re);
    }
  }

  protected ValueHolder<V> silentReplace(K key, V oldValue, V newValue) throws StoreAccessException {
    try {
      ConditionalReplaceOperation<K, V> operation = new ConditionalReplaceOperation<>(key, oldValue, newValue, timeSource
        .getTimeMillis());
      ByteBuffer payload = codec.encode(operation);
      long extractedKey = extractLongKey(key);
      ServerStoreProxy.ChainEntry chain = storeProxy.getAndAppend(extractedKey, payload);
      return resolver.resolve(chain, key, timeSource.getTimeMillis(), Integer.MAX_VALUE);
    } catch (Exception re) {
      throw handleException(re);
    }
  }

  public class WriteBehindServerCallback implements ServerStoreProxy.ServerCallback {

    private final ServerStoreProxy.ServerCallback delegate;

    WriteBehindServerCallback(ServerStoreProxy.ServerCallback delegate) {
      this.delegate = delegate;
    }

    @Override
    public void onInvalidateHash(long hash) {
      this.delegate.onInvalidateHash(hash);
    }

    @Override
    public void onInvalidateAll() {
      this.delegate.onInvalidateAll();
    }

    @Override
    public void compact(ServerStoreProxy.ChainEntry chain) {
      this.delegate.compact(chain);
    }

    @Override
    public void compact(ServerStoreProxy.ChainEntry chain, long hash) {
      clusteredWriteBehind.flushWriteBehindQueue(chain, hash);
    }
  }

  private ServerStoreProxy.ServerCallback getWriteBehindServerCallback(ServerStoreProxy.ServerCallback delegate) {
    return new WriteBehindServerCallback(delegate);
  }

  /**
   * Provider of {@link ClusteredWriteBehindStore} instances.
   */
  @ServiceDependencies({ TimeSourceService.class, ClusteringService.class})
  public static class Provider extends ClusteredLoaderWriterStore.Provider {
    @Override
    protected <K, V> ClusteredStore<K, V> createStore(Configuration<K, V> storeConfig,
                                                      OperationsCodec<K, V> codec,
                                                      ChainResolver<K, V> resolver,
                                                      TimeSource timeSource,
                                                      boolean useLoaderInAtomics,
                                                      Object[] serviceConfigs) {
      WriteBehindConfiguration writeBehindConfiguration = findSingletonAmongst(WriteBehindConfiguration.class, serviceConfigs);
      if (writeBehindConfiguration != null) {
        ExecutorService executorService =
          executionService.getOrderedExecutor(writeBehindConfiguration.getThreadPoolAlias(),
                                              new LinkedBlockingQueue<>());
        return new ClusteredWriteBehindStore<>(storeConfig,
                                               codec,
                                               resolver,
                                               timeSource,
                                               storeConfig.getCacheLoaderWriter(),
                                               executorService);
      }
      throw new AssertionError();
    }

    @Override
    protected ServerStoreProxy.ServerCallback getServerCallback(ClusteredStore<?, ?> clusteredStore) {
      if (clusteredStore instanceof ClusteredWriteBehindStore) {
        return ((ClusteredWriteBehindStore<?, ?>)clusteredStore).getWriteBehindServerCallback(super.getServerCallback(clusteredStore));
      }
      throw new AssertionError();
    }

    @Override
    public int rank(Set<ResourceType<?>> resourceTypes, Collection<ServiceConfiguration<?>> serviceConfigs) {
      int parentRank = super.rank(resourceTypes, serviceConfigs);
      if (parentRank == 0 || serviceConfigs.stream().noneMatch(WriteBehindConfiguration.class::isInstance)) {
        return 0;
      }
      return parentRank + 1;
    }

    @Override
    public int rankAuthority(ResourceType<?> authorityResource, Collection<ServiceConfiguration<?>> serviceConfigs) {
      int parentRank = super.rankAuthority(authorityResource, serviceConfigs);
      if (parentRank == 0 || serviceConfigs.stream().noneMatch(WriteBehindConfiguration.class::isInstance)) {
        return 0;
      }
      return parentRank + 1;
    }
  }
}
