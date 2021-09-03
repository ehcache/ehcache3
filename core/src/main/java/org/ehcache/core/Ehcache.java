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

package org.ehcache.core;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.ehcache.Cache;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.core.events.CacheEventDispatcher;
import org.ehcache.core.util.CollectionUtil;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.Store.ValueHolder;
import org.ehcache.spi.resilience.ResilienceStrategy;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.core.statistics.BulkOps;
import org.ehcache.core.statistics.CacheOperationOutcomes.GetOutcome;
import org.ehcache.core.statistics.CacheOperationOutcomes.PutOutcome;
import org.ehcache.core.statistics.CacheOperationOutcomes.RemoveOutcome;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.slf4j.Logger;

/**
 * Implementation of the {@link Cache} interface when no {@link CacheLoaderWriter} is involved.
 * <p>
 * {@code Ehcache} users should not have to depend on this type but rely exclusively on the api types in package
 * {@code org.ehcache}.
 *
 */
public class Ehcache<K, V> extends EhcacheBase<K, V> {

  private final CacheLoaderWriter<? super K, V> cacheLoaderWriter;

  /**
   * Creates a new {@code Ehcache} based on the provided parameters.
   *
   * @param configuration the cache configuration
   * @param store the store to use
   * @param eventDispatcher the event dispatcher
   * @param logger the logger
   */
  public Ehcache(CacheConfiguration<K, V> configuration, final Store<K, V> store, ResilienceStrategy<K, V> resilienceStrategy,
                 CacheEventDispatcher<K, V> eventDispatcher, Logger logger) {
    this(new EhcacheRuntimeConfiguration<>(configuration), store, resilienceStrategy, eventDispatcher, logger, new StatusTransitioner(logger), null);
  }

  Ehcache(EhcacheRuntimeConfiguration<K, V> runtimeConfiguration, Store<K, V> store, ResilienceStrategy<K, V> resilienceStrategy,
          CacheEventDispatcher<K, V> eventDispatcher, Logger logger, StatusTransitioner statusTransitioner, CacheLoaderWriter<? super K, V> cacheLoaderWriter) {
    super(runtimeConfiguration, store, resilienceStrategy, eventDispatcher, logger, statusTransitioner);
    this.cacheLoaderWriter = cacheLoaderWriter;
  }

  public Ehcache(CacheConfiguration<K, V> configuration, final Store<K, V> store, ResilienceStrategy<K, V> resilienceStrategy,
                 CacheEventDispatcher<K, V> eventDispatcher, Logger logger, CacheLoaderWriter<? super K, V> cacheLoaderWriter) {
    super(new EhcacheRuntimeConfiguration<>(configuration), store, resilienceStrategy, eventDispatcher, logger, new StatusTransitioner(logger));
    this.cacheLoaderWriter = cacheLoaderWriter;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Store.ValueHolder<V> doGet(K key) throws StoreAccessException {
    return store.get(key);
  }

  protected Store.PutStatus doPut(K key, V value) throws StoreAccessException {
    return store.put(key, value);
  }

  protected boolean doRemoveInternal(final K key) throws StoreAccessException {
    return store.remove(key);
  }

  protected Map<K, V> doGetAllInternal(Set<? extends K> keys, boolean includeNulls) throws StoreAccessException {
      Map<K, Store.ValueHolder<V>> computedMap = store.bulkComputeIfAbsent(keys, new GetAllFunction<>());
      Map<K, V> result = new HashMap<>(computedMap.size());

      int hits = 0;
      int keyCount = 0;
      for (Map.Entry<K, Store.ValueHolder<V>> entry : computedMap.entrySet()) {
        keyCount++;
        if (entry.getValue() != null) {
          result.put(entry.getKey(), entry.getValue().get());
          hits++;
        } else if (includeNulls) {
          result.put(entry.getKey(), null);
        }
      }

      addBulkMethodEntriesCount(BulkOps.GET_ALL_HITS, hits);
      addBulkMethodEntriesCount(BulkOps.GET_ALL_MISS, keyCount - hits);
      return result;
  }

  @Override
  public void doPutAll(final Map<? extends K, ? extends V> entries) throws StoreAccessException {
    // Copy all entries to write into a Map
    Map<K, V> entriesToRemap = CollectionUtil.copyMapButFailOnNull(entries);

    PutAllFunction<K, V> putAllFunction = new PutAllFunction<>(logger, entriesToRemap, runtimeConfiguration.getExpiryPolicy());
    store.bulkCompute(entries.keySet(), putAllFunction);
    addBulkMethodEntriesCount(BulkOps.PUT_ALL, putAllFunction.getActualPutCount().get());
    addBulkMethodEntriesCount(BulkOps.UPDATE_ALL, putAllFunction.getActualUpdateCount().get());
  }

  protected void doRemoveAll(final Set<? extends K> keys) throws BulkCacheWritingException, StoreAccessException {
    RemoveAllFunction<K, V> removeAllFunction = new RemoveAllFunction<>();
    store.bulkCompute(keys, removeAllFunction);
    addBulkMethodEntriesCount(BulkOps.REMOVE_ALL, removeAllFunction.getActualRemoveCount().get());
  }

  @Override
  public ValueHolder<V> doPutIfAbsent(final K key, final V value, Consumer<Boolean> put) throws StoreAccessException {
    ValueHolder<V> result = store.putIfAbsent(key, value, put);
    if(result == null) {
      put.accept(true);
    }
    return result;
  }

  @Override
  protected Store.RemoveStatus doRemove(K key, V value) throws StoreAccessException {
    return store.remove(key, value);
  }

  @Override
  protected V doReplace(K key, V value) throws StoreAccessException {
    ValueHolder<V> old = store.replace(key, value);
    return old == null ? null : old.get();
  }

  @Override
  protected Store.ReplaceStatus doReplace(final K key, final V oldValue, final V newValue) throws StoreAccessException {
      return store.replace(key, oldValue, newValue);
  }

  @Override
  public Jsr107Cache<K, V> createJsr107Cache() {
    return new Jsr107CacheImpl();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CacheLoaderWriter<? super K, V> getCacheLoaderWriter() {
    return this.cacheLoaderWriter;
  }

  private final class Jsr107CacheImpl extends Jsr107CacheBase {

    @Override
    public void compute(K key, final BiFunction<? super K, ? super V, ? extends V> computeFunction,
        final Supplier<Boolean> replaceEqual, final Supplier<Boolean> invokeWriter, final Supplier<Boolean> withStatsAndEvents) {
      putObserver.begin();
      removeObserver.begin();
      getObserver.begin();

      try {
        BiFunction<K, V, V> fn = (mappedKey, mappedValue) -> {
          if (mappedValue == null) {
            getObserver.end(GetOutcome.MISS);
          } else {
            getObserver.end(GetOutcome.HIT);
          }

          return computeFunction.apply(mappedKey, mappedValue);

        };

        ValueHolder<V> compute = store.computeAndGet(key, fn, replaceEqual, invokeWriter);
        V newValue = compute == null ? null : compute.get();
        if (withStatsAndEvents.get()) {
          if (newValue == null) {
            removeObserver.end(RemoveOutcome.SUCCESS);
          } else {
            putObserver.end(PutOutcome.PUT);
          }
        }
      } catch (StoreAccessException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public V getAndRemove(K key) {
      getObserver.begin();
      removeObserver.begin();

      ValueHolder<V> existingValue;
      try {
        existingValue = store.getAndCompute(key, (mappedKey, mappedValue) -> null);
      } catch (StoreAccessException e) {
        getObserver.end(org.ehcache.core.statistics.CacheOperationOutcomes.GetOutcome.FAILURE);
        removeObserver.end(RemoveOutcome.FAILURE);
        throw new RuntimeException(e);
      }

      V returnValue = existingValue == null ? null : existingValue.get();
      if (returnValue != null) {
        getObserver.end(org.ehcache.core.statistics.CacheOperationOutcomes.GetOutcome.HIT);
        removeObserver.end(RemoveOutcome.SUCCESS);
      } else {
        getObserver.end(org.ehcache.core.statistics.CacheOperationOutcomes.GetOutcome.MISS);
      }
      return returnValue;
    }

    @Override
    public V getAndPut(K key, final V value) {
      getObserver.begin();
      putObserver.begin();

      ValueHolder<V> existingValue;
      try {
        existingValue = store.getAndCompute(key, (mappedKey, mappedValue) -> value);
      } catch (StoreAccessException e) {
        getObserver.end(org.ehcache.core.statistics.CacheOperationOutcomes.GetOutcome.FAILURE);
        putObserver.end(PutOutcome.FAILURE);
        throw new RuntimeException(e);
      }

      V returnValue = existingValue == null ? null : existingValue.get();
      if (returnValue != null) {
        getObserver.end(org.ehcache.core.statistics.CacheOperationOutcomes.GetOutcome.HIT);
      } else {
        getObserver.end(org.ehcache.core.statistics.CacheOperationOutcomes.GetOutcome.MISS);
      }
      putObserver.end(PutOutcome.PUT);
      return returnValue;
    }
  }

  // The compute function that will return the keys to their NEW values, taking the keys to their old values as input;
  // but this could happen in batches, i.e. not necessary containing all of the entries of the Iterable passed to this method
  public static class PutAllFunction<K, V> implements Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> {

    private final Logger logger;
    private final Map<K, V> entriesToRemap;
    private final ExpiryPolicy<? super K, ? super V> expiry;
    private final AtomicInteger actualPutCount = new AtomicInteger();
    private final AtomicInteger actualUpdateCount = new AtomicInteger();

    public PutAllFunction(Logger logger, Map<K, V> entriesToRemap, ExpiryPolicy<? super K, ? super V> expiry) {
      this.logger = logger;
      this.entriesToRemap = entriesToRemap;
      this.expiry = expiry;
    }

    @Override
    public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(final Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) {
      Map<K, V> mutations = new LinkedHashMap<>();

      // then record we handled these mappings
      for (Map.Entry<? extends K, ? extends V> entry: entries) {
        K key = entry.getKey();
        V existingValue = entry.getValue();
        V newValue = entriesToRemap.remove(key);

        if (newValueAlreadyExpired(key, existingValue, newValue)) {
          mutations.put(key, null);
        } else {
          actualPutCount.incrementAndGet();
          if(existingValue != null) {
            actualUpdateCount.incrementAndGet();
          }
          mutations.put(key, newValue);
        }
      }

      // Finally return the values to be installed in the Cache's Store
      return mutations.entrySet();
    }

    public Map<K, V> getEntriesToRemap() {
      return entriesToRemap;
    }

    private boolean newValueAlreadyExpired(K key, V oldValue, V newValue) {
      return EhcacheBase.newValueAlreadyExpired(logger, expiry, key, oldValue, newValue);
    }

    public AtomicInteger getActualPutCount() {
      return actualPutCount;
    }

    public AtomicInteger getActualUpdateCount() {
      return actualUpdateCount;
    }
  }

  public static class RemoveAllFunction<K, V> implements Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> {

    private final AtomicInteger actualRemoveCount = new AtomicInteger();

    @Override
    public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(final Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) {
      Map<K, V> results = new LinkedHashMap<>();

      for (Map.Entry<? extends K, ? extends V> entry : entries) {
        K key = entry.getKey();
        V existingValue = entry.getValue();

        if (existingValue != null) {
          actualRemoveCount.incrementAndGet();
        }
        results.put(key, null);
      }

      return results.entrySet();
    }

    public AtomicInteger getActualRemoveCount() {
      return actualRemoveCount;
    }
  }

  public static class GetAllFunction<K, V> implements Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> {

    @Override
    public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(final Iterable<? extends K> keys) {
      int size = CollectionUtil.findBestCollectionSize(keys, 1); // in our current implementation, we have one entry all the time

      List<Map.Entry<K, V>> computeResult = new ArrayList<>(size);

      for (K key : keys) {
        computeResult.add(new AbstractMap.SimpleImmutableEntry<>(key, null));
      }

      return computeResult;
    }
  }

}
