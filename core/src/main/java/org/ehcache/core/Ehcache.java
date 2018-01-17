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

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.ehcache.Cache;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.core.events.CacheEventDispatcher;
import org.ehcache.core.resilience.RobustResilienceStrategy;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.Store.PutStatus;
import org.ehcache.core.spi.store.Store.RemoveStatus;
import org.ehcache.core.spi.store.Store.ReplaceStatus;
import org.ehcache.core.spi.store.Store.ValueHolder;
import org.ehcache.resilience.ResilienceStrategy;
import org.ehcache.resilience.StoreAccessException;
import org.ehcache.core.statistics.BulkOps;
import org.ehcache.core.statistics.CacheOperationOutcomes.ConditionalRemoveOutcome;
import org.ehcache.core.statistics.CacheOperationOutcomes.GetAllOutcome;
import org.ehcache.core.statistics.CacheOperationOutcomes.GetOutcome;
import org.ehcache.core.statistics.CacheOperationOutcomes.PutAllOutcome;
import org.ehcache.core.statistics.CacheOperationOutcomes.PutIfAbsentOutcome;
import org.ehcache.core.statistics.CacheOperationOutcomes.PutOutcome;
import org.ehcache.core.statistics.CacheOperationOutcomes.RemoveAllOutcome;
import org.ehcache.core.statistics.CacheOperationOutcomes.RemoveOutcome;
import org.ehcache.core.statistics.CacheOperationOutcomes.ReplaceOutcome;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.spi.loaderwriter.BulkCacheLoadingException;
import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.slf4j.Logger;

/**
 * Implementation of the {@link Cache} interface when no {@link CacheLoaderWriter} is involved.
 * <p>
 * {@code Ehcache} users should not have to depend on this type but rely exclusively on the api types in package
 * {@code org.ehcache}.
 *
 * @see EhcacheWithLoaderWriter
 */
public class Ehcache<K, V> extends EhcacheBase<K, V> {

  /**
   * Creates a new {@code Ehcache} based on the provided parameters.
   *
   * @param configuration the cache configuration
   * @param store the store to use
   * @param eventDispatcher the event dispatcher
   * @param logger the logger
   */
  public Ehcache(CacheConfiguration<K, V> configuration, final Store<K, V> store, CacheEventDispatcher<K, V> eventDispatcher, Logger logger) {
    this(new EhcacheRuntimeConfiguration<>(configuration), store, eventDispatcher, logger, new StatusTransitioner(logger));
  }

  Ehcache(EhcacheRuntimeConfiguration<K, V> runtimeConfiguration, Store<K, V> store,
          CacheEventDispatcher<K, V> eventDispatcher, Logger logger, StatusTransitioner statusTransitioner) {
    super(runtimeConfiguration, store, new RobustResilienceStrategy<>(store), eventDispatcher, logger, statusTransitioner);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Store.ValueHolder<V> doGet(K key) throws StoreAccessException {
    return store.get(key);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void put(final K key, final V value) {
    putObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(key, value);

    try {
      PutStatus status = store.put(key, value);
      switch (status) {
      case PUT:
        putObserver.end(PutOutcome.PUT);
        break;
      case NOOP:
        putObserver.end(PutOutcome.NOOP);
        break;
      default:
        throw new AssertionError("Invalid Status.");
      }
    } catch (StoreAccessException e) {
      try {
        resilienceStrategy.putFailure(key, value, e);
      } finally {
        putObserver.end(PutOutcome.FAILURE);
      }
    }
  }

  protected boolean removeInternal(final K key) {
    removeObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(key);

    boolean removed = false;
    try {
      removed = store.remove(key);
      if (removed) {
        removeObserver.end(RemoveOutcome.SUCCESS);
      } else {
        removeObserver.end(RemoveOutcome.NOOP);
      }
    } catch (StoreAccessException e) {
      try {
        resilienceStrategy.removeFailure(key, e);
      } finally {
        removeObserver.end(RemoveOutcome.FAILURE);
      }
    }

    return removed;
  }

  protected Map<K, V> getAllInternal(Set<? extends K> keys, boolean includeNulls) throws BulkCacheLoadingException {
    getAllObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNullContent(keys);
    if(keys.isEmpty()) {
      getAllObserver.end(GetAllOutcome.SUCCESS);
      return Collections.emptyMap();
    }

    Map<K, V> result = new HashMap<>();
    try {
      Map<K, Store.ValueHolder<V>> computedMap = store.bulkComputeIfAbsent(keys, new GetAllFunction<>());

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
      getAllObserver.end(GetAllOutcome.SUCCESS);
      return result;
    } catch (StoreAccessException e) {
      try {
         return resilienceStrategy.getAllFailure(keys, e);
      } finally {
        getAllObserver.end(GetAllOutcome.FAILURE);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void putAll(final Map<? extends K, ? extends V> entries) throws BulkCacheWritingException {
    putAllObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(entries);
    if(entries.isEmpty()) {
      putAllObserver.end(PutAllOutcome.SUCCESS);
      return;
    }

    // Copy all entries to write into a Map
    final Map<K, V> entriesToRemap = new HashMap<>();
    for (Map.Entry<? extends K, ? extends V> entry: entries.entrySet()) {
      // If a key/value is null, throw NPE, nothing gets mutated
      if (entry.getKey() == null || entry.getValue() == null) {
        throw new NullPointerException();
      }
      entriesToRemap.put(entry.getKey(), entry.getValue());
    }

    try {
      PutAllFunction<K, V> putAllFunction = new PutAllFunction<>(logger, entriesToRemap, runtimeConfiguration.getExpiryPolicy());
      store.bulkCompute(entries.keySet(), putAllFunction);
      addBulkMethodEntriesCount(BulkOps.PUT_ALL, putAllFunction.getActualPutCount().get());
      addBulkMethodEntriesCount(BulkOps.UPDATE_ALL, putAllFunction.getActualUpdateCount().get());
      putAllObserver.end(PutAllOutcome.SUCCESS);
    } catch (StoreAccessException e) {
      try {
        resilienceStrategy.putAllFailure(entries, e);
      } finally {
        putAllObserver.end(PutAllOutcome.FAILURE);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void removeAll(final Set<? extends K> keys) throws BulkCacheWritingException {
    removeAllObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(keys);
    if(keys.isEmpty()) {
      removeAllObserver.end(RemoveAllOutcome.SUCCESS);
      return;
    }

    for (K key: keys) {
      if (key == null) {
        throw new NullPointerException();
      }
    }


    try {
      RemoveAllFunction<K, V> removeAllFunction = new RemoveAllFunction<>();
      store.bulkCompute(keys, removeAllFunction);
      addBulkMethodEntriesCount(BulkOps.REMOVE_ALL, removeAllFunction.getActualRemoveCount().get());
      removeAllObserver.end(RemoveAllOutcome.SUCCESS);
    } catch (StoreAccessException e) {
      try {
        resilienceStrategy.removeAllFailure(keys, e);
      } finally {
        removeAllObserver.end(RemoveAllOutcome.FAILURE);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V putIfAbsent(final K key, final V value) {
    putIfAbsentObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(key, value);

    try {
      ValueHolder<V> inCache = store.putIfAbsent(key, value);
      boolean absent = (inCache == null);
      if (absent) {
        putIfAbsentObserver.end(PutIfAbsentOutcome.PUT);
        return null;
      } else {
        putIfAbsentObserver.end(PutIfAbsentOutcome.HIT);
        return inCache.get();
      }
    } catch (StoreAccessException e) {
      try {
        return resilienceStrategy.putIfAbsentFailure(key, value, null, e, false); // FIXME: We can't know if it's absent or not
      } finally {
        putIfAbsentObserver.end(PutIfAbsentOutcome.FAILURE);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean remove(final K key, final V value) {
    conditionalRemoveObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(key, value);
    boolean removed = false;

    try {
      RemoveStatus status = store.remove(key, value);
      switch (status) {
      case REMOVED:
        removed = true;
        conditionalRemoveObserver.end(ConditionalRemoveOutcome.SUCCESS);
        break;
      case KEY_MISSING:
        conditionalRemoveObserver.end(ConditionalRemoveOutcome.FAILURE_KEY_MISSING);
        break;
      case KEY_PRESENT:
        conditionalRemoveObserver.end(ConditionalRemoveOutcome.FAILURE_KEY_PRESENT);
        break;
      default:
        throw new AssertionError("Invalid Status.");
      }
    } catch (StoreAccessException e) {
      try {
        return resilienceStrategy.removeFailure(key, value, e, false); // FIXME: We can't know if it's removed or not
      } finally {
        conditionalRemoveObserver.end(ConditionalRemoveOutcome.FAILURE);
      }
    }
    return removed;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V replace(final K key, final V value) {
    replaceObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(key, value);

    try {
      ValueHolder<V> old = store.replace(key, value);
      if (old != null) {
        replaceObserver.end(ReplaceOutcome.HIT);
      } else {
        replaceObserver.end(ReplaceOutcome.MISS_NOT_PRESENT);
      }
      return old == null ? null : old.get();
    } catch (StoreAccessException e) {
      try {
        return resilienceStrategy.replaceFailure(key, value, e);
      } finally {
        replaceObserver.end(ReplaceOutcome.FAILURE);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean replace(final K key, final V oldValue, final V newValue) {
    replaceObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(key, oldValue, newValue);

    boolean success = false;

    try {
      ReplaceStatus status = store.replace(key, oldValue, newValue);
      switch (status) {
      case HIT:
        success = true;
        replaceObserver.end(ReplaceOutcome.HIT);
        break;
      case MISS_PRESENT:
        replaceObserver.end(ReplaceOutcome.MISS_PRESENT);
        break;
      case MISS_NOT_PRESENT:
        replaceObserver.end(ReplaceOutcome.MISS_NOT_PRESENT);
        break;
      default:
        throw new AssertionError("Invalid Status.");
      }

      return success;
    } catch (StoreAccessException e) {
      try {
        return resilienceStrategy.replaceFailure(key, oldValue, newValue, e, false); // FIXME: We can't know if there was a match
      } finally {
        replaceObserver.end(ReplaceOutcome.FAILURE);
      }
    }
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
    return null;
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

          V newValue = computeFunction.apply(mappedKey, mappedValue);

          if (newValue == mappedValue) {
            if (! replaceEqual.get()) {
              return mappedValue;
            }
          }

          if (newValueAlreadyExpired(mappedKey, mappedValue, newValue)) {
            return null;
          }

          if (withStatsAndEvents.get()) {
            if (newValue == null) {
              removeObserver.end(RemoveOutcome.SUCCESS);
            } else {
              putObserver.end(PutOutcome.PUT);
            }
          }

          return newValue;
        };

        store.compute(key, fn, replaceEqual);
      } catch (StoreAccessException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public V getAndRemove(K key) {
      getObserver.begin();
      removeObserver.begin();

      final AtomicReference<V> existingValue = new AtomicReference<>();
      try {
        store.compute(key, (mappedKey, mappedValue) -> {
          existingValue.set(mappedValue);

          return null;
        });
      } catch (StoreAccessException e) {
        getObserver.end(org.ehcache.core.statistics.CacheOperationOutcomes.GetOutcome.FAILURE);
        removeObserver.end(RemoveOutcome.FAILURE);
        throw new RuntimeException(e);
      }

      V returnValue = existingValue.get();
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

      final AtomicReference<V> existingValue = new AtomicReference<>();
      try {
        store.compute(key, (mappedKey, mappedValue) -> {
          existingValue.set(mappedValue);

          if (newValueAlreadyExpired(mappedKey, mappedValue, value)) {
            return null;
          }

          return value;
        });
      } catch (StoreAccessException e) {
        getObserver.end(org.ehcache.core.statistics.CacheOperationOutcomes.GetOutcome.FAILURE);
        putObserver.end(PutOutcome.FAILURE);
        throw new RuntimeException(e);
      }

      V returnValue = existingValue.get();
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
      Map<K, V> computeResult = new LinkedHashMap<>();

      // put all the entries to get ordering correct
      for (K key : keys) {
        computeResult.put(key, null);
      }

      return computeResult.entrySet();
    }
  }

}
