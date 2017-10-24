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
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.ehcache.Cache;
import org.ehcache.Status;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheRuntimeConfiguration;
import org.ehcache.core.events.CacheEventDispatcher;
import org.ehcache.core.internal.resilience.LoggingRobustResilienceStrategy;
import org.ehcache.core.internal.resilience.RecoveryCache;
import org.ehcache.core.internal.resilience.ResilienceStrategy;
import org.ehcache.core.spi.LifeCycled;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.Store.PutStatus;
import org.ehcache.core.spi.store.Store.RemoveStatus;
import org.ehcache.core.spi.store.Store.ReplaceStatus;
import org.ehcache.core.spi.store.Store.ValueHolder;
import org.ehcache.core.spi.store.StoreAccessException;
import org.ehcache.core.statistics.BulkOps;
import org.ehcache.core.statistics.CacheOperationOutcomes.ClearOutcome;
import org.ehcache.core.statistics.CacheOperationOutcomes.ConditionalRemoveOutcome;
import org.ehcache.core.statistics.CacheOperationOutcomes.GetAllOutcome;
import org.ehcache.core.statistics.CacheOperationOutcomes.GetOutcome;
import org.ehcache.core.statistics.CacheOperationOutcomes.PutAllOutcome;
import org.ehcache.core.statistics.CacheOperationOutcomes.PutIfAbsentOutcome;
import org.ehcache.core.statistics.CacheOperationOutcomes.PutOutcome;
import org.ehcache.core.statistics.CacheOperationOutcomes.RemoveAllOutcome;
import org.ehcache.core.statistics.CacheOperationOutcomes.RemoveOutcome;
import org.ehcache.core.statistics.CacheOperationOutcomes.ReplaceOutcome;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expiry;
import org.ehcache.spi.loaderwriter.BulkCacheLoadingException;
import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.slf4j.Logger;
import org.terracotta.statistics.StatisticsManager;
import org.terracotta.statistics.jsr166e.LongAdder;
import org.terracotta.statistics.observer.OperationObserver;

import static org.ehcache.core.exceptions.ExceptionFactory.newCacheLoadingException;
import static org.ehcache.core.internal.util.ValueSuppliers.supplierOf;
import static org.terracotta.statistics.StatisticBuilder.operation;

/**
 * Implementation of the {@link Cache} interface when no {@link CacheLoaderWriter} is involved.
 * <p>
 * {@code Ehcache} users should not have to depend on this type but rely exclusively on the api types in package
 * {@code org.ehcache}.
 *
 * @see EhcacheWithLoaderWriter
 */
public class Ehcache<K, V> implements InternalCache<K, V> {

  private final StatusTransitioner statusTransitioner;

  private final Store<K, V> store;
  private final ResilienceStrategy<K, V> resilienceStrategy;
  private final EhcacheRuntimeConfiguration<K, V> runtimeConfiguration;
  private final Jsr107CacheImpl jsr107Cache;
  protected final Logger logger;

  private final OperationObserver<GetOutcome> getObserver = operation(GetOutcome.class).named("get").of(this).tag("cache").build();
  private final OperationObserver<GetAllOutcome> getAllObserver = operation(GetAllOutcome.class).named("getAll").of(this).tag("cache").build();
  private final OperationObserver<PutOutcome> putObserver = operation(PutOutcome.class).named("put").of(this).tag("cache").build();
  private final OperationObserver<PutAllOutcome> putAllObserver = operation(PutAllOutcome.class).named("putAll").of(this).tag("cache").build();
  private final OperationObserver<RemoveOutcome> removeObserver = operation(RemoveOutcome.class).named("remove").of(this).tag("cache").build();
  private final OperationObserver<RemoveAllOutcome> removeAllObserver = operation(RemoveAllOutcome.class).named("removeAll").of(this).tag("cache").build();
  private final OperationObserver<ConditionalRemoveOutcome> conditionalRemoveObserver = operation(ConditionalRemoveOutcome.class).named("conditionalRemove").of(this).tag("cache").build();
  private final OperationObserver<PutIfAbsentOutcome> putIfAbsentObserver = operation(PutIfAbsentOutcome.class).named("putIfAbsent").of(this).tag("cache").build();
  private final OperationObserver<ReplaceOutcome> replaceObserver = operation(ReplaceOutcome.class).named("replace").of(this).tag("cache").build();
  private final Map<BulkOps, LongAdder> bulkMethodEntries = new EnumMap<>(BulkOps.class);
  private final OperationObserver<ClearOutcome> clearObserver = operation(ClearOutcome.class).named("clear").of(this).tag("cache").build();

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
    this.store = store;
    runtimeConfiguration.addCacheConfigurationListener(store.getConfigurationChangeListeners());
    StatisticsManager.associate(store).withParent(this);

    if (store instanceof RecoveryCache) {
      this.resilienceStrategy = new LoggingRobustResilienceStrategy<>(castToRecoveryCache(store));
    } else {
      this.resilienceStrategy = new LoggingRobustResilienceStrategy<>(recoveryCache(store));
    }

    this.runtimeConfiguration = runtimeConfiguration;
    runtimeConfiguration.addCacheConfigurationListener(eventDispatcher.getConfigurationChangeListeners());
    this.jsr107Cache = new Jsr107CacheImpl();

    this.logger=logger;
    this.statusTransitioner = statusTransitioner;
    for (BulkOps bulkOp : BulkOps.values()) {
      bulkMethodEntries.put(bulkOp, new LongAdder());
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<BulkOps, LongAdder> getBulkMethodEntries() {
    return bulkMethodEntries;
  }

  @SuppressWarnings("unchecked")
  private RecoveryCache<K> castToRecoveryCache(Store<K, V> store) {
    return (RecoveryCache<K>) store;
  }

  private V getNoLoader(K key) {
    return get(key);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V get(final K key) {
    getObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(key);

    try {
      final Store.ValueHolder<V> valueHolder = store.get(key);

      // Check for expiry first
      if (valueHolder == null) {
        getObserver.end(GetOutcome.MISS);
        return null;
      } else {
        getObserver.end(GetOutcome.HIT);
        return valueHolder.value();
      }
    } catch (StoreAccessException e) {
      try {
        return resilienceStrategy.getFailure(key, e);
      } finally {
        getObserver.end(GetOutcome.FAILURE);
      }
    }
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

  private boolean newValueAlreadyExpired(K key, V oldValue, V newValue) {
    return newValueAlreadyExpired(logger, runtimeConfiguration.getExpiry(), key, oldValue, newValue);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean containsKey(final K key) {
    statusTransitioner.checkAvailable();
    checkNonNull(key);
    try {
      return store.containsKey(key);
    } catch (StoreAccessException e) {
      return resilienceStrategy.containsKeyFailure(key, e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void remove(K key) {
    removeInternal(key); // ignore return value;
  }


  private boolean removeInternal(final K key) {
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

  /**
   * {@inheritDoc}
   */
  @Override
  public void clear() {
    this.clearObserver.begin();
    statusTransitioner.checkAvailable();
    try {
      store.clear();
        this.clearObserver.end(ClearOutcome.SUCCESS);
    } catch (StoreAccessException e) {
      this.clearObserver.end(ClearOutcome.FAILURE);
      resilienceStrategy.clearFailure(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<Entry<K, V>> iterator() {
    statusTransitioner.checkAvailable();
    return new CacheEntryIterator(false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<K, V> getAll(Set<? extends K> keys) throws BulkCacheLoadingException {
    return getAllInternal(keys, true);
  }

  private Map<K, V> getAllInternal(Set<? extends K> keys, boolean includeNulls) throws BulkCacheLoadingException {
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
          result.put(entry.getKey(), entry.getValue().value());
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

  LinkedHashSet<Map.Entry<? extends K, ? extends V>> nullValuesForKeys(final Iterable<? extends K> keys) {
    final LinkedHashSet<Map.Entry<? extends K, ? extends V>> entries = new LinkedHashSet<>();
    for (K key : keys) {
      entries.add(new AbstractMap.SimpleEntry<>(key, null));
    }
    return entries;
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
      PutAllFunction<K, V> putAllFunction = new PutAllFunction<>(logger, entriesToRemap, runtimeConfiguration.getExpiry());
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

    boolean absent = false;
    try {
      ValueHolder<V> inCache = store.putIfAbsent(key, value);
      absent = (inCache == null);
      if (absent) {
        putIfAbsentObserver.end(PutIfAbsentOutcome.PUT);
        return null;
      } else {
        putIfAbsentObserver.end(PutIfAbsentOutcome.HIT);
        return inCache.value();
      }
    } catch (StoreAccessException e) {
      try {
        return resilienceStrategy.putIfAbsentFailure(key, value, null, e, absent);
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
    RemoveStatus status = null;
    boolean removed = false;

    try {
      status = store.remove(key, value);
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
        return resilienceStrategy.removeFailure(key, value, e, removed);
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
      return old == null ? null : old.value();
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

    ReplaceStatus status = null;
    boolean success = false;

    try {
      status = store.replace(key, oldValue, newValue);
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
        return resilienceStrategy.replaceFailure(key, oldValue, newValue, e, success);
      } finally {
        replaceObserver.end(ReplaceOutcome.FAILURE);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CacheRuntimeConfiguration<K, V> getRuntimeConfiguration() {
    return runtimeConfiguration;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void init() {
    statusTransitioner.init().succeeded();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
    statusTransitioner.close().succeeded();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Status getStatus() {
    return statusTransitioner.currentStatus();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addHook(LifeCycled hook) {
    statusTransitioner.addHook(hook);
  }

  void removeHook(LifeCycled hook) {
    statusTransitioner.removeHook(hook);
  }

  private static void checkNonNull(Object thing) {
    if(thing == null) {
      throw new NullPointerException();
    }
  }

  private static void checkNonNull(Object... things) {
    for (Object thing : things) {
      checkNonNull(thing);
    }
  }

  private void checkNonNullContent(Collection<?> collectionOfThings) {
    checkNonNull(collectionOfThings);
    for (Object thing : collectionOfThings) {
      checkNonNull(thing);
    }
  }

  private void addBulkMethodEntriesCount(BulkOps op, long count) {
    bulkMethodEntries.get(op).add(count);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Jsr107Cache<K, V> getJsr107Cache() {
    return jsr107Cache;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CacheLoaderWriter<? super K, V> getCacheLoaderWriter() {
    return null;
  }

  private final class Jsr107CacheImpl implements Jsr107Cache<K, V> {
    @Override
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, Function<Iterable<? extends K>, Map<K, V>> loadFunction) {
      if(keys.isEmpty()) {
        return ;
      }
      if (replaceExistingValues) {
        loadAllReplace(keys, loadFunction);
      } else {
        loadAllAbsent(keys, loadFunction);
      }
    }

    @Override
    public Iterator<Entry<K, V>> specIterator() {
      return new SpecIterator<>(this, store);
    }

    @Override
    public V getNoLoader(K key) {
      return Ehcache.this.getNoLoader(key);
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys) {
      return Ehcache.this.getAllInternal(keys, false);
    }

    private void loadAllAbsent(Set<? extends K> keys, final Function<Iterable<? extends K>, Map<K, V>> loadFunction) {
      try {
        store.bulkComputeIfAbsent(keys, absentKeys -> cacheLoaderWriterLoadAllForKeys(absentKeys, loadFunction).entrySet());
      } catch (StoreAccessException e) {
        throw newCacheLoadingException(e);
      }
    }

    Map<K, V> cacheLoaderWriterLoadAllForKeys(Iterable<? extends K> keys, Function<Iterable<? extends K>, Map<K, V>> loadFunction) {
      try {
        Map<? super K, ? extends V> loaded = loadFunction.apply(keys);

        // put into a new map since we can't assume the 107 cache loader returns things ordered, or necessarily with all the desired keys
        Map<K, V> rv = new LinkedHashMap<>();
        for (K key : keys) {
          rv.put(key, loaded.get(key));
        }
        return rv;
      } catch (Exception e) {
        throw newCacheLoadingException(e);
      }
    }

    private void loadAllReplace(Set<? extends K> keys, final Function<Iterable<? extends K>, Map<K, V>> loadFunction) {
      try {
        store.bulkCompute(keys, entries -> {
          Collection<K> keys1 = new ArrayList<>();
          for (Map.Entry<? extends K, ? extends V> entry : entries) {
            keys1.add(entry.getKey());
          }
          return cacheLoaderWriterLoadAllForKeys(keys1, loadFunction).entrySet();
        });
      } catch (StoreAccessException e) {
        throw newCacheLoadingException(e);
      }
    }

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

    @Override
    public boolean remove(K key) {
      return removeInternal(key);
    }

    @Override
    public void removeAll() {
      Store.Iterator<Entry<K, ValueHolder<V>>> iterator = store.iterator();
      while (iterator.hasNext()) {
        try {
          Entry<K, ValueHolder<V>> next = iterator.next();
          remove(next.getKey());
        } catch (StoreAccessException cae) {
          // skip
        }
      }
    }
  }

  private class CacheEntryIterator implements Iterator<Entry<K, V>> {

    private final Store.Iterator<Entry<K, Store.ValueHolder<V>>> iterator;
    private final boolean quiet;
    private Cache.Entry<K, ValueHolder<V>> current;
    private Cache.Entry<K, ValueHolder<V>> next;
    private StoreAccessException nextException;

    public CacheEntryIterator(boolean quiet) {
      this.quiet = quiet;
      this.iterator = store.iterator();
      advance();
    }

    private void advance() {
      try {
        while (iterator.hasNext()) {
          next = iterator.next();
          if (getNoLoader(next.getKey()) != null) {
            return;
          }
        }
        next = null;
      } catch (RuntimeException re) {
        nextException = new StoreAccessException(re);
        next = null;
      } catch (StoreAccessException cae) {
        nextException = cae;
        next = null;
      }
    }

    @Override
    public boolean hasNext() {
      statusTransitioner.checkAvailable();
      return nextException != null || next != null;
    }

    @Override
    public Entry<K, V> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      if (!quiet) getObserver.begin();
      if (nextException == null) {
        if (!quiet) getObserver.end(org.ehcache.core.statistics.CacheOperationOutcomes.GetOutcome.HIT);
        current = next;
        advance();
        return new ValueHolderBasedEntry<>(current);
      } else {
        if (!quiet) getObserver.end(org.ehcache.core.statistics.CacheOperationOutcomes.GetOutcome.FAILURE);
        StoreAccessException cae = nextException;
        nextException = null;
        return resilienceStrategy.iteratorFailure(cae);
      }
    }

    @Override
    public void remove() {
      statusTransitioner.checkAvailable();
      if (current == null) {
        throw new IllegalStateException("No current element");
      }
      Ehcache.this.remove(current.getKey(), current.getValue().value());
      current = null;
    }
  }

  private static <K> RecoveryCache<K> recoveryCache(final Store<K, ?> store) {
    return new RecoveryCache<K>() {

      @Override
      public void obliterate() throws StoreAccessException {
        store.clear();
      }

      @Override
      public void obliterate(K key) throws StoreAccessException {
        store.remove(key);
      }

      @Override
      public void obliterate(Iterable<? extends K> keys) throws StoreAccessException {
        for (K key : keys) {
          obliterate(key);
        }
      }
    };
  }

  private static <K, V> boolean newValueAlreadyExpired(Logger logger, Expiry<? super K, ? super V> expiry, K key, V oldValue, V newValue) {
    if (newValue == null) {
      return false;
    }

    Duration duration;
    try {
      if (oldValue == null) {
          duration = expiry.getExpiryForCreation(key, newValue);
      } else {
          duration = expiry.getExpiryForUpdate(key, supplierOf(oldValue), newValue);
      }
    } catch (RuntimeException re) {
      logger.error("Expiry computation caused an exception - Expiry duration will be 0 ", re);
      return true;
    }

    return Duration.ZERO.equals(duration);
  }

  private static class ValueHolderBasedEntry<K, V> implements Cache.Entry<K, V> {
    private final Cache.Entry<K, ValueHolder<V>> storeEntry;

    ValueHolderBasedEntry(Cache.Entry<K, Store.ValueHolder<V>> storeEntry) {
      this.storeEntry = storeEntry;
    }

    @Override
    public K getKey() {
      return storeEntry.getKey();
    }

    @Override
    public V getValue() {
      return storeEntry.getValue().value();
    }

  }

  // The compute function that will return the keys to their NEW values, taking the keys to their old values as input;
  // but this could happen in batches, i.e. not necessary containing all of the entries of the Iterable passed to this method
  public static class PutAllFunction<K, V> implements Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> {

    private final Logger logger;
    private final Map<K, V> entriesToRemap;
    private final Expiry<? super K, ? super V> expiry;
    private final AtomicInteger actualPutCount = new AtomicInteger();
    private final AtomicInteger actualUpdateCount = new AtomicInteger();

    public PutAllFunction(Logger logger, Map<K, V> entriesToRemap, Expiry<? super K, ? super V> expiry) {
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
      return Ehcache.newValueAlreadyExpired(logger, expiry, key, oldValue, newValue);
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
