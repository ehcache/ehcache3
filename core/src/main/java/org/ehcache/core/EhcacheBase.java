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

import org.ehcache.Cache;
import org.ehcache.Status;
import org.ehcache.config.CacheRuntimeConfiguration;
import org.ehcache.core.events.CacheEventDispatcher;
import org.ehcache.core.spi.LifeCycled;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.Store.ValueHolder;
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
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.spi.loaderwriter.BulkCacheLoadingException;
import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.spi.loaderwriter.CacheWritingException;
import org.ehcache.spi.resilience.ResilienceStrategy;
import org.ehcache.spi.resilience.StoreAccessException;
import org.slf4j.Logger;
import org.terracotta.statistics.StatisticsManager;
import org.terracotta.statistics.observer.OperationObserver;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.ehcache.core.exceptions.ExceptionFactory.newCacheLoadingException;
import static org.terracotta.statistics.StatisticBuilder.operation;

/**
 * Base implementation of the {@link Cache} interface that is common to all Ehcache implementation
 */
public abstract class EhcacheBase<K, V> implements InternalCache<K, V> {

  protected final Logger logger;

  protected final StatusTransitioner statusTransitioner;

  protected final Store<K, V> store;
  protected final ResilienceStrategy<K, V> resilienceStrategy;
  protected final EhcacheRuntimeConfiguration<K, V> runtimeConfiguration;

  protected final OperationObserver<GetOutcome> getObserver = operation(GetOutcome.class).named("get").of(this).tag("cache").build();
  protected final OperationObserver<GetAllOutcome> getAllObserver = operation(GetAllOutcome.class).named("getAll").of(this).tag("cache").build();
  protected final OperationObserver<PutOutcome> putObserver = operation(PutOutcome.class).named("put").of(this).tag("cache").build();
  protected final OperationObserver<PutAllOutcome> putAllObserver = operation(PutAllOutcome.class).named("putAll").of(this).tag("cache").build();
  protected final OperationObserver<RemoveOutcome> removeObserver = operation(RemoveOutcome.class).named("remove").of(this).tag("cache").build();
  protected final OperationObserver<RemoveAllOutcome> removeAllObserver = operation(RemoveAllOutcome.class).named("removeAll").of(this).tag("cache").build();
  protected final OperationObserver<ConditionalRemoveOutcome> conditionalRemoveObserver = operation(ConditionalRemoveOutcome.class).named("conditionalRemove").of(this).tag("cache").build();
  protected final OperationObserver<PutIfAbsentOutcome> putIfAbsentObserver = operation(PutIfAbsentOutcome.class).named("putIfAbsent").of(this).tag("cache").build();
  protected final OperationObserver<ReplaceOutcome> replaceObserver = operation(ReplaceOutcome.class).named("replace").of(this).tag("cache").build();
  protected final OperationObserver<ClearOutcome> clearObserver = operation(ClearOutcome.class).named("clear").of(this).tag("cache").build();

  protected final Map<BulkOps, LongAdder> bulkMethodEntries = new EnumMap<>(BulkOps.class);

  /**
   * Creates a new {@code EhcacheBase} based on the provided parameters.
   *
   * @param runtimeConfiguration the cache configuration
   * @param store the store to use
   * @param eventDispatcher the event dispatcher
   * @param logger the logger
   */
  EhcacheBase(EhcacheRuntimeConfiguration<K, V> runtimeConfiguration, Store<K, V> store, ResilienceStrategy<K, V> resilienceStrategy,
          CacheEventDispatcher<K, V> eventDispatcher, Logger logger, StatusTransitioner statusTransitioner) {
    this.store = store;
    runtimeConfiguration.addCacheConfigurationListener(store.getConfigurationChangeListeners());
    StatisticsManager.associate(store).withParent(this);

    this.resilienceStrategy = resilienceStrategy;

    this.runtimeConfiguration = runtimeConfiguration;
    runtimeConfiguration.addCacheConfigurationListener(eventDispatcher.getConfigurationChangeListeners());

    this.logger = logger;
    this.statusTransitioner = statusTransitioner;
    for (BulkOps bulkOp : BulkOps.values()) {
      bulkMethodEntries.put(bulkOp, new LongAdder());
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V get(K key) {
    getObserver.begin();
    try {
      statusTransitioner.checkAvailable();
      checkNonNull(key);

      try {
        Store.ValueHolder<V> valueHolder = doGet(key);

        // Check for expiry first
        if (valueHolder == null) {
          getObserver.end(GetOutcome.MISS);
          return null;
        } else {
          getObserver.end(GetOutcome.HIT);
          return valueHolder.get();
        }
      } catch (StoreAccessException e) {
        V value = resilienceStrategy.getFailure(key, e);
        getObserver.end(GetOutcome.FAILURE);
        return value;
      }
    } catch (Throwable e) {
      getObserver.end(GetOutcome.FAILURE);
      throw e;
    }
  }

  protected abstract Store.ValueHolder<V> doGet(K key) throws StoreAccessException;

  protected V getNoLoader(K key) {
    getObserver.begin();
    try {
      statusTransitioner.checkAvailable();
      checkNonNull(key);

      try {
        Store.ValueHolder<V> valueHolder = store.get(key);

        // Check for expiry first
        if (valueHolder == null) {
          getObserver.end(GetOutcome.MISS);
          return null;
        } else {
          getObserver.end(GetOutcome.HIT);
          return valueHolder.get();
        }
      } catch (StoreAccessException e) {
        V value = resilienceStrategy.getFailure(key, e);
        getObserver.end(GetOutcome.FAILURE);
        return value;
      }
    } catch (Throwable e) {
      getObserver.end(GetOutcome.FAILURE);
      throw e;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void put(K key, V value) {
    putObserver.begin();
    try {
      statusTransitioner.checkAvailable();
      checkNonNull(key, value);

      try {
        Store.PutStatus status = doPut(key, value);
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
        resilienceStrategy.putFailure(key, value, e);
        putObserver.end(PutOutcome.FAILURE);
      }
    } catch (Throwable e) {
      putObserver.end(PutOutcome.FAILURE);
      throw e;
    }
  }

  protected abstract Store.PutStatus doPut(K key, V value) throws StoreAccessException;

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
  public void remove(K key) throws CacheWritingException {
    removeInternal(key); // ignore return value;
  }

  protected boolean removeInternal(final K key) {
    removeObserver.begin();
    try {
      statusTransitioner.checkAvailable();
      checkNonNull(key);

      boolean removed = false;
      try {
        removed = doRemoveInternal(key);
        if (removed) {
          removeObserver.end(RemoveOutcome.SUCCESS);
        } else {
          removeObserver.end(RemoveOutcome.NOOP);
        }
      } catch (StoreAccessException e) {
        resilienceStrategy.removeFailure(key, e);
        removeObserver.end(RemoveOutcome.FAILURE);
      }

      return removed;
    } catch (Throwable e) {
      removeObserver.end(RemoveOutcome.FAILURE);
      throw e;
    }
  }

  protected abstract boolean doRemoveInternal(final K key) throws StoreAccessException;

  /**
   * {@inheritDoc}
   */
  @Override
  public void clear() {
    clearObserver.begin();
    try {
      statusTransitioner.checkAvailable();
      try {
        store.clear();
        clearObserver.end(ClearOutcome.SUCCESS);
      } catch (StoreAccessException e) {
        resilienceStrategy.clearFailure(e);
        clearObserver.end(ClearOutcome.FAILURE);
      }
    } catch (Throwable e) {
      clearObserver.end(ClearOutcome.FAILURE);
      throw e;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V putIfAbsent(final K key, final V value) {
    putIfAbsentObserver.begin();
    try {
      statusTransitioner.checkAvailable();
      checkNonNull(key, value);

      boolean[] put = { false };

      try {
        ValueHolder<V> inCache = doPutIfAbsent(key, value, b -> put[0] = b);
        if (put[0]) {
          putIfAbsentObserver.end(PutIfAbsentOutcome.PUT);
          return null;
        } else {
          putIfAbsentObserver.end(PutIfAbsentOutcome.HIT);
          return inCache.get();
        }
      } catch (StoreAccessException e) {
        V newValue = resilienceStrategy.putIfAbsentFailure(key, value, e);
        putIfAbsentObserver.end(PutIfAbsentOutcome.FAILURE);
        return newValue;
      }
    } catch (Throwable e) {
      putIfAbsentObserver.end(PutIfAbsentOutcome.FAILURE);
      throw e;
    }
  }

  protected abstract ValueHolder<V> doPutIfAbsent(K key, V value, Consumer<Boolean> put) throws StoreAccessException;

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<Entry<K, V>> iterator() {
    statusTransitioner.checkAvailable();
    return new CacheEntryIterator();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<K, V> getAll(Set<? extends K> keys) throws BulkCacheLoadingException {
    return getAllInternal(keys, true);
  }

  protected Map<K,V> getAllInternal(Set<? extends K> keys, boolean includeNulls) {
    getAllObserver.begin();
    try {
      statusTransitioner.checkAvailable();
      checkNonNullContent(keys);
      if (keys.isEmpty()) {
        getAllObserver.end(GetAllOutcome.SUCCESS);
        return Collections.emptyMap();
      }

      try {
        Map<K, V> result = doGetAllInternal(keys, includeNulls);
        getAllObserver.end(GetAllOutcome.SUCCESS);
        return result;
      } catch (StoreAccessException e) {
        Map<K, V> result = resilienceStrategy.getAllFailure(keys, e);
        getAllObserver.end(GetAllOutcome.FAILURE);
        return result;
      }
    } catch (Throwable e) {
      getAllObserver.end(GetAllOutcome.FAILURE);
      throw e;
    }
  }

  protected abstract Map<K,V> doGetAllInternal(Set<? extends K> keys, boolean includeNulls) throws StoreAccessException;

  /**
   * {@inheritDoc}
   */
  @Override
  public void putAll(Map<? extends K, ? extends V> entries) throws BulkCacheWritingException {
    putAllObserver.begin();
    try {
      statusTransitioner.checkAvailable();
      checkNonNull(entries);
      if(entries.isEmpty()) {
        putAllObserver.end(PutAllOutcome.SUCCESS);
        return;
      }

      try {
        doPutAll(entries);
        putAllObserver.end(PutAllOutcome.SUCCESS);
      } catch (StoreAccessException e) {
        resilienceStrategy.putAllFailure(entries, e);
        putAllObserver.end(PutAllOutcome.FAILURE);
      }
    } catch (Exception e) {
      putAllObserver.end(PutAllOutcome.FAILURE);
      throw e;
    }
  }

  protected abstract void doPutAll(Map<? extends K, ? extends V> entries) throws StoreAccessException, BulkCacheWritingException;

  protected boolean newValueAlreadyExpired(K key, V oldValue, V newValue) {
    return newValueAlreadyExpired(logger, runtimeConfiguration.getExpiryPolicy(), key, oldValue, newValue);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void removeAll(Set<? extends K> keys) throws BulkCacheWritingException {
    removeAllObserver.begin();
    try {
      statusTransitioner.checkAvailable();
      checkNonNull(keys);
      if (keys.isEmpty()) {
        removeAllObserver.end(RemoveAllOutcome.SUCCESS);
        return;
      }

      for (K key : keys) {
        if (key == null) {
          throw new NullPointerException();
        }
      }

      try {
        doRemoveAll(keys);
        removeAllObserver.end(RemoveAllOutcome.SUCCESS);
      } catch (StoreAccessException e) {
        resilienceStrategy.removeAllFailure(keys, e);
        removeAllObserver.end(RemoveAllOutcome.FAILURE);
      }
    } catch (Throwable e) {
      removeAllObserver.end(RemoveAllOutcome.FAILURE);
      throw e;
    }
  }

  protected abstract void doRemoveAll(Set<? extends K> keys) throws BulkCacheWritingException, StoreAccessException;

  protected static <K, V> boolean newValueAlreadyExpired(Logger logger, ExpiryPolicy<? super K, ? super V> expiry, K key, V oldValue, V newValue) {
    if (newValue == null) {
      return false;
    }

    Duration duration;
    try {
      if (oldValue == null) {
        duration = expiry.getExpiryForCreation(key, newValue);
      } else {
        duration = expiry.getExpiryForUpdate(key, () -> oldValue, newValue);
      }
    } catch (RuntimeException re) {
      logger.error("Expiry computation caused an exception - Expiry duration will be 0 ", re);
      return true;
    }

    return Duration.ZERO.equals(duration);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean remove(K key, V value) {
    conditionalRemoveObserver.begin();
    try {
      statusTransitioner.checkAvailable();
      checkNonNull(key, value);

      try {
        Store.RemoveStatus status = doRemove(key, value);
        switch (status) {
          case REMOVED:
            conditionalRemoveObserver.end(ConditionalRemoveOutcome.SUCCESS);
            return true;
          case KEY_MISSING:
            conditionalRemoveObserver.end(ConditionalRemoveOutcome.FAILURE_KEY_MISSING);
            return false;
          case KEY_PRESENT:
            conditionalRemoveObserver.end(ConditionalRemoveOutcome.FAILURE_KEY_PRESENT);
            return false;
          default:
            throw new AssertionError("Invalid Status: " + status);
        }
      } catch (StoreAccessException e) {
        boolean removed = resilienceStrategy.removeFailure(key, value, e);
        conditionalRemoveObserver.end(ConditionalRemoveOutcome.FAILURE);
        return removed;
      }
    } catch (Throwable e) {
      conditionalRemoveObserver.end(ConditionalRemoveOutcome.FAILURE);
      throw e;
    }
  }

  protected abstract Store.RemoveStatus doRemove(final K key, final V value) throws StoreAccessException;

  /**
   * {@inheritDoc}
   */
  @Override
  public V replace(K key, V value) {
    replaceObserver.begin();
    try {
      statusTransitioner.checkAvailable();
      checkNonNull(key, value);

      try {
        V result = doReplace(key, value);
        if(result == null) {
          replaceObserver.end(ReplaceOutcome.MISS_NOT_PRESENT);
        } else {
          replaceObserver.end(ReplaceOutcome.HIT);
        }
        return result;
      } catch (StoreAccessException e) {
        V result = resilienceStrategy.replaceFailure(key, value, e);
        replaceObserver.end(ReplaceOutcome.FAILURE);
        return result;
      }
    } catch (Throwable e) {
      replaceObserver.end(ReplaceOutcome.FAILURE);
      throw e;
    }
  }

  protected abstract V doReplace(final K key, final V value) throws StoreAccessException;

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean replace(final K key, final V oldValue, final V newValue) {
    replaceObserver.begin();
    try {
      statusTransitioner.checkAvailable();
      checkNonNull(key, oldValue, newValue);

      try {
        Store.ReplaceStatus status = doReplace(key, oldValue, newValue);
        switch (status) {
          case HIT:
            replaceObserver.end(ReplaceOutcome.HIT);
            return true;
          case MISS_PRESENT:
            replaceObserver.end(ReplaceOutcome.MISS_PRESENT);
            return false;
          case MISS_NOT_PRESENT:
            replaceObserver.end(ReplaceOutcome.MISS_NOT_PRESENT);
            return false;
          default:
            throw new AssertionError("Invalid Status:" + status);
        }
      } catch (StoreAccessException e) {
        boolean success = resilienceStrategy.replaceFailure(key, oldValue, newValue, e);
        replaceObserver.end(ReplaceOutcome.FAILURE);
        return success;
      }
    } catch (Throwable e) {
      replaceObserver.end(ReplaceOutcome.FAILURE);
      throw e;
    }
  }

  protected abstract Store.ReplaceStatus doReplace(K key, V oldValue, V newValue) throws StoreAccessException;

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

  protected void addBulkMethodEntriesCount(BulkOps op, long count) {
    bulkMethodEntries.get(op).add(count);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<BulkOps, LongAdder> getBulkMethodEntries() {
    return bulkMethodEntries;
  }

  protected static void checkNonNull(Object thing) {
    Objects.requireNonNull(thing);
  }

  protected static void checkNonNull(Object... things) {
    for (Object thing : things) {
      checkNonNull(thing);
    }
  }

  protected void checkNonNullContent(Collection<?> collectionOfThings) {
    checkNonNull(collectionOfThings);
    for (Object thing : collectionOfThings) {
      checkNonNull(thing);
    }
  }

  protected abstract class Jsr107CacheBase implements Jsr107Cache<K, V> {

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
      return EhcacheBase.this.getNoLoader(key);
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys) {
      return getAllInternal(keys, false);
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
    public boolean remove(K key) {
      return EhcacheBase.this.removeInternal(key);
    }

    @Override
    public void removeAll() {
      Collection<StoreAccessException> failures = new ArrayList<>();
      Store.Iterator<Entry<K, ValueHolder<V>>> iterator = store.iterator();
      while (iterator.hasNext()) {
        try {
          Entry<K, ValueHolder<V>> next = iterator.next();
          remove(next.getKey());
        } catch (StoreAccessException cae) {
          failures.add(cae);
        }
      }
      if (!failures.isEmpty()) {
        StoreAccessException removeAllFailure = new StoreAccessException("Iteration failures may have prevented a complete removal");
        failures.forEach(removeAllFailure::addSuppressed);
        resilienceStrategy.clearFailure(removeAllFailure);
      }
    }

  }

  private class CacheEntryIterator implements Iterator<Entry<K, V>> {

    private final Store.Iterator<Entry<K, ValueHolder<V>>> iterator;
    private Cache.Entry<K, ValueHolder<V>> current;
    private Cache.Entry<K, ValueHolder<V>> next;
    private StoreAccessException nextException;

    public CacheEntryIterator() {
      this.iterator = store.iterator();
      advance();
    }

    private void advance() {
      try {
        while (iterator.hasNext()) {
          next = iterator.next();
          if (next != null) {
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

      getObserver.begin();
      if (nextException == null) {
        getObserver.end(GetOutcome.HIT);
        current = next;
        advance();
        return new ValueHolderBasedEntry<>(current);
      } else {
        getObserver.end(GetOutcome.FAILURE);
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
      EhcacheBase.this.remove(current.getKey(), current.getValue().get());
      current = null;
    }
  }

  private static class ValueHolderBasedEntry<K, V> implements Cache.Entry<K, V> {
    private final Cache.Entry<K, ValueHolder<V>> storeEntry;

    ValueHolderBasedEntry(Cache.Entry<K, ValueHolder<V>> storeEntry) {
      this.storeEntry = storeEntry;
    }

    @Override
    public K getKey() {
      return storeEntry.getKey();
    }

    @Override
    public V getValue() {
      return storeEntry.getValue().get();
    }

  }
}

