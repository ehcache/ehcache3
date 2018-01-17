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
import org.ehcache.config.CacheConfiguration;
import org.ehcache.core.events.CacheEventDispatcher;
import org.ehcache.core.exceptions.StorePassThroughException;
import org.ehcache.core.resilience.RobustLoaderWriterResilienceStrategy;
import org.ehcache.spi.loaderwriter.BulkCacheLoadingException;
import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.resilience.StoreAccessException;
import org.ehcache.spi.loaderwriter.CacheLoadingException;
import org.ehcache.spi.loaderwriter.CacheWritingException;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.Store.ValueHolder;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.core.statistics.BulkOps;
import org.ehcache.core.statistics.CacheOperationOutcomes.ConditionalRemoveOutcome;
import org.ehcache.core.statistics.CacheOperationOutcomes.GetAllOutcome;
import org.ehcache.core.statistics.CacheOperationOutcomes.GetOutcome;
import org.ehcache.core.statistics.CacheOperationOutcomes.PutIfAbsentOutcome;
import org.ehcache.core.statistics.CacheOperationOutcomes.PutAllOutcome;
import org.ehcache.core.statistics.CacheOperationOutcomes.PutOutcome;
import org.ehcache.core.statistics.CacheOperationOutcomes.RemoveAllOutcome;
import org.ehcache.core.statistics.CacheOperationOutcomes.RemoveOutcome;
import org.ehcache.core.statistics.CacheOperationOutcomes.ReplaceOutcome;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.ehcache.core.internal.util.Functions.memoize;
import static org.ehcache.core.exceptions.ExceptionFactory.newCacheLoadingException;
import static org.ehcache.core.exceptions.ExceptionFactory.newCacheWritingException;

/**
 * Implementation of the {@link Cache} interface when a {@link CacheLoaderWriter} is involved.
 * <p>
 * {@code Ehcache} users should not have to depend on this type but rely exclusively on the api types in package
 * {@code org.ehcache}.
 *
 * @see Ehcache
 */
public class EhcacheWithLoaderWriter<K, V> extends EhcacheBase<K, V> {

  private static final Supplier<Boolean> REPLACE_FALSE = () -> Boolean.FALSE;

  private final CacheLoaderWriter<? super K, V> cacheLoaderWriter;
  private final boolean useLoaderInAtomics;

  /**
   * Constructs a new {@code EhcacheWithLoaderWriter} based on the provided parameters.
   *
   * @param configuration the cache configuration
   * @param store the underlying store
   * @param cacheLoaderWriter the cache loader writer
   * @param eventDispatcher the event dispatcher
   * @param logger the logger
   */
  public EhcacheWithLoaderWriter(CacheConfiguration<K, V> configuration, Store<K, V> store,
      final CacheLoaderWriter<? super K, V> cacheLoaderWriter,
      CacheEventDispatcher<K, V> eventDispatcher,
      Logger logger) {
    this(configuration, store, cacheLoaderWriter, eventDispatcher, true, logger);
  }

  EhcacheWithLoaderWriter(CacheConfiguration<K, V> runtimeConfiguration, Store<K, V> store,
          CacheLoaderWriter<? super K, V> cacheLoaderWriter,
          CacheEventDispatcher<K, V> eventDispatcher, boolean useLoaderInAtomics, Logger logger) {
    this(new EhcacheRuntimeConfiguration<>(runtimeConfiguration), store, cacheLoaderWriter, eventDispatcher, useLoaderInAtomics, logger, new StatusTransitioner(logger));
  }

  EhcacheWithLoaderWriter(EhcacheRuntimeConfiguration<K, V> runtimeConfiguration, Store<K, V> store,
            CacheLoaderWriter<? super K, V> cacheLoaderWriter,
            CacheEventDispatcher<K, V> eventDispatcher, boolean useLoaderInAtomics, Logger logger, StatusTransitioner statusTransitioner) {
    super(runtimeConfiguration, store, new RobustLoaderWriterResilienceStrategy<>(store, cacheLoaderWriter), eventDispatcher, logger, statusTransitioner);

    this.cacheLoaderWriter = Objects.requireNonNull(cacheLoaderWriter, "CacheLoaderWriter cannot be null");
    this.useLoaderInAtomics = useLoaderInAtomics;
  }

  @Override
  protected Store.ValueHolder<V> doGet(K key) throws StoreAccessException {
    Function<K, V> mappingFunction = k -> {
      try {
        return cacheLoaderWriter.load(k);
      } catch (Exception e) {
        throw new StorePassThroughException(newCacheLoadingException(e));
      }
    };

    return store.computeIfAbsent(key, mappingFunction);
  }

  @Override
  public Store.PutStatus doPut(K key, V value) throws StoreAccessException {
    BiFunction<K, V, V> remappingFunction = (key1, previousValue) -> {
      try {
        cacheLoaderWriter.write(key1, value);
      } catch (Exception e) {
        throw new StorePassThroughException(newCacheWritingException(e));
      }
      return value;
    };

    store.compute(key, remappingFunction);
    return Store.PutStatus.PUT;
  }

  protected boolean doRemoveInternal(final K key) throws StoreAccessException {
    AtomicBoolean modified = new AtomicBoolean();

    BiFunction<K, V, V> remappingFunction = memoize((key1, previousValue) -> {
      modified.set(previousValue != null);

      try {
        cacheLoaderWriter.delete(key1);
      } catch (Exception e) {
        throw new StorePassThroughException(newCacheWritingException(e));
      }
      return null;
    });

    store.compute(key, remappingFunction);
    return modified.get();
  }

  protected Map<K, V> getAllInternal(Set<? extends K> keys, boolean includeNulls) throws BulkCacheLoadingException {
    getAllObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNullContent(keys);
    if(keys.isEmpty()) {
      getAllObserver.end(GetAllOutcome.SUCCESS);
      return Collections.emptyMap();
    }
    final Map<K, V> successes = new HashMap<>();
    final Map<K, Exception> failures = new HashMap<>();


    Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> computeFunction =
      keys1 -> {
        Map<K, V> computeResult = new LinkedHashMap<>();

        // put all the entries to get ordering correct
        for (K key : keys1) {
          computeResult.put(key, null);
        }

        Map<? super K, ? extends V> loaded = Collections.emptyMap();
        try {
          loaded = cacheLoaderWriter.loadAll(computeResult.keySet());
        } catch(BulkCacheLoadingException bcle) {
          collectSuccessesAndFailures(bcle, successes, failures);
        } catch (Exception e) {
          for (K key : computeResult.keySet()) {
            failures.put(key, e);
          }
        }

        if (!loaded.isEmpty()) {
          for (K key : computeResult.keySet()) {
            V value = loaded.get(key);
            successes.put(key, value);
            computeResult.put(key, value);
          }
        }

        return computeResult.entrySet();
      };

    Map<K, V> result = new HashMap<>();
    try {
      Map<K, Store.ValueHolder<V>> computedMap = store.bulkComputeIfAbsent(keys, computeFunction);

      int hits = 0;
      int keyCount = 0;
      for (Map.Entry<K, Store.ValueHolder<V>> entry : computedMap.entrySet()) {
        keyCount++;
        if (entry.getValue() != null) {
          result.put(entry.getKey(), entry.getValue().get());
          hits++;
        } else if (includeNulls && failures.isEmpty()) {
          result.put(entry.getKey(), null);
        }
      }

      addBulkMethodEntriesCount(BulkOps.GET_ALL_HITS, hits);
      if (failures.isEmpty()) {
        addBulkMethodEntriesCount(BulkOps.GET_ALL_MISS, keyCount - hits);
        getAllObserver.end(GetAllOutcome.SUCCESS);
        return result;
      } else {
        successes.putAll(result);
        getAllObserver.end(GetAllOutcome.FAILURE);
        throw new BulkCacheLoadingException(failures, successes);
      }
    } catch (StoreAccessException e) {
      try {
        Set<K> toLoad = new HashSet<>(keys);
        toLoad.removeAll(successes.keySet());
        toLoad.removeAll(failures.keySet());
        computeFunction.apply(toLoad);
        if (failures.isEmpty()) {
          return resilienceStrategy.getAllFailure(keys, successes, e);
        } else {
          return resilienceStrategy.getAllFailure(keys, e, new BulkCacheLoadingException(failures, successes));
        }
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
    final Set<K> successes = new HashSet<>();
    final Map<K, Exception> failures = new HashMap<>();

    // Copy all entries to write into a Map
    final Map<K, V> entriesToRemap = new HashMap<>();
    for (Map.Entry<? extends K, ? extends V> entry: entries.entrySet()) {
      // If a key/value is null, throw NPE, nothing gets mutated
      if (entry.getKey() == null || entry.getValue() == null) {
        throw new NullPointerException();
      }
      entriesToRemap.put(entry.getKey(), entry.getValue());
    }

    final AtomicInteger actualPutCount = new AtomicInteger();

    // The compute function that will return the keys to their NEW values, taking the keys to their old values as input;
    // but this could happen in batches, i.e. not necessary containing all of the entries of the Iterable passed to this method
    Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> computeFunction =
      entries1 -> {
        // If we have a writer, first write this batch
        cacheLoaderWriterWriteAllCall(entries1, entriesToRemap, successes, failures);

        Map<K, V> mutations = new LinkedHashMap<>();

        // then record we handled these mappings
        for (Map.Entry<? extends K, ? extends V> entry: entries1) {
          K key = entry.getKey();
          V existingValue = entry.getValue();
          V newValue = entriesToRemap.remove(key);

          if (newValueAlreadyExpired(key, existingValue, newValue)) {
            mutations.put(key, null);
          } else if (successes.contains(key)) {
            actualPutCount.incrementAndGet();
            mutations.put(key, newValue);

          } else {
            mutations.put(key, existingValue);
          }
        }

        // Finally return the values to be installed in the Cache's Store
        return mutations.entrySet();
      };

    try {
      store.bulkCompute(entries.keySet(), computeFunction);
      addBulkMethodEntriesCount(BulkOps.PUT_ALL, actualPutCount.get());
      if (failures.isEmpty()) {
        putAllObserver.end(PutAllOutcome.SUCCESS);
      } else {
        BulkCacheWritingException cacheWritingException = new BulkCacheWritingException(failures, successes);
        tryRemoveFailedKeys(entries, failures, cacheWritingException);
        putAllObserver.end(PutAllOutcome.FAILURE);
        throw cacheWritingException;
      }
    } catch (StoreAccessException e) {
      try {
        // just in case not all writes happened:
        if (!entriesToRemap.isEmpty()) {
          cacheLoaderWriterWriteAllCall(entriesToRemap.entrySet(), entriesToRemap, successes, failures);
        }
        if (failures.isEmpty()) {
          resilienceStrategy.putAllFailure(entries, e);
        } else {
          resilienceStrategy.putAllFailure(entries, e, new BulkCacheWritingException(failures, successes));
        }
      } finally {
        putAllObserver.end(PutAllOutcome.FAILURE);
      }
    }
  }

  private void tryRemoveFailedKeys(Map<? extends K, ? extends V> entries, Map<K, Exception> failures, BulkCacheWritingException cacheWritingException) {
    try {
      store.bulkCompute(failures.keySet(), entries1 -> {
        HashMap<K, V> result = new HashMap<>();
        for (Map.Entry<? extends K, ? extends V> entry : entries1) {
          result.put(entry.getKey(), null);
        }
        return result.entrySet();
      });
    } catch (StoreAccessException e) {
      resilienceStrategy.putAllFailure(entries, e, cacheWritingException);
    }
  }

  private void cacheLoaderWriterWriteAllCall(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries, Map<K, V> entriesToRemap, Set<K> successes, Map<K, Exception> failures) throws IllegalStateException {
    Map<K, V> toWrite = new HashMap<>();
    for (Map.Entry<? extends K, ? extends V> entry: entries) {
      V value = entriesToRemap.get(entry.getKey());
      if (value == null) {
        continue;
      }

      toWrite.put(entry.getKey(), value);
    }
    try {
      if (! toWrite.isEmpty()) {
        // write all entries of this batch
        cacheLoaderWriter.writeAll(toWrite.entrySet());
        successes.addAll(toWrite.keySet());
      }
    } catch (BulkCacheWritingException bcwe) {
      collectSuccessesAndFailures(bcwe, successes, failures);
    } catch (Exception e) {
      for (K key: toWrite.keySet()) {
        failures.put(key, e);
      }
    }
  }

  @SuppressWarnings({ "unchecked" })
  private static <K> void collectSuccessesAndFailures(BulkCacheWritingException bcwe, Set<K> successes, Map<K, Exception> failures) {
    successes.addAll((Collection<K>)bcwe.getSuccesses());
    failures.putAll((Map<K, Exception>)bcwe.getFailures());
  }
  @SuppressWarnings({ "unchecked" })
  private void collectSuccessesAndFailures(BulkCacheLoadingException bcle, Map<K, V> successes, Map<K, Exception> failures) {
    successes.putAll((Map<K, V>)bcle.getSuccesses());
    failures.putAll((Map<K, Exception>)bcle.getFailures());
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
    final Set<K> successes = new HashSet<>();
    final Map<K, Exception> failures = new HashMap<>();

    final Map<K, ? extends V> entriesToRemove = new HashMap<>();
    for (K key: keys) {
      if (key == null) {
        throw new NullPointerException();
      }
      entriesToRemove.put(key, null);
    }

    final AtomicInteger actualRemoveCount = new AtomicInteger();

    Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> removalFunction =
      entries -> {
        Set<K> unknowns = cacheLoaderWriterDeleteAllCall(entries, entriesToRemove, successes, failures);

        Map<K, V> results = new LinkedHashMap<>();

        for (Map.Entry<? extends K, ? extends V> entry : entries) {
          K key = entry.getKey();
          V existingValue = entry.getValue();

          if (successes.contains(key)) {
            if (existingValue != null) {
              actualRemoveCount.incrementAndGet();
            }
            results.put(key, null);
            entriesToRemove.remove(key);
          } else {
            if (unknowns.contains(key)) {
              results.put(key, null);
            } else {
              results.put(key, existingValue);
            }
          }
        }

        return results.entrySet();
      };

    try {
      store.bulkCompute(keys, removalFunction);
      addBulkMethodEntriesCount(BulkOps.REMOVE_ALL, actualRemoveCount.get());
      if (failures.isEmpty()) {
        removeAllObserver.end(RemoveAllOutcome.SUCCESS);
      } else {
        removeAllObserver.end(RemoveAllOutcome.FAILURE);
        throw new BulkCacheWritingException(failures, successes);
      }
    } catch (StoreAccessException e) {
      try {
        // just in case not all writes happened:
        if (!entriesToRemove.isEmpty()) {
          cacheLoaderWriterDeleteAllCall(entriesToRemove.entrySet(), entriesToRemove, successes, failures);
        }
        if (failures.isEmpty()) {
          resilienceStrategy.removeAllFailure(keys, e);
        } else {
          resilienceStrategy.removeAllFailure(keys, e, new BulkCacheWritingException(failures, successes));
        }
      } finally {
        removeAllObserver.end(RemoveAllOutcome.FAILURE);
      }
    }
  }

  private Set<K> cacheLoaderWriterDeleteAllCall(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries, Map<K, ? extends V> entriesToRemove, Set<K> successes, Map<K, Exception> failures) {
    final Set<K> unknowns = new HashSet<>();
    Set<K> toDelete = new HashSet<>();
    for (Map.Entry<? extends K, ? extends V> entry : entries) {
      K key = entry.getKey();
      if (entriesToRemove.containsKey(key)) {
        toDelete.add(key);
      }
    }

    try {
      cacheLoaderWriter.deleteAll(toDelete);
      successes.addAll(toDelete);
    } catch (BulkCacheWritingException bcwe) {
      collectSuccessesAndFailures(bcwe, successes, failures);
    } catch (Exception e) {
      for (K key : toDelete) {
        failures.put(key, e);
        unknowns.add(key);
      }
    }
    return unknowns;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V putIfAbsent(final K key, final V value) throws CacheWritingException {
    putIfAbsentObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(key, value);
    final AtomicBoolean installed = new AtomicBoolean(false);

    final Function<K, V> mappingFunction = memoize(k -> {
      if (useLoaderInAtomics) {
        try {
          V loaded = cacheLoaderWriter.load(k);
          if (loaded != null) {
            return loaded; // populate the cache
          }
        } catch (Exception e) {
          throw new StorePassThroughException(newCacheLoadingException(e));
        }
      }

      try {
        cacheLoaderWriter.write(k, value);
      } catch (Exception e) {
        throw new StorePassThroughException(newCacheWritingException(e));
      }

      installed.set(true);
      return value;
    });

    try {
      ValueHolder<V> inCache = store.computeIfAbsent(key, mappingFunction);
      if (installed.get()) {
        putIfAbsentObserver.end(PutIfAbsentOutcome.PUT);
        return null;
      } else if (inCache == null) {
        putIfAbsentObserver.end(PutIfAbsentOutcome.HIT);
        return null;
      } else {
        putIfAbsentObserver.end(PutIfAbsentOutcome.HIT);
        return inCache.get();
      }
    } catch (StoreAccessException e) {
      try {
        V loaded;
        try {
          loaded = mappingFunction.apply(key);
        } catch (StorePassThroughException f) {
          Throwable cause = f.getCause();
          if(cause instanceof CacheLoadingException) {
            return resilienceStrategy.putIfAbsentFailure(key, value, e, (CacheLoadingException) cause);
          } else if(cause instanceof CacheWritingException) {
            return resilienceStrategy.putIfAbsentFailure(key, value, e, (CacheWritingException) cause);
          } else {
            throw new AssertionError();
          }
        }
        return resilienceStrategy.putIfAbsentFailure(key, value, loaded, e, installed.get());
      } finally {
        putIfAbsentObserver.end(PutIfAbsentOutcome.FAILURE);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean remove(final K key, final V value) throws CacheWritingException {
    conditionalRemoveObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(key, value);
    final AtomicBoolean hit = new AtomicBoolean();
    final AtomicBoolean removed = new AtomicBoolean();
    final BiFunction<K, V, V> remappingFunction = memoize((k, inCache) -> {
      inCache = loadFromLoaderWriter(key, inCache);
      if(inCache == null) {
        return null;
      }

      hit.set(true);
      if (value.equals(inCache)) {
        try {
          cacheLoaderWriter.delete(k);
        } catch (Exception e) {
          throw new StorePassThroughException(newCacheWritingException(e));
        }
        removed.set(true);
        return null;
      }
      return inCache;
    });
    try {
      store.compute(key, remappingFunction, REPLACE_FALSE);
      if (removed.get()) {
        conditionalRemoveObserver.end(ConditionalRemoveOutcome.SUCCESS);
      } else {
        if (hit.get()) {
          conditionalRemoveObserver.end(ConditionalRemoveOutcome.FAILURE_KEY_PRESENT);
        } else {
          conditionalRemoveObserver.end(ConditionalRemoveOutcome.FAILURE_KEY_MISSING);
        }
      }
    } catch (StoreAccessException e) {
      try {
        try {
          remappingFunction.apply(key, null);
        } catch (StorePassThroughException f) {
          Throwable cause = f.getCause();
          if(cause instanceof CacheLoadingException) {
            return resilienceStrategy.removeFailure(key, value, e, (CacheLoadingException) cause);
          } else if(cause instanceof CacheWritingException) {
            return resilienceStrategy.removeFailure(key, value, e, (CacheWritingException) cause);
          } else {
            throw new AssertionError();
          }
        }
        return resilienceStrategy.removeFailure(key, value, e, removed.get());
      } finally {
        conditionalRemoveObserver.end(ConditionalRemoveOutcome.FAILURE);
      }
    }
    return removed.get();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V replace(final K key, final V value) throws CacheLoadingException, CacheWritingException {
    replaceObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(key, value);
    final AtomicReference<V> old = new AtomicReference<>();
    final BiFunction<K, V, V> remappingFunction = memoize((k, inCache) -> {
      inCache = loadFromLoaderWriter(key, inCache);
      if(inCache == null) {
        return null;
      }

      try {
        cacheLoaderWriter.write(key, value);
      } catch (Exception e) {
        throw new StorePassThroughException(newCacheWritingException(e));
      }

      old.set(inCache);

      if (newValueAlreadyExpired(key, inCache, value)) {
        return null;
      }
      return value;
    });

    try {
      store.compute(key, remappingFunction);
      if (old.get() != null) {
        replaceObserver.end(ReplaceOutcome.HIT);
      } else {
        replaceObserver.end(ReplaceOutcome.MISS_NOT_PRESENT);
      }
      return old.get();
    } catch (StoreAccessException e) {
      try {
        try {
          remappingFunction.apply(key, null);
        } catch (StorePassThroughException f) {
          Throwable cause = f.getCause();
          if(cause instanceof CacheLoadingException) {
            return resilienceStrategy.replaceFailure(key, value, e, (CacheLoadingException) cause);
          } else if(cause instanceof CacheWritingException) {
            return resilienceStrategy.replaceFailure(key, value, e, (CacheWritingException)cause);
          } else {
            throw new AssertionError();
          }
        }
        return resilienceStrategy.replaceFailure(key, value, e);
      } finally {
        replaceObserver.end(ReplaceOutcome.FAILURE);
      }
    }
  }

  private V loadFromLoaderWriter(K key, V inCache) {
    if (inCache == null) {
      if (useLoaderInAtomics) {
        try {
          inCache = cacheLoaderWriter.load(key);
          if (inCache == null) {
            return null;
          }
        } catch (Exception e) {
          throw new StorePassThroughException(newCacheLoadingException(e));
        }
      } else {
        return null;
      }
    }
    return inCache;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean replace(final K key, final V oldValue, final V newValue) throws CacheLoadingException, CacheWritingException {
    replaceObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(key, oldValue, newValue);

    final AtomicBoolean success = new AtomicBoolean();
    final AtomicBoolean hit = new AtomicBoolean();

    final BiFunction<K, V, V> remappingFunction = memoize((k, inCache) -> {
      inCache = loadFromLoaderWriter(key, inCache);
      if(inCache == null) {
        return null;
      }

      hit.set(true);
      if (oldValue.equals(inCache)) {
        try {
          cacheLoaderWriter.write(key, newValue);
        } catch (Exception e) {
          throw new StorePassThroughException(newCacheWritingException(e));
        }

        success.set(true);

        if (newValueAlreadyExpired(key, oldValue, newValue)) {
          return null;
        }
        return newValue;
      }
      return inCache;
    });
    try {
      store.compute(key, remappingFunction, REPLACE_FALSE);
      if (success.get()) {
        replaceObserver.end(ReplaceOutcome.HIT);
      } else {
        if (hit.get()) {
          replaceObserver.end(ReplaceOutcome.MISS_PRESENT);
        } else {
          replaceObserver.end(ReplaceOutcome.MISS_NOT_PRESENT);
        }
      }
      return success.get();
    } catch (StoreAccessException e) {
      try {
        try {
          remappingFunction.apply(key, null);
        } catch (StorePassThroughException f) {
          Throwable cause = f.getCause();
          if(cause instanceof CacheLoadingException) {
            return resilienceStrategy.replaceFailure(key, oldValue, newValue, e, (CacheLoadingException) cause);
          } else if(cause instanceof CacheWritingException) {
            return resilienceStrategy.replaceFailure(key, oldValue, newValue, e, (CacheWritingException)cause);
          } else {
            throw new AssertionError();
          }
        }
        return resilienceStrategy.replaceFailure(key, oldValue, newValue, e, success.get());
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

          V newValue = computeFunction.apply(mappedKey, mappedValue);

          if (newValue == mappedValue) {
            if (! replaceEqual.get()) {
              return mappedValue;
            }
          }

          if (invokeWriter.get()) {
            try {
              if (newValue != null) {
                cacheLoaderWriter.write(mappedKey, newValue);
              } else {
                cacheLoaderWriter.delete(mappedKey);
              }
            } catch (Exception e) {
              throw new StorePassThroughException(newCacheWritingException(e));
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

          try {
            cacheLoaderWriter.delete(mappedKey);
          } catch (Exception e) {
            throw new StorePassThroughException(newCacheWritingException(e));
          }
          return null;
        });
      } catch (StoreAccessException e) {
        getObserver.end(GetOutcome.FAILURE);
        removeObserver.end(RemoveOutcome.FAILURE);
        throw new RuntimeException(e);
      }

      V returnValue = existingValue.get();
      if (returnValue != null) {
        getObserver.end(GetOutcome.HIT);
        removeObserver.end(RemoveOutcome.SUCCESS);
      } else {
        getObserver.end(GetOutcome.MISS);
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

          try {
            cacheLoaderWriter.write(mappedKey, value);
          } catch (Exception e) {
            throw new StorePassThroughException(newCacheWritingException(e));
          }

          if (newValueAlreadyExpired(mappedKey, mappedValue, value)) {
            return null;
          }

          return value;
        });
      } catch (StoreAccessException e) {
        getObserver.end(GetOutcome.FAILURE);
        putObserver.end(PutOutcome.FAILURE);
        throw new RuntimeException(e);
      }

      V returnValue = existingValue.get();
      if (returnValue != null) {
        getObserver.end(GetOutcome.HIT);
      } else {
        getObserver.end(GetOutcome.MISS);
      }
      putObserver.end(PutOutcome.PUT);
      return returnValue;
    }
  }

}
