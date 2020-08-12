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
import org.ehcache.core.internal.util.CollectionUtil;
import org.ehcache.spi.loaderwriter.BulkCacheLoadingException;
import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.spi.resilience.ResilienceStrategy;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.spi.loaderwriter.CacheLoadingException;
import org.ehcache.spi.loaderwriter.CacheWritingException;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.Store.ValueHolder;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.core.statistics.BulkOps;
import org.ehcache.core.statistics.CacheOperationOutcomes.GetOutcome;
import org.ehcache.core.statistics.CacheOperationOutcomes.PutOutcome;
import org.ehcache.core.statistics.CacheOperationOutcomes.RemoveOutcome;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

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

  private static final Supplier<Boolean> SUPPLY_FALSE = () -> Boolean.FALSE;

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
                                 ResilienceStrategy<K, V> resilienceStrategy, CacheLoaderWriter<? super K, V> cacheLoaderWriter,
      CacheEventDispatcher<K, V> eventDispatcher,
      Logger logger) {
    this(configuration, store, resilienceStrategy, cacheLoaderWriter, eventDispatcher, true, logger);
  }

  EhcacheWithLoaderWriter(CacheConfiguration<K, V> runtimeConfiguration, Store<K, V> store,
                          ResilienceStrategy<K, V> resilienceStrategy, CacheLoaderWriter<? super K, V> cacheLoaderWriter,
                          CacheEventDispatcher<K, V> eventDispatcher, boolean useLoaderInAtomics, Logger logger) {
    this(new EhcacheRuntimeConfiguration<>(runtimeConfiguration), store, resilienceStrategy, cacheLoaderWriter, eventDispatcher, useLoaderInAtomics, logger, new StatusTransitioner(logger));
  }

  EhcacheWithLoaderWriter(EhcacheRuntimeConfiguration<K, V> runtimeConfiguration, Store<K, V> store,
                          ResilienceStrategy<K, V> resilienceStrategy, CacheLoaderWriter<? super K, V> cacheLoaderWriter,
                          CacheEventDispatcher<K, V> eventDispatcher, boolean useLoaderInAtomics, Logger logger, StatusTransitioner statusTransitioner) {
    super(runtimeConfiguration, store, resilienceStrategy, eventDispatcher, logger, statusTransitioner);

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
    boolean[] modified = { false };

    BiFunction<K, V, V> remappingFunction = (key1, previousValue) -> {
      modified[0] = (previousValue != null);

      try {
        cacheLoaderWriter.delete(key1);
      } catch (Exception e) {
        throw new StorePassThroughException(newCacheWritingException(e));
      }
      return null;
    };

    store.compute(key, remappingFunction);
    return modified[0];
  }

  protected Map<K, V> doGetAllInternal(Set<? extends K> keys, boolean includeNulls) throws StoreAccessException {
    // we are not expecting failures and these two maps are only used in case of failures. So keep them small
    Map<K, V> successes = new HashMap<>(1);
    Map<K, Exception> failures = new HashMap<>(1);

    Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> computeFunction =
      keys1 -> {
        Map<? super K, ? extends V> loaded;
        try {
          loaded = cacheLoaderWriter.loadAll(keys1);
        } catch(BulkCacheLoadingException bcle) {
          loaded = Collections.emptyMap();
          collectSuccessesAndFailures(bcle, successes, failures);
        } catch (Exception e) {
          loaded = Collections.emptyMap();
          for (K key : keys1) {
            failures.put(key, e);
          }
        }

        int size = CollectionUtil.findBestCollectionSize(keys1, 1); // this function is actually called with one key at the time
        Map<K, V> computeResult = new LinkedHashMap<>(size);

        // put all the entries to get ordering correct
        for (K key : keys1) {
          computeResult.put(key, null);
        }

        if (!loaded.isEmpty()) {
          for (K key : keys1) {
            V value = loaded.get(key);
            successes.put(key, value);
            computeResult.put(key, value);
          }
        }

        return computeResult.entrySet();
      };

    Map<K, V> result = new HashMap<>();
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
      return result;
    } else {
      successes.putAll(result);
      throw new BulkCacheLoadingException(failures, successes);
    }
  }

  @Override
  public void doPutAll(Map<? extends K, ? extends V> entries) throws BulkCacheWritingException, StoreAccessException {
    // we are not expecting failures and these two maps are only used in case of failures. So keep them small
    Set<K> successes = new HashSet<>(1);
    Map<K, Exception> failures = new HashMap<>(1);

    // Copy all entries to write into a Map
    Map<K, V> entriesToRemap = CollectionUtil.copyMapButFailOnNull(entries);

    int[] actualPutCount = { 0 };

    // The compute function that will return the keys to their NEW values, taking the keys to their old values as input;
    // but this could happen in batches, i.e. not necessary containing all of the entries of the Iterable passed to this method
    Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> computeFunction =
      entries1 -> {
        // If we have a writer, first write this batch
        cacheLoaderWriterWriteAllCall(entries1, entriesToRemap, successes, failures);

        int size = CollectionUtil.findBestCollectionSize(entries1, 1);
        Map<K, V> mutations = new LinkedHashMap<>(size);

        // then record we handled these mappings
        for (Map.Entry<? extends K, ? extends V> entry: entries1) {
          K key = entry.getKey();
          V existingValue = entry.getValue();
          V newValue = entriesToRemap.remove(key);

          if (newValueAlreadyExpired(key, existingValue, newValue)) {
            mutations.put(key, null);
          } else if (successes.contains(key)) {
            ++actualPutCount[0];
            mutations.put(key, newValue);

          } else {
            mutations.put(key, existingValue);
          }
        }

        // Finally return the values to be installed in the Cache's Store
        return mutations.entrySet();
      };

    store.bulkCompute(entries.keySet(), computeFunction);
    addBulkMethodEntriesCount(BulkOps.PUT_ALL, actualPutCount[0]);
    if (!failures.isEmpty()) {
      throw new BulkCacheWritingException(failures, successes);
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

  protected void doRemoveAll(final Set<? extends K> keys) throws BulkCacheWritingException, StoreAccessException {
    // we are not expecting failures and these two maps are only used in case of failures. So keep them small
    Set<K> successes = new HashSet<>(1);
    Map<K, Exception> failures = new HashMap<>(1);

    Map<K, ? extends V> entriesToRemove = new HashMap<>(keys.size());
    for (K key: keys) {
      entriesToRemove.put(key, null);
    }

    int[] actualRemoveCount = { 0 };

    Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> removalFunction =
      entries -> {
        Set<K> unknowns = cacheLoaderWriterDeleteAllCall(entries, entriesToRemove, successes, failures);

        int size = CollectionUtil.findBestCollectionSize(entries, 1);
        Map<K, V> results = new LinkedHashMap<>(size);

        for (Map.Entry<? extends K, ? extends V> entry : entries) {
          K key = entry.getKey();
          V existingValue = entry.getValue();

          if (successes.contains(key)) {
            if (existingValue != null) {
              ++actualRemoveCount[0];
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

    store.bulkCompute(keys, removalFunction);
    addBulkMethodEntriesCount(BulkOps.REMOVE_ALL, actualRemoveCount[0]);
    if (!failures.isEmpty()) {
      throw new BulkCacheWritingException(failures, successes);
    }
  }

  private Set<K> cacheLoaderWriterDeleteAllCall(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries, Map<K, ? extends V> entriesToRemove, Set<K> successes, Map<K, Exception> failures) {
    Set<K> unknowns = new HashSet<>();
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
  public ValueHolder<V> doPutIfAbsent(K key, V value, Consumer<Boolean> put) throws StoreAccessException {

    Function<K, V> mappingFunction = k -> {
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

      put.accept(true);
      return value;
    };

    return store.computeIfAbsent(key, mappingFunction);
  }

  @Override
  protected Store.RemoveStatus doRemove(K key, V value) throws StoreAccessException {
    boolean[] hitRemoved = { false, false }; // index 0 = hit, 1 = removed
    BiFunction<K, V, V> remappingFunction = (k, inCache) -> {
      inCache = loadFromLoaderWriter(key, inCache);
      if(inCache == null) {
        return null;
      }

      hitRemoved[0] = true;
      if (value.equals(inCache)) {
        try {
          cacheLoaderWriter.delete(k);
        } catch (Exception e) {
          throw new StorePassThroughException(newCacheWritingException(e));
        }
        hitRemoved[1] = true;
        return null;
      }
      return inCache;
    };

    store.compute(key, remappingFunction, SUPPLY_FALSE);
    if (hitRemoved[1]) {
      return Store.RemoveStatus.REMOVED;
    }

    if (hitRemoved[0]) {
      return Store.RemoveStatus.KEY_PRESENT;
    } else {
      return Store.RemoveStatus.KEY_MISSING;
    }
  }

  @Override
  protected V doReplace(K key, V value) throws CacheLoadingException, CacheWritingException, StoreAccessException {
    @SuppressWarnings("unchecked")
    V[] old = (V[]) new Object[1];

    BiFunction<K, V, V> remappingFunction = (k, inCache) -> {
      inCache = loadFromLoaderWriter(key, inCache);
      if(inCache == null) {
        return null;
      }

      try {
        cacheLoaderWriter.write(key, value);
      } catch (Exception e) {
        throw new StorePassThroughException(newCacheWritingException(e));
      }

      old[0] = inCache;

      if (newValueAlreadyExpired(key, inCache, value)) {
        return null;
      }
      return value;
    };

    store.compute(key, remappingFunction);
    return old[0];
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

  @Override
  protected Store.ReplaceStatus doReplace(K key, V oldValue, V newValue) throws CacheLoadingException, CacheWritingException, StoreAccessException {
    boolean[] successHit = { false, false }; // index 0 = success, 1 = hit

    BiFunction<K, V, V> remappingFunction = (k, inCache) -> {
      inCache = loadFromLoaderWriter(key, inCache);
      if(inCache == null) {
        return null;
      }

      successHit[1] = true;
      if (oldValue.equals(inCache)) {
        try {
          cacheLoaderWriter.write(key, newValue);
        } catch (Exception e) {
          throw new StorePassThroughException(newCacheWritingException(e));
        }

        successHit[0] = true;

        if (newValueAlreadyExpired(key, oldValue, newValue)) {
          return null;
        }
        return newValue;
      }
      return inCache;
    };

    store.compute(key, remappingFunction, SUPPLY_FALSE);
    if (successHit[0]) {
      return Store.ReplaceStatus.HIT;
    } else {
      if (successHit[1]) {
        return Store.ReplaceStatus.MISS_PRESENT;
      } else {
        return Store.ReplaceStatus.MISS_NOT_PRESENT;
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

      @SuppressWarnings("unchecked")
      V[] existingValue = (V[]) new Object[1];

      try {
        store.compute(key, (mappedKey, mappedValue) -> {
          existingValue[0] = mappedValue;

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

      V returnValue = existingValue[0];
      if (returnValue != null) {
        getObserver.end(GetOutcome.HIT);
        removeObserver.end(RemoveOutcome.SUCCESS);
      } else {
        getObserver.end(GetOutcome.MISS);
        removeObserver.end(RemoveOutcome.NOOP);
      }
      return returnValue;
    }

    @Override
    public V getAndPut(K key, final V value) {
      getObserver.begin();
      putObserver.begin();

      @SuppressWarnings("unchecked")
      V[] existingValue = (V[]) new Object[1];

      try {
        store.compute(key, (mappedKey, mappedValue) -> {
          existingValue[0] = mappedValue;

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

      V returnValue = existingValue[0];
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
