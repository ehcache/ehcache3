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
package org.ehcache.impl.internal.store.loaderwriter;

import org.ehcache.Cache;
import org.ehcache.core.CacheConfigurationChangeListener;
import org.ehcache.core.Ehcache;
import org.ehcache.core.exceptions.StorePassThroughException;
import org.ehcache.core.util.CollectionUtil;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.WrapperStore;
import org.ehcache.core.spi.store.events.StoreEventSource;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.spi.loaderwriter.BulkCacheLoadingException;
import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.WriteBehindConfiguration;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.spi.service.ServiceConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.context.ContextManager;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.ehcache.core.exceptions.ExceptionFactory.newCacheLoadingException;
import static org.ehcache.core.exceptions.ExceptionFactory.newCacheWritingException;
import static org.ehcache.core.spi.service.ServiceUtils.findSingletonAmongst;

public class LocalLoaderWriterStore<K, V> implements WrapperStore<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(LocalLoaderWriterStore.class);
  private static final Supplier<Boolean> SUPPLY_FALSE = () -> Boolean.FALSE;

  private final Store<K, V> delegate;
  private final CacheLoaderWriter<? super K, V> cacheLoaderWriter;
  private final boolean useLoaderInAtomics;
  private final ExpiryPolicy<? super K, ? super V> expiry;

  public LocalLoaderWriterStore(Store<K, V> delegate, CacheLoaderWriter<? super K, V> cacheLoaderWriter, boolean useLoaderInAtomics,
                                ExpiryPolicy<? super K, ? super V> expiry) {
    this.delegate = delegate;
    this.cacheLoaderWriter = cacheLoaderWriter;
    this.useLoaderInAtomics = useLoaderInAtomics;
    this.expiry = expiry;
    ContextManager.associate(delegate).withParent(this);
  }

  @Override
  public ValueHolder<V> get(K key) throws StoreAccessException {
    Function<K, V> mappingFunction = k -> {
      try {
        return cacheLoaderWriter.load(k);
      } catch (Exception e) {
        throw new StorePassThroughException(newCacheLoadingException(e));
      }
    };
    return delegate.computeIfAbsent(key, mappingFunction);
  }

  @Override
  public boolean containsKey(K key) throws StoreAccessException {
    return delegate.containsKey(key);
  }

  @Override
  public PutStatus put(K key, V value) throws StoreAccessException {
    BiFunction<K, V, V> remappingFunction = (key1, previousValue) -> {
      try {
        cacheLoaderWriter.write(key1, value);
      } catch (Exception e) {
        throw new StorePassThroughException(newCacheWritingException(e));
      }
      return value;
    };

    delegate.getAndCompute(key, remappingFunction);
    return Store.PutStatus.PUT;
  }

  @Override
  public ValueHolder<V> putIfAbsent(K key, V value, Consumer<Boolean> put) throws StoreAccessException {
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

      // Here were a returning an actual value instead of null because the mappingFunction is called by a map.compute(). So we
      // want the compute to actually set the value to the backend. However, the putIfAbsent should return null since there
      // was no previous value. This is why we use put.accept(true). This will tell EhcacheBase: "Hey! A put was done, you should return null"
      put.accept(true);
      return value;
    };

    return delegate.computeIfAbsent(key, mappingFunction);
  }

  @Override
  public boolean remove(K key) throws StoreAccessException {
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

    delegate.getAndCompute(key, remappingFunction);
    return modified[0];
  }

  @Override
  public RemoveStatus remove(K key, V value) throws StoreAccessException {
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

    delegate.computeAndGet(key, remappingFunction, SUPPLY_FALSE, SUPPLY_FALSE);
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
  public ValueHolder<V> replace(K key, V value) throws StoreAccessException {
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

      if (newValueAlreadyExpired(LOG, expiry, key, inCache, value)) {
        return null;
      }
      return value;
    };

    delegate.getAndCompute(key, remappingFunction);
    if (old[0] == null) {
      return null;
    }
    return new LoaderWriterValueHolder<>(old[0]);
  }

  @Override
  public ReplaceStatus replace(K key, V oldValue, V newValue) throws StoreAccessException {
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

        if (newValueAlreadyExpired(LOG, expiry, key, oldValue, newValue)) {
          return null;
        }
        return newValue;
      }
      return inCache;
    };

    delegate.computeAndGet(key, remappingFunction, SUPPLY_FALSE, SUPPLY_FALSE);
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
  public void clear() throws StoreAccessException {
    delegate.clear();
  }

  @Override
  public StoreEventSource<K, V> getStoreEventSource() {
    return delegate.getStoreEventSource();
  }

  @Override
  public Iterator<Cache.Entry<K, ValueHolder<V>>> iterator() {
    return delegate.iterator();
  }

  @Override
  public ValueHolder<V> getAndCompute(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction) throws StoreAccessException {
    return delegate.getAndCompute(key, (mappedKey, mappedValue) -> {
      V newValue = mappingFunction.apply(mappedKey, mappedValue);
      if (newValue == null) {
        try {
          cacheLoaderWriter.delete(mappedKey);
        } catch (Exception e) {
          throw new StorePassThroughException(newCacheWritingException(e));
        }
        return null;
      } else {
        try {
          cacheLoaderWriter.write(mappedKey, newValue);
        } catch (Exception e) {
          throw new StorePassThroughException(newCacheWritingException(e));
        }
        if (newValueAlreadyExpired(LOG, expiry, mappedKey, mappedValue, newValue)) {
          return null;
        }
        return newValue;
      }
    });
  }

  @Override
  public ValueHolder<V> computeAndGet(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction, Supplier<Boolean> replaceEqual, Supplier<Boolean> invokeWriter) throws StoreAccessException {

    BiFunction<? super K, ? super V, ? extends V> remappingFunction = (mappedKey, mappedValue) -> {
      V newValue = mappingFunction.apply(mappedKey, mappedValue);
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
      return newValue;
    };

    return delegate.computeAndGet(key, remappingFunction, replaceEqual, SUPPLY_FALSE);
  }

  @Override
  public ValueHolder<V> computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) throws StoreAccessException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction) throws StoreAccessException {
    // we are not expecting failures and these two maps are only used in case of failures. So keep them small
    Set<K> successes = new HashSet<>(1);
    Map<K, Exception> failures = new HashMap<>(1);

    if(remappingFunction instanceof Ehcache.PutAllFunction) {
      return getkValueHolderMap((Ehcache.PutAllFunction<K, V>) remappingFunction, successes, failures);
    } else if (remappingFunction instanceof Ehcache.RemoveAllFunction) {
      return getkValueHolderMap(keys);
    } else {
      return delegate.bulkCompute(keys, remappingFunction);
    }
  }

  private Map<K, ValueHolder<V>> getkValueHolderMap(Set<? extends K> keys) throws StoreAccessException {
    // we are not expecting failures and these two maps are only used in case of failures. So keep them small
    Set<K> deleteSuccesses = new HashSet<>(1);
    Map<K, Exception> deleteFailures = new HashMap<>(1);

    Map<K, ? extends V> entriesToRemove = new HashMap<>(keys.size());
    for (K key: keys) {
      entriesToRemove.put(key, null);
    }

    int[] actualRemoveCount = { 0 };

    Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> removalFunction =
            entries -> {
              Set<K> unknowns = cacheLoaderWriterDeleteAllCall(entries, entriesToRemove, deleteSuccesses, deleteFailures);

              int size = CollectionUtil.findBestCollectionSize(entries, 1);
              Map<K, V> results = new LinkedHashMap<>(size);

              for (Map.Entry<? extends K, ? extends V> entry : entries) {
                K key = entry.getKey();
                V existingValue = entry.getValue();

                if (deleteSuccesses.contains(key)) {
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

    Map<K, ValueHolder<V>> map = delegate.bulkCompute(keys, removalFunction);
    if (!deleteFailures.isEmpty()) {
      throw new BulkCacheWritingException(deleteFailures, deleteSuccesses);
    } else {
      return map;
    }
  }

  private Map<K, ValueHolder<V>> getkValueHolderMap(Ehcache.PutAllFunction<K, V> remappingFunction, Set<K> successes, Map<K, Exception> failures) throws StoreAccessException {
    // Copy all entries to write into a Map
    Ehcache.PutAllFunction<K, V> putAllFunction = remappingFunction;
    Map<K, V> entriesToRemap = CollectionUtil.copyMapButFailOnNull(putAllFunction.getEntriesToRemap());

    int[] actualPutCount = {0};

    // The compute function that will return the keys to their NEW values, taking the keys to their old values as input;
    // but this could happen in batches, i.e. not necessary containing all of the entries of the Iterable passed to this method
    Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> computeFunction =
            entries1 -> {
              // If we have a writer, first write this batch
              cacheLoaderWriterWriteAllCall(entries1, entriesToRemap, successes, failures);

              int size = CollectionUtil.findBestCollectionSize(entries1, 1);
              Map<K, V> mutations = new LinkedHashMap<>(size);

              // then record we handled these mappings
              for (Map.Entry<? extends K, ? extends V> entry : entries1) {
                K key = entry.getKey();
                V existingValue = entry.getValue();
                V newValue = entriesToRemap.remove(key);

                if (newValueAlreadyExpired(LOG, expiry, key, existingValue, newValue)) {
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

    Map<K, ValueHolder<V>> computedMap = delegate.bulkCompute(putAllFunction.getEntriesToRemap().keySet(), computeFunction);
    if (!failures.isEmpty()) {
      throw new BulkCacheWritingException(failures, successes);
    }
    return computedMap;
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction, Supplier<Boolean> replaceEqual) throws StoreAccessException {
    return null;
  }

  @Override
  public Map<K, ValueHolder<V>> bulkComputeIfAbsent(Set<? extends K> keys, Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction) throws StoreAccessException {
    // we are not expecting failures and these two maps are only used in case of failures. So keep them small
    Map<K, V> successes = new HashMap<>(1);
    Map<K, Exception> failures = new HashMap<>(1);

    if (!(mappingFunction instanceof Ehcache.GetAllFunction)) {
      return delegate.bulkComputeIfAbsent(keys, mappingFunction);
    }

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
    Map<K, Store.ValueHolder<V>> computedMap = delegate.bulkComputeIfAbsent(keys, computeFunction);

    if (failures.isEmpty()) {
      return computedMap;
    } else {
      successes.putAll(result);
      throw new BulkCacheLoadingException(failures, successes);
    }
  }

  @Override
  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
    return delegate.getConfigurationChangeListeners();
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

  private static <K, V> boolean newValueAlreadyExpired(Logger logger, ExpiryPolicy<? super K, ? super V> expiry, K key, V oldValue, V newValue) {
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
}
