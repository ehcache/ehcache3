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

package org.ehcache;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheRuntimeConfiguration;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;
import org.ehcache.events.StateChangeListener;
import org.ehcache.exceptions.*;
import org.ehcache.expiry.Expiry;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.function.Predicate;
import org.ehcache.resilience.ResilienceStrategy;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.cache.Store.ValueHolder;
import org.ehcache.spi.loader.CacheLoader;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.writer.CacheWriter;
import org.ehcache.statistics.CacheOperationOutcomes.GetOutcome;
import org.ehcache.statistics.CacheOperationOutcomes.PutOutcome;
import org.ehcache.statistics.CacheOperationOutcomes.PutIfAbsentOutcome;
import org.ehcache.statistics.CacheOperationOutcomes.RemoveOutcome;
import org.ehcache.statistics.CacheOperationOutcomes.ReplaceOutcome;
import org.ehcache.statistics.CacheStatistics;
import org.terracotta.statistics.observer.OperationObserver;
import org.ehcache.statistics.CacheOperationOutcomes.CacheLoaderOutcome;
import org.ehcache.statistics.StatisticsGateway;
import org.ehcache.util.StatisticsThreadPoolUtil;
import org.terracotta.context.annotations.ContextChild;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.ehcache.Functions.memoize;
import static org.ehcache.exceptions.ExceptionFactory.newCacheLoaderException;
import static org.ehcache.exceptions.ExceptionFactory.newCacheWriterException;
import static org.terracotta.statistics.StatisticsBuilder.operation;

/**
 * @author Alex Snaps
 */
public class Ehcache<K, V> implements Cache<K, V>, StandaloneCache<K, V>, PersistentStandaloneCache<K, V> {

  private final StatusTransitioner statusTransitioner = new StatusTransitioner();

  @ContextChild
  private final Store<K, V> store;
  private final CacheLoader<? super K, ? extends V> cacheLoader;
  private final CacheWriter<? super K, ? super V> cacheWriter;
  private final ResilienceStrategy<K, V> resilienceStrategy;
  private final RuntimeConfiguration<K, V> runtimeConfiguration;

  private final OperationObserver<GetOutcome> getObserver = operation(GetOutcome.class).named("get").of(this).tag("cache").build();
  private final OperationObserver<PutOutcome> putObserver = operation(PutOutcome.class).named("put").of(this).tag("cache").build();
  private final OperationObserver<RemoveOutcome> removeObserver = operation(RemoveOutcome.class).named("remove").of(this).tag("cache").build();
  private final OperationObserver<CacheLoaderOutcome> cacheLoaderObserver = operation(CacheLoaderOutcome.class).named("cacheLoader").of(this).tag("cache").build();
  private final OperationObserver<PutIfAbsentOutcome> putIfAbsentObserver = operation(PutIfAbsentOutcome.class).named("putIfAbsent").of(this).tag("cache").build();
  private final OperationObserver<ReplaceOutcome> replaceObserver = operation(ReplaceOutcome.class).named("replace").of(this).tag("cache").build();  
  private final ConcurrentMap<String, AtomicLong> bulkMethodEntries = new ConcurrentHashMap<String, AtomicLong>();
  
  private final StatisticsGateway statisticsGateway;
  
  public Ehcache(CacheConfiguration<K, V> config, final Store<K, V> store) {
    this(config, store, null);
  }

  public Ehcache(CacheConfiguration<K, V> config, Store<K, V> store, final CacheLoader<? super K, ? extends V> cacheLoader) {
    this(config, store, cacheLoader, null, StatisticsThreadPoolUtil.getDefaultStatisticsExecutorService());
  }
  
  public Ehcache(CacheConfiguration<K, V> config, Store<K, V> store, final CacheLoader<? super K, ? extends V> cacheLoader, CacheWriter<? super K, ? super V> cacheWriter) {
    this(config, store, cacheLoader, cacheWriter, StatisticsThreadPoolUtil.getDefaultStatisticsExecutorService());
  }

  public Ehcache(CacheConfiguration<K, V> config, Store<K, V> store, final CacheLoader<? super K, ? extends V> cacheLoader, CacheWriter<? super K, ? super V> cacheWriter, 
      ScheduledExecutorService statisticsExecutor) {
    this.store = store;
    this.cacheLoader = cacheLoader;
    this.cacheWriter = cacheWriter;
    this.statisticsGateway = new StatisticsGateway(this, statisticsExecutor, bulkMethodEntries);
    this.resilienceStrategy = new ResilienceStrategy<K, V>() {
      @Override
      public void recoveredFrom(final K key, final Exception e) {
        // ignore
      }

      @Override
      public void possiblyInconsistent(final K key, final CacheAccessException root, final Exception... otherExceptions) {
        // ignore
      }
    };
    
    this.runtimeConfiguration = new RuntimeConfiguration<K, V>(config);
  }

  @Override
  public V get(final K key) throws CacheLoaderException {
    getObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(key);
    final Function<K, V> mappingFunction = memoize(
        new Function<K, V>() {
          @Override
          public V apply(final K k) {
            V loaded = null;
            try {
              if (cacheLoader != null) {
                cacheLoaderObserver.begin();
                loaded = cacheLoader.load(k);
                cacheLoaderObserver.end(CacheLoaderOutcome.SUCCESS);
                getObserver.end(loaded != null ? GetOutcome.HIT_WITH_LOADER : GetOutcome.MISS_WITH_LOADER);
              }
            } catch (Exception e) {
              cacheLoaderObserver.end(CacheLoaderOutcome.FAILURE);
              throw newCacheLoaderException(e);
            }
            
            return loaded;
          }
        });

    try {
      final Store.ValueHolder<V> valueHolder = store.computeIfAbsent(key, mappingFunction);
      
      // Check for expiry first
      if (valueHolder == null) {
        if (cacheLoader == null) {
          getObserver.end(GetOutcome.MISS_NO_LOADER);
        }
        return null;
      } else {
        if (cacheLoader == null) {
          getObserver.end(GetOutcome.HIT_NO_LOADER);
        }
        return valueHolder.value();
      }
    } catch (CacheAccessException e) {
      getObserver.end(GetOutcome.FAILURE);
      // So we either didn't load, or that's a miss in the SoR as well
      removeKeysWithStrategy(Collections.singleton(key), e);
      return mappingFunction.apply(key);
    } catch (CacheLoaderException e) {
      getObserver.end(GetOutcome.FAILURE);
      removeKeysWithStrategy(Collections.singleton(key), e);
      throw e;
    }
  }

  @Override
  public void put(final K key, final V value) throws CacheWriterException {
    putObserver.begin();    
    statusTransitioner.checkAvailable();
    checkNonNull(key, value);
    final BiFunction<K, V, V> remappingFunction = memoize(new BiFunction<K, V, V>() {
      @Override
      public V apply(final K key, final V previousValue) {
        try {
          if (cacheWriter != null) {
            cacheWriter.write(key, value);
          }
        } catch (Exception e) {
          throw newCacheWriterException(e);
        }
        return value;
      }
    });

    try {
      store.compute(key, remappingFunction);
      putObserver.end(PutOutcome.ADDED);
    } catch (CacheAccessException e) {
      putObserver.end(PutOutcome.FAILURE);
      try {
        remappingFunction.apply(key, value);
      } finally {
        removeKeysWithStrategy(Collections.singleton(key), e);
      }
    } catch(CacheWriterException e) {
      putObserver.end(PutOutcome.FAILURE);
      removeKeysWithStrategy(Collections.singleton(key), e);
      throw e;
    }

  }

  @Override
  public boolean containsKey(final K key) {
    statusTransitioner.checkAvailable();
    checkNonNull(key);
    try {
      return store.containsKey(key);
    } catch (CacheAccessException e) {
      try {
        store.remove(key);
        // fire an event? eviction?
      } catch (CacheAccessException e1) {
        // fall back to strategy?
      }
      return false;
    }
  }

  @Override
  public void remove(final K key) throws CacheWriterException {
    removeObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(key);

    final BiFunction<K, V, V> remappingFunction = memoize(new BiFunction<K, V, V>() {
      @Override
      public V apply(final K key, final V previousValue) {
        try {
          if (cacheWriter != null) {
            cacheWriter.delete(key);
          }
        } catch (Exception e) {
          throw newCacheWriterException(e);
        }
        return null;
      }
    });

    try {
      store.compute(key, remappingFunction);
      removeObserver.end(RemoveOutcome.SUCCESS);
    } catch (CacheAccessException e) {
      removeObserver.end(RemoveOutcome.FAILURE);
      try {
        remappingFunction.apply(key, null);
      } finally {
        removeKeysWithStrategy(Collections.singleton(key), e);
      }
    } catch(CacheWriterException e) {
      removeObserver.end(RemoveOutcome.FAILURE);
      removeKeysWithStrategy(Collections.singleton(key), e);
      throw e;
    }
  }

  @Override
  public void clear() {
    statusTransitioner.checkAvailable();
    try {
      store.clear();
    } catch (CacheAccessException e) {
      throw new RuntimeException("Couldn't clear cache", e);
    }
  }

  @Override
  public Iterator<Entry<K, V>> iterator() {
    statusTransitioner.checkAvailable();
    try {
      return new CacheEntryIterator(store.iterator());
    } catch (CacheAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map<K, V> getAll(final Iterable<? extends K> keys) throws BulkCacheLoaderException {
    getObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(keys);
    
    final Map<K, V> successes = new HashMap<K, V>();
    final Map<K, Exception> failures = new HashMap<K, Exception>();
    
    Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> computeFunction =
        new Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
      @Override
      public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) {
        Set<K> missingKeys = new HashSet<K>();
        Map<K, V> computeResult = new LinkedHashMap<K ,V>();
        
        // put all the entries to get ordering correct, and find missing keys
        for (Map.Entry<? extends K, ? extends V> entry : entries) {
          K key = entry.getKey();
          V value = entry.getValue();
          computeResult.put(key, value);
          if (value == null) {
            missingKeys.add(key);
          } else {
            successes.put(key, value);
            getObserver.end(GetOutcome.HIT_NO_LOADER);
            getObserver.begin();
          }
        }

        if (cacheLoader != null) {
          Map<K, V> loaded = Collections.emptyMap();
          try {
            loaded = (Map<K, V>) cacheLoader.loadAll(missingKeys);
          } catch (Exception e) {
            for (K key : missingKeys) {
              failures.put(key, e);
            }
          }
          
          for (Map.Entry<K, V> loadedEntry : loaded.entrySet()) {
            K key = loadedEntry.getKey();
            if (! missingKeys.contains(key)) {
              throw newCacheLoaderException(new RuntimeException("Cache loader returned value for key: " + key));
            }
            V value = loadedEntry.getValue();
            successes.put(key, value);
            if (value == null) {
              getObserver.end(GetOutcome.MISS_WITH_LOADER);
              getObserver.begin();
            } else {
              getObserver.end(GetOutcome.HIT_WITH_LOADER);
              getObserver.begin();
            }
            computeResult.put(key, value);
          }
        } else {
          for (int i = 0; i < missingKeys.size(); i++) {
            getObserver.end(GetOutcome.MISS_NO_LOADER);
            getObserver.begin();
          }
        }
        return computeResult.entrySet();
      }
    };
    try {
      Map<K, Store.ValueHolder<V>> computedMap = store.bulkCompute(keys, computeFunction);
      
      Map<K, V> result = new HashMap<K, V>();
      for (Map.Entry<K, Store.ValueHolder<V>> entry : computedMap.entrySet()) {
        ValueHolder<V> valueHolder = entry.getValue();
        result.put(entry.getKey(), valueHolder == null ? null : valueHolder.value());
      }
      
      addBulkMethodEntriesCount("getAll", result.size());
      return result;
    } catch (CacheAccessException e) {
      removeKeysWithStrategy(keys, e);
      if (failures.isEmpty()) {
        Map<K, V> toLoad = new LinkedHashMap<K, V>();
        for (K key: keys) {
          if (! successes.containsKey(key)) {
            toLoad.put(key, null);
          }
        }
        
        computeFunction.apply(toLoad.entrySet());
      }
        
      return successes;
    } finally {
      if (! failures.isEmpty()) {
        throw new BulkCacheLoaderException(failures, successes);
      }
    }
  }

  @Override
  public void putAll(final Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) throws CacheWriterException {
    putObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(entries);
    final Set<K> successes = new HashSet<K>();
    final Map<K, Exception> failures = new HashMap<K, Exception>();

    // Copy all entries to write into a Map
    final Map<K, V> entriesToRemap = new HashMap<K, V>();
    for (Map.Entry<? extends K, ? extends V> entry: entries) {
      // If a key/value to map is null, throw NPE, nothing gets mutated
      if (entry.getKey() == null || entry.getValue() == null) {
        throw new NullPointerException();
      }
      entriesToRemap.put(entry.getKey(), entry.getValue());
    }
    
    final int entriesCount = entriesToRemap.size();

    // The compute function that will return the keys to their NEW values, taking the keys to their old values as input;
    // but this could happen in batches, i.e. not necessary containing all of the entries of the Iterable passed to this method
    Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> computeFunction =
      new Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
      @Override
      public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) {

        // If we have a writer, first write this batch
        cacheWriterPutAllCall(entries, entriesToRemap, successes, failures);

        Map<K, V> computeResult = new LinkedHashMap<K, V>();

        // then record we handled these mappings
        for (Map.Entry<? extends K, ? extends V> entry: entries) {
          K key = entry.getKey();
          if (cacheLoader == null || successes.contains(key)) {
            computeResult.put(entry.getKey(), entriesToRemap.remove(entry.getKey()));
          } else {
            computeResult.put(entry.getKey(), entry.getValue());
          }
        }

        // Finally return the values to be installed in the Cache's Store
        return computeResult.entrySet();
      }
    };

    KeysIterable keys = new KeysIterable(entries);
    try {
      store.bulkCompute(keys, computeFunction);
      for (@SuppressWarnings("unused") K key : keys) {
        // XXX: this is going to mess with put times. One potentially slow followed by lots of fast ones
        putObserver.end(PutOutcome.ADDED);
        putObserver.begin();
      }
      addBulkMethodEntriesCount("putAll", entriesCount);
    } catch (CacheAccessException e) {
      for (@SuppressWarnings("unused") K key : keys) {
        putObserver.end(PutOutcome.FAILURE);
        putObserver.begin();
      }
      
      // just in case not all writes happened:
      try {
        if (! entriesToRemap.isEmpty()) {
          cacheWriterPutAllCall(entriesToRemap.entrySet(), entriesToRemap, successes, failures);
        }
      } finally {
        removeKeysWithStrategy(keys, e);
      }
    } finally {
      if (! failures.isEmpty()) {
        throw new BulkCacheWriterException(failures, successes);
      }
    }
  }

  private void cacheWriterPutAllCall(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries, Map<K, V> entriesToRemap, Set<K> successes, Map<K, Exception> failures) {
    if (cacheWriter != null) {
      Set<K> allKeys = new HashSet<K>();
      Collection<Map.Entry<? extends K, ? extends V>> toWrite = new ArrayList<Map.Entry<? extends K, ? extends V>>();
      for (Map.Entry<? extends K, ? extends V> entry: entries) {
        toWrite.add(entry);
        allKeys.add(entry.getKey());
      }
      
      try {
        if (! toWrite.isEmpty()) {
          cacheWriter.writeAll(toWrite);
          successes.addAll(allKeys);
        }
      } catch (Exception e) {
        // the keys left in the collection were not written
        for (Map.Entry<? extends K, ? extends V> entry : toWrite) {
          failures.put(entry.getKey(), e);
          allKeys.remove(entry.getKey());
        }
        successes.addAll(allKeys);
      }
    }
  }

  @Override
  public void removeAll(final Iterable<? extends K> keys) throws CacheWriterException {
    removeObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(keys);
    final Set<K> successes = new HashSet<K>();
    final Map<K, Exception> failures = new HashMap<K, Exception>();

    final Map<K, ? extends V> entriesToRemove = new HashMap<K, V>();
    for (K key: keys) {
      if (key == null) {
        throw new NullPointerException();
      }
      entriesToRemove.put(key, null);
    }
    final int entriesCount = entriesToRemove.size();
    
    Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> removalFunction =
      new Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
        @Override
        public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) {
          cacheWriterDeleteAllCall(entries, successes, failures);
          
          Map<K, V> computeResult = new LinkedHashMap<K, V>();
          for (Map.Entry<? extends K, ? extends V> entry : entries) {  
            K key = entry.getKey();
            V value = entry.getValue();
            if (cacheLoader == null || successes.contains(key)) {
              if (value != null) {
                // XXX: this is going to mess with remove times. One potentially slow followed by lots of fast ones
                removeObserver.end(RemoveOutcome.SUCCESS);
                removeObserver.begin();
              }
              computeResult.put(key, null);
            } else {
              computeResult.put(key, value);
            }
          }
          return computeResult.entrySet();
        }
      };

    try {
      store.bulkCompute(keys, removalFunction);
      addBulkMethodEntriesCount("removeAll", entriesCount);
    } catch (CacheAccessException e) {
      for (@SuppressWarnings("unused") K key : keys) {
        removeObserver.end(RemoveOutcome.FAILURE);
        removeObserver.begin();
      }
      
      // just in case the write didn't happen:
      try {
        if (! entriesToRemove.isEmpty()) {
          cacheWriterDeleteAllCall(entriesToRemove.entrySet(), successes, failures);
        }
      } finally {
        removeKeysWithStrategy(keys, e);
      }
    } finally {
      if (! failures.isEmpty()) {
        throw new BulkCacheWriterException(failures, successes);
      }
    }

  }

  private void cacheWriterDeleteAllCall(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries, Set<K> successes, Map<K, Exception> failures) {
    if (cacheWriter != null) {
      Collection<K> toDelete = new ArrayList<K>();
      for (Map.Entry<? extends K, ? extends V> entry: entries) {
        toDelete.add(entry.getKey());
      }
      Set<K> allKeys = new HashSet<K>(toDelete);
      try {
        cacheWriter.deleteAll(toDelete);
        successes.addAll(toDelete);
      } catch (Exception e) {
        // remaining keys were not deleted
        allKeys.removeAll(toDelete);
        for (K key : allKeys) {
          successes.add(key);
        }
        for (K key: toDelete) {
          failures.put(key, e);
        }
      }
    }
  }

  @Override
  public V putIfAbsent(final K key, final V value) throws CacheWriterException {
    putIfAbsentObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(key, value);
    final AtomicBoolean installed = new AtomicBoolean(false);
    
    final Function<K, V> mappingFunction = memoize(new Function<K, V>() {
      @Override
      public V apply(final K k) {
        if (cacheWriter != null) {
          try {
            // TODO decide how to handle a false return value (perhaps as part of issue #52)
            cacheWriter.write(k, null, value);
          } catch (Exception e) {
            throw newCacheWriterException(e);
          }
        }
        
        installed.set(true);
        return value;
      }
    });

    try {
      ValueHolder<V> inCache = store.computeIfAbsent(key, mappingFunction);
      if (installed.get()) {
        return null;
      } else {
        return inCache.value();
      }
    } catch (CacheAccessException e) {
      V computedValue = null;
      putIfAbsentObserver.end(PutIfAbsentOutcome.FAILURE);
      try {
        computedValue = mappingFunction.apply(key);
        if (cacheWriter != null) {
          store.remove(key);
        } else {
          final Store.ValueHolder<V> holder = store.get(key);
          if (holder == null) {
            resilienceStrategy.possiblyInconsistent(key, e);
          } else {
            return holder.value();
          }
        }
      } catch (CacheAccessException e1) {
        resilienceStrategy.possiblyInconsistent(key, e, e1);
      }
      
      return installed.get() ? null : computedValue;
    } finally {
      if (installed.get()) {
        putIfAbsentObserver.end(PutIfAbsentOutcome.PUT);
      } else {
        putIfAbsentObserver.end(PutIfAbsentOutcome.HIT);
      }
    }
  }

  @Override
  public boolean remove(final K key, final V value) throws CacheWriterException {
    getObserver.begin();
    removeObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(key, value);
    final AtomicBoolean removed = new AtomicBoolean();
    final AtomicBoolean hit = new AtomicBoolean();
    final BiFunction<K, V, V> computeFunction = memoize(new BiFunction<K, V, V>() {
      @Override
      public V apply(final K k, final V inCache) {
        if (inCache != null) {
          if (inCache.equals(value)) {
            if (cacheWriter != null) {
              try {
                if(! cacheWriter.delete(k, value)) {
                  // TODO: revisit for #52
                  return null; // remove cached value but don't set success flag - this is an inline repair
                }
              } catch (Exception e) {
                throw newCacheWriterException(e);
              }
            }

            hit.set(true);
            removed.set(true);
            return null;
          }

          return inCache;
        } else {
          if (cacheWriter != null) {
            try {
              removed.set(cacheWriter.delete(k, value));
            } catch (Exception e) {
              throw newCacheWriterException(e);
            }
          }
          return null;
        }
      }
    });
    try {
      store.compute(key, computeFunction);
      if (removed.get()) {
        removeObserver.end(RemoveOutcome.SUCCESS);
      } else {
        removeObserver.end(RemoveOutcome.FAILURE);
      }
      if (hit.get()) {
        getObserver.end(GetOutcome.HIT_NO_LOADER); 
      } else {
        getObserver.end(GetOutcome.MISS_NO_LOADER);
      }
    } catch (CacheAccessException e) {
      removeObserver.end(RemoveOutcome.FAILURE);
      try {
        computeFunction.apply(key, null);
      } finally {
        removeKeysWithStrategy(Collections.singleton(key), e);
      }
    }
    return removed.get();
  }

  @Override
  public V replace(final K key, final V value) throws CacheLoaderException, CacheWriterException {
    replaceObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(key, value);
    final AtomicReference<V> old = new AtomicReference<V>();
    final BiFunction<K, V, V> remappingFunction = memoize(new BiFunction<K, V, V>() {
      @Override
      public V apply(final K k, final V inCache) {
        if (cacheWriter != null) {
          try {
            cacheWriter.write(key, value);
          } catch (Exception e) {
            throw newCacheWriterException(e);
          }
        }
        
        old.set(inCache);
        return value;
      }
    });

    try {
      store.computeIfPresent(key, remappingFunction);
    } catch (CacheAccessException e) {
      replaceObserver.end(ReplaceOutcome.FAILURE);
      try {
        remappingFunction.apply(key, null);
      } finally {
        removeKeysWithStrategy(Collections.singleton(key), e);
      }
    } catch (CacheWriterException e) {
      replaceObserver.end(ReplaceOutcome.FAILURE);
      removeKeysWithStrategy(Collections.singleton(key), e);
      throw e;
    }
    
    if (old.get() != null) {
      replaceObserver.end(ReplaceOutcome.HIT);
    } else {
      replaceObserver.end(ReplaceOutcome.MISS);
    }
    return old.get();
  }

  @Override
  public boolean replace(final K key, final V oldValue, final V newValue) throws CacheLoaderException, CacheWriterException {
    replaceObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(key, oldValue, newValue);

    final AtomicBoolean success = new AtomicBoolean();
    final BiFunction<K, V, V> remappingFunction = memoize(new BiFunction<K, V, V>() {
      @Override
      public V apply(final K k, final V inCache) {
        if (inCache != null) {
          if (inCache.equals(oldValue)) {
            if (cacheWriter != null) {
              try {
                success.set(cacheWriter.write(key, oldValue, newValue));
              } catch (Exception e) {
                throw newCacheWriterException(e);
              }
              // repair cache 
              if (!success.get()) {
                // TODO: revisit under #52
                return null;
              }
            } else {
              success.set(true);
            }
            return newValue; 
          }
          return inCache;
        } else {
          if (cacheWriter != null) {
            try {
              success.set(cacheWriter.write(key, oldValue, newValue));
            } catch (Exception e) {
              throw newCacheWriterException(e);
            }
          }
          return null;
        }
      }
    });
    try {
      store.compute(key, remappingFunction);
    } catch (CacheAccessException e) {
      replaceObserver.end(ReplaceOutcome.FAILURE);
      try {
        remappingFunction.apply(key, null);
      } finally {
        removeKeysWithStrategy(Collections.singleton(key), e);
      }
    } catch (CacheWriterException e) {
      replaceObserver.end(ReplaceOutcome.FAILURE);
      removeKeysWithStrategy(Collections.singleton(key), e);
      throw e;
    }
    if (success.get()) {
      replaceObserver.end(ReplaceOutcome.HIT);
    } else {
      replaceObserver.end(ReplaceOutcome.MISS);
    }
    return success.get();

  }

  @Override
  public CacheRuntimeConfiguration<K, V> getRuntimeConfiguration() {
    return runtimeConfiguration;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void init() {
    final StatusTransitioner.Transition st = statusTransitioner.init();
    try {
      store.init();
    } catch (RuntimeException e) {
      st.failed();
      throw new StateTransitionException(e);
    }
    st.succeeded();
  }

  @Override
  public void close() {
    final StatusTransitioner.Transition st = statusTransitioner.close();
    try {
      store.close();
    } catch (RuntimeException e) {
      st.failed();
      throw new StateTransitionException(e);
    }
    st.succeeded();
  }

  @Override
  public Maintainable toMaintenance() {
    final StatusTransitioner.Transition st = statusTransitioner.maintenance();
    try {
      final Maintainable maintainable = new Maintainable() {
        @Override
        public void create() {
          Ehcache.this.create();
        }

        @Override
        public void destroy() {
          Ehcache.this.destroy();
        }
      };
      store.maintenance();
      st.succeeded();
      return maintainable;
    } catch (RuntimeException e) {
      st.failed();
      throw new StateTransitionException(e);
    }
  }

  void create() {
    statusTransitioner.checkMaintenance();
    try {
      store.create();
    } catch (CacheAccessException e) {
      throw new RuntimeException("Couldn't create Cache", e);
    }
  }

  void destroy() {
    statusTransitioner.checkMaintenance();
    try {
      store.destroy();
    } catch (CacheAccessException e) {
      throw new RuntimeException("Couldn't destroy Cache", e);
    }
  }

  @Override
  public Status getStatus() {
    return statusTransitioner.currentStatus();
  }

  void registerListener(StateChangeListener listener) {
    statusTransitioner.registerListener(listener);
  }

  void deregisterListener(StateChangeListener listener) {
    statusTransitioner.deregisterListener(listener);
  }

  private static void checkNonNull(Object... things) {
    for (Object thing : things) {
      if(thing == null) {
        throw new NullPointerException();
      }
    }
  }
  
  private void addBulkMethodEntriesCount(String key, long count) {
    AtomicLong current = bulkMethodEntries.get(key);
    if (current == null) {
      AtomicLong newCount = new AtomicLong();
      current = bulkMethodEntries.putIfAbsent(key, newCount);
      if (current == null) {
        current = newCount;
      }
    }
    current.addAndGet(count);
  }

  private void removeKeysWithStrategy(Iterable<? extends K> keys, Exception e) {
    for (K key: keys) {
      try {
        store.remove(key);
        resilienceStrategy.recoveredFrom(key, e);
      } catch (CacheAccessException e1) {
        resilienceStrategy.possiblyInconsistent(key, e1, e);
      }
    }
 
  }
  
  CacheLoader<? super K, ? extends V> getCacheLoader() {
    return cacheLoader;
  }
  
  @Override
  public CacheStatistics getStatistics() {
    return statisticsGateway;
  }

  CacheWriter<? super K, ? super V> getCacheWriter() {
    return cacheWriter;
  }

  private static class RuntimeConfiguration<K, V> implements CacheRuntimeConfiguration<K, V> {
    
    private final Collection<ServiceConfiguration<?>> serviceConfigurations;
    private final Class<K> keyType;
    private final Class<V> valueType;
    private final Comparable<Long> capacityConstraint;
    private final Predicate<Cache.Entry<K, V>> evictionVeto;
    private final Comparator<Cache.Entry<K, V>> evictionPrioritizer;
    private final Set<CacheEventListener<?, ?>> eventListeners;
    private final SerializationProvider serializationProvider;
    private final ClassLoader classLoader;
    private final Expiry<K, V> expiry;

    RuntimeConfiguration(CacheConfiguration<K, V> config) {
      this.serviceConfigurations = copy(config.getServiceConfigurations());
      this.keyType = config.getKeyType();
      this.valueType = config.getValueType();
      this.capacityConstraint = config.getCapacityConstraint();
      this.evictionVeto = config.getEvictionVeto();
      this.evictionPrioritizer = config.getEvictionPrioritizer();
      this.eventListeners = copy(config.getEventListeners());
      this.serializationProvider = config.getSerializationProvider();
      this.classLoader = config.getClassLoader();
      this.expiry = config.getExpiry();
    }
    
    private static <T> Set<T> copy(Set<T> set) {
      if (set == null) {
        return null;
      }
    
      return Collections.unmodifiableSet(new HashSet<T>(set));
    }
    
    private static <T> Collection<T> copy(Collection<T> collection) {
      if (collection == null) {
        return null;
      }
      
      return Collections.unmodifiableCollection(new ArrayList<T>(collection));
    }

    @Override
    public Collection<ServiceConfiguration<?>> getServiceConfigurations() {
      return this.serviceConfigurations;
    }

    @Override
    public Class<K> getKeyType() {
      return this.keyType;
    }

    @Override
    public Class<V> getValueType() {
      return this.valueType;
    }

    @Override
    public Comparable<Long> getCapacityConstraint() {
      return this.capacityConstraint;
    }

    @Override
    public Predicate<Cache.Entry<K, V>> getEvictionVeto() {
      return this.evictionVeto;
    }

    @Override
    public Comparator<Cache.Entry<K, V>> getEvictionPrioritizer() {
      return this.evictionPrioritizer;
    }

    @Override
    public Set<CacheEventListener<?, ?>> getEventListeners() {
      return this.eventListeners;
    }

    @Override
    public SerializationProvider getSerializationProvider() {
      return this.serializationProvider;
    }

    @Override
    public ClassLoader getClassLoader() {
      return this.classLoader;
    }
    
    @Override
    public Expiry<K, V> getExpiry() {
      return expiry;
    }
    
    @Override
    public void deregisterCacheEventListener(CacheEventListener<? super K, ? super V> listener) {
      throw new UnsupportedOperationException("implement me!"); // XXX:
    }
    
    @Override
    public void registerCacheEventListener(CacheEventListener<? super K, ? super V> listener, EventOrdering ordering,
        EventFiring firing, Set<EventType> forEventTypes) {
      throw new UnsupportedOperationException("implement me!"); // XXX:
    }
    
    @Override
    public void setCapacityConstraint(Comparable<Long> constraint) {
      throw new UnsupportedOperationException("implement me!"); // XXX:
    }
  }
  
  private class CacheEntryIterator implements Iterator<Entry<K, V>> {

    private final Store.Iterator<Entry<K, Store.ValueHolder<V>>> iterator;
    private Entry<K, Store.ValueHolder<V>> next;

    public CacheEntryIterator(final Store.Iterator<Entry<K, Store.ValueHolder<V>>> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      statusTransitioner.checkAvailable();
      try {
        return iterator.hasNext();
      } catch (CacheAccessException e) {
        return false; // really?!
      }
    }

    @Override
    public Entry<K, V> next() {
      getObserver.begin();
      statusTransitioner.checkAvailable();
      try {
        next = iterator.next();
        getObserver.end(GetOutcome.HIT_NO_LOADER);
      } catch (CacheAccessException e) {
        getObserver.end(GetOutcome.FAILURE);
        throw new RuntimeException("Crap! We said we had more... turns out we can't get to it now :(");
      }
      return new Entry<K, V>() {
        @Override
        public K getKey() {
          return next.getKey();
        }

        @Override
        public V getValue() {
          return next.getValue().value();
        }

        @Override
        public long getCreationTime(TimeUnit unit) {
          return next.getCreationTime(unit);
        }

        @Override
        public long getLastAccessTime(TimeUnit unit) {
          return next.getLastAccessTime(unit);
        }

        @Override
        public float getHitRate(TimeUnit unit) {
          return next.getHitRate(unit);
        }
      };
    }

    @Override
    public void remove() {
      statusTransitioner.checkAvailable();
      Ehcache.this.remove(next.getKey(), next.getValue().value());
    }
  }


  private class KeysIterable implements Iterable<K> {
    private final Iterable<? extends Map.Entry<? extends K, ? extends V>> entriesIterable;

    public KeysIterable(Iterable<? extends Map.Entry<? extends K, ? extends V>> entriesIterable) {
      this.entriesIterable = entriesIterable;
    }

    @Override
    public Iterator<K> iterator() {
      return new KeysIterator(entriesIterable.iterator());
    }
  }

  private class KeysIterator implements Iterator<K> {
    private final Iterator<? extends Map.Entry<? extends K, ? extends V>> entriesIterator;

    public KeysIterator(Iterator<? extends Map.Entry<? extends K, ? extends V>> entriesIterator) {
      this.entriesIterator = entriesIterator;
    }

    @Override
    public boolean hasNext() {
      return entriesIterator.hasNext();
    }

    @Override
    public K next() {
      return entriesIterator.next().getKey();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private class NullValuesIterable implements Iterable<Map.Entry<K, V>> {
    private final Iterable<? extends Map.Entry<? extends K, ? extends V>> entries;

    public NullValuesIterable(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) {
      this.entries = entries;
    }

    @Override
    public Iterator<Map.Entry<K, V>> iterator() {
      return new NullValuesIterator(entries.iterator());
    }
  }

  private class NullValuesIterator implements Iterator<Map.Entry<K, V>> {
    private final Iterator<? extends Map.Entry<? extends K, ? extends V>> iterator;

    public NullValuesIterator(Iterator<? extends Map.Entry<? extends K, ? extends V>> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Map.Entry<K, V> next() {
      final Map.Entry<? extends K, ? extends V> next = iterator.next();
      return new Map.Entry<K, V>() {
        @Override
        public K getKey() {
          return next.getKey();
        }

        @Override
        public V getValue() {
          return null;
        }

        @Override
        public V setValue(V value) {
          throw new UnsupportedOperationException();
        }
      };
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
