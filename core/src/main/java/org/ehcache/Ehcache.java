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
import org.ehcache.config.EvictionPrioritizer;
import org.ehcache.config.EvictionVeto;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.CacheEventListenerFactory;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;
import org.ehcache.events.CacheEventNotificationService;
import org.ehcache.events.CacheEvents;
import org.ehcache.events.StateChangeListener;
import org.ehcache.events.StoreEventListener;
import org.ehcache.exceptions.BulkCacheLoaderException;
import org.ehcache.exceptions.BulkCacheWriterException;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.exceptions.CacheLoaderException;
import org.ehcache.exceptions.CacheWriterException;
import org.ehcache.exceptions.StateTransitionException;
import org.ehcache.expiry.Expiry;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.resilience.LoggingRobustResilienceStrategy;
import org.ehcache.resilience.RecoveryCache;
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
import org.ehcache.statistics.DisabledStatistics;
import org.ehcache.statistics.StatisticsGateway;
import org.terracotta.context.annotations.ContextChild;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NoSuchElementException;
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
import static org.ehcache.util.KeysIterable.keysOf;
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
  private final RuntimeConfiguration runtimeConfiguration;
  private final CacheEventNotificationService<K, V> eventNotificationService;
  private final StoreEventListener<K, V> storeListener;

  private final OperationObserver<GetOutcome> getObserver = operation(GetOutcome.class).named("get").of(this).tag("cache").build();
  private final OperationObserver<PutOutcome> putObserver = operation(PutOutcome.class).named("put").of(this).tag("cache").build();
  private final OperationObserver<RemoveOutcome> removeObserver = operation(RemoveOutcome.class).named("remove").of(this).tag("cache").build();
  private final OperationObserver<CacheLoaderOutcome> cacheLoaderObserver = operation(CacheLoaderOutcome.class).named("cacheLoader").of(this).tag("cache").build();
  private final OperationObserver<PutIfAbsentOutcome> putIfAbsentObserver = operation(PutIfAbsentOutcome.class).named("putIfAbsent").of(this).tag("cache").build();
  private final OperationObserver<ReplaceOutcome> replaceObserver = operation(ReplaceOutcome.class).named("replace").of(this).tag("cache").build();  
  private final ConcurrentMap<String, AtomicLong> bulkMethodEntries = new ConcurrentHashMap<String, AtomicLong>();
  
  private final CacheStatistics cacheStatistics;

  public Ehcache(CacheConfiguration<K, V> config, final Store<K, V> store) {
    this(config, store, null);
  }

  public Ehcache(CacheConfiguration<K, V> config, Store<K, V> store, final CacheLoader<? super K, ? extends V> cacheLoader) {
    this(config, store, cacheLoader, null, null);
  }

  public Ehcache(CacheConfiguration<K, V> config, Store<K, V> store, final CacheLoader<? super K, ? extends V> cacheLoader, CacheWriter<? super K, ? super V> cacheWriter) {
    this(config, store, cacheLoader, cacheWriter, null);
  }

  public Ehcache(CacheConfiguration<K, V> config, Store<K, V> store, final CacheLoader<? super K, ? extends V> cacheLoader, CacheWriter<? super K, ? super V> cacheWriter,
      ScheduledExecutorService statisticsExecutor) {
    this(config, store, cacheLoader, cacheWriter, new CacheEventNotificationService<K, V>(), statisticsExecutor);
  }

  public Ehcache(CacheConfiguration<K, V> config, Store<K, V> store, 
      final CacheLoader<? super K, ? extends V> cacheLoader, 
      CacheWriter<? super K, ? super V> cacheWriter,
      CacheEventNotificationService<K, V> eventNotifier,
      ScheduledExecutorService statisticsExecutor) {
    this.store = store;
    this.cacheLoader = cacheLoader;
    this.cacheWriter = cacheWriter;
    if (statisticsExecutor != null) {
      this.cacheStatistics = new StatisticsGateway(this, statisticsExecutor, bulkMethodEntries);
    } else {
      this.cacheStatistics = new DisabledStatistics();
    }
    if (store instanceof RecoveryCache) {
      this.resilienceStrategy = new LoggingRobustResilienceStrategy<K, V>((RecoveryCache) store);
    } else {
      this.resilienceStrategy = new LoggingRobustResilienceStrategy<K, V>(recoveryCache(store));
    }
    
    this.eventNotificationService = eventNotifier;
    this.runtimeConfiguration = new RuntimeConfiguration(config);
    this.storeListener = new StoreListener();
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
      try {
        if (cacheLoader == null) {
          return resilienceStrategy.getFailure(key, e);
        } else {
          V fromLoader;
          try {
            fromLoader = mappingFunction.apply(key);
          } catch (CacheLoaderException f) {
            return resilienceStrategy.getFailure(key, e, f);
          }
          return resilienceStrategy.getFailure(key, fromLoader, e);
        }
      } finally {
        getObserver.end(GetOutcome.FAILURE);
      }
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
        Cache.Entry<K, V> entry = newCacheEntry(key, value);
        eventNotificationService.onEvent(previousValue == null ? 
            CacheEvents.creation(entry, Ehcache.this) : CacheEvents.update(newCacheEntry(key, previousValue), 
                entry, Ehcache.this));
        return value;
      }
    });

    try {
      store.compute(key, remappingFunction);
      putObserver.end(PutOutcome.ADDED);
    } catch (CacheAccessException e) {
      try {
        if (cacheWriter == null) {
          resilienceStrategy.putFailure(key, value, e);
        } else {
          try {
            remappingFunction.apply(key, value);
          } catch (CacheWriterException f) {
            resilienceStrategy.putFailure(key, value, e, f);
            return;
          }
          resilienceStrategy.putFailure(key, value, e);
        }
      } finally {
        putObserver.end(PutOutcome.FAILURE);
      }
    }
  }

  @Override
  public boolean containsKey(final K key) {
    statusTransitioner.checkAvailable();
    checkNonNull(key);
    try {
      return store.containsKey(key);
    } catch (CacheAccessException e) {
      return resilienceStrategy.containsKeyFailure(key, e);
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
        eventNotificationService.onEvent(CacheEvents.removal(newCacheEntry(key, previousValue), Ehcache.this));
        return null;
      }
    });

    try {
      store.compute(key, remappingFunction);
      removeObserver.end(RemoveOutcome.SUCCESS);
    } catch (CacheAccessException e) {
      try {
        try {
          remappingFunction.apply(key, null);
        } catch (CacheWriterException f) {
          resilienceStrategy.removeFailure(key, e, f);
        }
        resilienceStrategy.removeFailure(key, e);
      } finally {
        removeObserver.end(RemoveOutcome.FAILURE);
      }
    }
  }

  @Override
  public void clear() {
    statusTransitioner.checkAvailable();
    try {
      store.clear();
    } catch (CacheAccessException e) {
      resilienceStrategy.clearFailure(e);
    }
  }

  @Override
  public Iterator<Entry<K, V>> iterator() {
    statusTransitioner.checkAvailable();
    return new CacheEntryIterator();
  }

  @Override
  public Map<K, V> getAll(Iterable<? extends K> keys) throws BulkCacheLoaderException {
    statusTransitioner.checkAvailable();
    checkNonNull(keys);
    final Map<K, V> successes = new HashMap<K, V>();
    final Map<K, Exception> failures = new HashMap<K, Exception>();
    Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction = new Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
      @Override
      public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends K> keys) {
        if (cacheLoader != null) {
          Map<K, V> loaded;
          try {
            loaded = (Map<K, V>)cacheLoader.loadAll(keys);
            successes.putAll(loaded);
          } catch (Exception e) {
            for (K key: keys) {
              failures.put(key, e);
            }
            return nullValuesForKeys(keys);
          }
          return loaded.entrySet();
        }
        return nullValuesForKeys(keys);
      }
    };

    Map<K, V> result = new HashMap<K, V>();
    try {
      Map<K, Store.ValueHolder<V>> computedMap = store.bulkComputeIfAbsent(keys, mappingFunction);
     
      if (computedMap == null) {
        return Collections.emptyMap();
      }

      int hits = 0;
      for (Map.Entry<K, Store.ValueHolder<V>> entry : computedMap.entrySet()) {
        if (entry.getValue() != null) {
          result.put(entry.getKey(), entry.getValue().value());
          hits++;
        }
      }
      addBulkMethodEntriesCount("getAll", hits);
    } catch (CacheAccessException e) {
      if (cacheLoader == null) {
        return resilienceStrategy.getAllFailure(keys, e);
      } else {
        Set<K> toLoad = new HashSet<K>();
        for (K key: keys) {
          toLoad.add(key);
        }
        toLoad.removeAll(successes.keySet());
        toLoad.removeAll(failures.keySet());
        mappingFunction.apply(toLoad);
        if (failures.isEmpty()) {
          return resilienceStrategy.getAllFailure(keys, successes, e);
        } else {
          return resilienceStrategy.getAllFailure(keys, e, new BulkCacheLoaderException(failures, successes));
        }
      }
    }
    if (failures.isEmpty()) {
      return result;
    } else {
      throw new BulkCacheLoaderException(failures, successes);
    }
  }

  LinkedHashSet<Map.Entry<? extends K, ? extends V>> nullValuesForKeys(final Iterable<? extends K> keys) {
    final LinkedHashSet<Map.Entry<? extends K, ? extends V>> entries = new LinkedHashSet<Map.Entry<? extends K, ? extends V>>();
    for (K key : keys) {
      entries.add(new AbstractMap.SimpleEntry<K, V>(key, null));
    }
    return entries;
  }

  @Override
  public void putAll(final Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) throws CacheWriterException {
    statusTransitioner.checkAvailable();
    checkNonNull(entries);
    final Set<K> successes = new HashSet<K>();
    final Map<K, Exception> failures = new HashMap<K, Exception>();

    // Copy all entries to write into a Map
    final ConcurrentMap<K, V> entriesToRemap = new ConcurrentHashMap<K, V>();
    for (Map.Entry<? extends K, ? extends V> entry: entries) {
      // If a value to map is null, throw NPE, nothing gets mutated
      if(entry.getValue() == null) {
        throw new NullPointerException();
      }
      entriesToRemap.put(entry.getKey(), entry.getValue());
    }
    
    long entriesCount = entriesToRemap.size();

    // The remapping function that will return the keys to their NEW values, taking the keys to their old values as input;
    // but this could happen in batches, i.e. not the same unique Set as passed to this method
    Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction =
      new Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
      @Override
      public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) {

        // If we have a writer, first write this batch
        cacheWriterPutAllCall(entries, entriesToRemap, successes, failures);

        Map<K, V> mutations = new HashMap<K, V>();

        // then record we handled these mappings
        for (Map.Entry<? extends K, ? extends V> entry: entries) {
          K key = entry.getKey();
          V newValue = entriesToRemap.remove(key);
          mutations.put(entry.getKey(), newValue);
          
          Cache.Entry<K, V> newEntry = newCacheEntry(key, newValue);
          eventNotificationService.onEvent(entry.getValue() == null ? 
              CacheEvents.creation(newEntry, Ehcache.this) : CacheEvents.update(newEntry, 
                  newCacheEntry(key, (V)entry.getValue()), Ehcache.this));
        }

        // Finally return the values to be installed in the Cache's Store
        return mutations.entrySet();
      }
    };

    Iterable<K> keys = keysOf(entries);
    try {
      store.bulkCompute(keys, remappingFunction);
      addBulkMethodEntriesCount("putAll", entriesCount);
      if (!failures.isEmpty()) {
        throw new BulkCacheWriterException(failures, successes);
      }
    } catch (CacheAccessException e) {
      if (cacheWriter == null) {
        resilienceStrategy.putAllFailure(entries, e);
      } else {
        // just in case not all writes happened:
        if (!entriesToRemap.isEmpty()) {
          cacheWriterPutAllCall(entriesToRemap.entrySet(), entriesToRemap, successes, failures);
        }
        if (failures.isEmpty()) {
          resilienceStrategy.putAllFailure(entries, e);
        } else {
          resilienceStrategy.putAllFailure(entries, e, new BulkCacheWriterException(failures, successes));
        }
      }
    }
  }

  private void cacheWriterPutAllCall(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries, ConcurrentMap<K, V> entriesToRemap, Set<K> successes, Map<K, Exception> failures) {
    if (cacheWriter != null) {
      // Can't use an iterable again, so flatting down to a Map
      // todo might want to fix the typing here
      Map<K, V> toWrite = new HashMap<K, V>();
      for (Map.Entry<? extends K, ? extends V> entry: entries) {
        toWrite.put(entry.getKey(), entriesToRemap.get(entry.getKey()));
      }
      try {
        if (!toWrite.isEmpty()) {
          // write all entries of this batch
          cacheWriter.writeAll(toWrite.entrySet());
          successes.addAll(toWrite.keySet());
        }
      } catch (Exception e) {
        for (K key: toWrite.keySet()) {
          failures.put(key, e);
        }
      }
    }
  }

  @Override
  public void removeAll(final Iterable<? extends K> keys) throws CacheWriterException {
    statusTransitioner.checkAvailable();
    checkNonNull(keys);
    final Set<K> successes = new HashSet<K>();
    final Map<K, Exception> failures = new HashMap<K, Exception>();

    final Map<K, ? extends V> entriesToRemove = new HashMap<K, V>();
    for (K key: keys) {
      entriesToRemove.put(key, null);
    }
    long entriesCount = entriesToRemove.size();
    
    Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> removalFunction =
      new Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
        @Override
        public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) {
          cacheWriterDeleteAllCall(entries, successes, failures);
          Iterable<Map.Entry<K, V>> ret = new NullValuesIterable(entries);
          for (Map.Entry<K, V> entry: ret) {
            entriesToRemove.remove(entry.getKey());
            eventNotificationService.onEvent(CacheEvents.removal(newCacheEntry(entry.getKey(), 
                entry.getValue()), Ehcache.this));
          }
          return ret;
        }
      };

    try {
      store.bulkCompute(keys, removalFunction);
      addBulkMethodEntriesCount("removeAll", entriesCount);
      if (!failures.isEmpty()) {
        throw new BulkCacheWriterException(failures, successes);
      }
    } catch (CacheAccessException e) {
      if (cacheWriter == null) {
        resilienceStrategy.removeAllFailure(keys, e);
      } else {
        // just in case not all writes happened:
        if (!entriesToRemove.isEmpty()) {
          cacheWriterDeleteAllCall(entriesToRemove.entrySet(), successes, failures);
        }
        if (failures.isEmpty()) {
          resilienceStrategy.removeAllFailure(keys, e);
        } else {
          resilienceStrategy.removeAllFailure(keys, e, new BulkCacheWriterException(failures, successes));
        }
      }
    }
  }

  private void cacheWriterDeleteAllCall(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries, Set<K> successes, Map<K, Exception> failures) {
    if (cacheWriter != null) {
      Set<K> toDelete = new HashSet<K>();
      for (Map.Entry<? extends K, ? extends V> entry: entries) {
        toDelete.add(entry.getKey());
      }
      try {
        // XXX should the return value here be wrapped in NullValuesIterable and returned? This is a potential subset of "keys"
        cacheWriter.deleteAll(toDelete);
        successes.addAll(toDelete);
      } catch (Exception e) {
        for (Map.Entry<? extends K, ? extends V> entry: entries) {
          failures.put(entry.getKey(), e);
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
        try {
          if (cacheWriter != null) {
            try {
              cacheWriter.write(k, value);
            } catch (Exception e) {
              throw newCacheWriterException(e);
            }
          }
          
          installed.set(true);
          return value;
        } finally {
          if (installed.get()) {
            eventNotificationService.onEvent(CacheEvents.creation(newCacheEntry(k, value), Ehcache.this));
          }
        }
      }
    });

    try {
      ValueHolder<V> inCache = store.computeIfAbsent(key, mappingFunction);
      if (installed.get()) {
        putIfAbsentObserver.end(PutIfAbsentOutcome.PUT);
        return null;
      } else if (inCache == null) {
        //dubious - it's a 'hit' in the SoR, but a 'miss' in the cache
        putIfAbsentObserver.end(PutIfAbsentOutcome.HIT);
        return null;
      } else {
        putIfAbsentObserver.end(PutIfAbsentOutcome.HIT);
        return inCache.value();
      }
    } catch (CacheAccessException e) {
      try {
        if (cacheWriter == null) {
          return resilienceStrategy.putIfAbsentFailure(key, value, e, installed.get());
        } else {
          try {
            mappingFunction.apply(key);
          } catch (CacheWriterException f) {
            return resilienceStrategy.putIfAbsentFailure(key, value, e, f);
          }
          return resilienceStrategy.putIfAbsentFailure(key, value, e, installed.get());
        }
      } finally {
        putIfAbsentObserver.end(PutIfAbsentOutcome.FAILURE);
      }
    }
  }

  @Override
  public boolean remove(final K key, final V value) throws CacheWriterException {
    removeObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(key, value);
    final AtomicBoolean removed = new AtomicBoolean();
    final BiFunction<K, V, V> remappingFunction = memoize(new BiFunction<K, V, V>() {
      @Override
      public V apply(final K k, final V inCache) {
        try {
            if (value.equals(inCache)) {
              if (cacheWriter != null) {
                try {
                  cacheWriter.delete(k);
                } catch (Exception e) {
                  throw newCacheWriterException(e);
                }
              }
              removed.set(true);
              return null;
            }
            return inCache;
        } finally {
          if (removed.get()) {
            // Okay - the flag is always false when exceptions are thrown
            eventNotificationService.onEvent(CacheEvents.removal(newCacheEntry(k, value), Ehcache.this));
          }
        }
      }
    });
    try {
      store.computeIfPresent(key, remappingFunction);
      if (removed.get()) {
        removeObserver.end(RemoveOutcome.SUCCESS);
      } else {
        removeObserver.end(RemoveOutcome.FAILURE);
      }
    } catch (CacheAccessException e) {
      try {
        if (cacheWriter == null) {
          return resilienceStrategy.removeFailure(key, value, e, removed.get());
        } else {
          try {
            remappingFunction.apply(key, null);
          } catch (CacheWriterException f) {
            return resilienceStrategy.removeFailure(key, value, e, f);
          }
          return resilienceStrategy.removeFailure(key, value, e, removed.get());
        }
      } finally {
        removeObserver.end(RemoveOutcome.FAILURE);
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
        eventNotificationService.onEvent(CacheEvents.update(newCacheEntry(k, inCache), 
            newCacheEntry(k, value), Ehcache.this));
        return value;
      }
    });

    try {
      store.computeIfPresent(key, remappingFunction);
      if (old.get() != null) {
        replaceObserver.end(ReplaceOutcome.HIT);
      } else {
        replaceObserver.end(ReplaceOutcome.MISS);
      }
      return old.get();
    } catch (CacheAccessException e) {
      try {
        try {
          remappingFunction.apply(key, null);
        } catch (CacheWriterException f) {
          return resilienceStrategy.replaceFailure(key, value, e, f);
        }
        return resilienceStrategy.replaceFailure(key, value, e);
      } finally {
        replaceObserver.end(ReplaceOutcome.FAILURE);
      }
    }
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
        try {
          if (oldValue.equals(inCache)) {
            if (cacheWriter != null) {
              try {
                cacheWriter.write(key, newValue);
              } catch (Exception e) {
                throw newCacheWriterException(e);
              }
            }
            success.set(true);
            return newValue;
          }
          return inCache;
        } finally {
          if (success.get()) {
            eventNotificationService.onEvent(CacheEvents.update(newCacheEntry(key, oldValue), 
                newCacheEntry(key, newValue), Ehcache.this));
          }
        }
      }
    });
    try {
      store.computeIfPresent(key, remappingFunction);
      if (success.get()) {
        replaceObserver.end(ReplaceOutcome.HIT);
      } else {
        replaceObserver.end(ReplaceOutcome.MISS);
      }
      return success.get();
    } catch (CacheAccessException e) {
      try {
        if (cacheWriter == null) {
          return resilienceStrategy.replaceFailure(key, oldValue, newValue, e, success.get());
        } else {
          try {
            remappingFunction.apply(key, null);
          } catch (CacheWriterException f) {
            return resilienceStrategy.replaceFailure(key, oldValue, newValue, e, f);
          }
          return resilienceStrategy.replaceFailure(key, oldValue, newValue, e, success.get());
        }
      } finally {
        replaceObserver.end(ReplaceOutcome.FAILURE);
      }        
    }
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

  private static <K, V> Cache.Entry<K, V> newCacheEntry(final K key, final V value) {
    return new Cache.Entry<K, V>() {
      @Override
      public K getKey() {
        return key;
      }

      @Override
      public V getValue() {
        return value;
      }

      @Override
      public long getCreationTime(TimeUnit unit) {
        throw new UnsupportedOperationException(); // XXX
      }

      @Override
      public long getLastAccessTime(TimeUnit unit) {
        throw new UnsupportedOperationException(); // XXX
      }

      @Override
      public float getHitRate(TimeUnit unit) {
        throw new UnsupportedOperationException(); // XXX
      }

    };
  }
  CacheLoader<? super K, ? extends V> getCacheLoader() {
    return cacheLoader;
  }
  
  @Override
  public CacheStatistics getStatistics() {
    return cacheStatistics;
  }

  CacheWriter<? super K, ? super V> getCacheWriter() {
    return cacheWriter;
  }

  private class RuntimeConfiguration implements CacheRuntimeConfiguration<K, V> {
    
    private final Collection<ServiceConfiguration<?>> serviceConfigurations;
    private final Class<K> keyType;
    private final Class<V> valueType;
    private final Comparable<Long> capacityConstraint;
    private final EvictionVeto<? super K, ? super V> evictionVeto;
    private final EvictionPrioritizer<? super K, ? super V> evictionPrioritizer;
    private final SerializationProvider serializationProvider;
    private final ClassLoader classLoader;
    private final Expiry<? super K, ? super V> expiry;

    RuntimeConfiguration(CacheConfiguration<K, V> config) {
      this.serviceConfigurations = copy(config.getServiceConfigurations());
      this.keyType = config.getKeyType();
      this.valueType = config.getValueType();
      this.capacityConstraint = config.getCapacityConstraint();
      this.evictionVeto = config.getEvictionVeto();
      this.evictionPrioritizer = config.getEvictionPrioritizer();
      this.serializationProvider = config.getSerializationProvider();
      this.classLoader = config.getClassLoader();
      this.expiry = config.getExpiry();
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
    public EvictionVeto<? super K, ? super V> getEvictionVeto() {
      return this.evictionVeto;
    }

    @Override
    public EvictionPrioritizer<? super K, ? super V> getEvictionPrioritizer() {
      return this.evictionPrioritizer;
    }

    @Override
    public SerializationProvider getSerializationProvider() {
      return this.serializationProvider;
    }

    public ClassLoader getClassLoader() {
      return this.classLoader;
    }
    
    @Override
    public Expiry<? super K, ? super V> getExpiry() {
      return expiry;
    }
    
    public synchronized void deregisterCacheEventListener(CacheEventListener<? super K, ? super V> listener) {
      eventNotificationService.deregisterCacheEventListener(listener);
      if (!eventNotificationService.hasListeners()) {
        store.disableStoreEventNotifications();
      }
    }
    
    @Override
    public synchronized void registerCacheEventListener(CacheEventListener<? super K, ? super V> listener, EventOrdering ordering,
        EventFiring firing, Set<EventType> forEventTypes) {
      boolean doRegister = forEventTypes.contains(EventType.EVICTED) || forEventTypes.contains(EventType.EXPIRED);
      eventNotificationService.registerCacheEventListener(listener, ordering, firing, EnumSet.copyOf(forEventTypes));
      if (doRegister) {
        store.enableStoreEventNotifications(storeListener);
      }
    }
    
    
    @Override
    public void setCapacityConstraint(Comparable<Long> constraint) {
      throw new UnsupportedOperationException("implement me!"); // XXX:
    }

    @Override
    public void releaseAllEventListeners(CacheEventListenerFactory factory) {
      eventNotificationService.releaseAllListeners(factory);
    }
    
    private <T> Collection<T> copy(Collection<T> collection) {
      if (collection == null) {
        return null;
      }
      
      return Collections.unmodifiableCollection(new ArrayList<T>(collection));
    }
  }
  
  private class CacheEntryIterator implements Iterator<Entry<K, V>> {

    private final Store.Iterator<Entry<K, Store.ValueHolder<V>>> iterator;
    private Entry<K, Store.ValueHolder<V>> current;
    private Entry<K, Store.ValueHolder<V>> next;

    public CacheEntryIterator() {
      Store.Iterator<Entry<K, Store.ValueHolder<V>>> storeIterator;
      try {
        storeIterator = store.iterator();
      } catch (CacheAccessException e) {
        resilienceStrategy.iteratorFailure(e);
        storeIterator = new Store.Iterator<Entry<K, Store.ValueHolder<V>>>() {

          @Override
          public boolean hasNext() throws CacheAccessException {
            return false;
          }

          @Override
          public Entry<K, ValueHolder<V>> next() throws CacheAccessException {
            throw new NoSuchElementException();
          }
        };
      }
      this.iterator = storeIterator;
      advance();
    }

    private void advance() {
      Entry<K, ValueHolder<V>> nextNext;
      try {
        if (iterator.hasNext()) {
          nextNext = iterator.next();
        } else {
          nextNext = null;
        }
      } catch (CacheAccessException e) {
        resilienceStrategy.iteratorFailure(e);
        nextNext = null;
      }
      current = next;
      next = nextNext;
    }
    
    @Override
    public boolean hasNext() {
      statusTransitioner.checkAvailable();
      return next != null;
    }

    @Override
    public Entry<K, V> next() {
      getObserver.begin();
      statusTransitioner.checkAvailable();
      if (next == null) {
        throw new NoSuchElementException();
      } else {
        advance();
      }
      getObserver.end(GetOutcome.HIT_NO_LOADER);
      return new Entry<K, V>() {
        @Override
        public K getKey() {
          return current.getKey();
        }

        @Override
        public V getValue() {
          return current.getValue().value();
        }

        @Override
        public long getCreationTime(TimeUnit unit) {
          return current.getCreationTime(unit);
        }

        @Override
        public long getLastAccessTime(TimeUnit unit) {
          return current.getLastAccessTime(unit);
        }

        @Override
        public float getHitRate(TimeUnit unit) {
          return current.getHitRate(unit);
        }
      };
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
          return null;
        }
      };
    }

    @Override
    public void remove() {
      iterator.remove();
    }
  }

  private static <K> RecoveryCache<K> recoveryCache(final Store<K, ?> store) {
    return new RecoveryCache<K>() {

      @Override
      public void obliterate() throws CacheAccessException {
        store.clear();
      }

      @Override
      public void obliterate(K key) throws CacheAccessException {
        store.remove(key);
      }

      @Override
      public void obliterate(Iterable<? extends K> keys) throws CacheAccessException {
        for (K key : keys) {
          obliterate(key);
        }
      }
    };
  }
  
  private final class StoreListener implements StoreEventListener<K, V> {

    @Override
    public void onEviction(Cache.Entry<K, V> entry) {
      eventNotificationService.onEvent(CacheEvents.eviction(entry, Ehcache.this));
    }

    @Override
    public void onExpiration(Cache.Entry<K, V> entry) {
      eventNotificationService.onEvent(CacheEvents.expiry(entry, Ehcache.this));
    }
  }
  
}
