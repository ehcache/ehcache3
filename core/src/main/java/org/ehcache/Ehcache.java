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
import org.ehcache.event.CacheEvent;
import org.ehcache.events.CacheEventNotificationService;
import org.ehcache.events.CacheEvents;
import org.ehcache.events.DisabledCacheEventNotificationService;
import org.ehcache.events.EventThreadLocal;
import org.ehcache.exceptions.BulkCacheLoadingException;
import org.ehcache.exceptions.BulkCacheWritingException;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.exceptions.CacheLoadingException;
import org.ehcache.exceptions.CacheWritingException;
import org.ehcache.expiry.Duration;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.function.NullaryFunction;
import org.ehcache.resilience.LoggingRobustResilienceStrategy;
import org.ehcache.resilience.RecoveryCache;
import org.ehcache.resilience.ResilienceStrategy;
import org.ehcache.spi.LifeCycled;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.cache.Store.ValueHolder;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.statistics.BulkOps;
import org.ehcache.statistics.CacheOperationOutcomes.CacheLoadingOutcome;
import org.ehcache.statistics.CacheOperationOutcomes.ConditionalRemoveOutcome;
import org.ehcache.statistics.CacheOperationOutcomes.GetAllOutcome;
import org.ehcache.statistics.CacheOperationOutcomes.GetOutcome;
import org.ehcache.statistics.CacheOperationOutcomes.PutIfAbsentOutcome;
import org.ehcache.statistics.CacheOperationOutcomes.PutAllOutcome;
import org.ehcache.statistics.CacheOperationOutcomes.PutOutcome;
import org.ehcache.statistics.CacheOperationOutcomes.RemoveAllOutcome;
import org.ehcache.statistics.CacheOperationOutcomes.RemoveOutcome;
import org.ehcache.statistics.CacheOperationOutcomes.ReplaceOutcome;
import org.slf4j.Logger;
import org.terracotta.statistics.StatisticsManager;
import org.terracotta.statistics.jsr166e.LongAdder;
import org.terracotta.statistics.observer.OperationObserver;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.ehcache.Functions.memoize;
import static org.ehcache.exceptions.ExceptionFactory.newCacheLoadingException;
import static org.ehcache.exceptions.ExceptionFactory.newCacheWritingException;
import static org.terracotta.statistics.StatisticBuilder.operation;

/**
 * @author Alex Snaps
 */
public class Ehcache<K, V> implements Cache<K, V>, UserManagedCache<K, V> {

  private final StatusTransitioner statusTransitioner;

  private final Store<K, V> store;
  private final CacheLoaderWriter<? super K, V> cacheLoaderWriter;
  private final ResilienceStrategy<K, V> resilienceStrategy;
  private final EhcacheRuntimeConfiguration<K, V> runtimeConfiguration;
  private final CacheEventNotificationService<K, V> eventNotificationService;
  private final Jsr107CacheImpl jsr107Cache;
  private final boolean useLoaderInAtomics;
  protected final Logger logger;
  
  private final OperationObserver<GetOutcome> getObserver = operation(GetOutcome.class).named("get").of(this).tag("cache").build();
  private final OperationObserver<GetAllOutcome> getAllObserver = operation(GetAllOutcome.class).named("getAll").of(this).tag("cache").build();
  private final OperationObserver<PutOutcome> putObserver = operation(PutOutcome.class).named("put").of(this).tag("cache").build();
  private final OperationObserver<PutAllOutcome> putAllObserver = operation(PutAllOutcome.class).named("putAll").of(this).tag("cache").build();
  private final OperationObserver<RemoveOutcome> removeObserver = operation(RemoveOutcome.class).named("remove").of(this).tag("cache").build();
  private final OperationObserver<RemoveAllOutcome> removeAllObserver = operation(RemoveAllOutcome.class).named("removeAll").of(this).tag("cache").build();
  private final OperationObserver<ConditionalRemoveOutcome> conditionalRemoveObserver = operation(ConditionalRemoveOutcome.class).named("conditionalRemove").of(this).tag("cache").build();
  private final OperationObserver<CacheLoadingOutcome> cacheLoadingObserver = operation(CacheLoadingOutcome.class).named("cacheLoading").of(this).tag("cache").build();
  private final OperationObserver<PutIfAbsentOutcome> putIfAbsentObserver = operation(PutIfAbsentOutcome.class).named("putIfAbsent").of(this).tag("cache").build();
  private final OperationObserver<ReplaceOutcome> replaceObserver = operation(ReplaceOutcome.class).named("replace").of(this).tag("cache").build();  
  private final Map<BulkOps, LongAdder> bulkMethodEntries = new EnumMap<BulkOps, LongAdder>(BulkOps.class);

  private static final NullaryFunction<Boolean> REPLACE_FALSE = new NullaryFunction<Boolean>() {
    @Override
    public Boolean apply() {
      return Boolean.FALSE;
    }
  };

  public Ehcache(CacheConfiguration<K, V> configuration, final Store<K, V> store, Logger logger) {
    this(configuration, store, null,logger);
  }

  public Ehcache(CacheConfiguration<K, V> configuration, Store<K, V> store, final CacheLoaderWriter<? super K, V> cacheLoaderWriter, Logger logger) {
    this(configuration, store, cacheLoaderWriter, null,logger);
  }

  public Ehcache(CacheConfiguration<K, V> configuration, Store<K, V> store,
      final CacheLoaderWriter<? super K, V> cacheLoaderWriter, 
      CacheEventNotificationService<K, V> eventNotifier,
      Logger logger) {
    this(configuration, store, cacheLoaderWriter, eventNotifier, true, logger);
  }

  Ehcache(CacheConfiguration<K, V> runtimeConfiguration, Store<K, V> store,
          CacheLoaderWriter<? super K, V> cacheLoaderWriter,
          CacheEventNotificationService<K, V> eventNotifier, boolean useLoaderInAtomics, Logger logger) {
    this(new EhcacheRuntimeConfiguration<K, V>(runtimeConfiguration, eventNotifier), store, cacheLoaderWriter, eventNotifier, useLoaderInAtomics, logger, new StatusTransitioner(logger));
  }

  Ehcache(EhcacheRuntimeConfiguration<K, V> runtimeConfiguration, Store<K, V> store,
            CacheLoaderWriter<? super K, V> cacheLoaderWriter,
            CacheEventNotificationService<K, V> eventNotifier, boolean useLoaderInAtomics, Logger logger, StatusTransitioner statusTransitioner) {
    this.store = store;
    runtimeConfiguration.addCacheConfigurationListener(store.getConfigurationChangeListeners());
    StatisticsManager.associate(store).withParent(this);
    this.cacheLoaderWriter = cacheLoaderWriter;
    if (store instanceof RecoveryCache) {
      this.resilienceStrategy = new LoggingRobustResilienceStrategy<K, V>(castToRecoveryCache(store));
    } else {
      this.resilienceStrategy = new LoggingRobustResilienceStrategy<K, V>(recoveryCache(store));
    }

    if (eventNotifier != null) {
      this.eventNotificationService = eventNotifier;
    } else {
      this.eventNotificationService = new DisabledCacheEventNotificationService<K, V>();
    }
    this.runtimeConfiguration = runtimeConfiguration;
    this.jsr107Cache = new Jsr107CacheImpl();

    this.useLoaderInAtomics = useLoaderInAtomics;
    this.logger=logger;
    this.statusTransitioner = statusTransitioner;
    for (BulkOps bulkOp : BulkOps.values()) {
      bulkMethodEntries.put(bulkOp, new LongAdder());
    }
  }

  Map<BulkOps, LongAdder> getBulkMethodEntries() {
    return bulkMethodEntries;
  }

  @SuppressWarnings("unchecked")
  private RecoveryCache<K> castToRecoveryCache(Store<K, V> store) {
    return (RecoveryCache<K>) store;
  }

  @Override
  public V get(final K key) throws CacheLoadingException {
    getObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(key);
    final Function<K, V> mappingFunction = memoize(new Function<K, V>() {
          @Override
          public V apply(final K k) {
            V loaded = null;
            try {
              if (cacheLoaderWriter != null) {
                cacheLoadingObserver.begin();
                loaded = cacheLoaderWriter.load(k);
                cacheLoadingObserver.end(CacheLoadingOutcome.SUCCESS);
              }
            } catch (Exception e) {
              cacheLoadingObserver.end(CacheLoadingOutcome.FAILURE);
              throw newCacheLoadingException(e);
            }
            
            return loaded;
          }
        });

    try {
      final Store.ValueHolder<V> valueHolder = store.computeIfAbsent(key, mappingFunction);
      
      // Check for expiry first
      if (valueHolder == null) {
        getObserver.end(cacheLoaderWriter == null ? GetOutcome.MISS_NO_LOADER : GetOutcome.MISS_WITH_LOADER);
        return null;
      } else {
        getObserver.end(cacheLoaderWriter == null ? GetOutcome.HIT_NO_LOADER : GetOutcome.HIT_WITH_LOADER);
        return valueHolder.value();
      }
    } catch (CacheAccessException e) {
      try {
        if (cacheLoaderWriter == null) {
          return resilienceStrategy.getFailure(key, e);
        } else {
          V fromLoader;
          try {
            fromLoader = mappingFunction.apply(key);
          } catch (CacheLoadingException f) {
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
  public void put(final K key, final V value) throws CacheWritingException {
    putObserver.begin();    
    statusTransitioner.checkAvailable();
    checkNonNull(key, value);
    final BiFunction<K, V, V> remappingFunction = memoize(new BiFunction<K, V, V>() {
      @Override
      public V apply(final K key, final V previousValue) {
        try {
          if (cacheLoaderWriter != null) {
            cacheLoaderWriter.write(key, value);
          }
        } catch (Exception e) {
          throw newCacheWritingException(e);
        }
        
        if (newValueAlreadyExpired(key, previousValue, value)) {
          return null;
        }
        
        eventNotificationService.onEvent(previousValue == null ? 
            CacheEvents.creation(key, value, Ehcache.this) : CacheEvents.update(key, previousValue, value, Ehcache.this));
        return value;
      }
    });

    try {
      ValueHolder<V> computed = store.compute(key, remappingFunction);
      if (computed != null) {
        putObserver.end(PutOutcome.ADDED);
        markAllCacheEventsFireable();
      } else {
        putObserver.end(PutOutcome.NOOP);
      }
    } catch (CacheAccessException e) {
      try {
        if (cacheLoaderWriter == null) {
          resilienceStrategy.putFailure(key, value, e);
        } else {
          try {
            remappingFunction.apply(key, value);
          } catch (CacheWritingException f) {
            resilienceStrategy.putFailure(key, value, e, f);
            return;
          }
          resilienceStrategy.putFailure(key, value, e);
        }
      } finally {
        putObserver.end(PutOutcome.FAILURE);
        markAllCacheEventsFailed();
      }
    }
  }
  
  private boolean newValueAlreadyExpired(K key, V oldValue, V newValue) {
    if (newValue == null) {
      return false;
    }
    
    final Duration duration;
    if (oldValue == null) {
      duration = runtimeConfiguration.getExpiry().getExpiryForCreation(key, newValue);
    } else {
      duration = runtimeConfiguration.getExpiry().getExpiryForUpdate(key, oldValue, newValue);
    }
    
    return Duration.ZERO.equals(duration); 
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
  public void remove(K key) throws CacheWritingException {
    removeInternal(key); // ignore return value;
  }
  
  
  private boolean removeInternal(final K key) throws CacheWritingException {
    removeObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(key);
    
    final AtomicBoolean modified = new AtomicBoolean();
    
    final BiFunction<K, V, V> remappingFunction = memoize(new BiFunction<K, V, V>() {
      @Override
      public V apply(final K key, final V previousValue) {
        modified.set(previousValue != null);
        
        try {
          if (cacheLoaderWriter != null) {
            cacheLoaderWriter.delete(key);
          }
        } catch (Exception e) {
          throw newCacheWritingException(e);
        }
        eventNotificationService.onEvent(CacheEvents.removal(key, previousValue, Ehcache.this));
        return null;
      }
    });

    try {
      store.compute(key, remappingFunction);
      if (modified.get()) {
        removeObserver.end(RemoveOutcome.SUCCESS);
        markAllCacheEventsFireable();
      } else {
        removeObserver.end(RemoveOutcome.NOOP);
      }
    } catch (CacheAccessException e) {
      try {
        try {
          remappingFunction.apply(key, null);
        } catch (CacheWritingException f) {
          resilienceStrategy.removeFailure(key, e, f);
        }
        resilienceStrategy.removeFailure(key, e);
      } finally {
        removeObserver.end(RemoveOutcome.FAILURE);
        markAllCacheEventsFailed();
      }
    }
    
    return modified.get();
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
    return new CacheEntryIterator(false);
  }

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
    final Map<K, V> successes;
    final Map<K, Exception> failures;
    if (cacheLoaderWriter != null) {
      successes = new HashMap<K, V>();
      failures = new HashMap<K, Exception>();
    } else {
      successes = Collections.emptyMap();
      failures = Collections.emptyMap();
    }
    
    Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> computeFunction =
        new Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
      @Override
      public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends K> keys) {
        Map<K, V> computeResult = new LinkedHashMap<K ,V>();
        
        // put all the entries to get ordering correct
        for (K key : keys) {
          computeResult.put(key, null);
        }

        if (cacheLoaderWriter != null) {
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

        }

        return computeResult.entrySet();
      }
    };

    Map<K, V> result = new HashMap<K, V>();
    try {
      Map<K, Store.ValueHolder<V>> computedMap = store.bulkComputeIfAbsent(keys, computeFunction);

      int hits = 0;
      int keyCount = 0;
      for (Map.Entry<K, Store.ValueHolder<V>> entry : computedMap.entrySet()) {
        keyCount++;
        if (entry.getValue() != null) {
          result.put(entry.getKey(), entry.getValue().value());
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
    } catch (CacheAccessException e) {
      try {
        if (cacheLoaderWriter == null) {
          return resilienceStrategy.getAllFailure(keys, e);
        } else {
          Set<K> toLoad = new HashSet<K>();
          for (K key : keys) {
            toLoad.add(key);
          }
          toLoad.removeAll(successes.keySet());
          toLoad.removeAll(failures.keySet());
          computeFunction.apply(toLoad);
          if (failures.isEmpty()) {
            return resilienceStrategy.getAllFailure(keys, successes, e);
          } else {
            return resilienceStrategy.getAllFailure(keys, e, new BulkCacheLoadingException(failures, successes));
          }
        }
      } finally {
        getAllObserver.end(GetAllOutcome.FAILURE);
      }
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
  public void putAll(final Map<? extends K, ? extends V> entries) throws BulkCacheWritingException {
    putAllObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(entries);
    if(entries.isEmpty()) {
      putAllObserver.end(PutAllOutcome.SUCCESS);
      return;
    }
    final Set<K> successes;
    final Map<K, Exception> failures;
    if (cacheLoaderWriter != null) {
      successes = new HashSet<K>();
      failures = new HashMap<K, Exception>();
    } else {
      successes = Collections.emptySet();
      failures = Collections.emptyMap();
    }
    
    // Copy all entries to write into a Map
    final Map<K, V> entriesToRemap = new HashMap<K, V>();
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
      new Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
      @Override
      public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) {
        // If we have a writer, first write this batch
        cacheLoaderWriterWriteAllCall(entries, entriesToRemap, successes, failures);

        Map<K, V> mutations = new LinkedHashMap<K, V>();

        // then record we handled these mappings
        for (Map.Entry<? extends K, ? extends V> entry: entries) {
          K key = entry.getKey();
          V existingValue = entry.getValue();
          V newValue = entriesToRemap.remove(key);
         
          if (newValueAlreadyExpired(key, existingValue, newValue)) {
            mutations.put(key, null);
          } else if (cacheLoaderWriter == null || successes.contains(key)) {
            actualPutCount.incrementAndGet();
            mutations.put(key, newValue);
          
            eventNotificationService.onEvent(existingValue == null ? 
              CacheEvents.creation(key, newValue, Ehcache.this) : CacheEvents.update( 
                  key, existingValue, newValue, Ehcache.this));
          } else {
            mutations.put(key, existingValue);
          }
        }

        // Finally return the values to be installed in the Cache's Store
        return mutations.entrySet();
      }
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
      markAllCacheEventsFireable();
    } catch (CacheAccessException e) {
      try {
        if (cacheLoaderWriter == null) {
          resilienceStrategy.putAllFailure(entries, e);
        } else {
          // just in case not all writes happened:
          if (!entriesToRemap.isEmpty()) {
            cacheLoaderWriterWriteAllCall(entriesToRemap.entrySet(), entriesToRemap, successes, failures);
          }
          if (failures.isEmpty()) {
            resilienceStrategy.putAllFailure(entries, e);
          } else {
            resilienceStrategy.putAllFailure(entries, e, new BulkCacheWritingException(failures, successes));
          }
        }
      } finally {
        putAllObserver.end(PutAllOutcome.FAILURE);
      }
      markAllCacheEventsFailed();
    }
  }

  private void tryRemoveFailedKeys(Map<? extends K, ? extends V> entries, Map<K, Exception> failures, BulkCacheWritingException cacheWritingException) {
    try {
      store.bulkCompute(failures.keySet(), new Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
        @Override
        public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) {
          HashMap<K, V> result = new HashMap<K, V>();
          for (Map.Entry<? extends K, ? extends V> entry : entries) {
            result.put(entry.getKey(), null);
          }
          return result.entrySet();
        }
      });
    } catch (CacheAccessException e) {
      resilienceStrategy.putAllFailure(entries, e, cacheWritingException);
    }
  }

  private void cacheLoaderWriterWriteAllCall(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries, Map<K, V> entriesToRemap, Set<K> successes, Map<K, Exception> failures) {
    if (cacheLoaderWriter != null) {
      Map<K, V> toWrite = new HashMap<K, V>();
      for (Map.Entry<? extends K, ? extends V> entry: entries) {
        toWrite.put(entry.getKey(), entriesToRemap.get(entry.getKey()));
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
  

  @Override
  public void removeAll(final Set<? extends K> keys) throws BulkCacheWritingException {
    removeAllObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(keys);
    if(keys.isEmpty()) {
      removeAllObserver.end(RemoveAllOutcome.SUCCESS);
      return;
    }
    final Set<K> successes;
    final Map<K, Exception> failures;
    if (cacheLoaderWriter != null) {
      successes = new HashSet<K>();
      failures = new HashMap<K, Exception>();
    } else {
      successes = Collections.emptySet();
      failures = Collections.emptyMap();
    }

    final Map<K, ? extends V> entriesToRemove = new HashMap<K, V>();
    for (K key: keys) {
      if (key == null) {
        throw new NullPointerException();
      }
      entriesToRemove.put(key, null);
    }
    
    final AtomicInteger actualRemoveCount = new AtomicInteger();
    
    Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> removalFunction =
      new Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
        @Override
        public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) {
          Set<K> unknowns = cacheLoaderWriterDeleteAllCall(entries, successes, failures);
          
          Map<K, V> results = new LinkedHashMap<K, V>();
          
          for (Map.Entry<? extends K, ? extends V> entry : entries) {
            K key = entry.getKey();
            V existingValue = entry.getValue();
            
            if (cacheLoaderWriter == null || successes.contains(key)) {
              if (existingValue != null) {
                actualRemoveCount.incrementAndGet();
              }
              results.put(key, null);
              entriesToRemove.remove(key);
              eventNotificationService.onEvent(CacheEvents.removal(key, existingValue, Ehcache.this));
            } else {
              if (unknowns.contains(key)) {
                results.put(key, null);
              } else {
                results.put(key, existingValue);
              }
            }
          }
          
          return results.entrySet();
        }
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
      markAllCacheEventsFireable();
    } catch (CacheAccessException e) {
      try {
        if (cacheLoaderWriter == null) {
          resilienceStrategy.removeAllFailure(keys, e);
        } else {
          // just in case not all writes happened:
          if (!entriesToRemove.isEmpty()) {
            cacheLoaderWriterDeleteAllCall(entriesToRemove.entrySet(), successes, failures);
          }
          if (failures.isEmpty()) {
            resilienceStrategy.removeAllFailure(keys, e);
          } else {
            resilienceStrategy.removeAllFailure(keys, e, new BulkCacheWritingException(failures, successes));
          }
        }
      } finally {
        removeAllObserver.end(RemoveAllOutcome.FAILURE);
      }
      markAllCacheEventsFailed();
    }
  }

  private Set<K> cacheLoaderWriterDeleteAllCall(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries, Set<K> successes, Map<K, Exception> failures) {
    final Set<K> unknowns = new HashSet<K>();
    if (cacheLoaderWriter != null) {
      Set<K> toDelete = new HashSet<K>();
      for (Map.Entry<? extends K, ? extends V> entry: entries) {
        toDelete.add(entry.getKey());
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
    }
    return unknowns;
  }

  @Override
  public V putIfAbsent(final K key, final V value) throws CacheWritingException {
    putIfAbsentObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(key, value);
    final AtomicBoolean installed = new AtomicBoolean(false);
    
    final Function<K, V> mappingFunction = memoize(new Function<K, V>() {
      @Override
      public V apply(final K k) {
        try {
          if (useLoaderInAtomics && cacheLoaderWriter != null) {
            try {
              V loaded = cacheLoaderWriter.load(k);
              if (loaded != null) {
                return loaded; // populate the cache
              }
            } catch (Exception e) {
              throw newCacheLoadingException(e);
            }
          }

          if (cacheLoaderWriter != null) {
            try {
              cacheLoaderWriter.write(k, value);
            } catch (Exception e) {
              throw newCacheWritingException(e);
            }
          }

          if (newValueAlreadyExpired(key, null, value)) {
            return null;
          }
          
          installed.set(true);
          return value;
        } finally {
          if (installed.get()) {
            eventNotificationService.onEvent(CacheEvents.creation(k, value, Ehcache.this));
          }
        }
      }
    });

    try {
      ValueHolder<V> inCache = store.computeIfAbsent(key, mappingFunction);
      if (installed.get()) {
        putIfAbsentObserver.end(PutIfAbsentOutcome.PUT);
        markAllCacheEventsFireable();
        return null;
      } else if (inCache == null) {
        /*
         * XXX : This needs reassessing - this means different things whether
         * there is a loader/writer or not.
         */
        putIfAbsentObserver.end(PutIfAbsentOutcome.HIT);
        return null;
      } else {
        putIfAbsentObserver.end(PutIfAbsentOutcome.HIT);
        return inCache.value();
      }
    } catch (CacheAccessException e) {
      try {
        if (cacheLoaderWriter == null) {
          return resilienceStrategy.putIfAbsentFailure(key, value, e, installed.get());
        } else {
          try {
            mappingFunction.apply(key);
          } catch (CacheLoadingException f) {
            return resilienceStrategy.putIfAbsentFailure(key, value, e, f);
          } catch (CacheWritingException f) {
            return resilienceStrategy.putIfAbsentFailure(key, value, e, f);
          }
          return resilienceStrategy.putIfAbsentFailure(key, value, e, installed.get());
        }
      } finally {
        putIfAbsentObserver.end(PutIfAbsentOutcome.FAILURE);
        markAllCacheEventsFailed();
      }
    }
  }

  @Override
  public boolean remove(final K key, final V value) throws CacheWritingException {
    conditionalRemoveObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(key, value);
    final AtomicBoolean hit = new AtomicBoolean();
    final AtomicBoolean removed = new AtomicBoolean();
    final BiFunction<K, V, V> remappingFunction = memoize(new BiFunction<K, V, V>() {
      @Override
      public V apply(final K k, V inCache) {
        if (inCache == null) {
          if (useLoaderInAtomics && cacheLoaderWriter != null) {
            try {
              inCache = cacheLoaderWriter.load(key);
              if (inCache == null) {
                return null;
              }
            } catch (Exception e) {
              throw newCacheLoadingException(e);
            }
          } else {
            return null;
          }
        }

        hit.set(true);
        if (value.equals(inCache)) {
          if (cacheLoaderWriter != null) {
            try {
              cacheLoaderWriter.delete(k);
            } catch (Exception e) {
              throw newCacheWritingException(e);
            }
          }
          eventNotificationService.onEvent(CacheEvents.removal(k, value, Ehcache.this));
          removed.set(true);
          return null;
        }
        return inCache;
      }
    });
    try {
      store.compute(key, remappingFunction, REPLACE_FALSE);
      if (removed.get()) {
        conditionalRemoveObserver.end(ConditionalRemoveOutcome.SUCCESS);
        markAllCacheEventsFireable();
      } else {
        if (hit.get()) {
          conditionalRemoveObserver.end(ConditionalRemoveOutcome.FAILURE_KEY_PRESENT);
        } else {
          conditionalRemoveObserver.end(ConditionalRemoveOutcome.FAILURE_KEY_MISSING);
        }
      }
    } catch (CacheAccessException e) {
      try {
        if (cacheLoaderWriter == null) {
          return resilienceStrategy.removeFailure(key, value, e, removed.get());
        } else {
          try {
            remappingFunction.apply(key, null);
          } catch (CacheLoadingException f) {
            return resilienceStrategy.removeFailure(key, value, e, f);
          } catch (CacheWritingException f) {
            return resilienceStrategy.removeFailure(key, value, e, f);
          }
          return resilienceStrategy.removeFailure(key, value, e, removed.get());
        }
      } finally {
        conditionalRemoveObserver.end(ConditionalRemoveOutcome.FAILURE);
        markAllCacheEventsFailed();
      }
    }
    return removed.get();
  }

  @Override
  public V replace(final K key, final V value) throws CacheLoadingException, CacheWritingException {
    replaceObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(key, value);
    final AtomicReference<V> old = new AtomicReference<V>();
    final BiFunction<K, V, V> remappingFunction = memoize(new BiFunction<K, V, V>() {
      @Override
      public V apply(final K k, V inCache) {
        if (inCache == null) {
          if (useLoaderInAtomics && cacheLoaderWriter != null) {
            try {
              inCache = cacheLoaderWriter.load(key);
              if (inCache == null) {
                return null;
              }
            } catch (Exception e) {
              throw newCacheLoadingException(e);
            }
          } else {
            return null;
          }
        }

        if (cacheLoaderWriter != null) {
          try {
            cacheLoaderWriter.write(key, value);
          } catch (Exception e) {
            throw newCacheWritingException(e);
          }
        }
        
        old.set(inCache);

        if (newValueAlreadyExpired(key, inCache, value)) {
          return null;
        }
        
        eventNotificationService.onEvent(CacheEvents.update(k, inCache, value, Ehcache.this));
        return value;
      }
    });

    try {
      store.compute(key, remappingFunction);
      if (old.get() != null) {
        replaceObserver.end(ReplaceOutcome.HIT);
        markAllCacheEventsFireable();
      } else {
        replaceObserver.end(ReplaceOutcome.MISS_NOT_PRESENT);
      }
      return old.get();
    } catch (CacheAccessException e) {
      try {
        try {
          remappingFunction.apply(key, null);
        } catch (CacheLoadingException f) {
          return resilienceStrategy.replaceFailure(key, value, e, f);
        } catch (CacheWritingException f) {
          return resilienceStrategy.replaceFailure(key, value, e, f);
        }
        return resilienceStrategy.replaceFailure(key, value, e);
      } finally {
        replaceObserver.end(ReplaceOutcome.FAILURE);
        markAllCacheEventsFailed();
      }
    }
  }

  @Override
  public boolean replace(final K key, final V oldValue, final V newValue) throws CacheLoadingException, CacheWritingException {
    replaceObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(key, oldValue, newValue);

    final AtomicBoolean success = new AtomicBoolean();
    final AtomicBoolean hit = new AtomicBoolean();
    
    final BiFunction<K, V, V> remappingFunction = memoize(new BiFunction<K, V, V>() {
      @Override
      public V apply(final K k, V inCache) {
        if (inCache == null) {
          if (useLoaderInAtomics && cacheLoaderWriter != null) {
            try {
              inCache = cacheLoaderWriter.load(key);
              if (inCache == null) {
                return null;
              }
            } catch (Exception e) {
              throw newCacheLoadingException(e);
            }
          } else {
            return null;
          }
        }

        hit.set(true);
        if (oldValue.equals(inCache)) {
          if (cacheLoaderWriter != null) {
            try {
              cacheLoaderWriter.write(key, newValue);
            } catch (Exception e) {
              throw newCacheWritingException(e);
            }
          }

          success.set(true);

          if (newValueAlreadyExpired(key, oldValue, newValue)) {
            return null;
          }

          eventNotificationService.onEvent(CacheEvents.update(key, oldValue, newValue, Ehcache.this));
          return newValue;
        }
        return inCache;
      }
    });
    try {
      store.compute(key, remappingFunction, REPLACE_FALSE);
      if (success.get()) {
        replaceObserver.end(ReplaceOutcome.HIT);
        markAllCacheEventsFireable();
      } else {
        if (hit.get()) {
          replaceObserver.end(ReplaceOutcome.MISS_PRESENT);
        } else {
          replaceObserver.end(ReplaceOutcome.MISS_NOT_PRESENT);
        }
      }
      return success.get();
    } catch (CacheAccessException e) {
      try {
        if (cacheLoaderWriter == null) {
          return resilienceStrategy.replaceFailure(key, oldValue, newValue, e, success.get());
        } else {
          try {
            remappingFunction.apply(key, null);
          } catch (CacheLoadingException f) {
            return resilienceStrategy.replaceFailure(key, oldValue, newValue, e, f);
          } catch (CacheWritingException f) {
            return resilienceStrategy.replaceFailure(key, oldValue, newValue, e, f);
          }
          return resilienceStrategy.replaceFailure(key, oldValue, newValue, e, success.get());
        }
      } finally {
        replaceObserver.end(ReplaceOutcome.FAILURE);
        markAllCacheEventsFailed();
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
    statusTransitioner.init().succeeded();
  }

  @Override
  public void close() {
    eventNotificationService.stopEventService();
    statusTransitioner.close().succeeded();
  }

  @Override
  public Status getStatus() {
    return statusTransitioner.currentStatus();
  }

  void addHook(LifeCycled hook) {
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
  
  Jsr107Cache<K, V> getJsr107Cache() {
    return jsr107Cache;
  }
  
  CacheLoaderWriter<? super K, V> getCacheLoaderWriter() {
    return this.cacheLoaderWriter;
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
    public Map<K, V> getAll(Set<? extends K> keys) {
      return Ehcache.this.getAllInternal(keys, false);
    }

    private void loadAllAbsent(Set<? extends K> keys, final Function<Iterable<? extends K>, Map<K, V>> loadFunction) {
      try {
        store.bulkComputeIfAbsent(keys, new Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
          @Override
          public Iterable<? extends java.util.Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends K> absentKeys) {
            return cacheLoaderWriterLoadAllForKeys(absentKeys, loadFunction).entrySet();
          }
        });
      } catch (CacheAccessException e) {
        throw newCacheLoadingException(e);
      }
    }

    Map<K, V> cacheLoaderWriterLoadAllForKeys(Iterable<? extends K> keys, Function<Iterable<? extends K>, Map<K, V>> loadFunction) {
      try {
        Map<? super K, ? extends V> loaded = loadFunction.apply(keys);
        
        // put into a new map since we can't assume the 107 cache loader returns things ordered, or necessarily with all the desired keys
        Map<K, V> rv = new LinkedHashMap<K, V>();
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
        store.bulkCompute(keys, new Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
          @Override
          public Iterable<? extends java.util.Map.Entry<? extends K, ? extends V>> apply(
              Iterable<? extends java.util.Map.Entry<? extends K, ? extends V>> entries) {
            Collection<K> keys = new ArrayList<K>();
            for (Map.Entry<? extends K, ? extends V> entry : entries) {
              keys.add(entry.getKey());
            }
            return cacheLoaderWriterLoadAllForKeys(keys, loadFunction).entrySet();
          }            
        });
      } catch (CacheAccessException e) {
        throw newCacheLoadingException(e);
      }
    }

    @Override
    public V compute(K key, final BiFunction<? super K, ? super V, ? extends V> computeFunction,
        final NullaryFunction<Boolean> replaceEqual, final NullaryFunction<Boolean> invokeWriter, final NullaryFunction<Boolean> withStatsAndEvents) {
      putObserver.begin();
      removeObserver.begin();
      getObserver.begin();
      
      try {
        BiFunction<K, V, V> fn = new BiFunction<K, V, V>() {
          @Override
          public V apply(K mappedKey, V mappedValue) {
            if (mappedValue == null) {
              getObserver.end(GetOutcome.MISS_NO_LOADER);
            } else {
              getObserver.end(GetOutcome.HIT_NO_LOADER);
            }
            
            V newValue = computeFunction.apply(mappedKey, mappedValue);
            
            if (newValue == mappedValue) {
              if (! replaceEqual.apply()) {
                return mappedValue;
              }
            }

            if (invokeWriter.apply() && cacheLoaderWriter != null) {
              try {
                if (newValue != null) {
                  cacheLoaderWriter.write(mappedKey, newValue);
                } else {
                  cacheLoaderWriter.delete(mappedKey);
                }
              } catch (Exception e) {
                throw newCacheWritingException(e);
              }
            }
            
            if (newValueAlreadyExpired(mappedKey, mappedValue, newValue)) {
              return null;
            }
            
            if (withStatsAndEvents.apply()) {
              final CacheEvent<K, V> event;
              if (newValue == null) {
                removeObserver.end(RemoveOutcome.SUCCESS);
                event = CacheEvents.removal(mappedKey, mappedValue, Ehcache.this);
              } else {
                putObserver.end(PutOutcome.ADDED);
                if (mappedValue == null) {
                  event = CacheEvents.creation(mappedKey, newValue, Ehcache.this);
                } else {
                  event = CacheEvents.update(mappedKey, mappedValue, newValue, Ehcache.this);
                }
              }
              eventNotificationService.onEvent(event);
            }
            
            return newValue;
          }
        };

        ValueHolder<V> valueHolder = store.compute(key, fn, replaceEqual);
        markAllCacheEventsFireable();
        return valueHolder == null ? null : valueHolder.value();
      } catch (CacheAccessException e) {
        markAllCacheEventsFailed();
        // XXX:
        throw new RuntimeException(e);
      }
    }

    @Override
    public V getAndRemove(K key) {
      getObserver.begin();
      removeObserver.begin();
      
      final AtomicReference<V> existingValue = new AtomicReference<V>();
      try {
        store.compute(key, new BiFunction<K, V, V>() {
          @Override
          public V apply(K mappedKey, V mappedValue) {
            existingValue.set(mappedValue);
            
            if (cacheLoaderWriter != null) {
              try {
                cacheLoaderWriter.delete(mappedKey);
              } catch (Exception e) {
                throw newCacheWritingException(e);
              }
            }

            eventNotificationService.onEvent(CacheEvents.removal(mappedKey, mappedValue, Ehcache.this));
            return null;
          }
        });
      } catch (CacheAccessException e) {
        getObserver.end(GetOutcome.FAILURE);
        removeObserver.end(RemoveOutcome.FAILURE);
        markAllCacheEventsFireable();
        // XXX:
        throw new RuntimeException(e);
      }
      
      V returnValue = existingValue.get();
      if (returnValue != null) {
        getObserver.end(GetOutcome.HIT_NO_LOADER);
        removeObserver.end(RemoveOutcome.SUCCESS);
        markAllCacheEventsFailed();
      } else {
        getObserver.end(GetOutcome.MISS_NO_LOADER);          
      }
      return returnValue;
    }

    @Override
    public V getAndPut(K key, final V value) {
      getObserver.begin();
      putObserver.begin();
      
      final AtomicReference<V> existingValue = new AtomicReference<V>();
      try {
        store.compute(key, new BiFunction<K, V, V>() {
          @Override
          public V apply(K mappedKey, V mappedValue) {              
            existingValue.set(mappedValue);
            
            if (cacheLoaderWriter != null) {
              try {
                cacheLoaderWriter.write(mappedKey, value);
              } catch (Exception e) {
                throw newCacheWritingException(e);
              }
            }
            
            if (newValueAlreadyExpired(mappedKey, mappedValue, value)) {
              return null;
            }
            
            if (mappedValue != null) {
              eventNotificationService.onEvent(CacheEvents.update(mappedKey, mappedValue, value, Ehcache.this)); 
            } else {
              eventNotificationService.onEvent(CacheEvents.creation(mappedKey, value, Ehcache.this));
            }
            
            return value;
          }
        });
      } catch (CacheAccessException e) {
        getObserver.end(GetOutcome.FAILURE);
        putObserver.end(PutOutcome.FAILURE);
        markAllCacheEventsFireable();
        // XXX:
        throw new RuntimeException(e);
      }
      
      V returnValue = existingValue.get();        
      if (returnValue != null) {
        getObserver.end(GetOutcome.HIT_NO_LOADER);
      } else {
        getObserver.end(GetOutcome.MISS_NO_LOADER);          
      }
      putObserver.end(PutOutcome.ADDED);
      markAllCacheEventsFailed();
      return returnValue;
    }

    @Override
    public boolean remove(K key) {
      return removeInternal(key);
    }

    @Override
    public void removeAll() {
      for (Iterator<Cache.Entry<K, V>> iter = new CacheEntryIterator(true); iter.hasNext(); ) {
        K key = iter.next().getKey();
        remove(key);
      }
    }
  }
  
  private void markAllCacheEventsFireable() {
    while (!EventThreadLocal.get().isEmpty()) {
      EventThreadLocal.get().remove().markFireable();
    }
  }

  private void markAllCacheEventsFailed() {
    while (!EventThreadLocal.get().isEmpty()) {
      EventThreadLocal.get().remove().markFailed();
    }
  }
  
  private class CacheEntryIterator implements Iterator<Entry<K, V>> {

    private final Store.Iterator<Entry<K, Store.ValueHolder<V>>> iterator;
    private final boolean quiet;
    private boolean cacheAccessError = false;
    private Cache.Entry<K, ValueHolder<V>> current;

    public CacheEntryIterator(boolean quiet) {
      this.quiet = quiet;
      
      Store.Iterator<Entry<K, Store.ValueHolder<V>>> storeIterator = null;
      try {
        storeIterator = store.iterator();
      } catch (CacheAccessException e) {
        cacheAccessError(e);
      }
      this.iterator = storeIterator;
    }

    private void cacheAccessError(CacheAccessException e) {
      resilienceStrategy.iteratorFailure(e);
      cacheAccessError = true;
    }
    
    @Override
    public boolean hasNext() {
      statusTransitioner.checkAvailable();
      
      if (cacheAccessError) {
        return false;
      }
      
      try {
        return iterator.hasNext();
      } catch (CacheAccessException e) {
        cacheAccessError(e);
        return false;
      }
    }

    @Override
    public Entry<K, V> next() {
      statusTransitioner.checkAvailable();
      
      if (cacheAccessError) {
        throw new NoSuchElementException();
      }
      
      if (!quiet) getObserver.begin();
      try {
        current = iterator.next();
        if (!quiet) getObserver.end(GetOutcome.HIT_NO_LOADER);
      } catch (CacheAccessException e) {
        if (!quiet) getObserver.end(GetOutcome.FAILURE);
        cacheAccessError(e);
        
        // XXX: not what we want!
        throw new RuntimeException(e);
      }
      
      return new ValueHolderBasedEntry<K, V>(current);
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

    @Override
    public long getCreationTime(TimeUnit unit) {
      return storeEntry.getCreationTime(unit);
    }

    @Override
    public long getLastAccessTime(TimeUnit unit) {
      return storeEntry.getLastAccessTime(unit);
    }

    @Override
    public float getHitRate(TimeUnit unit) {
      return storeEntry.getHitRate(unit);
    }
  }
  
}
