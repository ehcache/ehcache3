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
import org.ehcache.config.service.EhcacheService;
import org.ehcache.config.service.EhcacheServiceConfiguration;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.CacheEventListenerFactory;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;
import org.ehcache.events.CacheEventNotificationService;
import org.ehcache.events.CacheEvents;
import org.ehcache.events.DisabledCacheEventNotificationService;
import org.ehcache.events.StateChangeListener;
import org.ehcache.events.StoreEventListener;
import org.ehcache.exceptions.BulkCacheLoaderException;
import org.ehcache.exceptions.BulkCacheWriterException;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.exceptions.CacheLoaderException;
import org.ehcache.exceptions.CacheWriterException;
import org.ehcache.exceptions.StateTransitionException;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expiry;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.function.NullaryFunction;
import org.ehcache.resilience.LoggingRobustResilienceStrategy;
import org.ehcache.resilience.RecoveryCache;
import org.ehcache.resilience.ResilienceStrategy;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.cache.Store.ValueHolder;
import org.ehcache.spi.loader.CacheLoader;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.writer.CacheWriter;
import org.ehcache.statistics.BulkOps;
import org.ehcache.statistics.CacheOperationOutcomes.CacheLoaderOutcome;
import org.ehcache.statistics.CacheOperationOutcomes.ConditionalRemoveOutcome;
import org.ehcache.statistics.CacheOperationOutcomes.GetOutcome;
import org.ehcache.statistics.CacheOperationOutcomes.PutIfAbsentOutcome;
import org.ehcache.statistics.CacheOperationOutcomes.PutOutcome;
import org.ehcache.statistics.CacheOperationOutcomes.RemoveOutcome;
import org.ehcache.statistics.CacheOperationOutcomes.ReplaceOutcome;
import org.ehcache.statistics.CacheStatistics;
import org.ehcache.statistics.DisabledStatistics;
import org.ehcache.statistics.StatisticsGateway;
import org.terracotta.statistics.StatisticsManager;
import org.terracotta.statistics.observer.OperationObserver;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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

  private final Store<K, V> store;
  private final CacheLoader<? super K, ? extends V> cacheLoader;
  private final CacheWriter<? super K, ? super V> cacheWriter;
  private final ResilienceStrategy<K, V> resilienceStrategy;
  private final RuntimeConfiguration runtimeConfiguration;
  private final CacheEventNotificationService<K, V> eventNotificationService;
  private final StoreEventListener<K, V> storeListener;
  private final Jsr107CacheImpl jsr107Cache;
  private final boolean useLoaderInAtomics;

  private final OperationObserver<GetOutcome> getObserver = operation(GetOutcome.class).named("get").of(this).tag("cache").build();
  private final OperationObserver<PutOutcome> putObserver = operation(PutOutcome.class).named("put").of(this).tag("cache").build();
  private final OperationObserver<RemoveOutcome> removeObserver = operation(RemoveOutcome.class).named("remove").of(this).tag("cache").build();
  private final OperationObserver<ConditionalRemoveOutcome> conditionalRemoveObserver = operation(ConditionalRemoveOutcome.class).named("conditionalRemove").of(this).tag("cache").build();
  private final OperationObserver<CacheLoaderOutcome> cacheLoaderObserver = operation(CacheLoaderOutcome.class).named("cacheLoader").of(this).tag("cache").build();
  private final OperationObserver<PutIfAbsentOutcome> putIfAbsentObserver = operation(PutIfAbsentOutcome.class).named("putIfAbsent").of(this).tag("cache").build();
  private final OperationObserver<ReplaceOutcome> replaceObserver = operation(ReplaceOutcome.class).named("replace").of(this).tag("cache").build();  
  private final ConcurrentMap<BulkOps, AtomicLong> bulkMethodEntries = new ConcurrentHashMap<BulkOps, AtomicLong>();
  
  private final CacheStatistics cacheStatistics;

  private static final NullaryFunction<Boolean> REPLACE_FALSE = new NullaryFunction<Boolean>() {
    @Override
    public Boolean apply() {
      return Boolean.FALSE;
    }
  };

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
    this(config, store, cacheLoader, cacheWriter, null, statisticsExecutor);
  }

  public Ehcache(CacheConfiguration<K, V> config, Store<K, V> store, 
      final CacheLoader<? super K, ? extends V> cacheLoader, 
      CacheWriter<? super K, ? super V> cacheWriter,
      CacheEventNotificationService<K, V> eventNotifier,
      ScheduledExecutorService statisticsExecutor) {
    this.store = store;
    StatisticsManager.associate(store).withParent(this);
    this.cacheLoader = cacheLoader;
    this.cacheWriter = cacheWriter;
    if (statisticsExecutor != null) {
      this.cacheStatistics = new StatisticsGateway(this, statisticsExecutor, bulkMethodEntries);
    } else {
      this.cacheStatistics = new DisabledStatistics();
    }
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
    this.runtimeConfiguration = new RuntimeConfiguration(config);
    this.storeListener = new StoreListener();
    this.jsr107Cache = new Jsr107CacheImpl();

    boolean useLoaderInAtomics = true;
    Collection<ServiceConfiguration<?>> serviceConfigurations = config.getServiceConfigurations();
    for (ServiceConfiguration<?> serviceConfiguration : serviceConfigurations) {
      if (serviceConfiguration.getServiceType().equals(EhcacheService.class)) {
        EhcacheServiceConfiguration  ehcacheServiceConfiguration = (EhcacheServiceConfiguration) serviceConfiguration;
        useLoaderInAtomics = !ehcacheServiceConfiguration.jsr107CompliantAtomics();
        break;
      }
    }
    this.useLoaderInAtomics = useLoaderInAtomics;
  }

  @SuppressWarnings("unchecked")
  private RecoveryCache<K> castToRecoveryCache(Store<K, V> store) {
    return (RecoveryCache<K>) store;
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
        getObserver.end(cacheLoader == null ? GetOutcome.MISS_NO_LOADER : GetOutcome.MISS_WITH_LOADER);
        return null;
      } else {
        getObserver.end(cacheLoader == null ? GetOutcome.HIT_NO_LOADER : GetOutcome.HIT_WITH_LOADER);
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
        
        if (newValueAlreadyExpired(key, previousValue, value)) {
          return null;
        }
        
        Cache.Entry<K, V> entry = newCacheEntry(key, value);
        eventNotificationService.onEvent(previousValue == null ? 
            CacheEvents.creation(entry, Ehcache.this) : CacheEvents.update(newCacheEntry(key, previousValue), 
                entry, Ehcache.this));
        return value;
      }
    });

    try {
      ValueHolder<V> computed = store.compute(key, remappingFunction);
      if (computed != null) {
        putObserver.end(PutOutcome.ADDED);
      } else {
        // XXX: is there an outcome we want here?
      }
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
  public void remove(K key) throws CacheWriterException {
    removeInternal(key); // ignore return value;
  }
  
  
  private boolean removeInternal(final K key) throws CacheWriterException {
    removeObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(key);
    
    final AtomicBoolean modified = new AtomicBoolean();
    
    final BiFunction<K, V, V> remappingFunction = memoize(new BiFunction<K, V, V>() {
      @Override
      public V apply(final K key, final V previousValue) {
        modified.set(previousValue != null);
        
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
      if (modified.get()) {
        removeObserver.end(RemoveOutcome.SUCCESS);
      } else {
        // XXX: Is there an outcome we want here?
      }
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
  public Map<K, V> getAll(Set<? extends K> keys) throws BulkCacheLoaderException {
    return getAllInternal(keys, true);
  }
  
  private Map<K, V> getAllInternal(Set<? extends K> keys, boolean includeNulls) throws BulkCacheLoaderException {
    statusTransitioner.checkAvailable();
    checkNonNullContent(keys);

    final Map<K, V> successes;
    final Map<K, Exception> failures;
    if (cacheLoader != null) {
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

        if (cacheLoader != null) {
          Map<K, V> loaded = Collections.emptyMap();
          try {
            loaded = (Map<K, V>) cacheLoader.loadAll(computeResult.keySet());
          } catch (Exception e) {
            for (K key : computeResult.keySet()) {
              failures.put(key, e);
            }
          }
          
          for (Map.Entry<K, V> loadedEntry : loaded.entrySet()) {
            K key = loadedEntry.getKey();
            if (! computeResult.containsKey(key)) {
              throw newCacheLoaderException(new RuntimeException("Cache loader returned value for key: " + key));
            }
            V value = loadedEntry.getValue();
            successes.put(key, value);
            computeResult.put(key, value);
          }
        }

        return computeResult.entrySet();
      }
    };

    Map<K, V> result = new HashMap<K, V>();
    try {
      Map<K, Store.ValueHolder<V>> computedMap = store.bulkComputeIfAbsent(keys, computeFunction);

      int hits = 0;
      for (Map.Entry<K, Store.ValueHolder<V>> entry : computedMap.entrySet()) {
        if (entry.getValue() != null) {
          result.put(entry.getKey(), entry.getValue().value());
          hits++;
        } else if (includeNulls) {
          result.put(entry.getKey(), null);
        }
      }
      
      addBulkMethodEntriesCount(BulkOps.GET_ALL, hits);
      if (failures.isEmpty()) {
        return result;
      } else {
        throw new BulkCacheLoaderException(failures, successes);
      }
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
        computeFunction.apply(toLoad);
        if (failures.isEmpty()) {
          return resilienceStrategy.getAllFailure(keys, successes, e);
        } else {
          return resilienceStrategy.getAllFailure(keys, e, new BulkCacheLoaderException(failures, successes));
        }
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
  public void putAll(final Map<? extends K, ? extends V> entries) throws BulkCacheWriterException {
    statusTransitioner.checkAvailable();
    checkNonNull(entries);
    final Set<K> successes;
    final Map<K, Exception> failures;
    if (cacheWriter != null) {
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
        cacheWriterWriteAllCall(entries, entriesToRemap, successes, failures);

        Map<K, V> mutations = new LinkedHashMap<K, V>();

        // then record we handled these mappings
        for (Map.Entry<? extends K, ? extends V> entry: entries) {
          K key = entry.getKey();
          V existingValue = entry.getValue();
          V newValue = entriesToRemap.remove(key);
         
          if (newValueAlreadyExpired(key, existingValue, newValue)) {
            mutations.put(key, null);
          } else if (cacheWriter == null || successes.contains(key)) {
            actualPutCount.incrementAndGet();
            mutations.put(key, newValue);
          
            Cache.Entry<K, V> newEntry = newCacheEntry(key, newValue);
            eventNotificationService.onEvent(existingValue == null ? 
              CacheEvents.creation(newEntry, Ehcache.this) : CacheEvents.update( 
                  newCacheEntry(key, existingValue), newEntry, Ehcache.this));
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
      if (! failures.isEmpty()) {
        throw new BulkCacheWriterException(failures, successes);
      }
    } catch (CacheAccessException e) {
      if (cacheWriter == null) {
        resilienceStrategy.putAllFailure(entries, e);
      } else {
        // just in case not all writes happened:
        if (! entriesToRemap.isEmpty()) {
          cacheWriterWriteAllCall(entriesToRemap.entrySet(), entriesToRemap, successes, failures);
        }
        if (failures.isEmpty()) {
          resilienceStrategy.putAllFailure(entries, e);
        } else {
          resilienceStrategy.putAllFailure(entries, e, new BulkCacheWriterException(failures, successes));
        }
      }
    }
  }

  private void cacheWriterWriteAllCall(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries, Map<K, V> entriesToRemap, Set<K> successes, Map<K, Exception> failures) {
    if (cacheWriter != null) {
      Map<K, V> toWrite = new HashMap<K, V>();
      for (Map.Entry<? extends K, ? extends V> entry: entries) {
        toWrite.put(entry.getKey(), entriesToRemap.get(entry.getKey()));
      }
      try {
        if (! toWrite.isEmpty()) {
          // write all entries of this batch
          cacheWriter.writeAll(toWrite.entrySet());
          successes.addAll(toWrite.keySet());
        }
      } catch (BulkCacheWriterException bcwe) {
        collectSuccessesAndFailures(bcwe, successes, failures);
      } catch (Exception e) {
        for (K key: toWrite.keySet()) {
          failures.put(key, e);
        }
      }
    }
  }

  @SuppressWarnings({ "unchecked" })
  private static <K> void collectSuccessesAndFailures(BulkCacheWriterException bcwe, Set<K> successes, Map<K, Exception> failures) {
    successes.addAll((Collection<K>)bcwe.getSuccesses());
    failures.putAll((Map<K, Exception>)bcwe.getFailures());
  }

  @Override
  public void removeAll(final Set<? extends K> keys) throws BulkCacheWriterException {
    statusTransitioner.checkAvailable();
    checkNonNull(keys);
    final Set<K> successes;
    final Map<K, Exception> failures;
    if (cacheWriter != null) {
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
          cacheWriterDeleteAllCall(entries, successes, failures);
          
          Map<K, V> results = new LinkedHashMap<K, V>();
          
          for (Map.Entry<? extends K, ? extends V> entry : entries) {
            K key = entry.getKey();
            V existingValue = entry.getValue();
            
            if (cacheWriter == null || successes.contains(key)) {
              if (existingValue != null) {
                actualRemoveCount.incrementAndGet();
              }
              results.put(key, null);
              entriesToRemove.remove(key);
              eventNotificationService.onEvent(CacheEvents.removal(newCacheEntry(key, existingValue), Ehcache.this));
            } else {
              results.put(key, existingValue);
            }
          }
          
          return results.entrySet();
        }
      };

    try {
      store.bulkCompute(keys, removalFunction);
      addBulkMethodEntriesCount(BulkOps.REMOVE_ALL, actualRemoveCount.get());
      if (! failures.isEmpty()) {
        throw new BulkCacheWriterException(failures, successes);
      }
    } catch (CacheAccessException e) {
      if (cacheWriter == null) {
        resilienceStrategy.removeAllFailure(keys, e);
      } else {
        // just in case not all writes happened:
        if (! entriesToRemove.isEmpty()) {
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
        cacheWriter.deleteAll(toDelete);
        successes.addAll(toDelete);
      } catch (BulkCacheWriterException bcwe) {
        collectSuccessesAndFailures(bcwe, successes, failures);
      } catch (Exception e) {
        for (K key : toDelete) {
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
        try {
          if (useLoaderInAtomics && cacheLoader != null) {
            try {
              V loaded = cacheLoader.load(k);
              if (loaded != null) {
                return loaded; // populate the cache
              }
            } catch (Exception e) {
              throw newCacheLoaderException(e);
            }
          }

          if (cacheWriter != null) {
            try {
              cacheWriter.write(k, value);
            } catch (Exception e) {
              throw newCacheWriterException(e);
            }
          }

          if (newValueAlreadyExpired(key, null, value)) {
            return null;
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
          } catch (CacheLoaderException f) {
            return resilienceStrategy.putIfAbsentFailure(key, value, e, f);
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
    conditionalRemoveObserver.begin();
    statusTransitioner.checkAvailable();
    checkNonNull(key, value);
    final AtomicBoolean hit = new AtomicBoolean();
    final AtomicBoolean removed = new AtomicBoolean();
    final BiFunction<K, V, V> remappingFunction = memoize(new BiFunction<K, V, V>() {
      @Override
      public V apply(final K k, V inCache) {
        if (inCache == null) {
          if (useLoaderInAtomics && cacheLoader != null) {
            try {
              inCache = cacheLoader.load(key);
              if (inCache == null) {
                return null;
              }
            } catch (Exception e) {
              throw newCacheLoaderException(e);
            }
          } else {
            return null;
          }
        }

        hit.set(true);
        if (value.equals(inCache)) {
          if (cacheWriter != null) {
            try {
              cacheWriter.delete(k);
            } catch (Exception e) {
              throw newCacheWriterException(e);
            }
          }
          eventNotificationService.onEvent(CacheEvents.removal(newCacheEntry(k, value), Ehcache.this));
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
      } else {
        if (hit.get()) {
          conditionalRemoveObserver.end(ConditionalRemoveOutcome.FAILURE_KEY_PRESENT);
        } else {
          conditionalRemoveObserver.end(ConditionalRemoveOutcome.FAILURE_KEY_MISSING);
        }
      }
    } catch (CacheAccessException e) {
      try {
        if (cacheWriter == null) {
          return resilienceStrategy.removeFailure(key, value, e, removed.get());
        } else {
          try {
            remappingFunction.apply(key, null);
          } catch (CacheLoaderException f) {
            return resilienceStrategy.removeFailure(key, value, e, f);
          } catch (CacheWriterException f) {
            return resilienceStrategy.removeFailure(key, value, e, f);
          }
          return resilienceStrategy.removeFailure(key, value, e, removed.get());
        }
      } finally {
        conditionalRemoveObserver.end(ConditionalRemoveOutcome.FAILURE);
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
      public V apply(final K k, V inCache) {
        if (inCache == null) {
          if (useLoaderInAtomics && cacheLoader != null) {
            try {
              inCache = cacheLoader.load(key);
              if (inCache == null) {
                return null;
              }
            } catch (Exception e) {
              throw newCacheLoaderException(e);
            }
          } else {
            return null;
          }
        }

        if (cacheWriter != null) {
          try {
            cacheWriter.write(key, value);
          } catch (Exception e) {
            throw newCacheWriterException(e);
          }
        }
        
        if (newValueAlreadyExpired(key, inCache, value)) {
          return null;
        }
        
        old.set(inCache);
        eventNotificationService.onEvent(CacheEvents.update(newCacheEntry(k, inCache), 
            newCacheEntry(k, value), Ehcache.this));
        return value;
      }
    });

    try {
      store.compute(key, remappingFunction);
      if (old.get() != null) {
        replaceObserver.end(ReplaceOutcome.HIT);
      } else {
        replaceObserver.end(ReplaceOutcome.MISS_NOT_PRESENT);
      }
      return old.get();
    } catch (CacheAccessException e) {
      try {
        try {
          remappingFunction.apply(key, null);
        } catch (CacheLoaderException f) {
          return resilienceStrategy.replaceFailure(key, value, e, f);
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
    final AtomicBoolean hit = new AtomicBoolean();
    
    final BiFunction<K, V, V> remappingFunction = memoize(new BiFunction<K, V, V>() {
      @Override
      public V apply(final K k, V inCache) {
        try {
          if (inCache == null) {
            if (useLoaderInAtomics && cacheLoader != null) {
              try {
                inCache = cacheLoader.load(key);
                if (inCache == null) {
                  return null;
                }
              } catch (Exception e) {
                throw newCacheLoaderException(e);
              }
            } else {
              return null;
            }
          }

          hit.set(true);
          if (oldValue.equals(inCache)) {
            if (cacheWriter != null) {
              try {
                cacheWriter.write(key, newValue);
              } catch (Exception e) {
                throw newCacheWriterException(e);
              }
            }
            
            if (newValueAlreadyExpired(key, oldValue, newValue)) {
              return null;
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
    } catch (CacheAccessException e) {
      try {
        if (cacheWriter == null) {
          return resilienceStrategy.replaceFailure(key, oldValue, newValue, e, success.get());
        } else {
          try {
            remappingFunction.apply(key, null);
          } catch (CacheLoaderException f) {
            return resilienceStrategy.replaceFailure(key, oldValue, newValue, e, f);
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
  
  @Override
  public CacheStatistics getStatistics() {
    return cacheStatistics;
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
  
  private void checkNonNullContent(Collection<?> collectionOfThings) {
    checkNonNull(collectionOfThings);
    for (Object thing : collectionOfThings) {
      if (thing == null) {
        throw new NullPointerException();
      }
    }
  }

  private void addBulkMethodEntriesCount(BulkOps op, long count) {
    AtomicLong current = bulkMethodEntries.get(op);
    if (current == null) {
      AtomicLong newCount = new AtomicLong();
      current = bulkMethodEntries.putIfAbsent(op, newCount);
      if (current == null) {
        current = newCount;
      }
    }
    current.addAndGet(count);
  }
  
  Jsr107Cache<K, V> getJsr107Cache() {
    return jsr107Cache;
  }
  
  CacheLoader<? super K, ? extends V> getCacheLoader() {
    return this.cacheLoader;
  }
  
  CacheWriter<? super K, ? super V> getCacheWriter() {
    return this.cacheWriter;
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
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, Function<Iterable<? extends K>, Map<? super K, ? extends V>> loadFunction) {
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

    private void loadAllAbsent(Set<? extends K> keys, final Function<Iterable<? extends K>, Map<? super K, ? extends V>> loadFunction) {
      try {
        store.bulkComputeIfAbsent(keys, new Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
          @Override
          public Iterable<? extends java.util.Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends K> absentKeys) {
            return cacheLoaderLoadAllForKeys(absentKeys, loadFunction).entrySet();
          }
        });
      } catch (CacheAccessException e) {
        throw newCacheLoaderException(e);
      }
    }

    Map<K, V> cacheLoaderLoadAllForKeys(Iterable<? extends K> keys, Function<Iterable<? extends K>, Map<? super K, ? extends V>> loadFunction) {
      try {
        Map<? super K, ? extends V> loaded = loadFunction.apply(keys);
        
        // put into a new map since we can't assume the 107 cache loader returns things ordered, or necessarily with all the desired keys
        Map<K, V> rv = new LinkedHashMap<K, V>();
        for (K key : keys) {
          rv.put(key, loaded.get(key));
        }
        return rv;
      } catch (Exception e) {
        throw newCacheLoaderException(e);
      }
    }

    private void loadAllReplace(Set<? extends K> keys, final Function<Iterable<? extends K>, Map<? super K, ? extends V>> loadFunction) {
      try {
        store.bulkCompute(keys, new Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
          @Override
          public Iterable<? extends java.util.Map.Entry<? extends K, ? extends V>> apply(
              Iterable<? extends java.util.Map.Entry<? extends K, ? extends V>> entries) {
            Collection<K> keys = new ArrayList<K>();
            for (Map.Entry<? extends K, ? extends V> entry : entries) {
              keys.add(entry.getKey());
            }
            return cacheLoaderLoadAllForKeys(keys, loadFunction).entrySet();
          }            
        });
      } catch (CacheAccessException e) {
        throw newCacheLoaderException(e);
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

            if (invokeWriter.apply() && cacheWriter != null) {
              try {
                if (newValue != null) {
                  cacheWriter.write(mappedKey, newValue);
                } else {
                  cacheWriter.delete(mappedKey);
                }
              } catch (Exception e) {
                throw newCacheWriterException(e);
              }
            }
            
            if (newValueAlreadyExpired(mappedKey, mappedValue, newValue)) {
              return null;
            }
            
            if (withStatsAndEvents.apply()) {
              final CacheEvent<K, V> event;
              if (newValue == null) {
                removeObserver.end(RemoveOutcome.SUCCESS);
                event = CacheEvents.removal(newCacheEntry(mappedKey, mappedValue), Ehcache.this);
              } else {
                putObserver.end(PutOutcome.ADDED);
                if (mappedValue == null) {
                  event = CacheEvents.creation(newCacheEntry(mappedKey, newValue), Ehcache.this);
                } else {
                  event = CacheEvents.update(newCacheEntry(mappedKey, mappedValue), newCacheEntry(mappedKey, newValue), Ehcache.this);
                }
              }
              eventNotificationService.onEvent(event);
            }
            
            return newValue;
          }
        };

        ValueHolder<V> valueHolder = store.compute(key, fn, replaceEqual);
        return valueHolder == null ? null : valueHolder.value();
      } catch (CacheAccessException e) {
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
            
            if (cacheWriter != null) {
              try {
                cacheWriter.delete(mappedKey);
              } catch (Exception e) {
                throw newCacheWriterException(e);
              }
            }

            eventNotificationService.onEvent(CacheEvents.removal(newCacheEntry(mappedKey, mappedValue), Ehcache.this));
            return null;
          }
        });
      } catch (CacheAccessException e) {
        getObserver.end(GetOutcome.FAILURE);
        removeObserver.end(RemoveOutcome.FAILURE);
        // XXX:
        throw new RuntimeException(e);
      }
      
      V returnValue = existingValue.get();
      if (returnValue != null) {
        getObserver.end(GetOutcome.HIT_NO_LOADER);
        removeObserver.end(RemoveOutcome.SUCCESS);
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
            
            if (cacheWriter != null) {
              try {
                cacheWriter.write(mappedKey, value);
              } catch (Exception e) {
                throw newCacheWriterException(e);
              }
            }
            
            if (newValueAlreadyExpired(mappedKey, mappedValue, value)) {
              return null;
            }
            
            Cache.Entry<K, V> entry = newCacheEntry(mappedKey, value);
            if (mappedValue != null) {
              eventNotificationService.onEvent(CacheEvents.update(newCacheEntry(mappedKey, mappedValue), entry, Ehcache.this)); 
            } else {
              eventNotificationService.onEvent(CacheEvents.creation(entry, Ehcache.this));
            }
            
            return value;
          }
        });
      } catch (CacheAccessException e) {
        getObserver.end(GetOutcome.FAILURE);
        putObserver.end(PutOutcome.FAILURE);
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

    @Override
    public ClassLoader getClassLoader() {
      return this.classLoader;
    }
    
    @Override
    public Expiry<? super K, ? super V> getExpiry() {
      return expiry;
    }
    
    @Override
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
