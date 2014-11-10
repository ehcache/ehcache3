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
package org.ehcache.jsr107;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;

import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.jsr107.CacheResources.ListenerResources;
import org.ehcache.jsr107.EventListenerAdaptors.EventListenerAdaptor;

/**
 * @author teck
 */
class Eh107Cache<K, V> implements Cache<K, V> {

  private final org.ehcache.Cache<K, V> ehCache;
  private final Eh107CacheManager cacheManager;
  private final String name;
  private final AtomicBoolean closed = new AtomicBoolean();
  private final CacheResources<K, V> cacheResources;
  private final Eh107CacheMXBean managementBean;
  private final Eh107CacheStatisticsMXBean statisticsBean;
  private final CompleteConfiguration<K, V> config;

  // functions used by loadAll()
  private final Function<K, V> loadAllNonReplaceFunction;
  private final BiFunction<K, V, V> loadAllReplaceFunction;
  {
    this.loadAllNonReplaceFunction = new Function<K, V>() {
      @Override
      public V apply(K mappedKey) {
        return ehCache.get(mappedKey);
      }
    };

    this.loadAllReplaceFunction = new BiFunction<K, V, V>() {
      @Override
      public V apply(K mappedKey, V mappedValue) {
        return ehCache.get(mappedKey);
      }
    };
  }

  Eh107Cache(String name, CompleteConfiguration<K, V> config, CacheResources<K, V> cacheResources,
      org.ehcache.Cache<K, V> ehCache, Eh107CacheManager cacheManager) {
    this.config = config;
    this.ehCache = ehCache;
    this.cacheManager = cacheManager;
    this.name = name;
    this.cacheResources = cacheResources;
    this.managementBean = new Eh107CacheMXBean(name, cacheManager, config);
    this.statisticsBean = new Eh107CacheStatisticsMXBean(name, cacheManager, /*
                                                                              * ehCache
                                                                              * .
                                                                              * getStatistics
                                                                              * (
                                                                              * )
                                                                              */null);
  }

  @Override
  public V get(K key) {
    checkClosed();
    return ehCache.get(key);
  }

  @Override
  public Map<K, V> getAll(Set<? extends K> keys) {
    checkClosed();
    return ehCache.getAll(keys);
  }

  @Override
  public boolean containsKey(K key) {
    checkClosed();
    return ehCache.containsKey(key);
  }

  @Override
  public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
    // XXX: this method is allowed to be async. Hand it off to some thread(s)?
    checkClosed();

    if (keys == null) {
      throw new NullPointerException();
    }

    for (K key : keys) {
      if (key == null) {
        throw new NullPointerException();
      }
    }

    completionListener = completionListener != null ? completionListener : NullCompletionListener.INSTANCE;

    if (cacheResources.getCacheLoader() == null) {
      completionListener.onCompletion();
      return;
    }

    // XXX: use bulkCompute() methods instead? Not that they exist at the moment
    for (K key : keys) {
      try {
        if (replaceExistingValues) {
          ehCache.compute(key, loadAllReplaceFunction);
        } else {
          ehCache.computeIfAbsent(key, loadAllNonReplaceFunction);
        }
      } catch (Throwable t) {
        completionListener.onException(new CacheLoaderException(t));
        continue;
      }
    }

    completionListener.onCompletion();
  }

  @Override
  public void put(K key, V value) {
    checkClosed();
    ehCache.put(key, value);
  }

  @Override
  public V getAndPut(final K key, final V value) {
    checkClosed();

    final AtomicReference<V> prevValue = new AtomicReference<V>();

    ehCache.compute(key, new BiFunction<K, V, V>() {
      @Override
      public V apply(K mappedKey, V mappedValue) {
        prevValue.set(mappedValue);
        return value;
      }
    });

    return prevValue.get();
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> map) {
    checkClosed();
    ehCache.putAll(map.entrySet());
  }

  @Override
  public boolean putIfAbsent(K key, V value) {
    checkClosed();
    return ehCache.putIfAbsent(key, value) == null;
  }

  @Override
  public boolean remove(final K key) {
    checkClosed();

    final AtomicBoolean modified = new AtomicBoolean(false);
    ehCache.computeIfPresent(key, new BiFunction<K, V, V>() {
      @Override
      public V apply(K mappedKey, V mappedValue) {
        modified.set(true);
        return null;
      }
    });

    return modified.get();
  }

  @Override
  public boolean remove(K key, V oldValue) {
    checkClosed();
    return ehCache.remove(key, oldValue);
  }

  @Override
  public V getAndRemove(K key) {
    checkClosed();

    final AtomicReference<V> prevValue = new AtomicReference<V>(null);
    ehCache.computeIfPresent(key, new BiFunction<K, V, V>() {
      @Override
      public V apply(K mappedKey, V mappedValue) {
        prevValue.set(mappedValue);
        return null;
      }
    });

    return prevValue.get();
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    checkClosed();
    return ehCache.replace(key, oldValue, newValue);
  }

  @Override
  public boolean replace(K key, V value) {
    checkClosed();
    return ehCache.replace(key, value) != null;
  }

  @Override
  public V getAndReplace(K key, V value) {
    checkClosed();
    return ehCache.replace(key, value);
  }

  @Override
  public void removeAll(Set<? extends K> keys) {
    checkClosed();
    ehCache.removeAll(keys);
  }

  @Override
  public void removeAll() {
    checkClosed();

    for (org.ehcache.Cache.Entry<K, V> entry : ehCache) {
      ehCache.remove(entry.getKey());
    }
  }

  @Override
  public void clear() {
    clear(true);
  }

  private void clear(boolean checkClosed) {
    if (checkClosed) {
      checkClosed();
    }
    ehCache.clear();
  }

  @Override
  public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
    checkClosed();
    return Unwrap.unwrap(clazz, config);
  }

  @Override
  public <T> T invoke(final K key, final EntryProcessor<K, V, T> entryProcessor, final Object... arguments)
      throws EntryProcessorException {
    checkClosed();

    if (key == null || entryProcessor == null) {
      throw new NullPointerException();
    }

    final AtomicReference<T> invokeResult = new AtomicReference<T>();
    ehCache.compute(key, new BiFunction<K, V, V>() {
      @Override
      public V apply(K mappedKey, V mappedValue) {
        MutableEntry<K, V> mutableEntry = new MutableEntry<K, V>(mappedKey, mappedValue, cacheResources
            .getCacheLoader());
        T processResult;
        try {
          processResult = entryProcessor.process(mutableEntry, arguments);
        } catch (Exception e) {
          if (e instanceof EntryProcessorException) {
            throw (EntryProcessorException) e;
          }
          throw new EntryProcessorException(e);
        }

        V newValue = mutableEntry.applyTo(Eh107Cache.this);
        invokeResult.set(processResult);
        return newValue;
      }
    });

    return invokeResult.get();
  }

  @Override
  public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor,
      Object... arguments) {
    checkClosed();

    if (keys == null || entryProcessor == null) {
      throw new NullPointerException();
    }

    for (K key : keys) {
      if (key == null) {
        throw new NullPointerException();
      }
    }

    // XXX: hand off to threads for parallel execution?
    Map<K, EntryProcessorResult<T>> results = new HashMap<K, EntryProcessorResult<T>>(keys.size());
    for (K key : keys) {
      EntryProcessorResult<T> result = null;
      try {
        T invokeResult = invoke(key, entryProcessor, arguments);
        if (invokeResult != null) {
          result = newEntryProcessorResult(invokeResult);
        }
      } catch (final Exception e) {
        result = newErrorThrowingEntryProcessorResult(e);
      }

      if (result != null) {
        results.put(key, result);
      }
    }

    return results;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public CacheManager getCacheManager() {
    return cacheManager;
  }

  @Override
  public void close() {
    CacheException closeException = new CacheException();
    cacheManager.close(this, closeException);
    if (closeException.getSuppressed().length > 0) {
      throw closeException;
    }
  }

  @Override
  public boolean isClosed() {
    return closed.get();
  }

  void closeInternal(CacheException closeException) {
    closeInternal(false, closeException);
  }

  private void closeInternal(boolean destroy, CacheException closeException) {
    if (closed.compareAndSet(false, true)) {
      if (destroy) {
        try {
          clear(false);
        } catch (Throwable t) {
          closeException.addSuppressed(t);
        }
      }

      try {
        cacheResources.closeResources();
      } catch (Throwable t) {
        closeException.addSuppressed(t);
      }
    }
  }

  void destroy(CacheException destroyException) {
    closeInternal(true, destroyException);
  }

  @Override
  public <T> T unwrap(Class<T> clazz) {
    return Unwrap.unwrap(clazz, ehCache);
  }

  @Override
  public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
    checkClosed();

    if (cacheEntryListenerConfiguration == null) {
      throw new NullPointerException();
    }

    ListenerResources<K, V> resources = cacheResources.registerCacheEntryListener(cacheEntryListenerConfiguration);

    for (EventListenerAdaptor<K, V> ehListener : resources.getEhListeners(this)) {
      // XXX: what are the proper values for ordering and sync vs. asynch?

      ehCache.getRuntimeConfiguration().registerCacheEventListener(ehListener, EventOrdering.UNORDERED,
          EventFiring.ASYNCHRONOUS, EnumSet.of(ehListener.getEhcacheEventType()));
    }
  }

  @Override
  public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
    checkClosed();

    if (cacheEntryListenerConfiguration == null) {
      throw new NullPointerException();
    }

    ListenerResources<K, V> resources = cacheResources.deregisterCacheEntryListener(cacheEntryListenerConfiguration);

    if (resources != null) {
      for (EventListenerAdaptor<K, V> ehListener : resources.getEhListeners(this)) {
        ehCache.getRuntimeConfiguration().deregisterCacheEventListener(ehListener);
      }
    }
  }

  @Override
  public java.util.Iterator<Cache.Entry<K, V>> iterator() {
    return new Iterator<K, V>(ehCache.iterator());
  }

  private void checkClosed() {
    if (closed.get()) {
      throw new IllegalStateException("Cache[" + name + "] is closed");
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "[" + name + "]";
  }

  Eh107MXBean getManagementMBean() {
    return managementBean;
  }

  Eh107MXBean getStatisticsMBean() {
    return statisticsBean;
  }

  private static <T> EntryProcessorResult<T> newEntryProcessorResult(final T result) {
    if (result == null) {
      throw new NullPointerException();
    }

    return new EntryProcessorResult<T>() {
      @Override
      public T get() throws EntryProcessorException {
        return result;
      }
    };
  }

  private static <T> EntryProcessorResult<T> newErrorThrowingEntryProcessorResult(final Exception e) {
    return new EntryProcessorResult<T>() {
      @Override
      public T get() throws EntryProcessorException {
        if (e instanceof EntryProcessorException) {
          throw (EntryProcessorException) e;
        }
        throw new EntryProcessorException(e);
      }
    };
  }

  private static class Iterator<K, V> implements java.util.Iterator<javax.cache.Cache.Entry<K, V>> {

    private final java.util.Iterator<org.ehcache.Cache.Entry<K, V>> ehIterator;

    Iterator(java.util.Iterator<org.ehcache.Cache.Entry<K, V>> iterator) {
      this.ehIterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return ehIterator.hasNext();
    }

    @Override
    public javax.cache.Cache.Entry<K, V> next() {
      return new Entry<K, V>(ehIterator.next());
    }

    @Override
    public void remove() {
      ehIterator.remove();
    }
  }

  private static class Entry<K, V> implements javax.cache.Cache.Entry<K, V> {

    private final org.ehcache.Cache.Entry<K, V> ehEntry;

    Entry(org.ehcache.Cache.Entry<K, V> ehEntry) {
      this.ehEntry = ehEntry;
    }

    @Override
    public K getKey() {
      return ehEntry.getKey();
    }

    @Override
    public V getValue() {
      return ehEntry.getValue();
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
      return Unwrap.unwrap(clazz, ehEntry);
    }
  }

  private static enum MutableEntryOperation {
    NONE, ACCESS, CREATE, LOAD, REMOVE, UPDATE;
  }

  private static class MutableEntry<K, V> implements javax.cache.processor.MutableEntry<K, V> {

    private static final Object UNDEFINED = new Object();

    private final K key;
    private final V initialValue;
    private final CacheLoader<K, V> cacheLoader;
    private volatile V finalValue = undefined();
    private volatile MutableEntryOperation operation = MutableEntryOperation.NONE;

    MutableEntry(K key, V initialValue, CacheLoader<K, V> cacheLoader) {
      this.key = key;
      this.initialValue = initialValue;
      this.cacheLoader = cacheLoader;
    }

    @Override
    public K getKey() {
      return key;
    }

    @Override
    public V getValue() {
      if (finalValue == UNDEFINED) {
        if (initialValue == null && cacheLoader != null) {
          finalValue = tryLoad();
          if (finalValue != null) {
            operation = MutableEntryOperation.LOAD;
          }
        } else {
          finalValue = initialValue;
          operation = MutableEntryOperation.ACCESS;
        }
      }

      return finalValue;
    }

    private V tryLoad() {
      try {
        return cacheLoader.load(key);
      } catch (Exception e) {
        if (e instanceof CacheLoaderException) {
          throw (CacheLoaderException) e;
        }
        throw new CacheLoaderException(e);
      }
    }

    @Override
    public boolean exists() {
      // None of getValue(), remove() or setValue() have been called
      if (finalValue == UNDEFINED) {
        return initialValue != null;
      }

      return finalValue != null;
    }

    @Override
    public void remove() {
      operation = initialValue == null ? MutableEntryOperation.NONE : MutableEntryOperation.REMOVE;
      finalValue = null;
    }

    @Override
    public void setValue(V value) {
      if (value == null) {
        throw new NullPointerException();
      }

      operation = initialValue == null ? MutableEntryOperation.CREATE : MutableEntryOperation.UPDATE;
      finalValue = value;
    }

    V applyTo(Eh107Cache<K, V> cache) {
      switch (operation) {
      case ACCESS:
        return cache.get(key);
      case CREATE:
      case LOAD:
      case UPDATE:
        cache.put(key, finalValue);
        return finalValue;
      case NONE:
        return initialValue;
      case REMOVE:
        cache.remove(key);
        return null;
      }

      throw new AssertionError("unhandled case: " + operation);
    }

    @SuppressWarnings("unchecked")
    private V undefined() {
      return (V) UNDEFINED;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
      throw new IllegalArgumentException();
    }
  }

}
