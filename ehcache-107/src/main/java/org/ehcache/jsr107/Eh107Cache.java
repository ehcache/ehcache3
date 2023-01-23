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

import org.ehcache.core.InternalCache;
import org.ehcache.Status;
import org.ehcache.core.Jsr107Cache;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.core.exceptions.StorePassThroughException;
import org.ehcache.jsr107.EventListenerAdaptors.EventListenerAdaptor;
import org.ehcache.jsr107.internal.Jsr107CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.CacheLoadingException;
import org.ehcache.spi.loaderwriter.CacheWritingException;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;

/**
 * @author teck
 */
class Eh107Cache<K, V> implements Cache<K, V> {

  private final InternalCache<K, V> ehCache;
  private final Jsr107Cache<K, V> jsr107Cache;
  private final Eh107CacheManager cacheManager;
  private final String name;
  private final AtomicBoolean hypotheticallyClosed = new AtomicBoolean();
  private final CacheResources<K, V> cacheResources;
  private final Eh107CacheMXBean managementBean;
  private final Eh107CacheStatisticsMXBean statisticsBean;
  private final Eh107Configuration<K, V> config;
  private final Jsr107CacheLoaderWriter<? super K, V> cacheLoaderWriter;

  Eh107Cache(String name, Eh107Configuration<K, V> config, CacheResources<K, V> cacheResources,
             InternalCache<K, V> ehCache, StatisticsService statisticsService, Eh107CacheManager cacheManager) {
    this.cacheLoaderWriter = cacheResources.getCacheLoaderWriter();
    this.config = config;
    this.ehCache = ehCache;
    this.cacheManager = cacheManager;
    this.name = name;
    this.cacheResources = cacheResources;
    this.managementBean = new Eh107CacheMXBean(name, cacheManager.getURI(), config);
    this.statisticsBean = new Eh107CacheStatisticsMXBean(name, cacheManager.getURI(), statisticsService);

    for (Map.Entry<CacheEntryListenerConfiguration<K, V>, ListenerResources<K, V>> entry : cacheResources
        .getListenerResources().entrySet()) {
      registerEhcacheListeners(entry.getKey(), entry.getValue());
    }

    this.jsr107Cache = ehCache.createJsr107Cache();
  }

  @Override
  public V get(K key) {
    checkClosed();
    try {
      return ehCache.get(key);
    } catch (CacheLoadingException e) {
      throw jsr107CacheLoaderException(e);
    }
  }

  @Override
  public Map<K, V> getAll(Set<? extends K> keys) {
    checkClosed();
    try {
      return jsr107Cache.getAll(keys);
    } catch (CacheLoadingException e) {
      throw jsr107CacheLoaderException(e);
    }
  }

  @Override
  public boolean containsKey(K key) {
    checkClosed();
    return ehCache.containsKey(key);
  }

  @Override
  public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
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

    if (cacheLoaderWriter == null) {
      completionListener.onCompletion();
      return;
    }

    try {
      jsr107Cache.loadAll(keys, replaceExistingValues, this::loadAllFunction);
    } catch (Exception e) {
      final CacheLoaderException cle;
      cle = getCacheLoaderException(e);
      completionListener.onException(cle);
      return;
    }

    completionListener.onCompletion();
  }

  private CacheLoaderException getCacheLoaderException(Exception e) {
    CacheLoaderException cle;
    if (e instanceof CacheLoaderException) {
      cle = (CacheLoaderException) e;
    } else if (e.getCause() instanceof CacheLoaderException) {
      cle = (CacheLoaderException) e.getCause();
    } else {
      cle = new CacheLoaderException(e);
    }
    return cle;
  }

  private Map<K, V> loadAllFunction(Iterable<? extends K> keysIterable) {
    try {
      Map<? super K, ? extends V> loadResult = cacheLoaderWriter.loadAllAlways(keysIterable);
      HashMap<K, V> resultMap = new HashMap<>();
      for (K key : keysIterable) {
        resultMap.put(key, loadResult.get(key));
      }
      return resultMap;
    } catch (Exception e) {
      CacheLoaderException cle = getCacheLoaderException(e);
      throw cle;
    }
  }

  @Override
  public void put(K key, V value) {
    checkClosed();
    try {
      ehCache.put(key, value);
    } catch (CacheWritingException cwe) {
      throw jsr107CacheWriterException(cwe);
    }
  }

  @Override
  public V getAndPut(final K key, final V value) {
    checkClosed();

    if (key == null || value == null) {
      throw new NullPointerException();
    }

    try {
      return jsr107Cache.getAndPut(key, value);
    } catch (CacheWritingException e) {
      throw jsr107CacheWriterException(e);
    }
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> map) {
    checkClosed();
    try {
      ehCache.putAll(map);
    } catch (CacheWritingException e) {
      throw jsr107CacheWriterException(e);
    }
  }

  @Override
  public boolean putIfAbsent(K key, V value) {
    checkClosed();
    try {
      cacheResources.getExpiryPolicy().enableShortCircuitAccessCalls();
      return ehCache.putIfAbsent(key, value) == null;
    } catch (CacheWritingException e) {
      throw jsr107CacheWriterException(e);
    } finally {
      cacheResources.getExpiryPolicy().disableShortCircuitAccessCalls();
    }
  }

  @Override
  public boolean remove(final K key) {
    checkClosed();

    if (key == null) {
      throw new NullPointerException();
    }

    try {
      return jsr107Cache.remove(key);
    } catch (CacheWritingException e) {
      throw jsr107CacheWriterException(e);
    }
  }

  @Override
  public boolean remove(K key, V oldValue) {
    checkClosed();
    try {
      return ehCache.remove(key, oldValue);
    } catch (CacheWritingException e) {
      throw jsr107CacheWriterException(e);
    }
  }

  @Override
  public V getAndRemove(K key) {
    checkClosed();

    if (key == null) {
      throw new NullPointerException();
    }

    try {
      return jsr107Cache.getAndRemove(key);
    } catch (CacheWritingException e) {
      throw jsr107CacheWriterException(e);
    }
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    checkClosed();
    try {
      return ehCache.replace(key, oldValue, newValue);
    } catch (CacheWritingException e) {
      throw jsr107CacheWriterException(e);
    }
  }

  @Override
  public boolean replace(K key, V value) {
    checkClosed();
    try {
      return ehCache.replace(key, value) != null;
    } catch (CacheWritingException e) {
      throw jsr107CacheWriterException(e);
    }
  }

  @Override
  public V getAndReplace(K key, V value) {
    try {
      checkClosed();
      return ehCache.replace(key, value);
    } catch (CacheWritingException e) {
      throw jsr107CacheWriterException(e);
    }
  }

  @Override
  public void removeAll(Set<? extends K> keys) {
    checkClosed();
    try {
      ehCache.removeAll(keys);
    } catch (CacheWritingException e) {
      throw jsr107CacheWriterException(e);
    }
  }

  @Override
  public void removeAll() {
    checkClosed();
    try {
      jsr107Cache.removeAll();
    } catch (CacheWritingException e) {
      throw jsr107CacheWriterException(e);
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
    return config.unwrap(clazz);
  }

  @Override
  public <T> T invoke(final K key, final EntryProcessor<K, V, T> entryProcessor, final Object... arguments)
      throws EntryProcessorException {
    checkClosed();

    if (key == null || entryProcessor == null) {
      throw new NullPointerException();
    }

    final AtomicReference<MutableEntry> mutableEntryRef = new AtomicReference<>();
    final AtomicReference<T> invokeResult = new AtomicReference<>();

    jsr107Cache.compute(key, (mappedKey, mappedValue) -> {
      MutableEntry mutableEntry = new MutableEntry(mappedKey, mappedValue);
      mutableEntryRef.set(mutableEntry);

      T processResult;
      try {
        processResult = entryProcessor.process(mutableEntry, arguments);
      } catch (Exception e) {
        if (e instanceof EntryProcessorException) {
          throw new StorePassThroughException(e);
        }
        throw new StorePassThroughException(new EntryProcessorException(e));
      }

      invokeResult.set(processResult);

      return mutableEntry.apply(config.isWriteThrough(), cacheLoaderWriter);
    }, () -> {
      MutableEntry mutableEntry = mutableEntryRef.get();
      return mutableEntry.shouldReplace();
    }, () -> {
      MutableEntry mutableEntry = mutableEntryRef.get();
      return mutableEntry.shouldInvokeWriter();
    }, () -> {
      MutableEntry mutableEntry = mutableEntryRef.get();
      return mutableEntry.shouldGenerateEvent();
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

    Map<K, EntryProcessorResult<T>> results = new HashMap<>(keys.size());
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
    cacheManager.close(this);
  }

  @Override
  public boolean isClosed() {
    return syncedIsClose();
  }

  CacheException closeInternalAfter(CacheException failure) {
    if (hypotheticallyClosed.compareAndSet(false, true)) {
      return cacheResources.closeResourcesAfter(failure);
    } else {
      return failure;
    }
  }

  void closeInternal() {
    if (hypotheticallyClosed.compareAndSet(false, true)) {
      cacheResources.closeResources();
    }
  }

  private boolean syncedIsClose() {
    if (ehCache.getStatus() == Status.UNINITIALIZED && !hypotheticallyClosed.get()) {
      close();
    }
    return hypotheticallyClosed.get();
  }

  @Override
  public <T> T unwrap(Class<T> clazz) {
    return Unwrap.unwrap(clazz, this, ehCache);
  }

  @Override
  public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
    checkClosed();

    if (cacheEntryListenerConfiguration == null) {
      throw new NullPointerException();
    }

    ListenerResources<K, V> resources = cacheResources.registerCacheEntryListener(cacheEntryListenerConfiguration);
    config.addCacheEntryListenerConfiguration(cacheEntryListenerConfiguration);

    registerEhcacheListeners(cacheEntryListenerConfiguration, resources);
  }

  private void registerEhcacheListeners(CacheEntryListenerConfiguration<K, V> config, ListenerResources<K, V> resources) {
    final boolean synchronous = config.isSynchronous();
    final EventOrdering ordering = synchronous ? EventOrdering.ORDERED : EventOrdering.UNORDERED;
    final EventFiring firing = synchronous ? EventFiring.SYNCHRONOUS : EventFiring.ASYNCHRONOUS;

    final boolean requestsOld = config.isOldValueRequired();

    for (EventListenerAdaptor<K, V> ehcacheListener : resources.getEhcacheListeners(this, requestsOld)) {
      ehCache.getRuntimeConfiguration().registerCacheEventListener(ehcacheListener, ordering, firing,
          EnumSet.of(ehcacheListener.getEhcacheEventType()));
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
      config.removeCacheEntryListenerConfiguration(cacheEntryListenerConfiguration);

      for (EventListenerAdaptor<K, V> ehListener : resources.getEhcacheListeners(this,
          cacheEntryListenerConfiguration.isOldValueRequired())) {
        ehCache.getRuntimeConfiguration().deregisterCacheEventListener(ehListener);
      }
    }
  }

  @Override
  public Iterator<Entry<K, V>> iterator() {
    checkClosed();

    final Iterator<org.ehcache.Cache.Entry<K, V>> specIterator = jsr107Cache.specIterator();
    return new Iterator<Entry<K, V>>() {
      @Override
      public boolean hasNext() {
        checkClosed();
        return specIterator.hasNext();
      }

      @Override
      public Entry<K, V> next() {
        checkClosed();
        org.ehcache.Cache.Entry<K, V> next = specIterator.next();
        return next == null ? null : new WrappedEhcacheEntry<>(next);
      }

      @Override
      public void remove() {
        checkClosed();
        specIterator.remove();
      }
    };
  }

  private void checkClosed() {
    if (syncedIsClose()) {
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

  void setStatisticsEnabled(boolean enabled) {
    config.setStatisticsEnabled(enabled);
  }

  void setManagementEnabled(boolean enabled) {
    config.setManagementEnabled(enabled);
  }

  private static CacheLoaderException jsr107CacheLoaderException(CacheLoadingException e) {
    if (e.getCause() instanceof CacheLoaderException) {
      return (CacheLoaderException) e.getCause();
    }
    return new CacheLoaderException(e);
  }

  private static CacheWriterException jsr107CacheWriterException(CacheWritingException e) {
    if (e.getCause() instanceof CacheWriterException) {
      return (CacheWriterException) e.getCause();
    }
    throw new CacheWriterException(e);
  }

  private static <T> EntryProcessorResult<T> newEntryProcessorResult(final T result) {
    if (result == null) {
      throw new NullPointerException();
    }

    return () -> result;
  }

  private static <T> EntryProcessorResult<T> newErrorThrowingEntryProcessorResult(final Exception e) {
    return () -> {
      if (e instanceof EntryProcessorException) {
        throw (EntryProcessorException) e;
      }
      throw new EntryProcessorException(e);
    };
  }

  private static class WrappedEhcacheEntry<K, V> implements javax.cache.Cache.Entry<K, V> {

    private final org.ehcache.Cache.Entry<K, V> ehEntry;

    WrappedEhcacheEntry(org.ehcache.Cache.Entry<K, V> ehEntry) {
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
      return Unwrap.unwrap(clazz, this, ehEntry);
    }
  }

  private enum MutableEntryOperation {
    NONE, ACCESS, CREATE, LOAD, REMOVE, UPDATE
  }

  private static final Object UNDEFINED = new Object();

  private class MutableEntry implements javax.cache.processor.MutableEntry<K, V> {

    private final K key;
    private final V initialValue;
    private volatile V finalValue = undefined();
    private volatile MutableEntryOperation operation = MutableEntryOperation.NONE;

    MutableEntry(K key, V initialValue) {
      this.key = key;
      this.initialValue = initialValue;
    }

    @Override
    public K getKey() {
      return key;
    }

    @Override
    public V getValue() {
      if (finalValue == UNDEFINED) {
        if (initialValue == null && config.isReadThrough() && cacheLoaderWriter != null) {
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
        return cacheLoaderWriter.load(key);
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
      if (operation == MutableEntryOperation.CREATE) {
        operation = MutableEntryOperation.NONE;
      } else {
        operation = MutableEntryOperation.REMOVE;
      }
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

    V apply(boolean isWriteThrough, CacheLoaderWriter<? super K, ? super V> cacheLoaderWriter) {
      switch (operation) {
      case NONE:
      case ACCESS:
        return initialValue;
      case LOAD:
      case CREATE:
      case UPDATE:
        return finalValue;
      case REMOVE:
        return null;
      }

      throw new AssertionError("unhandled case: " + operation);
    }

    boolean shouldReplace() {
      switch (operation) {
      case NONE:
      case ACCESS:
        return false;
      case CREATE:
      case LOAD:
      case UPDATE:
      case REMOVE:
        return true;
      }

      throw new AssertionError("unhandled case: " + operation);
    }

    boolean shouldGenerateEvent() {
      switch (operation) {
      case NONE:
      case ACCESS:
      case LOAD:
        return false;
      case CREATE:
      case UPDATE:
      case REMOVE:
        return true;
      }

      throw new AssertionError("unhandled case: " + operation);
    }

    boolean shouldInvokeWriter() {
      switch (operation) {
      case NONE:
      case ACCESS:
      case LOAD:
        return false;
      case CREATE:
      case UPDATE:
      case REMOVE:
        return true;
      }

      throw new AssertionError("unhandled case: " + operation);
    }

    @SuppressWarnings("unchecked")
    private V undefined() {
      return (V) Eh107Cache.UNDEFINED;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
      throw new IllegalArgumentException();
    }
  }

}
