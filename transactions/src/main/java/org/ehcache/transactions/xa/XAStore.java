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
package org.ehcache.transactions.xa;

import org.ehcache.Cache;
import org.ehcache.CacheConfigurationChangeListener;
import org.ehcache.config.EvictionPrioritizer;
import org.ehcache.config.EvictionVeto;
import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.config.copy.CopierConfiguration;
import org.ehcache.config.copy.DefaultCopierConfiguration;
import org.ehcache.events.StoreEventListener;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expiry;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.function.NullaryFunction;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.TimeSourceService;
import org.ehcache.internal.concurrent.ConcurrentHashMap;
import org.ehcache.internal.copy.SerializingCopier;
import org.ehcache.internal.store.DefaultStoreProvider;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.copy.CopyProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.LocalPersistenceService;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.transactions.xa.commands.StoreEvictCommand;
import org.ehcache.transactions.xa.commands.StorePutCommand;
import org.ehcache.transactions.xa.commands.StoreRemoveCommand;
import org.ehcache.transactions.xa.configuration.XAStoreConfiguration;
import org.ehcache.transactions.xa.journal.Journal;
import org.ehcache.transactions.xa.journal.JournalProvider;
import org.ehcache.transactions.xa.txmgr.TransactionManagerWrapper;
import org.ehcache.transactions.xa.txmgr.provider.TransactionManagerProvider;
import org.ehcache.util.ConcurrentWeakIdentityHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.ehcache.spi.ServiceLocator.findAmongst;
import static org.ehcache.spi.ServiceLocator.findSingletonAmongst;

/**
 * A {@link Store} implementation wrapping another {@link Store} driven by a JTA
 * {@link javax.transaction.TransactionManager} using the XA 2-phase commit protocol.
 *
 * @author Ludovic Orban
 */
public class XAStore<K, V> implements Store<K, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(XAStore.class);
  private final Class<K> keyType;
  private final Class<V> valueType;
  private final Store<K, SoftLock<V>> underlyingStore;
  private final TransactionManagerWrapper transactionManagerWrapper;
  private final Map<Transaction, EhcacheXAResource<K, V>> xaResources = new ConcurrentHashMap<Transaction, EhcacheXAResource<K, V>>();
  private final TimeSource timeSource;
  private final Journal<K> journal;
  private final String uniqueXAResourceId;
  private final XATransactionContextFactory<K, V> transactionContextFactory;
  private final EhcacheXAResource recoveryXaResource;

  public XAStore(Class<K> keyType, Class<V> valueType, Store<K, SoftLock<V>> underlyingStore, TransactionManagerWrapper transactionManagerWrapper,
                 TimeSource timeSource, Journal<K> journal, String uniqueXAResourceId) {
    this.keyType = keyType;
    this.valueType = valueType;
    this.underlyingStore = underlyingStore;
    this.transactionManagerWrapper = transactionManagerWrapper;
    this.timeSource = timeSource;
    this.journal = journal;
    this.uniqueXAResourceId = uniqueXAResourceId;
    this.transactionContextFactory = new XATransactionContextFactory<K, V>(timeSource);
    this.recoveryXaResource = new EhcacheXAResource<K, V>(underlyingStore, journal, transactionContextFactory);
  }

  private boolean isInDoubt(SoftLock<V> softLock) {
    return softLock.getTransactionId() != null;
  }

  private ValueHolder<SoftLock<V>> getSoftLockValueHolderFromUnderlyingStore(K key) throws CacheAccessException {
    return underlyingStore.get(key);
  }

  private XATransactionContext<K, V> getCurrentContext() {
    try {
      final Transaction transaction = transactionManagerWrapper.getTransactionManager().getTransaction();
      if (transaction == null) {
        throw new XACacheException("Cannot access XA cache outside of XA transaction scope");
      }
      EhcacheXAResource<K, V> xaResource = xaResources.get(transaction);
      if (xaResource == null) {
        xaResource = new EhcacheXAResource<K, V>(underlyingStore, journal, transactionContextFactory);
        transactionManagerWrapper.registerXAResource(uniqueXAResourceId, xaResource);
        transactionManagerWrapper.getTransactionManager().getTransaction().enlistResource(xaResource);
        xaResources.put(transaction, xaResource);
        final EhcacheXAResource<K, V> finalXaResource = xaResource;
        transaction.registerSynchronization(new Synchronization() {
          @Override
          public void beforeCompletion() {
          }

          @Override
          public void afterCompletion(int status) {
            transactionManagerWrapper.unregisterXAResource(uniqueXAResourceId, finalXaResource);
            xaResources.remove(transaction);
          }
        });
      }
      XATransactionContext<K, V> currentContext = xaResource.getCurrentContext();
      if (currentContext.hasTimedOut()) {
        throw new XACacheException("Current XA transaction has timed out");
      }
      return currentContext;
    } catch (SystemException se) {
      throw new XACacheException("Cannot get current XA transaction", se);
    } catch (RollbackException re) {
      throw new XACacheException("XA Transaction has been marked for rollback only", re);
    }
  }

  private static boolean eq(Object o1, Object o2) {
    return (o1 == o2) || (o1 != null && o1.equals(o2));
  }

  private void checkKey(K keyObject) {
    if (keyObject == null) {
      throw new NullPointerException();
    }
    if (!keyType.isAssignableFrom(keyObject.getClass())) {
      throw new ClassCastException("Invalid key type, expected : " + keyType.getName() + " but was : " + keyObject.getClass().getName());
    }
  }

  private void checkValue(V valueObject) {
    if (valueObject == null) {
      throw new NullPointerException();
    }
    if (!valueType.isAssignableFrom(valueObject.getClass())) {
      throw new ClassCastException("Invalid value type, expected : " + valueType.getName() + " but was : " + valueObject.getClass().getName());
    }
  }

  private static final NullaryFunction<Boolean> REPLACE_EQUALS_TRUE = new NullaryFunction<Boolean>() {
    @Override
    public Boolean apply() {
      return Boolean.TRUE;
    }
  };


  @Override
  public ValueHolder<V> get(K key) throws CacheAccessException {
    checkKey(key);
    XATransactionContext<K, V> currentContext = getCurrentContext();

    if (currentContext.removed(key)) {
      return null;
    }
    XAValueHolder<V> newValueHolder = currentContext.newValueHolderOf(key);
    if (newValueHolder != null) {
      return newValueHolder;
    }

    ValueHolder<SoftLock<V>> softLockValueHolder = getSoftLockValueHolderFromUnderlyingStore(key);
    if (softLockValueHolder == null) {
      return null;
    }

    SoftLock<V> softLock = softLockValueHolder.value();
    if (isInDoubt(softLock)) {
      currentContext.addCommand(key, new StoreEvictCommand<V>(softLock.getOldValue()));
      return null;
    }

    return new XAValueHolder<V>(softLockValueHolder, softLock.getOldValue());
  }

  @Override
  public boolean containsKey(K key) throws CacheAccessException {
    checkKey(key);
    if (getCurrentContext().touched(key)) {
      return getCurrentContext().newValueHolderOf(key) != null;
    }
    ValueHolder<SoftLock<V>> softLockValueHolder = getSoftLockValueHolderFromUnderlyingStore(key);
    return softLockValueHolder != null && softLockValueHolder.value().getTransactionId() == null && softLockValueHolder.value().getOldValue() != null;
  }

  @Override
  public void put(K key, V value) throws CacheAccessException {
    checkKey(key);
    checkValue(value);
    XATransactionContext<K, V> currentContext = getCurrentContext();
    if (currentContext.touched(key)) {
      V oldValue = currentContext.oldValueOf(key);
      currentContext.addCommand(key, new StorePutCommand<V>(oldValue, new XAValueHolder<V>(value, timeSource.getTimeMillis())));
      return;
    }

    ValueHolder<SoftLock<V>> softLockValueHolder = getSoftLockValueHolderFromUnderlyingStore(key);
    if (softLockValueHolder != null) {
      SoftLock<V> softLock = softLockValueHolder.value();
      if (isInDoubt(softLock)) {
        currentContext.addCommand(key, new StoreEvictCommand<V>(softLock.getOldValue()));
      } else {
        currentContext.addCommand(key, new StorePutCommand<V>(softLock.getOldValue(), new XAValueHolder<V>(value, timeSource.getTimeMillis())));
      }
    } else {
      currentContext.addCommand(key, new StorePutCommand<V>(null, new XAValueHolder<V>(value, timeSource.getTimeMillis())));
    }
  }

  @Override
  public ValueHolder<V> putIfAbsent(K key, V value) throws CacheAccessException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void remove(K key) throws CacheAccessException {
    checkKey(key);
    XATransactionContext<K, V> currentContext = getCurrentContext();
    if (currentContext.touched(key)) {
      V oldValue = currentContext.oldValueOf(key);
      currentContext.addCommand(key, new StoreRemoveCommand<V>(oldValue));
      return;
    }

    ValueHolder<SoftLock<V>> softLockValueHolder = getSoftLockValueHolderFromUnderlyingStore(key);
    if (softLockValueHolder != null) {
      SoftLock<V> softLock = softLockValueHolder.value();
      if (isInDoubt(softLock)) {
        currentContext.addCommand(key, new StoreEvictCommand<V>(softLock.getOldValue()));
      } else {
        currentContext.addCommand(key, new StoreRemoveCommand<V>(softLock.getOldValue()));
      }
    }
  }

  @Override
  public boolean remove(K key, V value) throws CacheAccessException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ValueHolder<V> replace(K key, V value) throws CacheAccessException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) throws CacheAccessException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() throws CacheAccessException {
    // we don't want that to be transactional
    underlyingStore.clear();
  }

  @Override
  public void enableStoreEventNotifications(final StoreEventListener<K, V> listener) {
    underlyingStore.enableStoreEventNotifications(new StoreEventListener<K, SoftLock<V>>() {
      @Override
      public void onEviction(K key, ValueHolder<SoftLock<V>> valueHolder) {
        SoftLock<V> softLock = valueHolder.value();
        if (softLock.getTransactionId() == null || softLock.getOldValue() != null) {
          listener.onEviction(key, new XAValueHolder<V>(valueHolder, softLock.getOldValue()));
        } else {
          listener.onEviction(key, new XAValueHolder<V>(valueHolder, softLock.getNewValueHolder().value()));
        }
      }

      @Override
      public void onExpiration(K key, ValueHolder<SoftLock<V>> valueHolder) {
        SoftLock<V> softLock = valueHolder.value();
        if (softLock.getTransactionId() == null || softLock.getOldValue() != null) {
          listener.onEviction(key, new XAValueHolder<V>(valueHolder, softLock.getOldValue()));
        } else {
          listener.onEviction(key, new XAValueHolder<V>(valueHolder, softLock.getNewValueHolder().value()));
        }
      }
    });
  }

  @Override
  public void disableStoreEventNotifications() {
    underlyingStore.disableStoreEventNotifications();
  }

  @Override
  public Iterator<Cache.Entry<K, ValueHolder<V>>> iterator() {
    XATransactionContext<K, V> currentContext = getCurrentContext();
    Map<K, XAValueHolder<V>> valueHolderMap = transactionContextFactory.listPuts(currentContext.getTransactionId());
    return new XAIterator(valueHolderMap, underlyingStore.iterator(), currentContext.getTransactionId());
  }

  class XAIterator implements Iterator<Cache.Entry<K, ValueHolder<V>>> {

    private final java.util.Iterator<Map.Entry<K, XAValueHolder<V>>> iterator;
    private final Iterator<Cache.Entry<K, ValueHolder<SoftLock<V>>>> underlyingIterator;
    private final TransactionId transactionId;
    private Cache.Entry<K, ValueHolder<V>> next;
    private CacheAccessException prefetchFailure = null;

    XAIterator(Map<K, XAValueHolder<V>> valueHolderMap, Iterator<Cache.Entry<K, ValueHolder<SoftLock<V>>>> underlyingIterator, TransactionId transactionId) {
      this.transactionId = transactionId;
      this.iterator = valueHolderMap.entrySet().iterator();
      this.underlyingIterator = underlyingIterator;
      advance();
    }

    void advance() {
      next = null;

      if (iterator.hasNext()) {
        final Map.Entry<K, XAValueHolder<V>> entry = iterator.next();
        this.next = new Cache.Entry<K, ValueHolder<V>>() {
          @Override
          public K getKey() {
            return entry.getKey();
          }

          @Override
          public ValueHolder<V> getValue() {
            return entry.getValue();
          }

          @Override
          public long getCreationTime(TimeUnit unit) {
            return entry.getValue().creationTime(unit);
          }

          @Override
          public long getLastAccessTime(TimeUnit unit) {
            return entry.getValue().lastAccessTime(unit);
          }

          @Override
          public float getHitRate(TimeUnit unit) {
            return entry.getValue().hitRate(timeSource.getTimeMillis(), unit);
          }
        };
        return;
      }

      while (underlyingIterator.hasNext()) {
        final Cache.Entry<K, ValueHolder<SoftLock<V>>> next;
        try {
          next = underlyingIterator.next();
        } catch (CacheAccessException e) {
          prefetchFailure = e;
          break;
        }

        if (!transactionContextFactory.isTouched(transactionId, next.getKey())) {
          ValueHolder<SoftLock<V>> valueHolder = next.getValue();
          SoftLock<V> softLock = valueHolder.value();
          final XAValueHolder<V> xaValueHolder;
          if (softLock.getTransactionId() == transactionId) {
            xaValueHolder = new XAValueHolder<V>(valueHolder, softLock.getNewValueHolder().value());
          } else if (isInDoubt(softLock)) {
            continue;
          } else {
            xaValueHolder = new XAValueHolder<V>(valueHolder, softLock.getOldValue());
          }
          this.next = new Cache.Entry<K, ValueHolder<V>>() {
            @Override
            public K getKey() {
              return next.getKey();
            }

            @Override
            public ValueHolder<V> getValue() {
              return xaValueHolder;
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
          break;
        }
      }
    }

    @Override
    public boolean hasNext() {
      if (!getCurrentContext().getTransactionId().equals(transactionId)) {
        throw new IllegalStateException("Iterator has been created in another transaction, it can only be used in the transaction it has been created in.");
      }
      return next != null | prefetchFailure != null;
    }

    @Override
    public Cache.Entry<K, ValueHolder<V>> next() throws CacheAccessException {
      if(prefetchFailure != null) {
        throw prefetchFailure;
      }
      if (!getCurrentContext().getTransactionId().equals(transactionId)) {
        throw new IllegalStateException("Iterator has been created in another transaction, it can only be used in the transaction it has been created in.");
      }
      if (next == null) {
        throw new NoSuchElementException();
      }
      Cache.Entry<K, ValueHolder<V>> rc = next;
      advance();
      return rc;
    }
  }

  @Override
  public ValueHolder<V> compute(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction, NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
    checkKey(key);
    XATransactionContext<K, V> currentContext = getCurrentContext();
    if (currentContext.touched(key)) {
      return updateCommandForKey(key, mappingFunction, replaceEqual, currentContext);
    }

    ValueHolder<SoftLock<V>> softLockValueHolder = getSoftLockValueHolderFromUnderlyingStore(key);

    SoftLock<V> softLock = softLockValueHolder == null ? null : softLockValueHolder.value();
    V oldValue = softLock == null ? null : softLock.getOldValue();
    V newValue = mappingFunction.apply(key, oldValue);
    XAValueHolder<V> xaValueHolder = newValue == null ? null : new XAValueHolder<V>(newValue, timeSource.getTimeMillis());
    if (eq(oldValue, newValue) && !replaceEqual.apply()) {
      return xaValueHolder;
    }
    if (newValue != null) {
      checkValue(newValue);
    }

    if (softLock != null && isInDoubt(softLock)) {
      currentContext.addCommand(key, new StoreEvictCommand<V>(oldValue));
    } else {
      if (xaValueHolder == null) {
        if (oldValue != null) {
          currentContext.addCommand(key, new StoreRemoveCommand<V>(oldValue));
        }
      } else {
        currentContext.addCommand(key, new StorePutCommand<V>(oldValue, xaValueHolder));
      }
    }

    return xaValueHolder;
  }

  @Override
  public ValueHolder<V> compute(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction) throws CacheAccessException {
    return compute(key, mappingFunction, REPLACE_EQUALS_TRUE);
  }

  @Override
  public ValueHolder<V> computeIfAbsent(K key, final Function<? super K, ? extends V> mappingFunction) throws CacheAccessException {
    checkKey(key);
    XATransactionContext<K, V> currentContext = getCurrentContext();
    if (currentContext.removed(key)) {
      return updateCommandForKey(key, mappingFunction, currentContext);
    }
    if (currentContext.evicted(key)) {
      return new XAValueHolder<V>(currentContext.oldValueOf(key), timeSource.getTimeMillis());
    }
    boolean updated = currentContext.touched(key);

    XAValueHolder<V> xaValueHolder;
    ValueHolder<SoftLock<V>> softLockValueHolder = getSoftLockValueHolderFromUnderlyingStore(key);
    if (softLockValueHolder == null) {
      if (updated) {
        xaValueHolder = null;
      } else {
        V computed = mappingFunction.apply(key);
        if (computed != null) {
          xaValueHolder = new XAValueHolder<V>(computed, timeSource.getTimeMillis());
          currentContext.addCommand(key, new StorePutCommand<V>(null, xaValueHolder));
        } else {
          xaValueHolder = null;
        }
      }
    } else if (isInDoubt(softLockValueHolder.value())) {
      currentContext.addCommand(key, new StoreEvictCommand<V>(softLockValueHolder.value().getOldValue()));
      xaValueHolder = new XAValueHolder<V>(softLockValueHolder, softLockValueHolder.value().getNewValueHolder().value());
    } else {
      xaValueHolder = new XAValueHolder<V>(softLockValueHolder, softLockValueHolder.value().getOldValue());
    }

    return xaValueHolder;
  }

  @Override
  public ValueHolder<V> computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) throws CacheAccessException {
    return computeIfPresent(key, remappingFunction, REPLACE_EQUALS_TRUE);
  }

  @Override
  public ValueHolder<V> computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
    checkKey(key);
    XATransactionContext<K, V> currentContext = getCurrentContext();
    if (currentContext.updated(key)) {
      return updateCommandForKey(key, remappingFunction, replaceEqual, currentContext);
    }
    if (currentContext.evicted(key)) {
      return null;
    }
    boolean removed = currentContext.touched(key);

    ValueHolder<SoftLock<V>> softLockValueHolder = getSoftLockValueHolderFromUnderlyingStore(key);

    XAValueHolder<V> xaValueHolder;
    SoftLock<V> softLock = softLockValueHolder == null ? null : softLockValueHolder.value();
    V oldValue = softLock == null ? null : softLock.getOldValue();

    if (softLock != null && isInDoubt(softLock)) {
      currentContext.addCommand(key, new StoreEvictCommand<V>(oldValue));
      xaValueHolder = null;
    } else if (softLock == null) {
      xaValueHolder = null;
    } else if (removed) {
      xaValueHolder = null;
    } else {
      V newValue = remappingFunction.apply(key, oldValue);
      if (newValue != null) {
        checkValue(newValue);
        xaValueHolder = new XAValueHolder<V>(newValue, timeSource.getTimeMillis());
      } else {
        xaValueHolder = null;
      }
      currentContext.addCommand(key, new StorePutCommand<V>(oldValue, xaValueHolder));
    }

    return xaValueHolder;
  }

  private ValueHolder<V> updateCommandForKey(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction, NullaryFunction<Boolean> replaceEqual, XATransactionContext<K, V> currentContext) {
    V newValue = mappingFunction.apply(key, currentContext.newValueOf(key));
    XAValueHolder<V> xaValueHolder = null;
    V oldValue = currentContext.oldValueOf(key);
    if (newValue == null) {
      if (!(oldValue == null && !replaceEqual.apply())) {
        currentContext.addCommand(key, new StoreRemoveCommand<V>(oldValue));
      }
    } else {
      checkValue(newValue);
      xaValueHolder = new XAValueHolder<V>(newValue, timeSource.getTimeMillis());
      if (!(eq(oldValue, newValue) && !replaceEqual.apply())) {
        currentContext.addCommand(key, new StorePutCommand<V>(oldValue, xaValueHolder));
      }
    }
    return xaValueHolder;
  }

  private ValueHolder<V> updateCommandForKey(K key, Function<? super K, ? extends V> mappingFunction, XATransactionContext<K, V> currentContext) {
    V computed = mappingFunction.apply(key);
    XAValueHolder<V> xaValueHolder = null;
    if (computed != null) {
      checkValue(computed);
      xaValueHolder = new XAValueHolder<V>(computed, timeSource.getTimeMillis());
      V oldValue = currentContext.oldValueOf(key);
      currentContext.addCommand(key, new StorePutCommand<V>(oldValue, xaValueHolder));
    } // else do nothing
    return xaValueHolder;
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction) throws CacheAccessException {
    return bulkCompute(keys, remappingFunction, REPLACE_EQUALS_TRUE);
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, final Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction, NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
    Map<K, ValueHolder<V>> result = new HashMap<K, ValueHolder<V>>();
    for (K key : keys) {
      checkKey(key);

      final ValueHolder<V> newValue = compute(key, new BiFunction<K, V, V>() {
        @Override
        public V apply(final K k, final V oldValue) {
          final Set<Map.Entry<K, V>> entrySet = Collections.singletonMap(k, oldValue).entrySet();
          final Iterable<? extends Map.Entry<? extends K, ? extends V>> entries = remappingFunction.apply(entrySet);
          final java.util.Iterator<? extends Map.Entry<? extends K, ? extends V>> iterator = entries.iterator();
          final Map.Entry<? extends K, ? extends V> next = iterator.next();

          K key = next.getKey();
          V value = next.getValue();
          checkKey(key);
          if (value != null) {
            checkValue(value);
          }
          return value;
        }
      }, replaceEqual);
      result.put(key, newValue);
    }
    return result;
  }

  @Override
  public Map<K, ValueHolder<V>> bulkComputeIfAbsent(Set<? extends K> keys, final Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction) throws CacheAccessException {
    Map<K, ValueHolder<V>> result = new HashMap<K, ValueHolder<V>>();

    for (final K key : keys) {
      final ValueHolder<V> newValue = computeIfAbsent(key, new Function<K, V>() {
        @Override
        public V apply(final K k) {
          final Iterable<K> keySet = Collections.singleton(k);
          final Iterable<? extends Map.Entry<? extends K, ? extends V>> entries = mappingFunction.apply(keySet);
          final java.util.Iterator<? extends Map.Entry<? extends K, ? extends V>> iterator = entries.iterator();
          final Map.Entry<? extends K, ? extends V> next = iterator.next();

          K computedKey = next.getKey();
          V computedValue = next.getValue();
          checkKey(computedKey);
          if (computedValue == null) {
            return null;
          }

          checkValue(computedValue);
          return computedValue;
        }
      });
      result.put(key, newValue);
    }
    return result;
  }

  @Override
  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
    return underlyingStore.getConfigurationChangeListeners();
  }

  private static final class SoftLockValueCombinedSerializerLifecycleHelper {
    final AtomicReference<SoftLockSerializer> softLockSerializerRef;
    final ClassLoader classLoader;

    <K, V> SoftLockValueCombinedSerializerLifecycleHelper(AtomicReference<SoftLockSerializer> softLockSerializerRef, ClassLoader classLoader) {
      this.softLockSerializerRef = softLockSerializerRef;
      this.classLoader = classLoader;
    }
  }

  @ServiceDependencies({TransactionManagerProvider.class, TimeSourceService.class, JournalProvider.class, CopyProvider.class, DefaultStoreProvider.class})
  public static class Provider implements Store.Provider {

    private volatile ServiceProvider serviceProvider;
    private volatile Store.Provider underlyingStoreProvider;
    private volatile TransactionManagerProvider transactionManagerProvider;
    private final Map<Store<?, ?>, SoftLockValueCombinedSerializerLifecycleHelper> createdStores = new ConcurrentWeakIdentityHashMap<Store<?, ?>, SoftLockValueCombinedSerializerLifecycleHelper>();

    @Override
    public <K, V> Store<K, V> createStore(Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      XAStoreConfiguration xaServiceConfiguration = findSingletonAmongst(XAStoreConfiguration.class, (Object[]) serviceConfigs);
      Store<K, V> store;
      SoftLockValueCombinedSerializerLifecycleHelper helper;
      if (xaServiceConfiguration == null) {
        // non-tx cache
        store = underlyingStoreProvider.createStore(storeConfig, serviceConfigs);
        helper = new SoftLockValueCombinedSerializerLifecycleHelper(null, null);
      } else {
        String uniqueXAResourceId = xaServiceConfiguration.getUniqueXAResourceId();
        List<ServiceConfiguration<?>> underlyingServiceConfigs = new ArrayList<ServiceConfiguration<?>>();
        underlyingServiceConfigs.addAll(Arrays.asList(serviceConfigs));

        // TODO: do we want to support pluggable veto and prioritizer?

        // eviction veto
        EvictionVeto<? super K, ? super SoftLock> evictionVeto = new EvictionVeto<K, SoftLock>() {
          @Override
          public boolean test(Cache.Entry<K, SoftLock> argument) {
            return argument.getValue().getTransactionId() != null;
          }
        };

        // eviction prioritizer
        EvictionPrioritizer<? super K, ? super SoftLock> evictionPrioritizer = new EvictionPrioritizer<K, SoftLock>() {
          @Override
          public int compare(Cache.Entry<K, SoftLock> o1, Cache.Entry<K, SoftLock> o2) {
            if (o1.getValue().getTransactionId() != null && o2.getValue().getTransactionId() != null) {
              return 0;
            }
            if (o1.getValue().getTransactionId() == null && o2.getValue().getTransactionId() == null) {
              return 0;
            }
            return o1.getValue().getTransactionId() != null ? 1 : -1;
          }
        };

        // expiry
        final Expiry<? super K, ? super V> configuredExpiry = storeConfig.getExpiry();
        Expiry<? super K, ? super SoftLock> expiry = new Expiry<K, SoftLock>() {
          @Override
          public Duration getExpiryForCreation(K key, SoftLock softLock) {
            if (softLock.getTransactionId() != null) {
              // phase 1 prepare, create -> forever
              return Duration.FOREVER;
            } else {
              // phase 2 commit, or during a TX's lifetime, create -> some time
              Duration duration;
              try {
                duration = configuredExpiry.getExpiryForCreation(key, (V) softLock.getOldValue());
              } catch (RuntimeException re) {
                LOGGER.error("Expiry caused an exception ", re);
                return Duration.ZERO;
              }
              return duration;
            }
          }

          @Override
          public Duration getExpiryForAccess(K key, SoftLock softLock) {
            if (softLock.getTransactionId() != null) {
              // phase 1 prepare, access -> forever
              return Duration.FOREVER;
            } else {
              // phase 2 commit, or during a TX's lifetime, access -> some time
              Duration duration;
              try {
                duration = configuredExpiry.getExpiryForAccess(key, (V) softLock.getOldValue());
              } catch (RuntimeException re) {
                LOGGER.error("Expiry caused an exception ", re);
                return Duration.ZERO;
              }
              return duration;
            }
          }

          @Override
          public Duration getExpiryForUpdate(K key, SoftLock oldSoftLock, SoftLock newSoftLock) {
            if (oldSoftLock.getTransactionId() == null) {
              // phase 1 prepare, update -> forever
              return Duration.FOREVER;
            } else {
              // phase 2 commit, or during a TX's lifetime
              if (oldSoftLock.getOldValue() == null) {
                // there is no old value -> it's a CREATE, update -> create -> some time
                Duration duration;
                try {
                  duration = configuredExpiry.getExpiryForCreation(key, (V) oldSoftLock.getOldValue());
                } catch (RuntimeException re) {
                  LOGGER.error("Expiry caused an exception ", re);
                  return Duration.ZERO;
                }
                return duration;
              } else {
                // there is an old value -> it's an UPDATE, update -> some time
                V value = oldSoftLock.getNewValueHolder() == null ? null : (V) oldSoftLock.getNewValueHolder().value();
                Duration duration;
                try {
                  duration = configuredExpiry.getExpiryForUpdate(key, (V) oldSoftLock.getOldValue(), value);
                } catch (RuntimeException re) {
                  LOGGER.error("Expiry caused an exception ", re);
                  return Duration.ZERO;
                }
                return duration;
              }
            }
          }
        };

        // get the PersistenceSpaceIdentifier if the cache is persistent, null otherwise
        LocalPersistenceService.PersistenceSpaceIdentifier persistenceSpaceId = findSingletonAmongst(LocalPersistenceService.PersistenceSpaceIdentifier.class, serviceConfigs);

        // find the copiers
        Collection<DefaultCopierConfiguration> copierConfigs = findAmongst(DefaultCopierConfiguration.class, underlyingServiceConfigs.toArray());
        DefaultCopierConfiguration keyCopierConfig = null;
        DefaultCopierConfiguration valueCopierConfig = null;
        for (DefaultCopierConfiguration copierConfig : copierConfigs) {
          if (copierConfig.getType().equals(CopierConfiguration.Type.KEY)) {
            keyCopierConfig = copierConfig;
          } else if (copierConfig.getType().equals(CopierConfiguration.Type.VALUE)) {
            valueCopierConfig = copierConfig;
          }
          underlyingServiceConfigs.remove(copierConfig);
        }

        // force-in a key copier if none is configured
        if (keyCopierConfig == null) {
          underlyingServiceConfigs.add(new DefaultCopierConfiguration<K>((Class) SerializingCopier.class, CopierConfiguration.Type.KEY));
        } else {
          underlyingServiceConfigs.add(keyCopierConfig);
        }

        // force-in a value copier if none is configured, or wrap the configured one in a soft lock copier
        if (valueCopierConfig == null) {
          underlyingServiceConfigs.add(new DefaultCopierConfiguration<K>((Class) SerializingCopier.class, CopierConfiguration.Type.VALUE));
        } else {
          CopyProvider copyProvider = serviceProvider.getService(CopyProvider.class);
          Copier valueCopier = copyProvider.createValueCopier(storeConfig.getValueType(), storeConfig.getValueSerializer(), valueCopierConfig);
          SoftLockValueCombinedCopier<V> softLockValueCombinedCopier = new SoftLockValueCombinedCopier<V>(valueCopier);
          underlyingServiceConfigs.add(new DefaultCopierConfiguration<K>((Copier) softLockValueCombinedCopier, CopierConfiguration.Type.VALUE));
        }

        // lookup the required XAStore services
        Journal<K> journal = serviceProvider.getService(JournalProvider.class).getJournal(persistenceSpaceId, storeConfig.getKeySerializer());
        TimeSource timeSource = serviceProvider.getService(TimeSourceService.class).getTimeSource();

        // create the soft lock serializer
        AtomicReference<Serializer<SoftLock<V>>> softLockSerializerRef = new AtomicReference<Serializer<SoftLock<V>>>();
        SoftLockValueCombinedSerializer softLockValueCombinedSerializer = new SoftLockValueCombinedSerializer<V>(softLockSerializerRef, storeConfig.getValueSerializer());

        // create the underlying store
        Store.Configuration<K, SoftLock> underlyingStoreConfig = new StoreConfigurationImpl<K, SoftLock>(storeConfig.getKeyType(), SoftLock.class, evictionVeto, evictionPrioritizer,
            storeConfig.getClassLoader(), expiry, storeConfig.getResourcePools(), storeConfig.getKeySerializer(), softLockValueCombinedSerializer);
        Store<K, SoftLock<V>> underlyingStore = (Store) underlyingStoreProvider.createStore(underlyingStoreConfig,  underlyingServiceConfigs.toArray(new ServiceConfiguration[0]));

        // create the XA store
        TransactionManagerWrapper transactionManagerWrapper = transactionManagerProvider.getTransactionManagerWrapper();
        store = new XAStore<K, V>(storeConfig.getKeyType(), storeConfig.getValueType(), underlyingStore, transactionManagerWrapper, timeSource, journal, uniqueXAResourceId);

        // create the softLockSerializer lifecycle helper
        helper = new SoftLockValueCombinedSerializerLifecycleHelper((AtomicReference) softLockSerializerRef, storeConfig.getClassLoader());
      }

      createdStores.put(store, helper);
      return store;
    }

    @Override
    public void releaseStore(Store<?, ?> resource) {
      SoftLockValueCombinedSerializerLifecycleHelper helper = createdStores.remove(resource);
      if (helper == null) {
        throw new IllegalArgumentException("Given store is not managed by this provider : " + resource);
      }
      if (resource instanceof XAStore) {
        XAStore<?, ?> xaStore = (XAStore<?, ?>) resource;

        xaStore.transactionManagerWrapper.unregisterXAResource(xaStore.uniqueXAResourceId, xaStore.recoveryXaResource);
        // release the underlying store first, as it may still need the serializer to flush down to lower tiers
        underlyingStoreProvider.releaseStore(xaStore.underlyingStore);
        helper.softLockSerializerRef.set(null);
        try {
          xaStore.journal.close();
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      } else {
        underlyingStoreProvider.releaseStore(resource);
      }
    }

    @Override
    public void initStore(Store<?, ?> resource) {
      SoftLockValueCombinedSerializerLifecycleHelper helper = createdStores.get(resource);
      if (helper == null) {
        throw new IllegalArgumentException("Given store is not managed by this provider : " + resource);
      }
      if (resource instanceof XAStore) {
        XAStore<?, ?> xaStore = (XAStore<?, ?>) resource;

        underlyingStoreProvider.initStore(xaStore.underlyingStore);
        helper.softLockSerializerRef.set(new SoftLockSerializer(helper.classLoader));
        try {
          xaStore.journal.open();
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
        xaStore.transactionManagerWrapper.registerXAResource(xaStore.uniqueXAResourceId, xaStore.recoveryXaResource);
      } else {
        underlyingStoreProvider.initStore(resource);
      }
    }

    @Override
    public void start(ServiceProvider serviceProvider) {
      this.serviceProvider = serviceProvider;
      this.underlyingStoreProvider = serviceProvider.getService(DefaultStoreProvider.class);
      this.transactionManagerProvider = serviceProvider.getService(TransactionManagerProvider.class);
    }

    @Override
    public void stop() {
      this.transactionManagerProvider = null;
      this.underlyingStoreProvider = null;
      this.serviceProvider = null;
    }
  }

}
