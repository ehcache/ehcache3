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

package org.ehcache.transactions.xa.internal;

import org.ehcache.Cache;
import org.ehcache.ValueSupplier;
import org.ehcache.config.ResourceType;
import org.ehcache.core.CacheConfigurationChangeListener;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.core.internal.store.StoreConfigurationImpl;
import org.ehcache.core.internal.store.StoreSupport;
import org.ehcache.core.spi.store.StoreAccessException;
import org.ehcache.impl.config.copy.DefaultCopierConfiguration;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expiry;
import org.ehcache.core.spi.function.BiFunction;
import org.ehcache.core.spi.function.Function;
import org.ehcache.core.spi.function.NullaryFunction;
import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.ehcache.impl.copy.SerializingCopier;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.spi.time.TimeSourceService;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.events.StoreEventSource;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.copy.CopyProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.core.spi.service.LocalPersistenceService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.transactions.xa.XACacheException;
import org.ehcache.transactions.xa.internal.commands.StoreEvictCommand;
import org.ehcache.transactions.xa.internal.commands.StorePutCommand;
import org.ehcache.transactions.xa.internal.commands.StoreRemoveCommand;
import org.ehcache.transactions.xa.configuration.XAStoreConfiguration;
import org.ehcache.transactions.xa.internal.journal.Journal;
import org.ehcache.transactions.xa.internal.journal.JournalProvider;
import org.ehcache.transactions.xa.txmgr.TransactionManagerWrapper;
import org.ehcache.transactions.xa.txmgr.provider.TransactionManagerProvider;
import org.ehcache.core.internal.util.ConcurrentWeakIdentityHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;

import static org.ehcache.core.internal.service.ServiceLocator.findAmongst;
import static org.ehcache.core.internal.service.ServiceLocator.findSingletonAmongst;
import static org.ehcache.core.internal.util.ValueSuppliers.supplierOf;

/**
 * A {@link Store} implementation wrapping another {@link Store} driven by a JTA
 * {@link javax.transaction.TransactionManager} using the XA 2-phase commit protocol.
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
  private final StoreEventSourceWrapper<K, V> eventSourceWrapper;

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
    this.eventSourceWrapper = new StoreEventSourceWrapper<K, V>(underlyingStore.getStoreEventSource());
  }

  private static boolean isInDoubt(SoftLock<?> softLock) {
    return softLock.getTransactionId() != null;
  }

  private ValueHolder<SoftLock<V>> getSoftLockValueHolderFromUnderlyingStore(K key) throws StoreAccessException {
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
  public ValueHolder<V> get(K key) throws StoreAccessException {
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
  public boolean containsKey(K key) throws StoreAccessException {
    checkKey(key);
    if (getCurrentContext().touched(key)) {
      return getCurrentContext().newValueHolderOf(key) != null;
    }
    ValueHolder<SoftLock<V>> softLockValueHolder = getSoftLockValueHolderFromUnderlyingStore(key);
    return softLockValueHolder != null && softLockValueHolder.value().getTransactionId() == null && softLockValueHolder.value().getOldValue() != null;
  }

  @Override
  public PutStatus put(K key, V value) throws StoreAccessException {
    checkKey(key);
    checkValue(value);
    XATransactionContext<K, V> currentContext = getCurrentContext();
    if (currentContext.touched(key)) {
      V oldValue = currentContext.oldValueOf(key);
      V newValue = currentContext.newValueOf(key);
      currentContext.addCommand(key, new StorePutCommand<V>(oldValue, new XAValueHolder<V>(value, timeSource.getTimeMillis())));
      return newValue == null ? PutStatus.PUT : PutStatus.UPDATE;
    }

    ValueHolder<SoftLock<V>> softLockValueHolder = getSoftLockValueHolderFromUnderlyingStore(key);
    PutStatus status = PutStatus.NOOP;
    if (softLockValueHolder != null) {
      SoftLock<V> softLock = softLockValueHolder.value();
      if (isInDoubt(softLock)) {
        currentContext.addCommand(key, new StoreEvictCommand<V>(softLock.getOldValue()));
      } else {
        if (currentContext.addCommand(key, new StorePutCommand<V>(softLock.getOldValue(), new XAValueHolder<V>(value, timeSource.getTimeMillis())))) {
          status = PutStatus.UPDATE;
        }
      }
    } else {
      if (currentContext.addCommand(key, new StorePutCommand<V>(null, new XAValueHolder<V>(value, timeSource.getTimeMillis())))) {
        status = PutStatus.PUT;
      }
    }
    return status;
  }

  @Override
  public boolean remove(K key) throws StoreAccessException {
    checkKey(key);
    XATransactionContext<K, V> currentContext = getCurrentContext();
    if (currentContext.touched(key)) {
      V oldValue = currentContext.oldValueOf(key);
      V newValue = currentContext.newValueOf(key);
      currentContext.addCommand(key, new StoreRemoveCommand<V>(oldValue));
      return newValue != null;
    }

    ValueHolder<SoftLock<V>> softLockValueHolder = getSoftLockValueHolderFromUnderlyingStore(key);
    boolean status = false;
    if (softLockValueHolder != null) {
      SoftLock<V> softLock = softLockValueHolder.value();
      if (isInDoubt(softLock)) {
        currentContext.addCommand(key, new StoreEvictCommand<V>(softLock.getOldValue()));
      } else {
        status = currentContext.addCommand(key, new StoreRemoveCommand<V>(softLock.getOldValue()));
      }
    }
    return status;
  }

  @Override
  public ValueHolder<V> putIfAbsent(K key, V value) throws StoreAccessException {
    checkKey(key);
    checkValue(value);
    XATransactionContext<K, V> currentContext = getCurrentContext();
    if (currentContext.touched(key)) {
      V oldValue = currentContext.oldValueOf(key);
      V newValue = currentContext.newValueOf(key);
      if (newValue == null) {
        currentContext.addCommand(key, new StorePutCommand<V>(oldValue, new XAValueHolder<V>(value, timeSource.getTimeMillis())));
        return null;
      } else {
        return currentContext.newValueHolderOf(key);
      }
    }

    ValueHolder<SoftLock<V>> softLockValueHolder = getSoftLockValueHolderFromUnderlyingStore(key);
    if (softLockValueHolder != null) {
      SoftLock<V> softLock = softLockValueHolder.value();
      if (isInDoubt(softLock)) {
        currentContext.addCommand(key, new StoreEvictCommand<V>(softLock.getOldValue()));
        return null;
      } else {
        return new XAValueHolder<V>(softLockValueHolder, softLock.getOldValue());
      }
    } else {
      currentContext.addCommand(key, new StorePutCommand<V>(null, new XAValueHolder<V>(value, timeSource.getTimeMillis())));
      return null;
    }
  }

  @Override
  public RemoveStatus remove(K key, V value) throws StoreAccessException {
    checkKey(key);
    checkValue(value);
    XATransactionContext<K, V> currentContext = getCurrentContext();
    if (currentContext.touched(key)) {
      V oldValue = currentContext.oldValueOf(key);
      V newValue = currentContext.newValueOf(key);
      if (newValue == null) {
        return RemoveStatus.KEY_MISSING;
      } else if (!newValue.equals(value)) {
        return RemoveStatus.KEY_PRESENT;
      } else {
        currentContext.addCommand(key, new StoreRemoveCommand<V>(oldValue));
        return RemoveStatus.REMOVED;
      }
    }

    ValueHolder<SoftLock<V>> softLockValueHolder = getSoftLockValueHolderFromUnderlyingStore(key);
    if (softLockValueHolder != null) {
      SoftLock<V> softLock = softLockValueHolder.value();
      if (isInDoubt(softLock)) {
        currentContext.addCommand(key, new StoreEvictCommand<V>(softLock.getOldValue()));
        return RemoveStatus.KEY_MISSING;
      } else if (!softLock.getOldValue().equals(value)) {
        return RemoveStatus.KEY_PRESENT;
      } else {
        currentContext.addCommand(key, new StoreRemoveCommand<V>(softLock.getOldValue()));
        return RemoveStatus.REMOVED;
      }
    } else {
      return RemoveStatus.KEY_MISSING;
    }
  }

  @Override
  public ValueHolder<V> replace(K key, V value) throws StoreAccessException {
    checkKey(key);
    checkValue(value);
    XATransactionContext<K, V> currentContext = getCurrentContext();
    if (currentContext.touched(key)) {
      V newValue = currentContext.newValueOf(key);
      if (newValue == null) {
        return null;
      } else {
        V oldValue = currentContext.oldValueOf(key);
        XAValueHolder<V> newValueHolder = currentContext.newValueHolderOf(key);
        currentContext.addCommand(key, new StorePutCommand<V>(oldValue, new XAValueHolder<V>(value, timeSource.getTimeMillis())));
        return newValueHolder;
      }
    }

    ValueHolder<SoftLock<V>> softLockValueHolder = getSoftLockValueHolderFromUnderlyingStore(key);
    if (softLockValueHolder != null) {
      SoftLock<V> softLock = softLockValueHolder.value();
      if (isInDoubt(softLock)) {
        currentContext.addCommand(key, new StoreEvictCommand<V>(softLock.getOldValue()));
        return null;
      } else {
        V oldValue = softLock.getOldValue();
        currentContext.addCommand(key, new StorePutCommand<V>(oldValue, new XAValueHolder<V>(value, timeSource.getTimeMillis())));
        return new XAValueHolder<V>(oldValue, softLockValueHolder.creationTime(XAValueHolder.NATIVE_TIME_UNIT));
      }
    } else {
      return null;
    }
  }

  @Override
  public ReplaceStatus replace(K key, V oldValue, V newValue) throws StoreAccessException {
    checkKey(key);
    checkValue(oldValue);
    checkValue(newValue);
    XATransactionContext<K, V> currentContext = getCurrentContext();
    if (currentContext.touched(key)) {
      V modifiedValue = currentContext.newValueOf(key);
      if (modifiedValue == null) {
        return ReplaceStatus.MISS_NOT_PRESENT;
      } else if (!modifiedValue.equals(oldValue)) {
        return ReplaceStatus.MISS_PRESENT;
      } else {
        V previousValue = currentContext.oldValueOf(key);
        currentContext.addCommand(key, new StorePutCommand<V>(previousValue, new XAValueHolder<V>(newValue, timeSource.getTimeMillis())));
        return ReplaceStatus.HIT;
      }
    }

    ValueHolder<SoftLock<V>> softLockValueHolder = getSoftLockValueHolderFromUnderlyingStore(key);
    if (softLockValueHolder != null) {
      SoftLock<V> softLock = softLockValueHolder.value();
      V previousValue = softLock.getOldValue();
      if (isInDoubt(softLock)) {
        currentContext.addCommand(key, new StoreEvictCommand<V>(previousValue));
        return ReplaceStatus.MISS_NOT_PRESENT;
      } else if (!previousValue.equals(oldValue)) {
        return ReplaceStatus.MISS_PRESENT;
      } else {
        currentContext.addCommand(key, new StorePutCommand<V>(previousValue, new XAValueHolder<V>(newValue, timeSource.getTimeMillis())));
        return ReplaceStatus.HIT;
      }
    } else {
      return ReplaceStatus.MISS_NOT_PRESENT;
    }
  }

  @Override
  public void clear() throws StoreAccessException {
    // we don't want that to be transactional
    underlyingStore.clear();
  }

  @Override
  public StoreEventSource<K, V> getStoreEventSource() {
    return eventSourceWrapper;
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
    private StoreAccessException prefetchFailure = null;

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

        };
        return;
      }

      while (underlyingIterator.hasNext()) {
        final Cache.Entry<K, ValueHolder<SoftLock<V>>> next;
        try {
          next = underlyingIterator.next();
        } catch (StoreAccessException e) {
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
    public Cache.Entry<K, ValueHolder<V>> next() throws StoreAccessException {
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
  public ValueHolder<V> compute(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction, NullaryFunction<Boolean> replaceEqual) throws StoreAccessException {
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
  public ValueHolder<V> compute(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction) throws StoreAccessException {
    return compute(key, mappingFunction, REPLACE_EQUALS_TRUE);
  }

  @Override
  public ValueHolder<V> computeIfAbsent(K key, final Function<? super K, ? extends V> mappingFunction) throws StoreAccessException {
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
        xaValueHolder = currentContext.newValueHolderOf(key);
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
      if (updated) {
        xaValueHolder = currentContext.newValueHolderOf(key);
      } else {
        xaValueHolder = new XAValueHolder<V>(softLockValueHolder, softLockValueHolder.value().getOldValue());
      }
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
      } else {
        currentContext.removeCommand(key);
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
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction) throws StoreAccessException {
    return bulkCompute(keys, remappingFunction, REPLACE_EQUALS_TRUE);
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, final Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction, NullaryFunction<Boolean> replaceEqual) throws StoreAccessException {
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
  public Map<K, ValueHolder<V>> bulkComputeIfAbsent(Set<? extends K> keys, final Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction) throws StoreAccessException {
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

  private static final class CreatedStoreRef {
    final Store.Provider storeProvider;
    final SoftLockValueCombinedSerializerLifecycleHelper lifecycleHelper;

    public CreatedStoreRef(final Store.Provider storeProvider, final SoftLockValueCombinedSerializerLifecycleHelper lifecycleHelper) {
      this.storeProvider = storeProvider;
      this.lifecycleHelper = lifecycleHelper;
    }
  }

  @ServiceDependencies({TimeSourceService.class, JournalProvider.class, CopyProvider.class})
  public static class Provider implements Store.Provider {

    private volatile ServiceProvider<Service> serviceProvider;
    private volatile TransactionManagerProvider transactionManagerProvider;
    private final Map<Store<?, ?>, CreatedStoreRef> createdStores = new ConcurrentWeakIdentityHashMap<Store<?, ?>, CreatedStoreRef>();

    @Override
    public int rank(final Set<ResourceType<?>> resourceTypes, final Collection<ServiceConfiguration<?>> serviceConfigs) {
      final XAStoreConfiguration xaServiceConfiguration = findSingletonAmongst(XAStoreConfiguration.class, serviceConfigs);
      if (xaServiceConfiguration == null) {
        // An XAStore must be configured for use
        return 0;
      } else {
        if (this.transactionManagerProvider == null) {
          throw new IllegalStateException("A TransactionManagerProvider is mandatory to use XA caches");
        }
      }

      final Store.Provider candidateUnderlyingProvider = selectProvider(resourceTypes, serviceConfigs, xaServiceConfiguration);
      return 1000 + candidateUnderlyingProvider.rank(resourceTypes, serviceConfigs);
    }

    @Override
    public <K, V> Store<K, V> createStore(Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      Set<ResourceType.Core> supportedTypes = EnumSet.allOf(ResourceType.Core.class);

      Set<ResourceType<?>> configuredTypes = storeConfig.getResourcePools().getResourceTypeSet();

      for (ResourceType<?> type: configuredTypes) {
        if (!supportedTypes.contains(type)) {
          throw new IllegalStateException("Unsupported resource type : " + type.getResourcePoolClass());
        }
      }

      XAStoreConfiguration xaServiceConfiguration = findSingletonAmongst(XAStoreConfiguration.class, (Object[]) serviceConfigs);
      if (xaServiceConfiguration == null) {
        throw new IllegalStateException("XAStore.Provider.createStore called without XAStoreConfiguration");
      }

      final Store.Provider underlyingStoreProvider =
          selectProvider(configuredTypes, Arrays.asList(serviceConfigs), xaServiceConfiguration);

      String uniqueXAResourceId = xaServiceConfiguration.getUniqueXAResourceId();
      List<ServiceConfiguration<?>> underlyingServiceConfigs = new ArrayList<ServiceConfiguration<?>>();
      underlyingServiceConfigs.addAll(Arrays.asList(serviceConfigs));

      // eviction advisor
      EvictionAdvisor<? super K, ? super V> realEvictionAdvisor = storeConfig.getEvictionAdvisor();
      EvictionAdvisor<? super K, ? super SoftLock<V>> evictionAdvisor;
      if (realEvictionAdvisor == null) {
        evictionAdvisor = null;
      } else {
        evictionAdvisor = new XAEvictionAdvisor<K, V>(realEvictionAdvisor);
      }

      // expiry
      final Expiry<? super K, ? super V> configuredExpiry = storeConfig.getExpiry();
      Expiry<? super K, ? super SoftLock<V>> expiry = new Expiry<K, SoftLock<V>>() {
        @Override
        public Duration getExpiryForCreation(K key, SoftLock<V> softLock) {
          if (softLock.getTransactionId() != null) {
            // phase 1 prepare, create -> forever
            return Duration.INFINITE;
          } else {
            // phase 2 commit, or during a TX's lifetime, create -> some time
            Duration duration;
            try {
              duration = configuredExpiry.getExpiryForCreation(key, (V) softLock.getOldValue());
            } catch (RuntimeException re) {
              LOGGER.error("Expiry computation caused an exception - Expiry duration will be 0 ", re);
              return Duration.ZERO;
            }
            return duration;
          }
        }

        @Override
        public Duration getExpiryForAccess(K key, final ValueSupplier<? extends SoftLock<V>> softLock) {
          if (softLock.value().getTransactionId() != null) {
            // phase 1 prepare, access -> forever
            return Duration.INFINITE;
          } else {
            // phase 2 commit, or during a TX's lifetime, access -> some time
            Duration duration;
            try {
              duration = configuredExpiry.getExpiryForAccess(key, supplierOf(softLock.value().getOldValue()));
            } catch (RuntimeException re) {
              LOGGER.error("Expiry computation caused an exception - Expiry duration will be 0 ", re);
              return Duration.ZERO;
            }
            return duration;
          }
        }

        @Override
        public Duration getExpiryForUpdate(K key, ValueSupplier<? extends SoftLock<V>> oldSoftLockSupplier, SoftLock<V> newSoftLock) {
          SoftLock<V> oldSoftLock = oldSoftLockSupplier.value();
          if (oldSoftLock.getTransactionId() == null) {
            // phase 1 prepare, update -> forever
            return Duration.INFINITE;
          } else {
            // phase 2 commit, or during a TX's lifetime
            if (oldSoftLock.getOldValue() == null) {
              // there is no old value -> it's a CREATE, update -> create -> some time
              Duration duration;
              try {
                duration = configuredExpiry.getExpiryForCreation(key, oldSoftLock.getOldValue());
              } catch (RuntimeException re) {
                LOGGER.error("Expiry computation caused an exception - Expiry duration will be 0 ", re);
                return Duration.ZERO;
              }
              return duration;
            } else {
              // there is an old value -> it's an UPDATE, update -> some time
              V value = oldSoftLock.getNewValueHolder() == null ? null : oldSoftLock
                  .getNewValueHolder().value();
              Duration duration;
              try {
                duration = configuredExpiry.getExpiryForUpdate(key, supplierOf(oldSoftLock.getOldValue()), value);
              } catch (RuntimeException re) {
                LOGGER.error("Expiry computation caused an exception - Expiry duration will be 0 ", re);
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
      Collection<DefaultCopierConfiguration> copierConfigs = findAmongst(DefaultCopierConfiguration.class, underlyingServiceConfigs);
      DefaultCopierConfiguration keyCopierConfig = null;
      DefaultCopierConfiguration valueCopierConfig = null;
      for (DefaultCopierConfiguration copierConfig : copierConfigs) {
        if (copierConfig.getType().equals(DefaultCopierConfiguration.Type.KEY)) {
          keyCopierConfig = copierConfig;
        } else if (copierConfig.getType().equals(DefaultCopierConfiguration.Type.VALUE)) {
          valueCopierConfig = copierConfig;
        }
        underlyingServiceConfigs.remove(copierConfig);
      }

      // force-in a key copier if none is configured
      if (keyCopierConfig == null) {
        underlyingServiceConfigs.add(new DefaultCopierConfiguration<K>((Class) SerializingCopier.class, DefaultCopierConfiguration.Type.KEY));
      } else {
        underlyingServiceConfigs.add(keyCopierConfig);
      }

      // force-in a value copier if none is configured, or wrap the configured one in a soft lock copier
      if (valueCopierConfig == null) {
        underlyingServiceConfigs.add(new DefaultCopierConfiguration<K>((Class) SerializingCopier.class, DefaultCopierConfiguration.Type.VALUE));
      } else {
        CopyProvider copyProvider = serviceProvider.getService(CopyProvider.class);
        Copier valueCopier = copyProvider.createValueCopier(storeConfig.getValueType(), storeConfig.getValueSerializer(), valueCopierConfig);
        SoftLockValueCombinedCopier<V> softLockValueCombinedCopier = new SoftLockValueCombinedCopier<V>(valueCopier);
        underlyingServiceConfigs.add(new DefaultCopierConfiguration<K>((Copier) softLockValueCombinedCopier, DefaultCopierConfiguration.Type.VALUE));
      }

      // lookup the required XAStore services
      Journal<K> journal = serviceProvider.getService(JournalProvider.class).getJournal(persistenceSpaceId, storeConfig.getKeySerializer());
      TimeSource timeSource = serviceProvider.getService(TimeSourceService.class).getTimeSource();

      // create the soft lock serializer
      AtomicReference<Serializer<SoftLock<V>>> softLockSerializerRef = new AtomicReference<Serializer<SoftLock<V>>>();
      SoftLockValueCombinedSerializer softLockValueCombinedSerializer = new SoftLockValueCombinedSerializer<V>(softLockSerializerRef, storeConfig.getValueSerializer());

      // create the underlying store
      Store.Configuration<K, SoftLock<V>> underlyingStoreConfig = new StoreConfigurationImpl<K, SoftLock<V>>(storeConfig.getKeyType(), (Class) SoftLock.class, evictionAdvisor,
          storeConfig.getClassLoader(), expiry, storeConfig.getResourcePools(), storeConfig.getDispatcherConcurrency(), storeConfig.getKeySerializer(), softLockValueCombinedSerializer);
      Store<K, SoftLock<V>> underlyingStore = (Store) underlyingStoreProvider.createStore(underlyingStoreConfig,  underlyingServiceConfigs.toArray(new ServiceConfiguration[0]));

      // create the XA store
      TransactionManagerWrapper transactionManagerWrapper = transactionManagerProvider.getTransactionManagerWrapper();
      Store<K, V> store = new XAStore<K, V>(storeConfig.getKeyType(), storeConfig.getValueType(), underlyingStore,
          transactionManagerWrapper, timeSource, journal, uniqueXAResourceId);

      // create the softLockSerializer lifecycle helper
      SoftLockValueCombinedSerializerLifecycleHelper helper =
          new SoftLockValueCombinedSerializerLifecycleHelper((AtomicReference)softLockSerializerRef, storeConfig.getClassLoader());

      createdStores.put(store, new CreatedStoreRef(underlyingStoreProvider, helper));
      return store;
    }

    @Override
    public void releaseStore(Store<?, ?> resource) {
      CreatedStoreRef createdStoreRef = createdStores.remove(resource);
      if (createdStoreRef == null) {
        throw new IllegalArgumentException("Given store is not managed by this provider : " + resource);
      }

      Store.Provider underlyingStoreProvider = createdStoreRef.storeProvider;
      SoftLockValueCombinedSerializerLifecycleHelper helper = createdStoreRef.lifecycleHelper;
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
      CreatedStoreRef createdStoreRef = createdStores.get(resource);
      if (createdStoreRef == null) {
        throw new IllegalArgumentException("Given store is not managed by this provider : " + resource);
      }

      Store.Provider underlyingStoreProvider = createdStoreRef.storeProvider;
      SoftLockValueCombinedSerializerLifecycleHelper helper = createdStoreRef.lifecycleHelper;
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
    public void start(ServiceProvider<Service> serviceProvider) {
      this.serviceProvider = serviceProvider;
      this.transactionManagerProvider = serviceProvider.getService(TransactionManagerProvider.class);
    }

    @Override
    public void stop() {
      this.transactionManagerProvider = null;
      this.serviceProvider = null;
    }

    private Store.Provider selectProvider(final Set<ResourceType<?>> resourceTypes,
                                          final Collection<ServiceConfiguration<?>> serviceConfigs,
                                          final XAStoreConfiguration xaConfig) {
      List<ServiceConfiguration<?>> configsWithoutXA = new ArrayList<ServiceConfiguration<?>>(serviceConfigs);
      configsWithoutXA.remove(xaConfig);
      return StoreSupport.selectStoreProvider(serviceProvider, resourceTypes, configsWithoutXA);
    }
  }

  private static class XAEvictionAdvisor<K, V> implements EvictionAdvisor<K, SoftLock<V>> {

    private final EvictionAdvisor<? super K, ? super V> wrappedEvictionAdvisor;

    private XAEvictionAdvisor(EvictionAdvisor<? super K, ? super V> wrappedEvictionAdvisor) {
      this.wrappedEvictionAdvisor = wrappedEvictionAdvisor;
    }

    @Override
    public boolean adviseAgainstEviction(K key, SoftLock<V> softLock) {
      return isInDoubt(softLock) || wrappedEvictionAdvisor.adviseAgainstEviction(key, softLock.getOldValue());
    }
  }

}
