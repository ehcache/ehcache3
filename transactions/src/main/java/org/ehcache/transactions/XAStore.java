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
package org.ehcache.transactions;

import org.ehcache.Cache;
import org.ehcache.CacheConfigurationChangeListener;
import org.ehcache.events.StoreEventListener;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.function.NullaryFunction;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.concurrent.ConcurrentHashMap;
import org.ehcache.spi.cache.AbstractValueHolder;
import org.ehcache.spi.cache.Store;
import org.ehcache.transactions.commands.StorePutCommand;
import org.ehcache.transactions.commands.StoreRemoveCommand;

import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Ludovic Orban
 */
public class XAStore<K, V> implements Store<K, V> {

  private final Store<K, SoftLock<K, V>> underlyingStore;
  private final TransactionManager transactionManager;
  private final Map<Transaction, EhcacheXAResource<K, V>> xaResources = new ConcurrentHashMap<Transaction, EhcacheXAResource<K, V>>();
  private final TimeSource timeSource;

  public XAStore(Store<K, SoftLock<K, V>> underlyingStore, TransactionManager transactionManager, TimeSource timeSource) {
    this.underlyingStore = underlyingStore;
    this.transactionManager = transactionManager;
    this.timeSource = timeSource;
  }

  @Override
  public ValueHolder<V> get(K key) throws CacheAccessException {
    XATransactionContext<K, V> currentContext = getCurrentContext();

    if (currentContext.isRemoved(key)) {
      return null;
    }

    ValueHolder<V> newValueHolder = currentContext.getNewValueHolder(key);
    if (newValueHolder != null) {
      return newValueHolder;
    }

    ValueHolder<SoftLock<K, V>> softLockValueHolder = underlyingStore.get(key);
    if (softLockValueHolder == null) {
      return null;
    }

    SoftLock<K, V> softLock = softLockValueHolder.value();
    if (softLock.isRunning2PC()) {
      return null;
    }

    return softLock.getOldValueHolder();
  }

  @Override
  public boolean containsKey(K key) throws CacheAccessException {
    return false;
  }

  private XATransactionContext<K, V> getCurrentContext() throws CacheAccessException {
    try {
      Transaction transaction = transactionManager.getTransaction();
      if (transaction == null) {
        throw new CacheAccessException("Cannot access XA cache outside of XA transaction scope");
      }
      EhcacheXAResource<K, V> xaResource = xaResources.get(transaction);
      if (xaResource == null) {
        xaResource = new EhcacheXAResource<K, V>(underlyingStore);
        transactionManager.getTransaction().enlistResource(xaResource);
        xaResources.put(transaction, xaResource);
      }
      return xaResource.getCurrentContext();
    } catch (SystemException se) {
      throw new CacheAccessException("Cannot get current transaction", se);
    } catch (RollbackException re) {
      throw new CacheAccessException("Transaction has been marked for rollback", re);
    }
  }

  @Override
  public void put(K key, V value) throws CacheAccessException {
    XATransactionContext<K, V> currentContext = getCurrentContext();
    if (currentContext.containsCommandFor(key)) {
      ValueHolder<V> oldValueHolder = currentContext.getOldValueHolder(key);
      currentContext.addCommand(key, new StorePutCommand<V>(newValueHolder(value), oldValueHolder));
      return;
    }

    ValueHolder<SoftLock<K, V>> softLockValueHolder = underlyingStore.get(key);
    if (softLockValueHolder != null) {
      SoftLock<K, V> softLock = softLockValueHolder.value();
      if (softLock.isRunning2PC()) {
        /*
        There are 3 things we can do here:
         - lock and wait until 2PC is done
         - drop the put
         - gamble: there are different bets we can take:
           # assume the other transaction will commit
           # assume the other transaction will rollback
         */

        // assume the other transaction will commit
        currentContext.addCommand(key, new StorePutCommand<V>(newValueHolder(value), softLock.getNewValueHolder()));
      } else {
        currentContext.addCommand(key, new StorePutCommand<V>(newValueHolder(value), softLock.getOldValueHolder()));
      }
    } else {
      currentContext.addCommand(key, new StorePutCommand<V>(newValueHolder(value), null));
    }
  }

  private final AtomicLong valueHolderIdGenerator = new AtomicLong();

  private ValueHolder<V> newValueHolder(final V value) {
    return new XAValueHolder<V>(valueHolderIdGenerator.incrementAndGet(), timeSource.getTimeMillis() ,value);
  }

  @Override
  public ValueHolder<V> putIfAbsent(K key, V value) throws CacheAccessException {
    return null;
  }

  @Override
  public void remove(K key) throws CacheAccessException {
    XATransactionContext<K, V> currentContext = getCurrentContext();
    if (currentContext.containsCommandFor(key)) {
      ValueHolder<V> oldValueHolder = currentContext.getOldValueHolder(key);
      currentContext.addCommand(key, new StoreRemoveCommand<V>(oldValueHolder));
      return;
    }

    ValueHolder<SoftLock<K, V>> softLockValueHolder = underlyingStore.get(key);
    if (softLockValueHolder != null) {
      SoftLock<K, V> softLock = softLockValueHolder.value();
      if (softLock.isRunning2PC()) {
        /*
        There are 3 things we can do here:
         - lock and wait until 2PC is done
         - drop the put
         - gamble: there are different bets we can take:
           # assume the other transaction will commit
           # assume the other transaction will rollback
         */

        // assume the other transaction will commit
        currentContext.addCommand(key, new StoreRemoveCommand<V>(softLock.getNewValueHolder()));
      } else {
        currentContext.addCommand(key, new StoreRemoveCommand<V>(softLock.getOldValueHolder()));
      }
    } else {
      // it's probably not needed not record a remove of a non-existent key
      currentContext.addCommand(key, new StoreRemoveCommand<V>(null));
    }
  }

  @Override
  public boolean remove(K key, V value) throws CacheAccessException {
    return false;
  }

  @Override
  public ValueHolder<V> replace(K key, V value) throws CacheAccessException {
    return null;
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) throws CacheAccessException {
    return false;
  }

  @Override
  public void clear() throws CacheAccessException {

  }

  @Override
  public void enableStoreEventNotifications(StoreEventListener<K, V> listener) {

  }

  @Override
  public void disableStoreEventNotifications() {

  }

  @Override
  public Iterator<Cache.Entry<K, ValueHolder<V>>> iterator() throws CacheAccessException {
    return null;
  }

  @Override
  public ValueHolder<V> compute(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction) throws CacheAccessException {
    return null;
  }

  @Override
  public ValueHolder<V> compute(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction, NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
    return null;
  }

  @Override
  public ValueHolder<V> computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) throws CacheAccessException {
    return null;
  }

  @Override
  public ValueHolder<V> computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) throws CacheAccessException {
    return null;
  }

  @Override
  public ValueHolder<V> computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
    return null;
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction) throws CacheAccessException {
    return null;
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction, NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
    return null;
  }

  @Override
  public Map<K, ValueHolder<V>> bulkComputeIfAbsent(Set<? extends K> keys, Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction) throws CacheAccessException {
    return null;
  }

  @Override
  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
    return null;
  }
}
