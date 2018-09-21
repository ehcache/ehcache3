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
import org.ehcache.config.ResourceType;
import org.ehcache.core.CacheConfigurationChangeListener;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.core.spi.service.DiskResourceService;
import org.ehcache.core.spi.store.WrapperStore;
import org.ehcache.core.store.StoreConfigurationImpl;
import org.ehcache.core.store.StoreSupport;
import org.ehcache.impl.store.BaseStore;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.config.copy.DefaultCopierConfiguration;
import org.ehcache.impl.copy.SerializingCopier;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.spi.time.TimeSourceService;
import org.ehcache.spi.serialization.StatefulSerializer;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.events.StoreEventSource;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.copy.CopyProvider;
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
import org.ehcache.core.collections.ConcurrentWeakIdentityHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.context.ContextManager;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;

import static org.ehcache.core.spi.service.ServiceUtils.findAmongst;
import static org.ehcache.core.spi.service.ServiceUtils.findSingletonAmongst;
import static org.ehcache.transactions.xa.internal.TypeUtil.uncheckedCast;

/**
 * A {@link Store} implementation wrapping another {@link Store} driven by a JTA
 * {@link javax.transaction.TransactionManager} using the XA 2-phase commit protocol.
 */
public class XAStore<K, V> extends BaseStore<K, V> implements WrapperStore<K, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(XAStore.class);

  private static final Supplier<Boolean> REPLACE_EQUALS_TRUE = () -> Boolean.TRUE;

  private final Store<K, SoftLock<V>> underlyingStore;
  private final TransactionManagerWrapper transactionManagerWrapper;
  private final Map<Transaction, EhcacheXAResource<K, V>> xaResources = new ConcurrentHashMap<>();
  private final TimeSource timeSource;
  private final Journal<K> journal;
  private final String uniqueXAResourceId;
  private final XATransactionContextFactory<K, V> transactionContextFactory;
  private final EhcacheXAResource<K, V> recoveryXaResource;
  private final StoreEventSourceWrapper<K, V> eventSourceWrapper;

  public XAStore(Class<K> keyType, Class<V> valueType, Store<K, SoftLock<V>> underlyingStore, TransactionManagerWrapper transactionManagerWrapper,
                 TimeSource timeSource, Journal<K> journal, String uniqueXAResourceId) {
    super(keyType, valueType, true);
    this.underlyingStore = underlyingStore;
    this.transactionManagerWrapper = transactionManagerWrapper;
    this.timeSource = timeSource;
    this.journal = journal;
    this.uniqueXAResourceId = uniqueXAResourceId;
    this.transactionContextFactory = new XATransactionContextFactory<>(timeSource);
    this.recoveryXaResource = new EhcacheXAResource<>(underlyingStore, journal, transactionContextFactory);
    this.eventSourceWrapper = new StoreEventSourceWrapper<>(underlyingStore.getStoreEventSource());

    ContextManager.associate(underlyingStore).withParent(this);
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
        xaResource = new EhcacheXAResource<>(underlyingStore, journal, transactionContextFactory);
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

    SoftLock<V> softLock = softLockValueHolder.get();
    if (isInDoubt(softLock)) {
      currentContext.addCommand(key, new StoreEvictCommand<>(softLock.getOldValue()));
      return null;
    }

    return new XAValueHolder<>(softLockValueHolder, softLock.getOldValue());
  }

  @Override
  public boolean containsKey(K key) throws StoreAccessException {
    checkKey(key);
    if (getCurrentContext().touched(key)) {
      return getCurrentContext().newValueHolderOf(key) != null;
    }
    ValueHolder<SoftLock<V>> softLockValueHolder = getSoftLockValueHolderFromUnderlyingStore(key);
    return softLockValueHolder != null && softLockValueHolder.get().getTransactionId() == null && softLockValueHolder.get().getOldValue() != null;
  }

  @Override
  public PutStatus put(K key, V value) throws StoreAccessException {
    checkKey(key);
    checkValue(value);
    XATransactionContext<K, V> currentContext = getCurrentContext();
    if (currentContext.touched(key)) {
      V oldValue = currentContext.oldValueOf(key);
      currentContext.addCommand(key, new StorePutCommand<>(oldValue, new XAValueHolder<>(value, timeSource.getTimeMillis())));
      return PutStatus.PUT;
    }

    ValueHolder<SoftLock<V>> softLockValueHolder = getSoftLockValueHolderFromUnderlyingStore(key);
    if (softLockValueHolder != null) {
      SoftLock<V> softLock = softLockValueHolder.get();
      if (isInDoubt(softLock)) {
        currentContext.addCommand(key, new StoreEvictCommand<>(softLock.getOldValue()));
      } else {
        if (currentContext.addCommand(key, new StorePutCommand<>(softLock.getOldValue(), new XAValueHolder<>(value, timeSource
          .getTimeMillis())))) {
          return PutStatus.PUT;
        }
      }
    } else {
      if (currentContext.addCommand(key, new StorePutCommand<>(null, new XAValueHolder<>(value, timeSource.getTimeMillis())))) {
        return PutStatus.PUT;
      }
    }
    return PutStatus.NOOP;
  }

  @Override
  public boolean remove(K key) throws StoreAccessException {
    checkKey(key);
    XATransactionContext<K, V> currentContext = getCurrentContext();
    if (currentContext.touched(key)) {
      V oldValue = currentContext.oldValueOf(key);
      V newValue = currentContext.newValueOf(key);
      currentContext.addCommand(key, new StoreRemoveCommand<>(oldValue));
      return newValue != null;
    }

    ValueHolder<SoftLock<V>> softLockValueHolder = getSoftLockValueHolderFromUnderlyingStore(key);
    boolean status = false;
    if (softLockValueHolder != null) {
      SoftLock<V> softLock = softLockValueHolder.get();
      if (isInDoubt(softLock)) {
        currentContext.addCommand(key, new StoreEvictCommand<>(softLock.getOldValue()));
      } else {
        status = currentContext.addCommand(key, new StoreRemoveCommand<>(softLock.getOldValue()));
      }
    }
    return status;
  }

  @Override
  public ValueHolder<V> putIfAbsent(K key, V value, Consumer<Boolean> put) throws StoreAccessException {
    checkKey(key);
    checkValue(value);
    XATransactionContext<K, V> currentContext = getCurrentContext();
    if (currentContext.touched(key)) {
      V oldValue = currentContext.oldValueOf(key);
      V newValue = currentContext.newValueOf(key);
      if (newValue == null) {
        currentContext.addCommand(key, new StorePutCommand<>(oldValue, new XAValueHolder<>(value, timeSource.getTimeMillis())));
        return null;
      } else {
        return currentContext.newValueHolderOf(key);
      }
    }

    ValueHolder<SoftLock<V>> softLockValueHolder = getSoftLockValueHolderFromUnderlyingStore(key);
    if (softLockValueHolder != null) {
      SoftLock<V> softLock = softLockValueHolder.get();
      if (isInDoubt(softLock)) {
        currentContext.addCommand(key, new StoreEvictCommand<>(softLock.getOldValue()));
        return null;
      } else {
        return new XAValueHolder<>(softLockValueHolder, softLock.getOldValue());
      }
    } else {
      currentContext.addCommand(key, new StorePutCommand<>(null, new XAValueHolder<>(value, timeSource.getTimeMillis())));
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
        currentContext.addCommand(key, new StoreRemoveCommand<>(oldValue));
        return RemoveStatus.REMOVED;
      }
    }

    ValueHolder<SoftLock<V>> softLockValueHolder = getSoftLockValueHolderFromUnderlyingStore(key);
    if (softLockValueHolder != null) {
      SoftLock<V> softLock = softLockValueHolder.get();
      if (isInDoubt(softLock)) {
        currentContext.addCommand(key, new StoreEvictCommand<>(softLock.getOldValue()));
        return RemoveStatus.KEY_MISSING;
      } else if (!softLock.getOldValue().equals(value)) {
        return RemoveStatus.KEY_PRESENT;
      } else {
        currentContext.addCommand(key, new StoreRemoveCommand<>(softLock.getOldValue()));
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
        currentContext.addCommand(key, new StorePutCommand<>(oldValue, new XAValueHolder<>(value, timeSource.getTimeMillis())));
        return newValueHolder;
      }
    }

    ValueHolder<SoftLock<V>> softLockValueHolder = getSoftLockValueHolderFromUnderlyingStore(key);
    if (softLockValueHolder != null) {
      SoftLock<V> softLock = softLockValueHolder.get();
      if (isInDoubt(softLock)) {
        currentContext.addCommand(key, new StoreEvictCommand<>(softLock.getOldValue()));
        return null;
      } else {
        V oldValue = softLock.getOldValue();
        currentContext.addCommand(key, new StorePutCommand<>(oldValue, new XAValueHolder<>(value, timeSource.getTimeMillis())));
        return new XAValueHolder<>(oldValue, softLockValueHolder.creationTime());
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
        currentContext.addCommand(key, new StorePutCommand<>(previousValue, new XAValueHolder<>(newValue, timeSource.getTimeMillis())));
        return ReplaceStatus.HIT;
      }
    }

    ValueHolder<SoftLock<V>> softLockValueHolder = getSoftLockValueHolderFromUnderlyingStore(key);
    if (softLockValueHolder != null) {
      SoftLock<V> softLock = softLockValueHolder.get();
      V previousValue = softLock.getOldValue();
      if (isInDoubt(softLock)) {
        currentContext.addCommand(key, new StoreEvictCommand<>(previousValue));
        return ReplaceStatus.MISS_NOT_PRESENT;
      } else if (!previousValue.equals(oldValue)) {
        return ReplaceStatus.MISS_PRESENT;
      } else {
        currentContext.addCommand(key, new StorePutCommand<>(previousValue, new XAValueHolder<>(newValue, timeSource.getTimeMillis())));
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

  @Override
  protected String getStatisticsTag() {
    return "XaStore";
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
          SoftLock<V> softLock = valueHolder.get();
          final XAValueHolder<V> xaValueHolder;
          if (softLock.getTransactionId() == transactionId) {
            xaValueHolder = new XAValueHolder<>(valueHolder, softLock.getNewValueHolder().get());
          } else if (isInDoubt(softLock)) {
            continue;
          } else {
            xaValueHolder = new XAValueHolder<>(valueHolder, softLock.getOldValue());
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
  public ValueHolder<V> computeAndGet(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction, Supplier<Boolean> replaceEqual, Supplier<Boolean> invokeWriter) throws StoreAccessException {
    checkKey(key);
    XATransactionContext<K, V> currentContext = getCurrentContext();
    if (currentContext.touched(key)) {
      return updateCommandForKey(key, mappingFunction, replaceEqual, currentContext);
    }

    ValueHolder<SoftLock<V>> softLockValueHolder = getSoftLockValueHolderFromUnderlyingStore(key);

    SoftLock<V> softLock = softLockValueHolder == null ? null : softLockValueHolder.get();
    V oldValue = softLock == null ? null : softLock.getOldValue();
    V newValue = mappingFunction.apply(key, oldValue);
    XAValueHolder<V> xaValueHolder = newValue == null ? null : new XAValueHolder<>(newValue, timeSource.getTimeMillis());
    if (Objects.equals(oldValue, newValue) && !replaceEqual.get()) {
      return xaValueHolder;
    }
    if (newValue != null) {
      checkValue(newValue);
    }

    if (softLock != null && isInDoubt(softLock)) {
      currentContext.addCommand(key, new StoreEvictCommand<>(oldValue));
    } else {
      if (xaValueHolder == null) {
        if (oldValue != null) {
          currentContext.addCommand(key, new StoreRemoveCommand<>(oldValue));
        }
      } else {
        currentContext.addCommand(key, new StorePutCommand<>(oldValue, xaValueHolder));
      }
    }

    return xaValueHolder;
  }

  @Override
  public ValueHolder<V> getAndCompute(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction) throws StoreAccessException {
    checkKey(key);
    XATransactionContext<K, V> currentContext = getCurrentContext();
    if (currentContext.touched(key)) {
      V computed = mappingFunction.apply(key, currentContext.newValueOf(key));
      XAValueHolder<V> returnValueholder = null;
      if (computed != null) {
        checkValue(computed);
        XAValueHolder<V> xaValueHolder = new XAValueHolder<>(computed, timeSource.getTimeMillis());
        V returnValue = currentContext.newValueOf(key);
        V oldValue = currentContext.oldValueOf(key);
        if (returnValue != null) {
          returnValueholder = new XAValueHolder<>(returnValue, timeSource.getTimeMillis());
        }
        currentContext.addCommand(key, new StorePutCommand<>(oldValue, xaValueHolder));
      } else {
        V returnValue = currentContext.newValueOf(key);
        V oldValue = currentContext.oldValueOf(key);
        if (returnValue != null) {
          returnValueholder = new XAValueHolder<>(returnValue, timeSource.getTimeMillis());
        }
        if (oldValue != null) {
          currentContext.addCommand(key, new StoreRemoveCommand<>(oldValue));
        } else {
          currentContext.removeCommand(key);
        }
      }
      return returnValueholder;
    }

    ValueHolder<SoftLock<V>> softLockValueHolder = getSoftLockValueHolderFromUnderlyingStore(key);

    XAValueHolder<V> oldValueHolder = null;
    SoftLock<V> softLock = softLockValueHolder == null ? null : softLockValueHolder.get();
    V oldValue = softLock == null ? null : softLock.getOldValue();
    V newValue = mappingFunction.apply(key, oldValue);
    XAValueHolder<V> xaValueHolder = newValue == null ? null : new XAValueHolder<>(newValue, timeSource.getTimeMillis());
    if (newValue != null) {
      checkValue(newValue);
    }

    if (softLock != null && isInDoubt(softLock)) {
      currentContext.addCommand(key, new StoreEvictCommand<>(oldValue));
    } else {
      if (xaValueHolder == null) {
        if (oldValue != null) {
          currentContext.addCommand(key, new StoreRemoveCommand<>(oldValue));
        }
      } else {
        currentContext.addCommand(key, new StorePutCommand<>(oldValue, xaValueHolder));
      }
    }

    if (oldValue != null) {
      oldValueHolder = new XAValueHolder<>(oldValue, timeSource.getTimeMillis());
    }

    return oldValueHolder;
  }

  @Override
  public ValueHolder<V> computeIfAbsent(K key, final Function<? super K, ? extends V> mappingFunction) throws StoreAccessException {
    checkKey(key);
    XATransactionContext<K, V> currentContext = getCurrentContext();
    if (currentContext.removed(key)) {
      return updateCommandForKey(key, mappingFunction, currentContext);
    }
    if (currentContext.evicted(key)) {
      return new XAValueHolder<>(currentContext.oldValueOf(key), timeSource.getTimeMillis());
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
          xaValueHolder = new XAValueHolder<>(computed, timeSource.getTimeMillis());
          currentContext.addCommand(key, new StorePutCommand<>(null, xaValueHolder));
        } else {
          xaValueHolder = null;
        }
      }
    } else if (isInDoubt(softLockValueHolder.get())) {
      currentContext.addCommand(key, new StoreEvictCommand<>(softLockValueHolder.get().getOldValue()));
      xaValueHolder = new XAValueHolder<>(softLockValueHolder, softLockValueHolder.get().getNewValueHolder().get());
    } else {
      if (updated) {
        xaValueHolder = currentContext.newValueHolderOf(key);
      } else {
        xaValueHolder = new XAValueHolder<>(softLockValueHolder, softLockValueHolder.get().getOldValue());
      }
    }

    return xaValueHolder;
  }

  private ValueHolder<V> updateCommandForKey(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction, Supplier<Boolean> replaceEqual, XATransactionContext<K, V> currentContext) {
    V newValue = mappingFunction.apply(key, currentContext.newValueOf(key));
    XAValueHolder<V> xaValueHolder = null;
    V oldValue = currentContext.oldValueOf(key);
    if (newValue == null) {
      if (!(oldValue == null && !replaceEqual.get())) {
        currentContext.addCommand(key, new StoreRemoveCommand<>(oldValue));
      } else {
        currentContext.removeCommand(key);
      }
    } else {
      checkValue(newValue);
      xaValueHolder = new XAValueHolder<>(newValue, timeSource.getTimeMillis());
      if (!(Objects.equals(oldValue, newValue) && !replaceEqual.get())) {
        currentContext.addCommand(key, new StorePutCommand<>(oldValue, xaValueHolder));
      }
    }
    return xaValueHolder;
  }

  private ValueHolder<V> updateCommandForKey(K key, Function<? super K, ? extends V> mappingFunction, XATransactionContext<K, V> currentContext) {
    V computed = mappingFunction.apply(key);
    XAValueHolder<V> xaValueHolder = null;
    if (computed != null) {
      checkValue(computed);
      xaValueHolder = new XAValueHolder<>(computed, timeSource.getTimeMillis());
      V oldValue = currentContext.oldValueOf(key);
      currentContext.addCommand(key, new StorePutCommand<>(oldValue, xaValueHolder));
    } // else do nothing
    return xaValueHolder;
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction) throws StoreAccessException {
    return bulkCompute(keys, remappingFunction, REPLACE_EQUALS_TRUE);
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, final Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction, Supplier<Boolean> replaceEqual) throws StoreAccessException {
    Map<K, ValueHolder<V>> result = new HashMap<>();
    for (K key : keys) {
      checkKey(key);

      final ValueHolder<V> newValue = computeAndGet(key, (k, oldValue) -> {
        final Set<Map.Entry<K, V>> entrySet = Collections.singletonMap(k, oldValue).entrySet();
        final Iterable<? extends Map.Entry<? extends K, ? extends V>> entries = remappingFunction.apply(entrySet);
        final java.util.Iterator<? extends Map.Entry<? extends K, ? extends V>> iterator = entries.iterator();
        final Map.Entry<? extends K, ? extends V> next = iterator.next();

        K key1 = next.getKey();
        V value = next.getValue();
        checkKey(key1);
        if (value != null) {
          checkValue(value);
        }
        return value;
      }, replaceEqual, () -> false);
      result.put(key, newValue);
    }
    return result;
  }

  @Override
  public Map<K, ValueHolder<V>> bulkComputeIfAbsent(Set<? extends K> keys, final Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction) throws StoreAccessException {
    Map<K, ValueHolder<V>> result = new HashMap<>();

    for (final K key : keys) {
      final ValueHolder<V> newValue = computeIfAbsent(key, keyParam -> {
        final Iterable<K> keySet = Collections.singleton(keyParam);
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
      });
      result.put(key, newValue);
    }
    return result;
  }

  @Override
  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
    return underlyingStore.getConfigurationChangeListeners();
  }

  private static final class SoftLockValueCombinedSerializerLifecycleHelper<T> {
    final AtomicReference<SoftLockSerializer<T>> softLockSerializerRef;
    final ClassLoader classLoader;

    SoftLockValueCombinedSerializerLifecycleHelper(AtomicReference<SoftLockSerializer<T>> softLockSerializerRef, ClassLoader classLoader) {
      this.softLockSerializerRef = softLockSerializerRef;
      this.classLoader = classLoader;
    }
  }

  private static final class CreatedStoreRef {
    final Store.Provider storeProvider;
    final SoftLockValueCombinedSerializerLifecycleHelper<?> lifecycleHelper;

    public CreatedStoreRef(final Store.Provider storeProvider, final SoftLockValueCombinedSerializerLifecycleHelper<?> lifecycleHelper) {
      this.storeProvider = storeProvider;
      this.lifecycleHelper = lifecycleHelper;
    }
  }

  @ServiceDependencies({TimeSourceService.class, JournalProvider.class, CopyProvider.class, TransactionManagerProvider.class})
  public static class Provider implements WrapperStore.Provider {

    private volatile ServiceProvider<Service> serviceProvider;
    private volatile TransactionManagerProvider transactionManagerProvider;
    private final Map<Store<?, ?>, CreatedStoreRef> createdStores = new ConcurrentWeakIdentityHashMap<>();

    @Override
    public int rank(final Set<ResourceType<?>> resourceTypes, final Collection<ServiceConfiguration<?>> serviceConfigs) {
      throw new UnsupportedOperationException("Its a Wrapper store provider, does not support regular ranking");
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

      List<ServiceConfiguration<?>> serviceConfigList = Arrays.asList(serviceConfigs);

      Store.Provider underlyingStoreProvider = StoreSupport.selectStoreProvider(serviceProvider,
              storeConfig.getResourcePools().getResourceTypeSet(), serviceConfigList);

      String uniqueXAResourceId = xaServiceConfiguration.getUniqueXAResourceId();
      List<ServiceConfiguration<?>> underlyingServiceConfigs = new ArrayList<>(serviceConfigList.size() + 5); // pad a bit because we add stuff
      underlyingServiceConfigs.addAll(serviceConfigList);

      // eviction advisor
      EvictionAdvisor<? super K, ? super V> realEvictionAdvisor = storeConfig.getEvictionAdvisor();
      EvictionAdvisor<? super K, ? super SoftLock<V>> evictionAdvisor;
      if (realEvictionAdvisor == null) {
        evictionAdvisor = null;
      } else {
        evictionAdvisor = new XAEvictionAdvisor<>(realEvictionAdvisor);
      }

      // expiry
      final ExpiryPolicy<? super K, ? super V> configuredExpiry = storeConfig.getExpiry();
      ExpiryPolicy<? super K, ? super SoftLock<V>> expiry = new ExpiryPolicy<K, SoftLock<V>>() {
        @Override
        public Duration getExpiryForCreation(K key, SoftLock<V> softLock) {
          if (softLock.getTransactionId() != null) {
            // phase 1 prepare, create -> forever
            return ExpiryPolicy.INFINITE;
          } else {
            // phase 2 commit, or during a TX's lifetime, create -> some time
            Duration duration;
            try {
              duration = configuredExpiry.getExpiryForCreation(key, softLock.getOldValue());
            } catch (RuntimeException re) {
              LOGGER.error("Expiry computation caused an exception - Expiry duration will be 0 ", re);
              return Duration.ZERO;
            }
            return duration;
          }
        }

        @Override
        public Duration getExpiryForAccess(K key, Supplier<? extends SoftLock<V>> softLock) {
          if (softLock.get().getTransactionId() != null) {
            // phase 1 prepare, access -> forever
            return ExpiryPolicy.INFINITE;
          } else {
            // phase 2 commit, or during a TX's lifetime, access -> some time
            Duration duration;
            try {
              duration = configuredExpiry.getExpiryForAccess(key, () -> softLock.get().getOldValue());
            } catch (RuntimeException re) {
              LOGGER.error("Expiry computation caused an exception - Expiry duration will be 0 ", re);
              return Duration.ZERO;
            }
            return duration;
          }
        }

        @Override
        public Duration getExpiryForUpdate(K key, Supplier<? extends SoftLock<V>> oldSoftLockSupplier, SoftLock<V> newSoftLock) {
          SoftLock<V> oldSoftLock = oldSoftLockSupplier.get();
          if (oldSoftLock.getTransactionId() == null) {
            // phase 1 prepare, update -> forever
            return ExpiryPolicy.INFINITE;
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
                  .getNewValueHolder().get();
              Duration duration;
              try {
                duration = configuredExpiry.getExpiryForUpdate(key, oldSoftLock::getOldValue, value);
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
      DiskResourceService.PersistenceSpaceIdentifier<?> persistenceSpaceId = findSingletonAmongst(DiskResourceService.PersistenceSpaceIdentifier.class, (Object[]) serviceConfigs);

      // find the copiers
      Collection<DefaultCopierConfiguration<?>> copierConfigs = uncheckedCast(findAmongst(DefaultCopierConfiguration.class, underlyingServiceConfigs));
      DefaultCopierConfiguration<K> keyCopierConfig = null;
      DefaultCopierConfiguration<SoftLock<V>> valueCopierConfig = null;
      for (DefaultCopierConfiguration<?> copierConfig : copierConfigs) {
        if (copierConfig.getType().equals(DefaultCopierConfiguration.Type.KEY)) {
          keyCopierConfig = uncheckedCast(copierConfig);
        } else if (copierConfig.getType().equals(DefaultCopierConfiguration.Type.VALUE)) {
          valueCopierConfig = uncheckedCast(copierConfig);
        }
        underlyingServiceConfigs.remove(copierConfig);
      }

      // force-in a key copier if none is configured
      if (keyCopierConfig == null) {
        underlyingServiceConfigs.add(new DefaultCopierConfiguration<>(SerializingCopier.<K>asCopierClass(), DefaultCopierConfiguration.Type.KEY));
      } else {
        underlyingServiceConfigs.add(keyCopierConfig);
      }

      // force-in a value copier if none is configured, or wrap the configured one in a soft lock copier
      if (valueCopierConfig == null) {
        underlyingServiceConfigs.add(new DefaultCopierConfiguration<>(SerializingCopier.<V>asCopierClass(), DefaultCopierConfiguration.Type.VALUE));
      } else {
        CopyProvider copyProvider = serviceProvider.getService(CopyProvider.class);
        Copier<V> valueCopier = copyProvider.createValueCopier(storeConfig.getValueType(), storeConfig.getValueSerializer(), valueCopierConfig);
        Copier<SoftLock<V>> softLockValueCombinedCopier = new SoftLockValueCombinedCopier<>(valueCopier);
        underlyingServiceConfigs.add(new DefaultCopierConfiguration<>(softLockValueCombinedCopier, DefaultCopierConfiguration.Type.VALUE));
      }

      // lookup the required XAStore services
      Journal<K> journal = serviceProvider.getService(JournalProvider.class).getJournal(persistenceSpaceId, storeConfig.getKeySerializer());
      TimeSource timeSource = serviceProvider.getService(TimeSourceService.class).getTimeSource();

      // create the soft lock serializer
      AtomicReference<SoftLockSerializer<V>> softLockSerializerRef = new AtomicReference<>();
      SoftLockValueCombinedSerializer<V> softLockValueCombinedSerializer;
      if (storeConfig.getValueSerializer() instanceof StatefulSerializer) {
        softLockValueCombinedSerializer = new StatefulSoftLockValueCombinedSerializer<V>(softLockSerializerRef, storeConfig
          .getValueSerializer());
      } else {
        softLockValueCombinedSerializer = new SoftLockValueCombinedSerializer<V>(softLockSerializerRef, storeConfig
        .getValueSerializer());
      }

      // create the underlying store
      Class<SoftLock<V>> softLockClass = uncheckedCast(SoftLock.class);
      Store.Configuration<K, SoftLock<V>> underlyingStoreConfig = new StoreConfigurationImpl<>(storeConfig.getKeyType(), softLockClass, evictionAdvisor,
        storeConfig.getClassLoader(), expiry, storeConfig.getResourcePools(), storeConfig.getDispatcherConcurrency(), storeConfig
        .getKeySerializer(), softLockValueCombinedSerializer);
      Store<K, SoftLock<V>> underlyingStore = underlyingStoreProvider.createStore(underlyingStoreConfig, underlyingServiceConfigs.toArray(new ServiceConfiguration<?>[0]));

      // create the XA store
      TransactionManagerWrapper transactionManagerWrapper = transactionManagerProvider.getTransactionManagerWrapper();
      Store<K, V> store = new XAStore<>(storeConfig.getKeyType(), storeConfig.getValueType(), underlyingStore,
        transactionManagerWrapper, timeSource, journal, uniqueXAResourceId);

      // create the softLockSerializer lifecycle helper
      SoftLockValueCombinedSerializerLifecycleHelper<V> helper =
        new SoftLockValueCombinedSerializerLifecycleHelper<V>(softLockSerializerRef, storeConfig.getClassLoader());

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
      SoftLockValueCombinedSerializerLifecycleHelper<?> helper = createdStoreRef.lifecycleHelper;
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
      SoftLockValueCombinedSerializerLifecycleHelper<?> helper = createdStoreRef.lifecycleHelper;
      if (resource instanceof XAStore) {
        XAStore<?, ?> xaStore = (XAStore<?, ?>) resource;

        underlyingStoreProvider.initStore(xaStore.underlyingStore);
        helper.softLockSerializerRef.set(new SoftLockSerializer<>(helper.classLoader));
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

    @Override
    public int wrapperStoreRank(Collection<ServiceConfiguration<?>> serviceConfigs) {
      XAStoreConfiguration xaServiceConfiguration = findSingletonAmongst(XAStoreConfiguration.class, serviceConfigs);
      if (xaServiceConfiguration == null) {
        // An XAStore must be configured for use
        return 0;
      } else {
        if (this.transactionManagerProvider == null) {
          throw new IllegalStateException("A TransactionManagerProvider is mandatory to use XA caches");
        }
      }
      return 1;
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
