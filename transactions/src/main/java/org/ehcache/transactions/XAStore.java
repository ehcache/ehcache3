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
import org.ehcache.config.EvictionPrioritizer;
import org.ehcache.config.EvictionVeto;
import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.events.StoreEventListener;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.function.NullaryFunction;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.TimeSourceService;
import org.ehcache.internal.concurrent.ConcurrentHashMap;
import org.ehcache.internal.serialization.CompactJavaSerializer;
import org.ehcache.internal.store.DefaultStoreProvider;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.UnsupportedTypeException;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.transactions.commands.StoreEvictCommand;
import org.ehcache.transactions.commands.StorePutCommand;
import org.ehcache.transactions.commands.StoreRemoveCommand;
import org.ehcache.transactions.configuration.XAServiceConfiguration;
import org.ehcache.transactions.configuration.XAServiceProvider;
import org.ehcache.transactions.journal.Journal;
import org.ehcache.transactions.journal.JournalProvider;

import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.ehcache.spi.ServiceLocator.findSingletonAmongst;

/**
 * @author Ludovic Orban
 */
public class XAStore<K, V> implements Store<K, V> {

  private final Store<K, SoftLock<V>> underlyingStore;
  private final XAServiceProvider xaServiceProvider;
  private final Map<Transaction, EhcacheXAResource<K, V>> xaResources = new ConcurrentHashMap<Transaction, EhcacheXAResource<K, V>>();
  private final TimeSource timeSource;
  private final Journal journal;
  private final XATransactionContextFactory<K, V> transactionContextFactory = new XATransactionContextFactory<K, V>();
  private final EhcacheXAResource recoveryXaResource;

  public XAStore(Store<K, SoftLock<V>> underlyingStore, XAServiceProvider xaServiceProvider, TimeSource timeSource, Journal journal) {
    this.underlyingStore = underlyingStore;
    this.xaServiceProvider = xaServiceProvider;
    this.timeSource = timeSource;
    this.journal = journal;
    this.recoveryXaResource = new EhcacheXAResource<K, V>(underlyingStore, journal, transactionContextFactory);
  }

  private boolean isInDoubt(SoftLock<V> softLock) {
    return softLock.getTransactionId() != null;
  }

  @Override
  public ValueHolder<V> get(K key) throws CacheAccessException {
    XATransactionContext<K, V> currentContext = getCurrentContext();

    XAValueHolder<V> newValueHolder = currentContext.getNewValueHolder(key);
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
    if (getCurrentContext().containsCommandFor(key)) {
      return getCurrentContext().getNewValueHolder(key) != null;
    }
    ValueHolder<SoftLock<V>> softLockValueHolder = getSoftLockValueHolderFromUnderlyingStore(key);
    return softLockValueHolder != null && softLockValueHolder.value() != null && softLockValueHolder.value().getOldValue() != null;
  }

  private XATransactionContext<K, V> getCurrentContext() throws CacheAccessException {
    try {
      final Transaction transaction = xaServiceProvider.getTransactionManager().getTransaction();
      if (transaction == null) {
        throw new CacheAccessException("Cannot access XA cache outside of XA transaction scope");
      }
      EhcacheXAResource<K, V> xaResource = xaResources.get(transaction);
      if (xaResource == null) {
        xaResource = new EhcacheXAResource<K, V>(underlyingStore, journal, transactionContextFactory);
        xaServiceProvider.registerXAResource(xaResource);
        xaServiceProvider.getTransactionManager().getTransaction().enlistResource(xaResource);
        xaResources.put(transaction, xaResource);
        final EhcacheXAResource<K, V> finalXaResource = xaResource;
        transaction.registerSynchronization(new Synchronization() {
          @Override
          public void beforeCompletion() {
          }

          @Override
          public void afterCompletion(int status) {
            xaServiceProvider.unregisterXAResource(finalXaResource);
            xaResources.remove(transaction);
          }
        });
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
      V oldValue = currentContext.getOldValue(key);
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
    return null;
  }

  @Override
  public void remove(K key) throws CacheAccessException {
    XATransactionContext<K, V> currentContext = getCurrentContext();
    if (currentContext.containsCommandFor(key)) {
      V oldValue = currentContext.getOldValue(key);
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
    // we don't want that to be transactional
    underlyingStore.clear();
  }

  @Override
  public void enableStoreEventNotifications(final StoreEventListener<K, V> listener) {
    underlyingStore.enableStoreEventNotifications(new StoreEventListener<K, SoftLock<V>>() {
      @Override
      public void onEviction(K key, ValueHolder<SoftLock<V>> valueHolder) {
        listener.onEviction(key, new XAValueHolder<V>(valueHolder, valueHolder.value().getOldValue()));
      }

      @Override
      public void onExpiration(K key, ValueHolder<SoftLock<V>> valueHolder) {
        listener.onExpiration(key, new XAValueHolder<V>(valueHolder, valueHolder.value().getOldValue()));
      }
    });
  }

  @Override
  public void disableStoreEventNotifications() {
    underlyingStore.disableStoreEventNotifications();
  }

  @Override
  public Iterator<Cache.Entry<K, ValueHolder<V>>> iterator() throws CacheAccessException {
    XATransactionContext<K, V> currentContext = getCurrentContext();
    Map<K, XAValueHolder<V>> valueHolderMap = transactionContextFactory.listPuts(currentContext.getTransactionId());
    return new XAIterator(valueHolderMap, underlyingStore.iterator(), currentContext.getTransactionId());
  }

  class XAIterator implements Iterator<Cache.Entry<K, ValueHolder<V>>> {

    private final java.util.Iterator<Map.Entry<K, XAValueHolder<V>>> iterator;
    private final Iterator<Cache.Entry<K, ValueHolder<SoftLock<V>>>> underlyingIterator;
    private final TransactionId transactionId;
    private Cache.Entry<K, ValueHolder<V>> next;

    XAIterator(Map<K, XAValueHolder<V>> valueHolderMap, Iterator<Cache.Entry<K, ValueHolder<SoftLock<V>>>> underlyingIterator, TransactionId transactionId) throws CacheAccessException {
      this.transactionId = transactionId;
      this.iterator = valueHolderMap.entrySet().iterator();
      this.underlyingIterator = underlyingIterator;
      advance();
    }

    void advance() throws CacheAccessException {
      if (!getCurrentContext().getTransactionId().equals(transactionId)) {
        throw new IllegalStateException("Iterator has been created in another transaction, it can only be used in the transaction it has been created in.");
      }
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
        final Cache.Entry<K, ValueHolder<SoftLock<V>>> next = underlyingIterator.next();

        if (!transactionContextFactory.isTouched(transactionId, next.getKey())) {
          ValueHolder<SoftLock<V>> valueHolder = next.getValue();
          SoftLock<V> softLock = valueHolder.value();
          final XAValueHolder<V> xaValueHolder;
          if (softLock.getTransactionId() == transactionId) {
            xaValueHolder = new XAValueHolder(valueHolder, softLock.getNewValueHolder());
          } else if (isInDoubt(softLock)) {
            continue;
          } else {
            xaValueHolder = new XAValueHolder(valueHolder, softLock.getOldValue());
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
    public boolean hasNext() throws CacheAccessException {
      return next != null;
    }

    @Override
    public Cache.Entry<K, ValueHolder<V>> next() throws CacheAccessException {
      if (next == null) {
        throw new NoSuchElementException();
      }
      Cache.Entry<K, ValueHolder<V>> rc = next;
      advance();
      return rc;
    }
  }

  private static final NullaryFunction<Boolean> REPLACE_EQUALS_TRUE = new NullaryFunction<Boolean>() {
    @Override
    public Boolean apply() {
      return Boolean.TRUE;
    }
  };

  private static boolean eq(Object o1, Object o2) {
    return (o1 == o2) || (o1 != null && o1.equals(o2));
  }

  @Override
  public ValueHolder<V> compute(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction, NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
    XATransactionContext<K, V> currentContext = getCurrentContext();
    if (currentContext.containsCommandFor(key)) {
      V newValue = mappingFunction.apply(key, currentContext.latestValueFor(key));

      XAValueHolder<V> xaValueHolder = null;
      V oldValue = currentContext.getOldValue(key);
      if (newValue == null) {
        if (!(eq(oldValue, newValue) && !replaceEqual.apply())) {
          currentContext.addCommand(key, new StoreRemoveCommand<V>(oldValue));
        }
      } else {
        xaValueHolder = new XAValueHolder<V>(newValue, timeSource.getTimeMillis());
        if (!(eq(oldValue, newValue) && !replaceEqual.apply())) {
          currentContext.addCommand(key, new StorePutCommand<V>(oldValue, xaValueHolder));
        }
      }
      return xaValueHolder;
    }

    ValueHolder<SoftLock<V>> softLockValueHolder = getSoftLockValueHolderFromUnderlyingStore(key);

    SoftLock<V> softLock = softLockValueHolder == null ? null : softLockValueHolder.value();
    V oldValue = softLock == null ? null : softLock.getOldValue();
    V newValue = mappingFunction.apply(key, oldValue);
    XAValueHolder<V> xaValueHolder = newValue == null ? null : new XAValueHolder<V>(newValue, timeSource.getTimeMillis());
    if (eq(oldValue, newValue) && !replaceEqual.apply()) {
      return xaValueHolder;
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

  private ValueHolder<SoftLock<V>> getSoftLockValueHolderFromUnderlyingStore(K key) throws CacheAccessException {
    return underlyingStore.get(key);
  }

  @Override
  public ValueHolder<V> compute(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction) throws CacheAccessException {
    return compute(key, mappingFunction, REPLACE_EQUALS_TRUE);
  }

  @Override
  public ValueHolder<V> computeIfAbsent(K key, final Function<? super K, ? extends V> mappingFunction) throws CacheAccessException {
    //TODO: implement this properly
    ValueHolder<V> valueHolder = get(key);
    if (valueHolder == null) {
      return compute(key, new BiFunction<K, V, V>() {
        @Override
        public V apply(K k, V v) {
          return mappingFunction.apply(k);
        }
      });
    }
    return valueHolder;
  }

  @Override
  public ValueHolder<V> computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) throws CacheAccessException {
    //TODO: implement this properly
    if (containsKey(key)) {
      return compute(key, remappingFunction);
    }
    return null;
  }

  @Override
  public ValueHolder<V> computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
    //TODO: implement this properly
    if (containsKey(key)) {
      return compute(key, remappingFunction, replaceEqual);
    }
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
    return underlyingStore.getConfigurationChangeListeners();
  }

  //TODO: keep track of stores with a ConcurrentWeakIdentityHashMap
  @ServiceDependencies({TimeSourceService.class, JournalProvider.class, DefaultStoreProvider.class})
  public static class Provider implements Store.Provider {

    private volatile ServiceProvider serviceProvider;
    private volatile DefaultStoreProvider defaultStoreProvider;
    private volatile XAServiceProvider xaServiceProvider;

    @Override
    public <K, V> Store<K, V> createStore(Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      TimeSource timeSource = serviceProvider.getService(TimeSourceService.class).getTimeSource();
      Journal journal = serviceProvider.getService(JournalProvider.class).getJournal();
      SerializationProvider serializationProvider = serviceProvider.getService(SerializationProvider.class);

      XAServiceConfiguration XAServiceConfiguration = findSingletonAmongst(XAServiceConfiguration.class, (Object[]) serviceConfigs);
      Store<K, V> store;
      if (XAServiceConfiguration == null || !XAServiceConfiguration.isTransactional()) {
        // non-tx cache
        store = defaultStoreProvider.createStore(storeConfig, serviceConfigs);
      } else {

        try {
          // TODO: adapt from underlying store's config
          Expiry<Object, Object> expiry = Expirations.noExpiration();
          EvictionVeto<? super K, ? super SoftLock> evictionVeto = null;
          EvictionPrioritizer<? super K, ? super SoftLock> evictionPrioritizer = null;

          Serializer<V> valueSerializer = serializationProvider.createValueSerializer(storeConfig.getValueType(), storeConfig.getClassLoader());


          Serializer<SoftLock> softLockSerializer = new CompactJavaSerializer<SoftLock>(storeConfig.getClassLoader());
          // TODO </end>

          Store.Configuration<K, SoftLock> underlyingStoreConfig = new StoreConfigurationImpl<K, SoftLock>(storeConfig.getKeyType(), SoftLock.class, evictionVeto, evictionPrioritizer, storeConfig.getClassLoader(), expiry, storeConfig.getResourcePools(), storeConfig.getKeySerializer(), softLockSerializer);
          Store<K, SoftLock<V>> underlyingStore = (Store) defaultStoreProvider.createStore(underlyingStoreConfig, serviceConfigs);
          store = new XAStore<K, V>(underlyingStore, xaServiceProvider, timeSource, journal);
        } catch (UnsupportedTypeException ute) {
          throw new RuntimeException(ute);
        }
      }

      return store;
    }

    @Override
    public void releaseStore(Store<?, ?> resource) {
      if (resource instanceof XAStore) {
        XAStore<?, ?> xaStore = (XAStore<?, ?>) resource;
        defaultStoreProvider.releaseStore(xaStore.underlyingStore);
        xaServiceProvider.unregisterXAResource(xaStore.recoveryXaResource);
      } else {
        defaultStoreProvider.releaseStore(resource);
      }
    }

    @Override
    public void initStore(Store<?, ?> resource) {
      if (resource instanceof XAStore) {
        XAStore<?, ?> xaStore = (XAStore<?, ?>) resource;
        xaServiceProvider.registerXAResource(xaStore.recoveryXaResource);
        defaultStoreProvider.initStore(xaStore.underlyingStore);
      } else {
        defaultStoreProvider.initStore(resource);
      }
    }

    @Override
    public void start(ServiceProvider serviceProvider) {
      this.serviceProvider = serviceProvider;
      this.defaultStoreProvider = serviceProvider.getService(DefaultStoreProvider.class);
      this.xaServiceProvider = serviceProvider.getService(XAServiceProvider.class);
    }

    @Override
    public void stop() {
      this.xaServiceProvider = null;
      this.defaultStoreProvider = null;
      this.serviceProvider = null;
    }
  }

}
