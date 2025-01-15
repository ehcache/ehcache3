/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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
package org.ehcache.impl.internal.store.copy;

import org.ehcache.Cache;
import org.ehcache.core.CacheConfigurationChangeListener;
import org.ehcache.core.Ehcache;
import org.ehcache.core.Ehcache.GetAllFunction;
import org.ehcache.core.spi.store.AbstractWrapperStoreProvider;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.events.StoreEventSource;
import org.ehcache.impl.copy.IdentityCopier;
import org.ehcache.impl.internal.spi.copy.DefaultCopyProvider;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.copy.CopyProvider;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.spi.service.ServiceProvider;

import javax.annotation.Nonnull;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toSet;

/*
 * <copier>
 * <txn-store-wrapper>
 * <real-store>
 */
public class CopierStore<K, V> implements Store<K, V> {

  private final Copier<K> keyCopier;
  private final Copier<V> valueCopier;
  private final Store<K, V> delegate;

  public CopierStore(Copier<K> keyCopier, Copier<V> valueCopier, Store<K, V> delegate) {
    this.keyCopier = keyCopier;
    this.valueCopier = valueCopier;
    this.delegate = delegate;
  }

  @Override
  public ValueHolder<V> get(K key) throws StoreAccessException {
    return copyValueForRead(delegate.get(key));
  }

  @Override
  public boolean containsKey(K key) throws StoreAccessException {
    return delegate.containsKey(key);
  }

  @Override
  public PutStatus put(K key, V value) throws StoreAccessException {
    return delegate.put(keyCopier.copyForWrite(key), valueCopier.copyForWrite(value));
  }

  @Override
  public ValueHolder<V> putIfAbsent(K key, V value, Consumer<Boolean> put) throws StoreAccessException {
    return copyValueForRead(delegate.putIfAbsent(keyCopier.copyForWrite(key), valueCopier.copyForWrite(value), put));
  }

  @Override
  public boolean remove(K key) throws StoreAccessException {
    return delegate.remove(key);
  }

  @Override
  public RemoveStatus remove(K key, V value) throws StoreAccessException {
    return delegate.remove(key, value);
  }

  @Override
  public ValueHolder<V> replace(K key, V value) throws StoreAccessException {
    return copyValueForRead(delegate.replace(key, valueCopier.copyForWrite(value)));
  }

  @Override
  public ReplaceStatus replace(K key, V oldValue, V newValue) throws StoreAccessException {
    return delegate.replace(key, oldValue, valueCopier.copyForWrite(newValue));
  }

  @Override
  public void clear() throws StoreAccessException {
    delegate.clear();
  }

  @Override
  public StoreEventSource<K, V> getStoreEventSource() {
    return delegate.getStoreEventSource();
  }

  @Override
  public Iterator<Cache.Entry<K, ValueHolder<V>>> iterator() {
    Iterator<Cache.Entry<K, ValueHolder<V>>> iterator = delegate.iterator();
    return new Iterator<Cache.Entry<K, ValueHolder<V>>>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public Cache.Entry<K, ValueHolder<V>> next() throws StoreAccessException {
        Cache.Entry<K, ValueHolder<V>> next = iterator.next();
        return new Cache.Entry<K, ValueHolder<V>>() {
          @Override
          public K getKey() {
            return keyCopier.copyForRead(next.getKey());
          }

          @Override
          public ValueHolder<V> getValue() {
            return copyValueForRead(next.getValue());
          }
        };
      }
    };
  }

  @Override
  public ValueHolder<V> getAndPut(K key, V value) throws StoreAccessException {
    return copyValueForRead(delegate.getAndPut(keyCopier.copyForWrite(key), valueCopier.copyForWrite(value)));
  }

  @Override
  public ValueHolder<V> getAndRemove(K key) throws StoreAccessException {
    return copyValueForRead(delegate.getAndRemove(key));
  }

  @Override
  public ValueHolder<V> getAndCompute(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction) throws StoreAccessException {
    return copyValueForRead(delegate.getAndCompute(key, (k, v) -> valueCopier.copyForWrite(mappingFunction.apply(keyCopier.copyForRead(k), valueCopier.copyForRead(v)))));
  }

  @Override
  public ValueHolder<V> computeAndGet(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction, Supplier<Boolean> replaceEqual, Supplier<Boolean> invokeWriter) throws StoreAccessException {
    return copyValueForRead(delegate.computeAndGet(key, (k, v) -> valueCopier.copyForWrite(mappingFunction.apply(keyCopier.copyForRead(k), valueCopier.copyForRead(v))), replaceEqual, invokeWriter));
  }

  @Override
  public ValueHolder<V> computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) throws StoreAccessException {
    return copyValueForRead(delegate.computeIfAbsent(keyCopier.copyForWrite(key), k -> valueCopier.copyForWrite(mappingFunction.apply(keyCopier.copyForRead(k)))));
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction) throws StoreAccessException {
    if (remappingFunction instanceof Ehcache.RemoveAllFunction) {
      return copyForRead(delegate.bulkCompute(keys, remappingFunction));
    } else if (remappingFunction instanceof Ehcache.PutAllFunction) {
      Ehcache.PutAllFunction<K, V> putAllFunction = (Ehcache.PutAllFunction<K, V>) remappingFunction;
      return copyForRead(delegate.bulkCompute(keys, new Ehcache.PutAllFunction<K, V>() {
        @Override
        public Map<K, V> getEntriesToRemap() {
          return copyForWrite(putAllFunction.getEntriesToRemap());
        }

        @Override
        public boolean newValueAlreadyExpired(K key, V oldValue, V newValue) {
          return putAllFunction.newValueAlreadyExpired(key, oldValue, newValue);
        }

        @Override
        public AtomicInteger getActualPutCount() {
          return putAllFunction.getActualPutCount();
        }

        @Override
        public AtomicInteger getActualUpdateCount() {
          return putAllFunction.getActualUpdateCount();
        }
      }));
    } else {
      return copyForRead(delegate.bulkCompute(keys, entries -> copyForWrite(remappingFunction.apply(copyForRead(entries)))));
    }
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction, Supplier<Boolean> replaceEqual) throws StoreAccessException {
    return copyForRead(delegate.bulkCompute(keys, entries -> copyForWrite(remappingFunction.apply(copyForRead(entries))), replaceEqual));
  }

  @Override
  public Map<K, ValueHolder<V>> bulkComputeIfAbsent(Set<? extends K> keys, Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction) throws StoreAccessException {
    if (mappingFunction instanceof GetAllFunction) {
      return copyForRead(delegate.bulkComputeIfAbsent(keys, mappingFunction));
    } else {
      return copyForRead(delegate.bulkComputeIfAbsent(copyKeysForWrite(keys), ks -> copyForWrite(mappingFunction.apply(copyKeysForRead(ks)))));
    }
  }

  private Set<? extends K> copyKeysForWrite(Set<? extends K> keys) {
    return keys.stream().map(keyCopier::copyForWrite).collect(toSet());
  }

  private Iterable<? extends K> copyKeysForRead(Iterable<? extends K> keys) {
    return new CopyingIterable<>(keys, keyCopier::copyForRead);
  }

  @Override
  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
    return delegate.getConfigurationChangeListeners();
  }

  private ValueHolder<V> copyValueForRead(ValueHolder<V> valueHolder) {
    if (valueHolder == null) {
      return null;
    } else {
      return new ValueHolder<V>() {
        @Override
        public long creationTime() {
          return valueHolder.creationTime();
        }

        @Override
        public long expirationTime() {
          return valueHolder.expirationTime();
        }

        @Override
        public boolean isExpired(long expirationTime) {
          return valueHolder.isExpired(expirationTime);
        }

        @Override
        public long lastAccessTime() {
          return valueHolder.lastAccessTime();
        }

        @Override
        public long getId() {
          return valueHolder.getId();
        }

        @Nonnull
        @Override
        public V get() {
          return valueCopier.copyForRead(valueHolder.get());
        }
      };
    }
  }

  private Map<K, ValueHolder<V>> copyForRead(Map<K, ValueHolder<V>> entries) {
    return new ImmutableCopyingMap<>(entries, keyCopier::copyForRead, this::copyValueForRead);
  }

  private Iterable<? extends Map.Entry<? extends K, ? extends V>> copyForRead(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) {
    return new CopyingIterable<>(entries, e -> new AbstractMap.SimpleImmutableEntry<>(keyCopier.copyForRead(e.getKey()), valueCopier.copyForRead(e.getValue())));
  }

  private Map<K, V> copyForWrite(Map<K, V> entries) {
    return new ImmutableCopyingMap<>(entries, keyCopier::copyForWrite, valueCopier::copyForWrite);
  }

  private Iterable<? extends Map.Entry<K, ? extends V>> copyForWrite(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) {
    return new CopyingIterable<>(entries, e -> new AbstractMap.SimpleImmutableEntry<>(keyCopier.copyForWrite(e.getKey()), valueCopier.copyForWrite(e.getValue())));
  }

  @ServiceDependencies(DefaultCopyProvider.class)
  public static class Provider extends AbstractWrapperStoreProvider {

    private CopyProvider copyProvider;

    @Override
    protected int wrapperRank() {
      return 3;
    }

    @Override
    protected <K, V> Store<K, V> wrap(Store<K, V> store, Configuration<K, V> storeConfig, ServiceConfiguration<?, ?>... serviceConfigs) {
      Copier<K> keyCopier = copyProvider.createKeyCopier(storeConfig.getKeyType(), storeConfig.getKeySerializer(), serviceConfigs);
      Copier<V> valueCopier = copyProvider.createValueCopier(storeConfig.getValueType(), storeConfig.getValueSerializer(), serviceConfigs);
      if (keyCopier instanceof IdentityCopier<?> && valueCopier instanceof IdentityCopier<?>) {
        return store;
      } else {
        return new CopierStore<>(keyCopier, valueCopier, store);
      }
    }

    @Override
    public void releaseStore(Store<?, ?> resource) {
      try {
        if (resource instanceof CopierStore) {
          try {
            CopierStore<?, ?> copierStore = (CopierStore<?, ?>) resource;
            try {
              copyProvider.releaseCopier(copierStore.keyCopier);
            } finally {
              copyProvider.releaseCopier(copierStore.valueCopier);
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      } finally {
        super.releaseStore(resource);
      }
    }

    @Override
    public void start(ServiceProvider<Service> serviceProvider) {
      super.start(serviceProvider);
      this.copyProvider = serviceProvider.getService(CopyProvider.class);
    }
  }
}
