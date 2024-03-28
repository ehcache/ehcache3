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

package org.ehcache.impl.internal.store.shared.store;

import org.ehcache.Cache;
import org.ehcache.core.CacheConfigurationChangeListener;
import org.ehcache.core.Ehcache;
import org.ehcache.core.spi.store.AbstractValueHolder;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.events.StoreEventSource;
import org.ehcache.core.spi.store.tiering.CachingTier;
import org.ehcache.impl.internal.store.shared.AbstractPartition;
import org.ehcache.impl.internal.store.shared.composites.CompositeValue;
import org.ehcache.spi.resilience.StoreAccessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@SuppressWarnings({"unchecked", "rawtypes"})
public class StorePartition<K, V> extends AbstractPartition<Store<CompositeValue<K>, CompositeValue<V>>> implements Store<K, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(StorePartition.class);

  private static final Supplier<Boolean> SUPPLY_TRUE = () -> Boolean.TRUE;
  private final Class<K> keyType;
  private final Class<V> valueType;

  public StorePartition(int id, Class<K> keyType, Class<V> valueType,
                        Store<CompositeValue<K>, CompositeValue<V>> store) {
    super(id, store);
    this.keyType = keyType;
    this.valueType = valueType;
  }

  protected K checkKey(K keyObject) {
    if (keyType.isInstance(Objects.requireNonNull((Object) keyObject))) {
      return keyObject;
    } else {
      throw new ClassCastException("Invalid key type, expected : " + keyType.getName() + " but was : " + keyObject.getClass().getName());
    }
  }

  protected V checkValue(V valueObject) {
    if (valueType.isInstance(Objects.requireNonNull((Object) valueObject))) {
      return valueObject;
    } else {
      throw new ClassCastException("Invalid value type, expected : " + valueType.getName() + " but was : " + valueObject.getClass().getName());
    }
  }

  @Override
  public ValueHolder<V> get(K key) throws StoreAccessException {
    checkKey(key);
    return decode(shared().get(composite(key)));
  }

  @Override
  public boolean containsKey(K key) throws StoreAccessException {
    checkKey(key);
    return shared().containsKey(composite(key));
  }

  @Override
  public PutStatus put(K key, V value) throws StoreAccessException {
    checkKey(key);
    checkValue(value);
    return shared().put(composite(key), composite(value));
  }

  @Override
  public ValueHolder<V> getAndPut(K key, V value) throws StoreAccessException {
    checkKey(key);
    checkValue(value);
    return decode(shared().getAndPut(composite(key), composite(value)));
  }

  @Override
  public ValueHolder<V> putIfAbsent(K key, V value, Consumer<Boolean> put) throws StoreAccessException {
    checkKey(key);
    checkValue(value);
    return decode(shared().putIfAbsent(composite(key), composite(value), put));
  }

  @Override
  public boolean remove(K key) throws StoreAccessException {
    checkKey(key);
    return shared().remove(composite(key));
  }

  @Override
  public ValueHolder<V> getAndRemove(K key) throws StoreAccessException {
    checkKey(key);
    return decode(shared().getAndRemove(composite(key)));
  }

  @Override
  public RemoveStatus remove(K key, V value) throws StoreAccessException {
    checkKey(key);
    checkValue(value);
    return shared().remove(composite(key), composite(value));
  }

  @Override
  public ValueHolder<V> replace(K key, V value) throws StoreAccessException {
    checkKey(key);
    checkValue(value);
    return decode(shared().replace(composite(key), composite(value)));
  }

  @Override
  public ReplaceStatus replace(K key, V oldValue, V newValue) throws StoreAccessException {
    checkKey(key);
    checkValue(oldValue);
    checkValue(newValue);
    return shared().replace(composite(key), composite(oldValue), composite(newValue));
  }

  @Override
  public ValueHolder<V> getAndCompute(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction) throws StoreAccessException {
    checkKey(key);
    return decode(shared().getAndCompute(composite(key), (k, v) -> composite(mappingFunction.apply(k.getValue(), v != null ? v.getValue() : null))));
  }

  @Override
  public ValueHolder<V> computeAndGet(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction, Supplier<Boolean> replaceEqual, Supplier<Boolean> invokeWriter) throws StoreAccessException {
    checkKey(key);
    return decode(shared().computeAndGet(composite(key), (k, v) -> composite(mappingFunction.apply(k.getValue(), v == null ? null : v.getValue())), replaceEqual, invokeWriter));
  }

  @Override
  public ValueHolder<V> computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) throws StoreAccessException {
    checkKey(key);
    return decode(shared().computeIfAbsent(composite(key), k -> composite(mappingFunction.apply(k.getValue()))));
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys,
                                            Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction) throws StoreAccessException {
    return bulkCompute(keys, remappingFunction, SUPPLY_TRUE);
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys,
                                            Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction,
                                            Supplier<Boolean> replaceEqual) throws StoreAccessException {
    Map<CompositeValue<K>, ValueHolder<CompositeValue<V>>> results;
    Map<K, ValueHolder<V>> decodedResults = new HashMap<>();
    if (remappingFunction instanceof Ehcache.PutAllFunction) {
      Ehcache.PutAllFunction<K, V> putAllFunction = (Ehcache.PutAllFunction<K, V>) remappingFunction;
      Map<CompositeValue<K>, CompositeValue<V>> encodedEntriesToRemap = new HashMap<>();
      putAllFunction.getEntriesToRemap().forEach((k, v) -> encodedEntriesToRemap.put(composite(k), composite(v)));
      Ehcache.PutAllFunction<CompositeValue<K>, CompositeValue<V>> encodedRemappingFunction = new Ehcache.PutAllFunction(putAllFunction.getLogger(), encodedEntriesToRemap, putAllFunction.getExpiry());
      results = shared().bulkCompute(compositeSet(keys), encodedRemappingFunction);
      results.forEach((k, v) -> decodedResults.put(k.getValue(), decode(v)));
      putAllFunction.setActualPutCount(encodedRemappingFunction.getActualPutCount());
      putAllFunction.setActualUpdateCount(encodedRemappingFunction.getActualUpdateCount());
    } else if (remappingFunction instanceof Ehcache.RemoveAllFunction) {
      Ehcache.RemoveAllFunction<K, V> removeAllFunction = (Ehcache.RemoveAllFunction<K, V>) remappingFunction;
      Ehcache.RemoveAllFunction<CompositeValue<K>, CompositeValue<V>> encodedRemappingFunction = new Ehcache.RemoveAllFunction<>();
      results = shared().bulkCompute(compositeSet(keys), encodedRemappingFunction);
      results.forEach((k, v) -> decodedResults.put(k.getValue(), decode(v)));
      removeAllFunction.setActualRemoveCount(encodedRemappingFunction.getActualRemoveCount());
    } else {
      results = shared().bulkCompute(compositeSet(keys), new BulkComputeMappingFunction(id(), keyType, valueType, remappingFunction));
      results.forEach((k, v) -> decodedResults.put(k.getValue(), decode(v)));
    }
    return decodedResults;
  }

  @Override
  public Map<K, ValueHolder<V>> bulkComputeIfAbsent(Set<? extends K> keys,
                                                    Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction) throws StoreAccessException {
    Map<K, ValueHolder<V>> decodedResults = new HashMap<>();
    Map<CompositeValue<K>, ValueHolder<CompositeValue<V>>> results =
      shared().bulkComputeIfAbsent(compositeSet(keys), new BulkComputeIfAbsentMappingFunction(id(), keyType, valueType, mappingFunction));
    results.forEach((k, v) -> decodedResults.put(k.getValue(), decode(v)));
    return decodedResults;
  }

  @Override
  public void clear() throws StoreAccessException {
    boolean completeRemoval = true;
    Iterator<Cache.Entry<K, ValueHolder<V>>> iterator = iterator();
    while (iterator.hasNext()) {
      try {
        shared().remove(composite(iterator.next().getKey()));
      } catch (StoreAccessException cae) {
        completeRemoval = false;
      }
    }
    if (!completeRemoval) {
      LOGGER.error("Iteration failures may have prevented a complete removal");
    }
  }

  @Override
  public StoreEventSource<K, V> getStoreEventSource() {
    return (StoreEventSource<K, V>) shared().getStoreEventSource();
  }

  @Override

  public Iterator<Cache.Entry<K, ValueHolder<V>>> iterator() {

    Iterator<Cache.Entry<CompositeValue<K>, ValueHolder<CompositeValue<V>>>> iterator = shared().iterator();
    return new Iterator<Cache.Entry<K, ValueHolder<V>>>() {
      private Cache.Entry<K, ValueHolder<V>> prefetched = advance();
      @Override
      public boolean hasNext() {
        return prefetched != null;
      }

      @Override
      public Cache.Entry<K, ValueHolder<V>> next() {
        if (prefetched == null) {
          throw new NoSuchElementException();
        } else {
          Cache.Entry<K, ValueHolder<V>> next = prefetched;
          prefetched = advance();
          return next;
        }
      }

      private Cache.Entry<K, ValueHolder<V>> advance() {
        while (iterator.hasNext()) {
          try {
            Cache.Entry<CompositeValue<K>, ValueHolder<CompositeValue<V>>> next = iterator.next();
            if (next.getKey().getStoreId() == id()) {
              return new Cache.Entry<K, ValueHolder<V>>() {
                @Override
                public K getKey() {
                  return next.getKey().getValue();
                }
                @Override
                public ValueHolder<V> getValue() {
                  return decode(next.getValue());
                }
              };
            }
          } catch (StoreAccessException ex) {
            throw new RuntimeException(ex);
          }
        }
        return null;
      }
    };
  }

  // ConfigurationChangeSupport

  @Override
  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
    return shared().getConfigurationChangeListeners();
  }

  public static class DecodedValueHolder<T> extends AbstractValueHolder<T> implements ValueHolder<T> {
    private final ValueHolder<CompositeValue<T>> compositeValueHolder;

    public DecodedValueHolder(ValueHolder<CompositeValue<T>> compositeValueHolder) {
      super(compositeValueHolder.getId(), compositeValueHolder.creationTime(), compositeValueHolder.expirationTime());
      setLastAccessTime(compositeValueHolder.lastAccessTime());
      this.compositeValueHolder = compositeValueHolder;
    }

    @Override
    @Nonnull
    public T get() {
      return compositeValueHolder.get().getValue();
    }
  }

  public static abstract class BaseRemappingFunction<K,V> {
    protected final int storeId;
    protected final Class<K> keyType;
    protected final Class<V> valueType;

    BaseRemappingFunction(int storeId, Class<K> keyType, Class<V> valueType) {
      this.storeId = storeId;
      this.keyType = keyType;
      this.valueType = valueType;
    }

    protected void keyCheck(Object keyObject) {
      if (!keyType.isInstance(Objects.requireNonNull(keyObject))) {
        throw new ClassCastException("Invalid key type, expected : " + keyType.getName() + " but was : " + keyObject.getClass().getName());
      }
    }

    protected void valueCheck(Object valueObject) {
      if (!valueType.isInstance(Objects.requireNonNull(valueObject))) {
        throw new ClassCastException("Invalid value type, expected : " + valueType.getName() + " but was : " + valueObject.getClass().getName());
      }
    }
  }

  public static class BulkComputeMappingFunction<K, V> extends BaseRemappingFunction implements Function<Iterable<Map.Entry<CompositeValue<K>, CompositeValue<V>>>, Iterable<Map.Entry<CompositeValue<K>, CompositeValue<V>>>> {
    private final Function<Iterable<Map.Entry<K, V>>, Iterable<Map.Entry<K, V>>> function;

    BulkComputeMappingFunction(int storeId, Class<K> keyType, Class<V> valueType, Function<Iterable<Map.Entry<K, V>>, Iterable<Map.Entry<K, V>>> function) {
      super(storeId, keyType, valueType);
      this.function = function;
    }

    @Override
    public Iterable<Map.Entry<CompositeValue<K>, CompositeValue<V>>> apply(Iterable<Map.Entry<CompositeValue<K>, CompositeValue<V>>> entries) {
      Map<K, V> decodedEntries = new HashMap<>();
      entries.forEach(entry -> {
        K key = entry.getKey().getValue();
        keyCheck(key);
        CompositeValue<V> compositeValue = entry.getValue();
        V value = null;
        if (compositeValue != null) {
          value = compositeValue.getValue();
          valueCheck(value);
        }
        decodedEntries.put(key, value);
      });
      Map<CompositeValue<K>, CompositeValue<V>> encodedResults = new HashMap<>();
      Iterable<Map.Entry<K, V>> results = function.apply(decodedEntries.entrySet());
      results.forEach(entry -> {
        keyCheck(entry.getKey());
        valueCheck(entry.getValue());
        encodedResults.put(new CompositeValue<>(storeId, entry.getKey()), new CompositeValue<>(storeId, entry.getValue()));
      });
      return encodedResults.entrySet();
    }
  }

  public static class BulkComputeIfAbsentMappingFunction<K, V> extends BaseRemappingFunction implements Function<Iterable<CompositeValue<K>>, Iterable<Map.Entry<CompositeValue<K>, CompositeValue<V>>>> {
    private final Function<Iterable<K>, Iterable<Map.Entry<K, V>>> function;

    BulkComputeIfAbsentMappingFunction(int storeId, Class<K> keyType, Class<V> valueType, Function<Iterable<K>, Iterable<Map.Entry<K, V>>> function) {
      super(storeId, keyType, valueType);
      this.function = function;
    }

    @Override
    public Iterable<Map.Entry<CompositeValue<K>, CompositeValue<V>>> apply(Iterable<CompositeValue<K>> compositeValues) {
      List<K> keys = new ArrayList<>();
      compositeValues.forEach(k -> {
        keyCheck(k.getValue());
        keys.add(k.getValue());
      });
      Map<CompositeValue<K>, CompositeValue<V>> encodedResults = new HashMap<>();
      Iterable<Map.Entry<K, V>> results = function.apply(keys);
      results.forEach(entry -> {
        keyCheck(entry.getKey());
        valueCheck(entry.getValue());
        encodedResults.put(new CompositeValue<>(storeId, entry.getKey()), new CompositeValue<>(storeId, entry.getValue()));
      });
      return encodedResults.entrySet();
    }
  }
}

