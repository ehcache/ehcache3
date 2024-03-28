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

package org.ehcache.impl.internal.store.shared.authoritative;

import org.ehcache.Cache;
import org.ehcache.core.CacheConfigurationChangeListener;
import org.ehcache.core.Ehcache;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.events.StoreEvent;
import org.ehcache.core.spi.store.events.StoreEventFilter;
import org.ehcache.core.spi.store.events.StoreEventListener;
import org.ehcache.core.spi.store.events.StoreEventSource;
import org.ehcache.core.spi.store.tiering.AuthoritativeTier;
import org.ehcache.event.EventType;
import org.ehcache.impl.internal.store.shared.AbstractPartition;
import org.ehcache.impl.internal.store.shared.composites.CompositeValue;
import org.ehcache.impl.internal.store.shared.store.StorePartition;
import org.ehcache.spi.resilience.StoreAccessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.InvalidParameterException;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@SuppressWarnings({"unchecked", "rawtypes"})
public class AuthoritativeTierPartition<K, V> extends StorePartition<K, V> implements AuthoritativeTier<K, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AuthoritativeTierPartition.class);

  @Override
  protected AuthoritativeTier<CompositeValue<K>, CompositeValue<V>> shared() {
    return (AuthoritativeTier<CompositeValue<K>, CompositeValue<V>>) super.shared();
  }

  public AuthoritativeTierPartition(int id, Class<K> keyType, Class<V> valueType, AuthoritativeTier<CompositeValue<K>, CompositeValue<V>> store) {
    super(id, keyType, valueType, store);
  }

  @Override
  public ValueHolder<V> getAndFault(K key) throws StoreAccessException {
    return decode(shared().getAndFault(composite(key)));
  }

  @Override
  public ValueHolder<V> computeIfAbsentAndFault(K key, Function<? super K, ? extends V> mappingFunction) throws StoreAccessException {
    return decode(shared().computeIfAbsentAndFault(composite(key), k -> composite(mappingFunction.apply(k.getValue()))));
  }

  @Override
  public boolean flush(K key, ValueHolder<V> valueHolder) {
    return shared().flush(composite(key), encode(valueHolder));
  }

  @Override
  public void setInvalidationValve(InvalidationValve valve) {
    shared().setInvalidationValve(valve);
  }

  @Override
  public Iterable<? extends Map.Entry<? extends K, ? extends ValueHolder<V>>> bulkComputeIfAbsentAndFault(Iterable<? extends K> keys, Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction) throws StoreAccessException {
    Iterable<? extends Map.Entry<? extends CompositeValue<K>, ? extends ValueHolder<CompositeValue<V>>>> results = shared()
      .bulkComputeIfAbsentAndFault(compositeSet(keys), compositeValues -> {
        Set<K> extractedKeys = new HashSet<>();
        compositeValues.forEach(k -> extractedKeys.add(checkKey(k.getValue())));
        Map<CompositeValue<K>, CompositeValue<V>> encodedResults = new HashMap<>();
        Iterable<? extends Map.Entry<? extends K, ? extends V>> extractedResults = mappingFunction.apply(keys);
          extractedResults.forEach(entry -> {
            checkKey(entry.getKey());
            V value = entry.getValue() == null ? null : checkValue(entry.getValue());
            encodedResults.put(composite(entry.getKey()), composite(value));
            });
          return encodedResults.entrySet();
      });

    Map<K, ValueHolder<V>> decodedResults = new HashMap<>();
    results.forEach(e -> decodedResults.put(e.getKey().getValue(), decode(e.getValue())));
    return decodedResults.entrySet();
  }

  @Override
  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
    return shared().getConfigurationChangeListeners();
  }
}
