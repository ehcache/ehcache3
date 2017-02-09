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

package org.ehcache.clustered.client.internal.service;

import org.ehcache.clustered.client.internal.EhcacheClientEntity;
import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.StateRepositoryMessageFactory;
import org.ehcache.clustered.common.internal.messages.StateRepositoryOpMessage;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;

import static org.ehcache.clustered.client.internal.service.ValueCodecFactory.getCodecForClass;

public class ConcurrentClusteredMap<K, V> implements ConcurrentMap<K, V> {

  private final StateRepositoryMessageFactory messageFactory;
  private final EhcacheClientEntity entity;
  private final Class<K> keyClass;
  private final ValueCodec<K> keyCodec;
  private final ValueCodec<V> valueCodec;

  public ConcurrentClusteredMap(final String cacheId, final String mapId, final EhcacheClientEntity entity, Class<K> keyClass, Class<V> valueClass) {
    this.keyClass = keyClass;
    this.keyCodec = getCodecForClass(keyClass);
    this.valueCodec = getCodecForClass(valueClass);
    this.messageFactory = new StateRepositoryMessageFactory(cacheId, mapId);
    this.entity = entity;
  }

  @Override
  public int size() {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public boolean isEmpty() {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public boolean containsKey(final Object key) {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public boolean containsValue(final Object value) {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public V get(final Object key) {
    if (!keyClass.isAssignableFrom(key.getClass())) {
      return null;
    }
    @SuppressWarnings("unchecked")
    Object response = getResponse(messageFactory.getMessage(keyCodec.encode((K) key)));
    return valueCodec.decode(response);
  }

  private Object getResponse(StateRepositoryOpMessage message) {
    try {
      EhcacheEntityResponse response = entity.invoke(message, true);
      return ((EhcacheEntityResponse.MapValue)response).getValue();
    } catch (ClusterException ce) {
      throw new ClusteredMapException(ce);
    } catch (TimeoutException te) {
      throw new ClusteredMapException(te);
    }
  }

  @Override
  public V put(final K key, final V value) {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public V remove(final Object key) {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public void putAll(final Map<? extends K, ? extends V> m) {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public Set<K> keySet() {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public Collection<V> values() {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    @SuppressWarnings("unchecked")
    Set<Entry<Object, Object>> response = (Set<Entry<Object, Object>>) getResponse(messageFactory.entrySetMessage());
    Set<Entry<K, V>> entries = new HashSet<Entry<K, V>>();
    for (Entry<Object, Object> objectEntry : response) {
      entries.add(new AbstractMap.SimpleEntry<K, V>(keyCodec.decode(objectEntry.getKey()),
                                                    valueCodec.decode(objectEntry.getValue())));
    }
    return entries;
  }

  @Override
  public V putIfAbsent(final K key, final V value) {
    Object response = getResponse(messageFactory.putIfAbsentMessage(keyCodec.encode(key), valueCodec.encode(value)));
    return valueCodec.decode(response);
  }

  @Override
  public boolean remove(final Object key, final Object value) {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public boolean replace(final K key, final V oldValue, final V newValue) {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public V replace(final K key, final V value) {
    throw new UnsupportedOperationException("TODO");
  }
}
