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

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;

public class ConcurrentClusteredMap<K, V> implements ConcurrentMap<K, V> {

  private final StateRepositoryMessageFactory messageFactory;
  private final EhcacheClientEntity entity;

  public ConcurrentClusteredMap(final String cacheId, final String mapId, final EhcacheClientEntity entity) {
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
    return (V) getResponse(messageFactory.getMessage(key));
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
    return (Set<Entry<K, V>>) getResponse(messageFactory.entrySetMessage());
  }

  @Override
  public V putIfAbsent(final K key, final V value) {
    return (V) getResponse(messageFactory.putIfAbsentMessage(key, value));
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
