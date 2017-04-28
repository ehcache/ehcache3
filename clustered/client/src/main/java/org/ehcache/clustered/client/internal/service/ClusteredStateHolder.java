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

import org.ehcache.clustered.client.internal.store.ClusterTierClientEntity;
import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.StateRepositoryMessageFactory;
import org.ehcache.clustered.common.internal.messages.StateRepositoryOpMessage;
import org.ehcache.spi.persistence.StateHolder;

import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.ehcache.clustered.client.internal.service.ValueCodecFactory.getCodecForClass;

public class ClusteredStateHolder<K, V> implements StateHolder<K, V> {

  private final StateRepositoryMessageFactory messageFactory;
  private final ClusterTierClientEntity entity;
  private final Class<K> keyClass;
  private final ValueCodec<K> keyCodec;
  private final ValueCodec<V> valueCodec;

  public ClusteredStateHolder(final String cacheId, final String mapId, final ClusterTierClientEntity entity, Class<K> keyClass, Class<V> valueClass) {
    this.keyClass = keyClass;
    this.keyCodec = getCodecForClass(keyClass);
    this.valueCodec = getCodecForClass(valueClass);
    this.messageFactory = new StateRepositoryMessageFactory(cacheId, mapId, entity.getClientId());
    this.entity = entity;
  }

  @Override
  @SuppressWarnings("unchecked")
  public V get(final Object key) {
    if (!keyClass.isAssignableFrom(key.getClass())) {
      return null;
    }
    @SuppressWarnings("unchecked")
    Object response = getResponse(messageFactory.getMessage(keyCodec.encode((K) key)), false);
    return valueCodec.decode(response);
  }

  private Object getResponse(StateRepositoryOpMessage message, boolean track) {
    try {
      EhcacheEntityResponse response = entity.invokeStateRepositoryOperation(message, track);
      return ((EhcacheEntityResponse.MapValue)response).getValue();
    } catch (ClusterException ce) {
      throw new ClusteredMapException(ce);
    } catch (TimeoutException te) {
      throw new ClusteredMapException(te);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Set<Map.Entry<K, V>> entrySet() {
    @SuppressWarnings("unchecked")
    Set<Map.Entry<Object, Object>> response = (Set<Map.Entry<Object, Object>>) getResponse(messageFactory.entrySetMessage(), true);
    Set<Map.Entry<K, V>> entries = new HashSet<Map.Entry<K, V>>();
    for (Map.Entry<Object, Object> objectEntry : response) {
      entries.add(new AbstractMap.SimpleEntry<K, V>(keyCodec.decode(objectEntry.getKey()),
                                                    valueCodec.decode(objectEntry.getValue())));
    }
    return entries;
  }

  @Override
  @SuppressWarnings("unchecked")
  public V putIfAbsent(final K key, final V value) {
    Object response = getResponse(messageFactory.putIfAbsentMessage(keyCodec.encode(key), valueCodec.encode(value)), true);
    return valueCodec.decode(response);
  }

}
