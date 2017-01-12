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

package org.ehcache.clustered.client.internal.store;

import org.ehcache.clustered.client.internal.service.ClusteredMapException;
import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.StateRepositoryMessageFactory;
import org.ehcache.clustered.common.internal.messages.StateRepositoryOpMessage;
import org.ehcache.spi.persistence.StateHolder;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

public class ClusteredStateHolder<K, V> implements StateHolder<K, V> {

  private final StateRepositoryMessageFactory messageFactory;
  private final ClusteredTierClientEntity entity;

  public ClusteredStateHolder(final String cacheId, final String mapId, final ClusteredTierClientEntity entity) {
    this.messageFactory = new StateRepositoryMessageFactory(cacheId, mapId, entity.getClientId());
    this.entity = entity;
  }

  @Override
  @SuppressWarnings("unchecked")
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
  @SuppressWarnings("unchecked")
  public Set<Map.Entry<K, V>> entrySet() {
    return (Set<Map.Entry<K, V>>) getResponse(messageFactory.entrySetMessage());
  }

  @Override
  @SuppressWarnings("unchecked")
  public V putIfAbsent(final K key, final V value) {
    return (V) getResponse(messageFactory.putIfAbsentMessage(key, value));
  }

}
