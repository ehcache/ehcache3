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

package org.ehcache.impl.serialization;

import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.ehcache.spi.persistence.StateHolder;
import org.ehcache.spi.persistence.StateRepository;

import java.io.Serializable;
import java.util.concurrent.ConcurrentMap;

/**
 * TransientStateRepository
 */
public class TransientStateRepository implements StateRepository {

  private ConcurrentMap<String, StateHolder<?, ?>> knownHolders = new ConcurrentHashMap<String, StateHolder<?, ?>>();

  @Override
  @SuppressWarnings("unchecked")
  public <K extends Serializable, V extends Serializable> StateHolder<K, V> getPersistentStateHolder(String name, Class<K> keyClass, Class<V> valueClass) {
    StateHolder<K, V> stateHolder = (StateHolder<K, V>) knownHolders.get(name);
    if (stateHolder != null) {
      return stateHolder;
    } else {
      StateHolder<K, V> newHolder = new TransientStateHolder<K, V>();
      stateHolder = (StateHolder<K, V>) knownHolders.putIfAbsent(name, newHolder);
      if (stateHolder == null) {
        return newHolder;
      } else {
        return stateHolder;
      }
    }
  }
}
