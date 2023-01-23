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

package org.ehcache.impl.internal.events;

import org.ehcache.event.EventType;
import org.ehcache.core.spi.store.events.StoreEvent;

import static java.lang.String.format;

/**
 * StoreEventImpl
 */
public class StoreEventImpl<K, V> implements StoreEvent<K, V> {

  private final EventType type;
  private final K key;
  private final V oldValue;
  private final V newValue;

  public StoreEventImpl(EventType type, K key, V oldValue, V newValue) {
    this.type = type;
    this.key = key;
    this.oldValue = oldValue;
    this.newValue = newValue;
  }

  @Override
  public EventType getType() {
    return type;
  }

  @Override
  public K getKey() {
    return key;
  }

  @Override
  public V getNewValue() {
    return newValue;
  }

  @Override
  public V getOldValue() {
    return oldValue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StoreEventImpl<?, ?> that = (StoreEventImpl<?, ?>) o;

    if (type != that.type) {
      return false;
    }
    if (!key.equals(that.key)) {
      return false;
    }
    if (oldValue != null ? !oldValue.equals(that.oldValue) : that.oldValue != null) {
      return false;
    }
    return newValue != null ? newValue.equals(that.newValue) : that.newValue == null;

  }

  @Override
  public int hashCode() {
    int result = type.hashCode();
    result = 31 * result + key.hashCode();
    result = 31 * result + (oldValue != null ? oldValue.hashCode() : 0);
    result = 31 * result + (newValue != null ? newValue.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return format("Event of type %s for key %s", type, key);
  }
}
