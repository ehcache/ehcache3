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
package org.ehcache.jsr107;

import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.EventType;
import org.ehcache.event.CacheEvent;

/**
 * @author teck
 */
abstract class Eh107CacheEntryEvent<K, V> extends CacheEntryEvent<K, V> {

  private static final long serialVersionUID = 8460535666272347345L;

  private final CacheEvent<? extends K, ? extends V> ehEvent;

  private final boolean hasOldValue;

  Eh107CacheEntryEvent(Cache<K, V> source, EventType eventType, CacheEvent<? extends K, ? extends V> ehEvent,
      boolean hasOldValue) {
    super(source, eventType);
    this.ehEvent = ehEvent;
    this.hasOldValue = hasOldValue;
  }

  @Override
  public K getKey() {
    return ehEvent.getKey();
  }

  @Override
  public abstract V getValue();

  @Override
  public <T> T unwrap(Class<T> clazz) {
    return Unwrap.unwrap(clazz, this, ehEvent);
  }

  @Override
  public V getOldValue() {
    return ehEvent.getOldValue();
  }

  @Override
  public boolean isOldValueAvailable() {
    return hasOldValue;
  }

  static class NormalEvent<K, V> extends Eh107CacheEntryEvent<K, V> {

    private static final long serialVersionUID = 1566947833363986792L;

    public NormalEvent(Cache<K, V> source, EventType eventType, CacheEvent<? extends K, ? extends V> ehEvent, boolean hasOldValue) {
      super(source, eventType, ehEvent, hasOldValue);
    }

    @Override
    public V getValue() {
      return super.ehEvent.getNewValue();
    }
  }

  static class RemovingEvent<K, V> extends Eh107CacheEntryEvent<K, V> {

    private static final long serialVersionUID = -1363817518693572909L;

    public RemovingEvent(Cache<K, V> source, EventType eventType, CacheEvent<? extends K, ? extends V> ehEvent, boolean hasOldValue) {
      super(source, eventType, ehEvent, hasOldValue);
    }

    @Override
    public V getValue() {
      return super.ehEvent.getOldValue();
    }

  }
}
