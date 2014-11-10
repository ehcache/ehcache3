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

/**
 * @author teck
 */
class Eh107CacheEntryEvent<K, V> extends CacheEntryEvent<K, V> {

  private static final long serialVersionUID = 8460535666272347345L;

  // XXX: remove "transient" and suppress findbugs error about non-seriaizable field
  private transient final org.ehcache.event.CacheEvent<K, V> ehEvent;

  Eh107CacheEntryEvent(Cache<K, V> source, EventType eventType, org.ehcache.event.CacheEvent<K, V> ehEvent) {
    super(source, eventType);
    this.ehEvent = ehEvent;
  }

  @Override
  public K getKey() {
    return ehEvent.getEntry().getKey();
  }

  @Override
  public V getValue() {
    return ehEvent.getEntry().getValue();

  }

  @Override
  public <T> T unwrap(Class<T> clazz) {
    return Unwrap.unwrap(clazz, ehEvent);
  }

  @Override
  public V getOldValue() {
    // XXX: is this always safe to call?
    return ehEvent.getPreviousValue();
  }

  @Override
  public boolean isOldValueAvailable() {
    // XXX: is this always true?
    throw new AssertionError();
  }

}