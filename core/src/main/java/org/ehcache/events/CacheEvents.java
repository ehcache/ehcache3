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

package org.ehcache.events;

import org.ehcache.Cache;
import org.ehcache.Cache.Entry;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.EventType;
import org.ehcache.spi.cache.Store;

public final class CacheEvents {
  private CacheEvents() { }
  
  public static <K, V> CacheEvent<K, V> expiry(K expiredKey, V expiredValue, Cache<K, V> source) {
    return new ExpiryEvent<K, V>(expiredKey, expiredValue, source);
  }
  
  public static <K, V> CacheEvent<K, V> eviction(K evictedKey, V evictedValue, Cache<K, V> source) {
    return new EvictionEvent<K, V>(evictedKey, evictedValue, source);
  }
  
  public static <K, V> CacheEvent<K, V> creation(K newKey, V newValue, Cache<K, V> source) {
    return new CreationEvent<K, V>(newKey, newValue, source);
  }
  
  public static <K, V> CacheEvent<K, V> removal(K removedKey, V removedValue, Cache<K, V> source) {
    return new RemovalEvent<K, V>(removedKey, removedValue, source);
  }
  
  public static <K, V> CacheEvent<K, V> update(K key, V oldValue, V newValue, Cache<K, V> source) {
    return new UpdateEvent<K, V>(key, oldValue, newValue, source);
  }

  public static <K, V> StoreEventListener<K, V> nullStoreEventListener() {
    return new StoreEventListener<K, V>() {
      @Override
      public void onEviction(final K key, final Store.ValueHolder<V> valueHolder) {
      }

      @Override
      public void onExpiration(final K key, final Store.ValueHolder<V> valueHolder) {
      }
    };
  }
  
  private static abstract class BaseCacheEvent<K, V> implements CacheEvent<K, V> {
    final K key;
    final Cache<K, V> src;
    
    protected BaseCacheEvent(K key, Cache<K, V> from) {
      this.key = key;
      this.src = from;
    }
    
    @Override
    public K getKey() {
      return key;
    }
    
    @Override
    @Deprecated
    public Cache<K, V> getSource() {
      return src;
    }
    
  }
  
  private final static class ExpiryEvent<K, V> extends BaseCacheEvent<K, V> {
    final V expiredValue;
    
    ExpiryEvent(K expiredKey, V expiredValue, Cache<K, V> src) {
      super(expiredKey, src);
      this.expiredValue = expiredValue;
    }
    
    @Override
    public EventType getType() {
      return EventType.EXPIRED;
    }

    @Override
    public V getNewValue() {
      return null;
    }

    @Override
    public V getOldValue() {
      return expiredValue;
    }
  }
  
  private final static class EvictionEvent<K, V> extends BaseCacheEvent<K, V> {
    final V evictedValue;
    
    EvictionEvent(K evictedKey, V evictedValue, Cache<K, V> src) {
      super(evictedKey, src);
      this.evictedValue = evictedValue;
    }
    
    @Override
    public EventType getType() {
      return EventType.EVICTED;
    }

    @Override
    public V getNewValue() {
      return null;
    }

    @Override
    public V getOldValue() {
      return evictedValue;
    }
  }

  private final static class CreationEvent<K, V> extends BaseCacheEvent<K, V> {
    final V newValue;
    
    CreationEvent(K newKey, V newValue, Cache<K, V> src) {
      super(newKey, src);
      this.newValue = newValue;
    }

    @Override
    public EventType getType() {
      return EventType.CREATED;
    }

    @Override
    public V getNewValue() {
      return newValue;
    }

    @Override
    public V getOldValue() {
      return null;
    }
  }
  
  private final static class RemovalEvent<K, V> extends BaseCacheEvent<K, V> {
    final V removedValue;
    
    RemovalEvent(K removedKey, V removedValue, Cache<K, V> src) {
      super(removedKey, src);
      this.removedValue = removedValue;
    }

    @Override
    public EventType getType() {
      return EventType.REMOVED;
    }

    @Override
    public V getNewValue() {
      return null;
    }

    @Override
    public V getOldValue() {
      return removedValue;
    }
  }

  private final static class UpdateEvent<K, V> extends BaseCacheEvent<K, V> {
    final V oldValue;
    final V newValue;
    UpdateEvent(K key, V oldValue, V newValue, Cache<K, V> src) {
      super(key, src);
      this.oldValue = oldValue;
      this.newValue = newValue;
    }

    @Override
    public EventType getType() {
      return EventType.UPDATED;
    }

    @Override
    public V getNewValue() {
      return newValue;
    }

    @Override
    public V getOldValue() {
      return oldValue;
    }
  }

}
