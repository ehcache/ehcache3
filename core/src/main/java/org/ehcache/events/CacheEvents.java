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

public final class CacheEvents {
  private CacheEvents() { }
  
  public static <K, V> CacheEvent<K, V> expiry(Cache.Entry<K, V> entry, Cache<K, V> source) {
    return new ExpiryEvent<K, V>(entry, source);
  }
  
  public static <K, V> CacheEvent<K, V> eviction(Cache.Entry<K, V> entry, Cache<K, V> source) {
    return new EvictionEvent<K, V>(entry, source);
  }
  
  public static <K, V> CacheEvent<K, V> creation(Cache.Entry<K, V> entry, Cache<K, V> source) {
    return new CreationEvent<K, V>(entry, source);
  }
  
  public static <K, V> CacheEvent<K, V> removal(Cache.Entry<K, V> entry, Cache<K, V> source) {
    return new RemovalEvent<K, V>(entry, source);
  }
  
  public static <K, V> CacheEvent<K, V> update(Cache.Entry<K, V> oldEntry, Cache.Entry<K, V> newEntry, 
      Cache<K, V> source) {
    return new UpdateEvent<K, V>(oldEntry, newEntry, source);
  }

  public static <K, V> StoreEventListener<K, V> nullStoreEventListener() {
    return new StoreEventListener<K, V>() {
      @Override
      public void onEviction(Entry<K, V> entry) {
      }

      @Override
      public void onExpiration(Entry<K, V> entry) {
      }
    };
  }
  
  private static abstract class BaseCacheEvent<K, V> implements CacheEvent<K, V> {
    final Cache.Entry<K, V> entry;
    final Cache<K, V> src;
    
    protected BaseCacheEvent(Cache.Entry<K, V> entry, Cache<K, V> from) {
      this.entry = entry;
      this.src = from;
    }
    
    @Override
    public Entry<K, V> getEntry() {
      return entry;
    }

    @Override
    @Deprecated
    public Cache<K, V> getSource() {
      return src;
    }
    
  }
  
  private final static class ExpiryEvent<K, V> extends BaseCacheEvent<K, V> {
    ExpiryEvent(Cache.Entry<K, V> expiredEntry, Cache<K, V> src) {
      super(expiredEntry, src);
    }
    
    @Override
    public EventType getType() {
      return EventType.EXPIRED;
    }

    @Override
    public V getPreviousValue() {
      return this.entry.getValue();
    }
    
  }
  
  private final static class EvictionEvent<K, V> extends BaseCacheEvent<K, V> {
    EvictionEvent(Cache.Entry<K, V> evictedEntry, Cache<K, V> src) {
      super(evictedEntry, src);
    }
    
    @Override
    public EventType getType() {
      return EventType.EVICTED;
    }

    @Override
    public V getPreviousValue() {
      return this.entry.getValue();
    }
    
  }

  private final static class CreationEvent<K, V> extends BaseCacheEvent<K, V> {
    CreationEvent(Cache.Entry<K, V> newEntry, Cache<K, V> src) {
      super(newEntry, src);
    }

    @Override
    public EventType getType() {
      return EventType.CREATED;
    }

    @Override
    public V getPreviousValue() {
      return null;
    }
  }
  
  private final static class RemovalEvent<K, V> extends BaseCacheEvent<K, V> {
    RemovalEvent(Cache.Entry<K, V> newEntry, Cache<K, V> src) {
      super(newEntry, src);
    }

    @Override
    public EventType getType() {
      return EventType.REMOVED;
    }

    @Override
    public V getPreviousValue() {
      return this.entry.getValue();
    }
  }

  private final static class UpdateEvent<K, V> extends BaseCacheEvent<K, V> {
    final Cache.Entry<K, V> previous;
    UpdateEvent(Cache.Entry<K, V> prevEntry, Cache.Entry<K, V> newEntry, Cache<K, V> src) {
      super(newEntry, src);
      this.previous = prevEntry;
    }

    @Override
    public EventType getType() {
      return EventType.UPDATED;
    }

    @Override
    public V getPreviousValue() {
      return previous.getValue();
    }
  }

}
