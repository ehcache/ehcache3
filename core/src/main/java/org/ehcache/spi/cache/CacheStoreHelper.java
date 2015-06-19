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

package org.ehcache.spi.cache;

import org.ehcache.Cache;

import java.util.concurrent.TimeUnit;

/**
 * @author Alex Snaps
 */
public final class CacheStoreHelper {

  private CacheStoreHelper() {
    // thou shalt not instantiate me
  }

  public static <K, V> Cache.Entry<K, V> cacheEntry(final K key, final Store.ValueHolder<V> mappedValue) {
    return new Cache.Entry<K, V>() {

      @Override
      public K getKey() {
        return key;
      }

      @Override
      public V getValue() {
        return mappedValue.value();
      }

      @Override
      public long getCreationTime(TimeUnit unit) {
        return mappedValue.creationTime(unit);
      }

      @Override
      public long getLastAccessTime(TimeUnit unit) {
        return mappedValue.lastAccessTime(unit);
      }

      @Override
      public float getHitRate(TimeUnit unit) {
        return mappedValue.hitRate(unit);
      }
    };
  }
}
