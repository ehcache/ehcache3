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

package org.ehcache.internal.store;

import org.ehcache.spi.cache.Store.ValueHolder;

import java.util.concurrent.TimeUnit;

/**
 * @author vfunshteyn
 */
final class OnHeapStoreValueHolder<V> implements ValueHolder<V> {
  private static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;
  private final V value;
  private final long createTime;
  private volatile long accessTime;
  
  OnHeapStoreValueHolder(V value) {
    if (value == null) {
      throw new NullPointerException("Value cannot be null");
    }
    this.value = value;
    createTime = accessTime = System.currentTimeMillis();
  }

  @Override
  public V value() {
    accessTime = System.currentTimeMillis();
    return value;
  }

  @Override
  public long creationTime(TimeUnit unit) {
    return DEFAULT_TIME_UNIT.convert(createTime, unit);
  }

  @Override
  public long lastAccessTime(TimeUnit unit) {
    return DEFAULT_TIME_UNIT.convert(accessTime, unit);
  }
  
  @Override
  public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof ValueHolder)) return false;
    return value.equals(((ValueHolder<V>)o).value());
  }
  
  @Override
  public int hashCode() {
    return value.hashCode();
  }

  @Override
  public float hitRate(TimeUnit unit) {
    throw new UnsupportedOperationException("Implement me!");
  }

  public String toString() {
    StringBuilder sb = new StringBuilder(128);
    sb.append(getClass().getSimpleName()).append("={value: ").append(value).
      append(", createTime: ").append(createTime).append(", accessTime: ").
      append(accessTime).append("}");
    return sb.toString();
  }
}
