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

import java.util.concurrent.TimeUnit;

import org.ehcache.spi.cache.Store.ValueHolder;

abstract class BaseOnHeapValueHolder<V> implements OnHeapValueHolder<V> {
  private final V value;

  protected BaseOnHeapValueHolder(V value) {
    if (value == null) {
      throw new NullPointerException("null value");
    }
    this.value = value;
  }
  
  @Override
  public final V value() {
    return value;
  }
  
  @Override
  public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof ValueHolder)) return false;
    return value.equals(((ValueHolder<?>)o).value());
  }
  
  @Override
  public int hashCode() {
    return value.hashCode();
  }
  
  @Override
  public long creationTime(TimeUnit unit) {
    throw new AssertionError();
  }

  @Override
  public float hitRate(TimeUnit unit) {
    throw new AssertionError();
  }

  @Override
  public long lastAccessTime(TimeUnit unit) {
    throw new AssertionError();
  }

  @Override
  public boolean isExpired(long now) {
    throw new AssertionError();
  }

  @Override
  public void setAccessTimeMillis(long accessTime) {
    throw new AssertionError();
  }

  @Override
  public void setExpireTimeMillis(long expireTime) {
    throw new AssertionError();
  }
}
