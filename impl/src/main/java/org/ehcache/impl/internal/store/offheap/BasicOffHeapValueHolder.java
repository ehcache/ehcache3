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

package org.ehcache.impl.internal.store.offheap;

import org.ehcache.core.spi.store.Store;

import java.util.concurrent.TimeUnit;

/**
 * BasicOffHeapValueHolder
 */
public class BasicOffHeapValueHolder<V> extends OffHeapValueHolder<V> {

  private final V value;

  public BasicOffHeapValueHolder(long id, V value, long creationTime, long expireTime) {
    this(id, value, creationTime, expireTime, 0);
  }

  public BasicOffHeapValueHolder(long id, V value, long creationTime, long expireTime, long lastAccessTime) {
    super(id, creationTime, expireTime);
    setLastAccessTime(lastAccessTime);
    this.value = value;
  }

  @Override
  void updateMetadata(Store.ValueHolder<V> valueFlushed) {
    throw new UnsupportedOperationException();
  }

  @Override
  void writeBack() {
    throw new UnsupportedOperationException();
  }

  @Override
  void forceDeserialization() {
    throw new UnsupportedOperationException();
  }

  @Override
  void detach() {
    throw new UnsupportedOperationException();
  }

  @Override
  public V get() {
    return value;
  }
}
