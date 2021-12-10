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
package org.ehcache.impl.internal.store.basic;

import org.ehcache.core.spi.store.Store;

import java.util.concurrent.TimeUnit;

/**
 * A value holder that always contains null
 *
 * @author Henri Tremblay
 */
public class EmptyValueHolder<V> implements Store.ValueHolder<V> {

  private static final Store.ValueHolder<Object> EMPTY = new EmptyValueHolder<>();

  @SuppressWarnings("unchecked")
  public static <V> Store.ValueHolder<V> empty() {
    return (Store.ValueHolder<V>) EMPTY;
  }

  @Override
  public V get() {
    return null;
  }

  @Override
  public long creationTime(TimeUnit unit) {
    return 0;
  }

  @Override
  public long expirationTime(TimeUnit unit) {
    return 0;
  }

  @Override
  public boolean isExpired(long expirationTime, TimeUnit unit) {
    return false;
  }

  @Override
  public long lastAccessTime(TimeUnit unit) {
    return 0;
  }

  @Override
  public float hitRate(long now, TimeUnit unit) {
    return 0;
  }

  @Override
  public long hits() {
    return 0;
  }

  @Override
  public long getId() {
    return 0;
  }
}
