/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

package org.ehcache.impl.internal.store.heap.holders;

import org.ehcache.core.spi.store.Store;

/**
 * @author Albin Suresh
 */
public class SimpleOnHeapValueHolder<V> extends OnHeapValueHolder<V> {
  private final V value;

  protected SimpleOnHeapValueHolder(long id, V value, long creationTime, long expirationTime, boolean evictionAdvice) {
    super(id, creationTime, expirationTime, evictionAdvice);
    if (value == null) {
      throw new NullPointerException("null value");
    }
    this.value = value;
  }

  /**
   * Constructor to build this value holder from another value holder.
   * As such we assume that the passed in value is internal and so has been copied for write already elsewhere.
   *
   * @param valueHolder reference value holder
   * @param value the value
   * @param now timestamp in millis
   * @param expiration computed expiration duration
   */
  public SimpleOnHeapValueHolder(Store.ValueHolder<V> valueHolder, V value, boolean evictionAdvice, long now, java.time.Duration expiration) {
    super(valueHolder.getId(), valueHolder.creationTime(), valueHolder.expirationTime(), evictionAdvice);
    if (value == null) {
      throw new NullPointerException("null value");
    }
    this.value = value;
    this.accessed(now, expiration);
  }

  public SimpleOnHeapValueHolder(V value, long creationTime, boolean evictionAdvice) {
    this(value, creationTime, NO_EXPIRE, evictionAdvice);
  }

  public SimpleOnHeapValueHolder(V value, long creationTime, long expirationTime, boolean evictionAdvice) {
    this(-1, value, creationTime, expirationTime, evictionAdvice);
  }

  @Override
  public V get() {
    return value;
  }
}
