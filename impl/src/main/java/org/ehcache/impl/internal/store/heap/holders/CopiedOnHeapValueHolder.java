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

package org.ehcache.impl.internal.store.heap.holders;

import org.ehcache.sizeof.annotations.IgnoreSizeOf;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.copy.Copier;

import java.util.concurrent.TimeUnit;

/**
 * @author Albin Suresh
 */
public class CopiedOnHeapValueHolder<V> extends OnHeapValueHolder<V> {
  private final V copiedValue;
  @IgnoreSizeOf
  private final Copier<V> valueCopier;

  protected CopiedOnHeapValueHolder(long id, V value, long creationTime, long expirationTime, boolean evictionAdvice, Copier<V> valueCopier) {
    super(id, creationTime, expirationTime, evictionAdvice);
    if (value == null) {
      throw new NullPointerException("null value");
    }
    if (valueCopier == null) {
      throw new NullPointerException("null copier");
    }
    this.valueCopier = valueCopier;
    this.copiedValue = valueCopier.copyForWrite(value);
  }

  /**
   * Constructor to build this value holder from another value holder.
   * As such we assume that the passed in value is internal and so has been copied for write already elsewhere.
   *
   * @param valueHolder reference value holder
   * @param value the value
   * @param valueCopier the copier to use for copyForRead
   * @param now timestamp in millis
   * @param expiration computed expiration duration
   */
  public CopiedOnHeapValueHolder(Store.ValueHolder<V> valueHolder, V value, boolean evictionAdvice, Copier<V> valueCopier, long now, java.time.Duration expiration) {
    super(valueHolder.getId(), valueHolder.creationTime(), valueHolder.expirationTime(), evictionAdvice);
    if (value == null) {
      throw new NullPointerException("null value");
    }
    if (valueCopier == null) {
      throw new NullPointerException("null copier");
    }
    this.valueCopier = valueCopier;
    this.copiedValue = value;
    this.accessed(now, expiration);
  }

  public CopiedOnHeapValueHolder(V value, long creationTime, boolean evictionAdvice, Copier<V> valueCopier) {
    this(value, creationTime, NO_EXPIRE, evictionAdvice, valueCopier);
  }

  public CopiedOnHeapValueHolder(V value, long creationTime, long expirationTime, boolean evictionAdvice, Copier<V> valueCopier) {
    this(-1, value, creationTime, expirationTime, evictionAdvice, valueCopier);
  }

  @Override
  public V get() {
    return valueCopier.copyForRead(copiedValue);
  }
}
