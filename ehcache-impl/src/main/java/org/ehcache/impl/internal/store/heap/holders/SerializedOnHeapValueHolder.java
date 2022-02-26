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

import org.ehcache.spi.serialization.SerializerException;
import org.ehcache.sizeof.annotations.IgnoreSizeOf;
import org.ehcache.core.spi.store.Store;
import org.ehcache.impl.internal.store.BinaryValueHolder;
import org.ehcache.spi.serialization.Serializer;

import java.nio.ByteBuffer;

public class SerializedOnHeapValueHolder<V> extends OnHeapValueHolder<V> implements BinaryValueHolder {
  private final ByteBuffer buffer;
  @IgnoreSizeOf
  private final Serializer<V> serializer;

  protected SerializedOnHeapValueHolder(long id, V value, long creationTime, long expirationTime, boolean evictionAdvice, Serializer<V> serializer) {
    super(id, creationTime, expirationTime, evictionAdvice);
    if (value == null) {
      throw new NullPointerException("null value");
    }
    if (serializer == null) {
      throw new NullPointerException("null serializer");
    }
    this.serializer = serializer;
    this.buffer = serializer.serialize(value).asReadOnlyBuffer();
  }

  public SerializedOnHeapValueHolder(V value, long creationTime, boolean evictionAdvice, Serializer<V> serializer) {
    this(value, creationTime, NO_EXPIRE, evictionAdvice, serializer);
  }

  public SerializedOnHeapValueHolder(V value, long creationTime, long expirationTime, boolean evictionAdvice, Serializer<V> serializer) {
    this(-1, value, creationTime, expirationTime, evictionAdvice, serializer);
  }

  public SerializedOnHeapValueHolder(Store.ValueHolder<V> valueHolder, V value, boolean evictionAdvice, Serializer<V> serializer, long now, java.time.Duration expiration) {
    this(valueHolder.getId(), value, valueHolder.creationTime(), valueHolder.expirationTime(), evictionAdvice, serializer);
    this.accessed(now, expiration);
  }

  public SerializedOnHeapValueHolder(Store.ValueHolder<V> valueHolder, ByteBuffer binaryValue, boolean evictionAdvice, Serializer<V> serializer, long now, java.time.Duration expiration) {
    super(valueHolder.getId(), valueHolder.creationTime(), valueHolder.expirationTime(), evictionAdvice);
    this.buffer = binaryValue;
    this.serializer = serializer;
    this.accessed(now, expiration);
  }

  @Override
  public final V get() {
    try {
      return serializer.read(buffer.duplicate());
    } catch (ClassNotFoundException cnfe) {
      throw new SerializerException(cnfe);
    }
  }

  @Override
  public ByteBuffer getBinaryValue() throws IllegalStateException {
    return buffer.duplicate();
  }

  @Override
  public boolean isBinaryValueAvailable() {
    return true;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || getClass() != other.getClass()) return false;

    @SuppressWarnings("unchecked")
    SerializedOnHeapValueHolder<V> that = (SerializedOnHeapValueHolder<V>)other;

    if (!super.equals(that)) return false;
    try {
      if (!serializer.equals(that.get(), buffer)) return false;
    } catch (ClassNotFoundException cnfe) {
      throw new SerializerException(cnfe);
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = 31 * result + super.hashCode();
    return result;
  }

}
