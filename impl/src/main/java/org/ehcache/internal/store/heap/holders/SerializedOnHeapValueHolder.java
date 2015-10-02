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
package org.ehcache.internal.store.heap.holders;

import org.ehcache.exceptions.SerializerException;
import org.ehcache.expiry.Duration;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.serialization.Serializer;

import java.nio.ByteBuffer;

public class SerializedOnHeapValueHolder<V> extends OnHeapValueHolder<V> {
  private final ByteBuffer buffer;
  private final Serializer<V> serializer;

  protected SerializedOnHeapValueHolder(long id, V value, long creationTime, long expirationTime, Serializer<V> serializer) {
    super(id, creationTime, expirationTime);
    if (value == null) {
      throw new NullPointerException("null value");
    }
    if (serializer == null) {
      throw new NullPointerException("null serializer");
    }
    this.serializer = serializer;
    this.buffer = serializer.serialize(value).asReadOnlyBuffer();
  }

  public SerializedOnHeapValueHolder(V value, long creationTime, Serializer<V> serializer) {
    this(value, creationTime, NO_EXPIRE, serializer);
  }

  public SerializedOnHeapValueHolder(V value, long creationTime, long expirationTime, Serializer<V> serializer) {
    this(-1, value, creationTime, expirationTime, serializer);
  }

  public SerializedOnHeapValueHolder(Store.ValueHolder<V> valueHolder, V value, Serializer<V> serializer, long now, Duration expiration) {
    this(valueHolder.getId(), value, valueHolder.creationTime(TIME_UNIT), valueHolder.expirationTime(TIME_UNIT), serializer);
    this.setHits(valueHolder.hits());
    this.accessed(now, expiration);
  }

  public SerializedOnHeapValueHolder(Store.ValueHolder<V> valueHolder, ByteBuffer binaryValue, Serializer<V> serializer, long now, Duration expiration) {
    super(valueHolder.getId(), valueHolder.creationTime(TIME_UNIT), valueHolder.expirationTime(TIME_UNIT));
    this.buffer = binaryValue;
    this.serializer = serializer;
    this.setHits(valueHolder.hits());
    this.accessed(now, expiration);
  }

  @Override
  public final V value() {
    try {
      return serializer.read(buffer.duplicate());
    } catch (ClassNotFoundException cnfe) {
      throw new SerializerException(cnfe);
    }
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || getClass() != other.getClass()) return false;

    SerializedOnHeapValueHolder<V> that = (SerializedOnHeapValueHolder)other;

    if (!super.equals(that)) return false;
    try {
      if (!serializer.equals(that.value(), buffer)) return false;
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
