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
package org.ehcache.internal.store.heap;

import org.ehcache.exceptions.SerializerException;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.serialization.Serializer;

import java.io.IOException;
import java.nio.ByteBuffer;

class ByValueOnHeapValueHolder<V> extends OnHeapValueHolder<V> {
  private final ByteBuffer buffer;
  private final int hash;
  private final Serializer<V> serializer;

  protected ByValueOnHeapValueHolder(V value, long creationTime, Serializer<V> serializer) {
    this(value, creationTime, NO_EXPIRE, serializer);
  }

  protected ByValueOnHeapValueHolder(V value, long creationTime, long expirationTime, Serializer<V> serializer) {
    super(-1, creationTime, expirationTime);
    if (value == null) {
      throw new NullPointerException("null value");
    }
    if (serializer == null) {
      throw new NullPointerException("null serializer");
    }
    this.serializer = serializer;
    this.hash = value.hashCode();
    try {
      this.buffer = serializer.serialize(value).asReadOnlyBuffer();
    } catch (IOException ioe) {
      throw new SerializerException(ioe);
    }
  }

  protected ByValueOnHeapValueHolder(Store.ValueHolder<V> valueHolder, Serializer<V> serializer) {
    this(valueHolder.value(), valueHolder.creationTime(TIME_UNIT), valueHolder.expirationTime(TIME_UNIT), serializer);
    this.setLastAccessTime(valueHolder.lastAccessTime(TIME_UNIT), TIME_UNIT);
  }

  @Override
  public final V value() {
    try {
      return serializer.read(buffer.duplicate());
    } catch (IOException ioe) {
      throw new SerializerException(ioe);
    } catch (ClassNotFoundException cnfe) {
      throw new SerializerException(cnfe);
    }
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || getClass() != other.getClass()) return false;

    ByValueOnHeapValueHolder<V> that = (ByValueOnHeapValueHolder)other;

    if (!super.equals(that)) return false;
    try {
      if (!serializer.equals(that.value(), buffer)) return false;
    } catch (IOException ioe) {
      throw new SerializerException(ioe);
    } catch (ClassNotFoundException cnfe) {
      throw new SerializerException(cnfe);
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = 31 * result + hash;
    result = 31 * result + super.hashCode();
    return result;
  }

}
