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

package org.ehcache.transactions.xa.internal;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.ehcache.core.spi.store.AbstractValueHolder;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.serialization.Serializer;

import java.io.Serializable;
import java.nio.ByteBuffer;

import static org.ehcache.transactions.xa.internal.TypeUtil.uncheckedCast;

/**
 * The {@link XAStore} {@link Store.ValueHolder} implementation.
 *
 * @author Ludovic Orban
 */
@SuppressFBWarnings("SE_NO_SUITABLE_CONSTRUCTOR")
@SuppressWarnings("serial") //this class has writeReplace/readResolve methods
public class XAValueHolder<V> extends AbstractValueHolder<V> implements Serializable {

  private final V value;
  private final byte[] valueSerialized;

  public XAValueHolder(Store.ValueHolder<SoftLock<V>> valueHolder, V value) {
    super(-1, valueHolder.creationTime(), valueHolder.expirationTime());
    this.value = value;
    this.valueSerialized = null;
  }

  public XAValueHolder(V value, long creationTime) {
    super(-1, creationTime, NO_EXPIRE);
    if (value == null) {
      throw new NullPointerException("null value");
    }
    this.value = value;
    this.valueSerialized = null;
  }

  private XAValueHolder(XAValueHolder<V> valueHolder, ByteBuffer serializedValue) {
    super(-1, valueHolder.creationTime(), valueHolder.expirationTime());
    this.value = null;
    this.valueSerialized = new byte[serializedValue.remaining()];
    serializedValue.get(this.valueSerialized);
  }

  public XAValueHolder(XAValueHolder<V> valueHolder, V value) {
    super(-1, valueHolder.creationTime(), valueHolder.expirationTime());
    this.value = value;
    this.valueSerialized = null;
  }

  private XAValueHolder(long id, long creationTime, long lastAccessTime, long expirationTime, V value, byte[] valueSerialized) {
    super(id, creationTime, expirationTime);
    setLastAccessTime(lastAccessTime);
    this.value = value;
    this.valueSerialized = valueSerialized;
  }

  protected XAValueHolder<V> copyForSerialization(Serializer<V> valueSerializer) {
    ByteBuffer serializedValue = valueSerializer.serialize(value);
    return new XAValueHolder<>(this, serializedValue);
  }

  protected XAValueHolder<V> copyAfterDeserialization(Serializer<V> valueSerializer) throws ClassNotFoundException {
    return new XAValueHolder<>(this, valueSerializer.read(ByteBuffer.wrap(valueSerialized)));
  }

  @Override
  public V get() {
    return value;
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = 31 * result + value.hashCode();
    result = 31 * result + super.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || getClass() != other.getClass()) return false;

    XAValueHolder<V> that = uncheckedCast(other);

    if (!super.equals(that)) return false;
    return value.equals(that.value);
  }

  private Object writeReplace() {
    return new SerializedXAValueHolder<>(getId(), creationTime(), lastAccessTime(), expirationTime(),
      get(), valueSerialized);
  }

  /**
   * Synthetic type used as serialized form of XAValueHolder
   *
   * @param <V> the value type
   */
  private static class SerializedXAValueHolder<V> implements Serializable {
    private static final long serialVersionUID = -9126450990666297321L;
    private final long id;
    private final long creationTime;
    private final long lastAccessTime;
    private final long expirationTime;
    private final V value;
    private final byte[] valueSerialized;

    SerializedXAValueHolder(long id, long creationTime, long lastAccessTime, long expirationTime, V value, byte[] valueSerialized) {
      this.id = id;
      this.creationTime = creationTime;
      this.lastAccessTime = lastAccessTime;
      this.expirationTime = expirationTime;
      this.value = value;
      this.valueSerialized = valueSerialized;
    }

    private Object readResolve() {
      return new XAValueHolder<>(id, creationTime, lastAccessTime, expirationTime, value, valueSerialized);
    }
  }

}
