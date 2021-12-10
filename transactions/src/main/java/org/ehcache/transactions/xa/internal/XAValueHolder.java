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

import org.ehcache.core.spi.store.AbstractValueHolder;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.serialization.Serializer;
import org.terracotta.offheapstore.util.FindbugsSuppressWarnings;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * The {@link XAStore} {@link Store.ValueHolder} implementation.
 *
 * @author Ludovic Orban
 */
@FindbugsSuppressWarnings("SE_NO_SUITABLE_CONSTRUCTOR")
public class XAValueHolder<V> extends AbstractValueHolder<V> implements Serializable {

  static final TimeUnit NATIVE_TIME_UNIT = TimeUnit.MILLISECONDS;

  private final V value;
  private final byte[] valueSerialized;

  public XAValueHolder(Store.ValueHolder<SoftLock<V>> valueHolder, V value) {
    super(-1, valueHolder.creationTime(TimeUnit.MILLISECONDS), valueHolder.expirationTime(TimeUnit.MILLISECONDS));
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
    super(-1, valueHolder.creationTime(TimeUnit.MILLISECONDS), valueHolder.expirationTime(TimeUnit.MILLISECONDS));
    this.value = null;
    this.valueSerialized = new byte[serializedValue.remaining()];
    serializedValue.get(this.valueSerialized);
  }

  public XAValueHolder(XAValueHolder<V> valueHolder, V value) {
    super(-1, valueHolder.creationTime(TimeUnit.MILLISECONDS), valueHolder.expirationTime(TimeUnit.MILLISECONDS));
    this.value = value;
    this.valueSerialized = null;
  }

  private XAValueHolder(long id, long creationTime, long lastAccessTime, long expirationTime, long hits, V value, byte[] valueSerialized) {
    super(id, creationTime, expirationTime);
    setLastAccessTime(lastAccessTime, NATIVE_TIME_UNIT);
    setHits(hits);
    this.value = value;
    this.valueSerialized = valueSerialized;
  }

  protected XAValueHolder<V> copyForSerialization(Serializer<V> valueSerializer) {
    ByteBuffer serializedValue = valueSerializer.serialize(value);
    return new XAValueHolder<V>(this, serializedValue);
  }

  protected XAValueHolder<V> copyAfterDeserialization(Serializer<V> valueSerializer) throws ClassNotFoundException {
    return new XAValueHolder<V>(this, valueSerializer.read(ByteBuffer.wrap(valueSerialized)));
  }

  @Override
  protected TimeUnit nativeTimeUnit() {
    return NATIVE_TIME_UNIT;
  }

  @Override
  public V value() {
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

    @SuppressWarnings("unchecked")
    XAValueHolder<V> that = (XAValueHolder<V>) other;

    if (!super.equals(that)) return false;
    return value.equals(that.value);
  }

  private Object writeReplace() throws ObjectStreamException {
    return new SerializedXAValueHolder<V>(getId(), creationTime(NATIVE_TIME_UNIT), lastAccessTime(NATIVE_TIME_UNIT), expirationTime(NATIVE_TIME_UNIT),
        hits(), value(), valueSerialized);
  }

  /**
   * Synthetic type used as serialized form of XAValueHolder
   *
   * @param <V> the value type
   */
  private static class SerializedXAValueHolder<V> implements Serializable {
    private final long id;
    private final long creationTime;
    private final long lastAccessTime;
    private final long expirationTime;
    private final long hits;
    private final V value;
    private final byte[] valueSerialized;

    SerializedXAValueHolder(long id, long creationTime, long lastAccessTime, long expirationTime, long hits, V value, byte[] valueSerialized) {
      this.id = id;
      this.creationTime = creationTime;
      this.lastAccessTime = lastAccessTime;
      this.expirationTime = expirationTime;
      this.hits = hits;
      this.value = value;
      this.valueSerialized = valueSerialized;
    }

    private Object readResolve() throws ObjectStreamException {
      return new XAValueHolder<V>(id, creationTime, lastAccessTime, expirationTime, hits, value, valueSerialized);
    }
  }

}
