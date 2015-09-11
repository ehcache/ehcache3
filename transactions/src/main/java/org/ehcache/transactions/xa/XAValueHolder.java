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
package org.ehcache.transactions.xa;

import org.ehcache.spi.cache.AbstractValueHolder;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * @author Ludovic Orban
 */
public class XAValueHolder<V> extends AbstractValueHolder<V> {

  private static final TimeUnit NATIVE_TIME_UNIT = TimeUnit.MILLISECONDS;

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

    XAValueHolder<V> that = (XAValueHolder) other;

    if (!super.equals(that)) return false;
    return value.equals(that.value);
  }

}
