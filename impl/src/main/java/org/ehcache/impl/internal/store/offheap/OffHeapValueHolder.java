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

import org.ehcache.exceptions.SerializerException;
import org.ehcache.impl.internal.store.offheap.portability.OffHeapValueHolderPortability;
import org.ehcache.core.spi.cache.AbstractValueHolder;
import org.ehcache.core.spi.cache.Store;
import org.ehcache.spi.cache.tiering.BinaryValueHolder;
import org.ehcache.spi.serialization.Serializer;
import org.terracotta.offheapstore.storage.portability.WriteContext;
import org.terracotta.offheapstore.util.FindbugsSuppressWarnings;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
* OffHeapValueHolder
*/
@FindbugsSuppressWarnings("SE_NO_SERIALVERSIONID")
public class OffHeapValueHolder<V> extends AbstractValueHolder<V> implements BinaryValueHolder {

  public static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;

  private Mode mode;
  private transient ByteBuffer binaryValue;
  private transient Serializer<V> valueSerializer;
  private V value;
  private transient final WriteContext writeContext;

  public OffHeapValueHolder(long id, V value, long creationTime, long expireTime) {
    this(id, value, creationTime, expireTime, 0, 0);
  }

  public OffHeapValueHolder(long id, V value, long creationTime, long expireTime, long lastAccessTime, long hits) {
    super(id, creationTime, expireTime);
    setLastAccessTime(lastAccessTime, TIME_UNIT);
    this.value = value;
    this.setHits(hits);
    this.writeContext = null;
    this.mode = Mode.VALUE;
  }

  public OffHeapValueHolder(long id, ByteBuffer binaryValue, Serializer<V> serializer, long creationTime, long expireTime, long lastAccessTime, long hits, WriteContext writeContext) {
    super(id, creationTime, expireTime);
    setLastAccessTime(lastAccessTime, TIME_UNIT);
    this.binaryValue = binaryValue;
    this.valueSerializer = serializer;
    this.setHits(hits);
    this.writeContext = writeContext;
    this.mode = Mode.BINARY;
  }

  @Override
  public V value() {
    forceDeserialization();
    return value;
  }

  @Override
  public ByteBuffer getBinaryValue() throws IllegalStateException {
    if (mode != Mode.BOTH) {
      throw new IllegalStateException("This OffHeapValueHolder has not been prepared to hand off its binary form");
    }
    return binaryValue;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || getClass() != other.getClass()) return false;

    OffHeapValueHolder that = (OffHeapValueHolder)other;

    if (!super.equals(that)) return false;
    return value().equals(that.value());

  }

  @Override
  final protected TimeUnit nativeTimeUnit() {
    return TIME_UNIT;
  }

  @Override
  public int hashCode() {
    forceDeserialization();
    int result = 1;
    result = 31 * result + value.hashCode();
    result = 31 * result + super.hashCode();
    return result;
  }

  public void updateMetadata(final Store.ValueHolder<V> valueFlushed) {
    if(getId() != valueFlushed.getId()) {
      throw new IllegalArgumentException("Wrong id passed in [this.id != id] : " + getId() + " != " + valueFlushed.getId());
    }
    this.setLastAccessTime(valueFlushed.lastAccessTime(OffHeapValueHolder.TIME_UNIT), OffHeapValueHolder.TIME_UNIT);
    this.setExpirationTime(valueFlushed.expirationTime(OffHeapValueHolder.TIME_UNIT), OffHeapValueHolder.TIME_UNIT);
    this.setHits(valueFlushed.hits());
  }

  void writeBack() {
    writeContext.setLong(OffHeapValueHolderPortability.ACCESS_TIME_OFFSET, lastAccessTime(TimeUnit.MILLISECONDS));
    writeContext.setLong(OffHeapValueHolderPortability.EXPIRE_TIME_OFFSET, expirationTime(TimeUnit.MILLISECONDS));
    writeContext.setLong(OffHeapValueHolderPortability.HITS_OFFSET, hits());
    writeContext.flush();
  }

  void forceDeserialization() {
    if (value == null && mode != Mode.VALUE) {
      value = deserialize();
    }
  }

  V deserialize() {
    try {
      return valueSerializer.read(binaryValue.duplicate());
    } catch (ClassNotFoundException e) {
      throw new SerializerException(e);
    } finally {
      if (mode == Mode.BINARY) {
        binaryValue = null;
        valueSerializer = null;
        mode = Mode.VALUE;
      }
    }
  }

  void prepareForDelayedDeserialization() {
    if (mode == Mode.BINARY) {
      byte[] bytes = new byte[binaryValue.remaining()];
      binaryValue.get(bytes);
      binaryValue = ByteBuffer.wrap(bytes);
      mode = Mode.BOTH;
    } else {
      throw new IllegalStateException("OffHeapValueHolder in mode " + mode + " cannot be prepared for delayed deserialization");
    }
  }

  private enum Mode {
    VALUE, BINARY, BOTH
  }

  private void writeObject(java.io.ObjectOutputStream out) throws IOException {
    throw new UnsupportedOperationException("This subclass of AbstractValueHolder is NOT serializable");
  }

}
