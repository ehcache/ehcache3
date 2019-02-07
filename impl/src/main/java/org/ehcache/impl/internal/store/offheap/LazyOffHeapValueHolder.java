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

import org.ehcache.core.spi.store.Store;
import org.ehcache.impl.internal.store.BinaryValueHolder;
import org.ehcache.spi.serialization.SerializerException;
import org.ehcache.impl.internal.store.offheap.portability.OffHeapValueHolderPortability;
import org.ehcache.spi.serialization.Serializer;
import org.terracotta.offheapstore.storage.portability.WriteContext;

import java.nio.ByteBuffer;

/**
* OffHeapValueHolder variant that supports lazy deserialization and also serving the binary value if detached.
*/
public final class LazyOffHeapValueHolder<V> extends OffHeapValueHolder<V> implements BinaryValueHolder {

  private final Serializer<V> valueSerializer;
  private final WriteContext writeContext;
  private ByteBuffer binaryValue;
  private V value;

  public LazyOffHeapValueHolder(long id, ByteBuffer binaryValue, Serializer<V> serializer, long creationTime, long expireTime, long lastAccessTime, WriteContext writeContext) {
    super(id, creationTime, expireTime);
    setLastAccessTime(lastAccessTime);
    this.binaryValue = binaryValue;
    this.valueSerializer = serializer;
    this.writeContext = writeContext;
  }

  @Override
  public V get() {
    forceDeserialization();
    return value;
  }

  @Override
  public ByteBuffer getBinaryValue() throws IllegalStateException {
    return binaryValue.duplicate();
  }

  @Override
  void updateMetadata(final Store.ValueHolder<V> valueFlushed) {
    if(getId() != valueFlushed.getId()) {
      throw new IllegalArgumentException("Wrong id passed in [this.id != id] : " + getId() + " != " + valueFlushed.getId());
    }
    this.setLastAccessTime(valueFlushed.lastAccessTime());
    this.setExpirationTime(valueFlushed.expirationTime());
  }

  /**
   * Must be called under offheap lock, may corrupt memory otherwise
   */
  @Override
  void writeBack() {
    writeContext.setLong(OffHeapValueHolderPortability.ACCESS_TIME_OFFSET, lastAccessTime());
    writeContext.setLong(OffHeapValueHolderPortability.EXPIRE_TIME_OFFSET, expirationTime());
    writeContext.flush();
  }

  /**
   * Must be called under offheap lock (when it actually does something)
   */
  @Override
  void forceDeserialization() {
    if (value == null) {
      try {
        value = valueSerializer.read(binaryValue.duplicate());
      } catch (ClassNotFoundException e) {
        throw new SerializerException(e);
      } catch (SerializerException e) {
        throw new SerializerException("Seeing this exception and having no other " +
                                      "serialization related issues is a red flag!", e);
      }
    }
  }

  private void writeObject(java.io.ObjectOutputStream out) {
    throw new UnsupportedOperationException("This subclass of AbstractValueHolder is NOT serializable");
  }

}
