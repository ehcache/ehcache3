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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * BinaryOffHeapValueHolder
 */
final class BinaryOffHeapValueHolder<V> extends OffHeapValueHolder<V> implements BinaryValueHolder {

  private final ByteBuffer binaryValue;
  private final V value;

  BinaryOffHeapValueHolder(long id, V value, ByteBuffer binaryValue, long creationTime, long expireTime, long lastAccessTime) {
    super(id, creationTime, expireTime);
    this.value = value;
    setLastAccessTime(lastAccessTime);
    this.binaryValue = binaryValue;
  }


  @Override
  public ByteBuffer getBinaryValue() throws IllegalStateException {
    return binaryValue.duplicate();
  }

  @Override
  public boolean isBinaryValueAvailable() {
    return true;
  }

  @Override
  void updateMetadata(Store.ValueHolder<V> valueFlushed) {
    throw new UnsupportedOperationException();
  }

  @Override
  void writeBack() {
    throw new UnsupportedOperationException();
  }

  @Override
  void forceDeserialization() {
    throw new UnsupportedOperationException();
  }

  @Override
  void detach() {
    throw new UnsupportedOperationException();
  }

  @Override
  public V get() {
    return value;
  }

  private void writeObject(java.io.ObjectOutputStream out) {
    throw new UnsupportedOperationException("This subclass of AbstractValueHolder is NOT serializable");
  }
}
