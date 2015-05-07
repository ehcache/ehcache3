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

package org.ehcache.internal.store.offheap.portability;

import org.ehcache.internal.store.offheap.OffHeapValueHolder;
import org.ehcache.spi.serialization.Serializer;

import org.terracotta.offheapstore.storage.portability.WriteBackPortability;
import org.terracotta.offheapstore.storage.portability.WriteContext;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * OffHeapValueHolderPortability
 */
public class OffHeapValueHolderPortability<V> implements WriteBackPortability<OffHeapValueHolder<V>> {

  public static final int ACCESS_TIME_OFFSET = 8;
  public static final int EXPIRE_TIME_OFFSET = 16;

  // 3 longs: access, expire, creation time
  private static final int FIELDS_OVERHEAD = 24;

  private final Serializer<V> serializer;

  public OffHeapValueHolderPortability(Serializer<V> serializer) {
    this.serializer = serializer;
  }

  @Override
  public ByteBuffer encode(OffHeapValueHolder<V> valueHolder) {
    try {
      ByteBuffer serialized = serializer.serialize(valueHolder.value());
      ByteBuffer byteBuffer = ByteBuffer.allocate(serialized.remaining() + FIELDS_OVERHEAD);
      byteBuffer.putLong(valueHolder.creationTime(OffHeapValueHolder.TIME_UNIT));
      byteBuffer.putLong(valueHolder.lastAccessTime(OffHeapValueHolder.TIME_UNIT));
      byteBuffer.putLong(valueHolder.expirationTime(OffHeapValueHolder.TIME_UNIT));
      byteBuffer.put(serialized);
      byteBuffer.flip();
      return byteBuffer;
    } catch (IOException e) {
      throw new RuntimeException("Failure while encoding value holder " + valueHolder, e);
    }
  }

  @Override
  public OffHeapValueHolder<V> decode(ByteBuffer byteBuffer) {
    return decode(byteBuffer, null);
  }

  @Override
  public boolean equals(Object o, ByteBuffer byteBuffer) {
    return o.equals(decode(byteBuffer));
  }

  @Override
  public OffHeapValueHolder<V> decode(ByteBuffer byteBuffer, WriteContext writeContext) {
    try {
      long creationTime = byteBuffer.getLong();
      long lastAccessTime = byteBuffer.getLong();
      long expireTime = byteBuffer.getLong();
      OffHeapValueHolder<V> valueHolder = new OffHeapValueHolder<V>(-1, serializer.read(byteBuffer), creationTime, expireTime, lastAccessTime, writeContext);
      return valueHolder;
    } catch (IOException e) {
      // TODO find a proper exception type
      throw new RuntimeException(e);
    } catch (ClassNotFoundException e) {
      // TODO find a proper exception type
      throw new RuntimeException(e);
    }
  }
}
