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

package org.ehcache.impl.internal.store.offheap.portability;

import org.ehcache.impl.internal.store.BinaryValueHolder;
import org.ehcache.impl.internal.store.offheap.LazyOffHeapValueHolder;
import org.ehcache.impl.internal.store.offheap.OffHeapValueHolder;
import org.ehcache.spi.serialization.Serializer;
import org.terracotta.offheapstore.storage.portability.WriteBackPortability;
import org.terracotta.offheapstore.storage.portability.WriteContext;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * OffHeapValueHolderPortability
 */
public class OffHeapValueHolderPortability<V> implements WriteBackPortability<OffHeapValueHolder<V>> {

  public static final int ACCESS_TIME_OFFSET = 16;
  public static final int EXPIRE_TIME_OFFSET = 24;

  // 5 longs: id, access, expire, creation time, hits (which is kept for compatibility)
  private static final int FIELDS_OVERHEAD = 40;

  private final Serializer<V> serializer;

  public OffHeapValueHolderPortability(Serializer<V> serializer) {
    this.serializer = serializer;
  }

  @Override
  public ByteBuffer encode(OffHeapValueHolder<V> valueHolder) {
    ByteBuffer serialized;
    if (valueHolder instanceof BinaryValueHolder && ((BinaryValueHolder)valueHolder).isBinaryValueAvailable()) {
      serialized = ((BinaryValueHolder)valueHolder).getBinaryValue();
    } else {
      serialized = serializer.serialize(valueHolder.get());
    }
    ByteBuffer byteBuffer = ByteBuffer.allocate(serialized.remaining() + FIELDS_OVERHEAD);
    byteBuffer.putLong(valueHolder.getId());
    byteBuffer.putLong(valueHolder.creationTime());
    byteBuffer.putLong(valueHolder.lastAccessTime());
    byteBuffer.putLong(valueHolder.expirationTime());
    byteBuffer.putLong(0L); // represent the hits on previous versions. It is kept for compatibility reasons with previously saved data
    byteBuffer.put(serialized);
    byteBuffer.flip();
    return byteBuffer;
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
    long id = byteBuffer.getLong();
    long creationTime = byteBuffer.getLong();
    long lastAccessTime = byteBuffer.getLong();
    long expireTime = byteBuffer.getLong();
    byteBuffer.getLong(); // hits read from disk. It is kept for compatibility reasons with previously saved data
    return createLazyOffHeapValueHolder(id, byteBuffer.slice(), serializer, creationTime, expireTime, lastAccessTime, writeContext);
  }

  protected OffHeapValueHolder<V> createLazyOffHeapValueHolder(long id, ByteBuffer byteBuffer, Serializer<V> serializer, long creationTime, long expireTime, long lastAccessTime, WriteContext writeContext) {
    return new LazyOffHeapValueHolder<>(id, byteBuffer, serializer, creationTime, expireTime, lastAccessTime, writeContext);
  }
}
