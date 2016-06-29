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

/**
 * OffHeapValueHolderPortability
 */
public class OffHeapValueHolderPortability<V> implements WriteBackPortability<OffHeapValueHolder<V>> {

  public static final int ACCESS_TIME_OFFSET = 16;
  public static final int EXPIRE_TIME_OFFSET = 24;
  public static final int HITS_OFFSET = 32;

  // 5 longs: id, access, expire, creation time, hits
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
      serialized = serializer.serialize(valueHolder.value());
    }
    ByteBuffer byteBuffer = ByteBuffer.allocate(serialized.remaining() + FIELDS_OVERHEAD);
    byteBuffer.putLong(valueHolder.getId());
    byteBuffer.putLong(valueHolder.creationTime(OffHeapValueHolder.TIME_UNIT));
    byteBuffer.putLong(valueHolder.lastAccessTime(OffHeapValueHolder.TIME_UNIT));
    byteBuffer.putLong(valueHolder.expirationTime(OffHeapValueHolder.TIME_UNIT));
    byteBuffer.putLong(valueHolder.hits());
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
    long hits = byteBuffer.getLong();
    return new LazyOffHeapValueHolder<V>(id, byteBuffer.slice(), serializer,
        creationTime, expireTime, lastAccessTime, hits, writeContext);
  }
}
