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

import org.ehcache.impl.internal.store.offheap.AssertingOffHeapValueHolder;
import org.ehcache.impl.internal.store.offheap.OffHeapValueHolder;
import org.ehcache.spi.serialization.Serializer;
import org.terracotta.offheapstore.storage.portability.WriteContext;

import java.nio.ByteBuffer;

public class AssertingOffHeapValueHolderPortability<V> extends OffHeapValueHolderPortability<V> {

  public AssertingOffHeapValueHolderPortability(Serializer<V> serializer) {
    super(serializer);
  }

  @Override
  protected OffHeapValueHolder<V> createLazyOffHeapValueHolder(long id, ByteBuffer byteBuffer, Serializer<V> serializer, long creationTime, long expireTime, long lastAccessTime, WriteContext writeContext) {
    return new AssertingOffHeapValueHolder<>(id, byteBuffer, serializer, creationTime, expireTime, lastAccessTime, writeContext);
  }
}
