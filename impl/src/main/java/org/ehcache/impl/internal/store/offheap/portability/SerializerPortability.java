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

import org.ehcache.spi.serialization.SerializerException;
import org.ehcache.spi.serialization.Serializer;

import org.terracotta.offheapstore.storage.portability.Portability;

import java.nio.ByteBuffer;

/**
 * SerializerWrapper
 */
public class SerializerPortability<T> implements Portability<T> {

  private final Serializer<T> serializer;

  public SerializerPortability(Serializer<T> serializer) {
    this.serializer = serializer;
  }

  @Override
  public ByteBuffer encode(T t) {
    return serializer.serialize(t);
  }

  @Override
  public T decode(ByteBuffer byteBuffer) {
    try {
      return serializer.read(byteBuffer);
    } catch (ClassNotFoundException e) {
      throw new SerializerException(e);
    }
  }

  @Override
  public boolean equals(Object o, ByteBuffer byteBuffer) {
    try {
      return serializer.equals((T)o, byteBuffer);
    } catch (ClassNotFoundException e) {
      throw new SerializerException(e);
    }
  }
}
