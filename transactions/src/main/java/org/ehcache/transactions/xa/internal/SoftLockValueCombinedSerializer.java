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

import org.ehcache.spi.serialization.SerializerException;
import org.ehcache.spi.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The {@link Serializer} that is responsible for serializing a {@link SoftLock} containing a value.
 *
 * @author Ludovic Orban
 */
class SoftLockValueCombinedSerializer<T> implements Serializer<SoftLock<T>> {

  private final AtomicReference<? extends Serializer<SoftLock<T>>> softLockSerializerRef;
  private final Serializer<T> valueSerializer;

  SoftLockValueCombinedSerializer(AtomicReference<? extends Serializer<SoftLock<T>>> softLockSerializerRef, Serializer<T> valueSerializer) {
    this.softLockSerializerRef = softLockSerializerRef;
    this.valueSerializer = valueSerializer;
  }

  @Override
  public ByteBuffer serialize(SoftLock<T> softLock) throws SerializerException {
    return softLockSerializerRef.get().serialize(softLock.copyForSerialization(valueSerializer));
  }

  @Override
  public SoftLock<T> read(ByteBuffer binary) throws ClassNotFoundException, SerializerException {
    SoftLock<T> serializedSoftLock = softLockSerializerRef.get().read(binary);
    return serializedSoftLock.copyAfterDeserialization(valueSerializer, serializedSoftLock);
  }

  @Override
  public boolean equals(SoftLock<T> object, ByteBuffer binary) throws ClassNotFoundException, SerializerException {
    return object.equals(read(binary));
  }
}
