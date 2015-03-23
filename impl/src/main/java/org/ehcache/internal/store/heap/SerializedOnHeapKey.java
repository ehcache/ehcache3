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

package org.ehcache.internal.store.heap;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.ehcache.exceptions.SerializerException;
import org.ehcache.spi.serialization.Serializer;

class SerializedOnHeapKey<K> extends BaseOnHeapKey<K> {

  private final ByteBuffer keyAsStored;
  private final Serializer<K> serializer;

  SerializedOnHeapKey(K actualKeyObject, Serializer<K> serializer) {
    super(actualKeyObject.hashCode());
    try {
      this.keyAsStored = serializer.serialize(actualKeyObject);
    } catch (IOException e) {
      throw new SerializerException(e);
    }

    this.serializer = serializer;
  }

  @Override
  public K getActualKeyObject() {
    try {
      return serializer.read(keyAsStored);
    } catch (ClassNotFoundException e) {
      throw new SerializerException(e);
    } catch (IOException e) {
      throw new SerializerException(e);
    }
  }

}
