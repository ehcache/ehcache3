/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

package org.ehcache.impl.internal.store.shared.composites;

import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.SerializerException;

import java.nio.ByteBuffer;
import java.util.Map;

public class CompositeSerializer implements Serializer<CompositeValue<?>> {
  private final Map<Integer, Serializer<?>> serializerMap;

  public CompositeSerializer(Map<Integer, Serializer<?>> serializerMap) {
    this.serializerMap = serializerMap;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public ByteBuffer serialize(CompositeValue<?> compositeValue) throws SerializerException {
    int id = compositeValue.getStoreId();
    ByteBuffer valueForm = ((Serializer) serializerMap.get(id)).serialize(compositeValue.getValue());
    ByteBuffer compositeForm = ByteBuffer.allocate(4 + valueForm.remaining());
    compositeForm.putInt(compositeValue.getStoreId());
    compositeForm.put(valueForm);
    compositeForm.flip();
    return compositeForm;
  }

  @Override
  public CompositeValue<?> read(ByteBuffer binary) throws ClassNotFoundException, SerializerException {
    int id = binary.getInt();
    Serializer<?> serializer = serializerMap.get(id);
    if (serializer == null) {
      return new CompositeValue<>(id, null);
    } else {
      return new CompositeValue<>(id, serializer.read(binary));
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public boolean equals(CompositeValue<?> object, ByteBuffer binary) throws ClassNotFoundException, SerializerException {
    int id = binary.getInt();
    if (id == object.getStoreId()) {
      Serializer<?> serializer = serializerMap.get(id);
      if (serializer == null) {
        return false;
      } else {
        return ((Serializer) serializer).equals(object.getValue(), binary.slice());
      }
    } else {
      return false;
    }
  }
}
