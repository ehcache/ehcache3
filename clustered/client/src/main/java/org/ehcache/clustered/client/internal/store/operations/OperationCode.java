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

package org.ehcache.clustered.client.internal.store.operations;

import org.ehcache.spi.serialization.Serializer;

import java.nio.ByteBuffer;

public enum OperationCode {

  PUT((byte)1) {
    @Override
    public <K, V> Operation<K, V> decode(ByteBuffer buffer, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
      return new PutOperation<>(buffer, keySerializer, valueSerializer);
    }
  },
  REMOVE((byte)2) {
    @Override
    public <K, V> Operation<K, V> decode(final ByteBuffer buffer, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
      return new RemoveOperation<>(buffer, keySerializer);
    }
  },
  PUT_IF_ABSENT((byte)3) {
    @Override
    public <K, V> Operation<K, V> decode(final ByteBuffer buffer, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
      return new PutIfAbsentOperation<>(buffer, keySerializer, valueSerializer);
    }
  },
  REMOVE_CONDITIONAL((byte)4) {
    @Override
    public <K, V> Operation<K, V> decode(final ByteBuffer buffer, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
      return new ConditionalRemoveOperation<>(buffer, keySerializer, valueSerializer);
    }
  },
  REPLACE((byte)5) {
    @Override
    public <K, V> Operation<K, V> decode(final ByteBuffer buffer, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
      return new ReplaceOperation<>(buffer, keySerializer, valueSerializer);
    }
  },
  REPLACE_CONDITIONAL((byte)6) {
    @Override
    public <K, V> Operation<K, V> decode(final ByteBuffer buffer, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
      return new ConditionalReplaceOperation<>(buffer, keySerializer, valueSerializer);
    }
  };

  private final byte value;

  OperationCode(byte value) {
    this.value = value;
  }

  public byte getValue() {
    return value;
  }

  public abstract  <K, V> Operation<K, V> decode(ByteBuffer buffer, Serializer<K> keySerializer, Serializer<V> valueSerializer);

  public static OperationCode valueOf(byte value) {
    switch (value) {
      case 1:
        return PUT;
      case 2:
        return REMOVE;
      case 3:
        return PUT_IF_ABSENT;
      case 4:
        return REMOVE_CONDITIONAL;
      case 5:
        return REPLACE;
      case 6:
        return REPLACE_CONDITIONAL;
      default:
        throw new IllegalArgumentException("Operation undefined for the value " + value);
    }
  }
}
