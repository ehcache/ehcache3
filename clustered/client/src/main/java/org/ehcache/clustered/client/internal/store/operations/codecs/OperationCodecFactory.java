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

package org.ehcache.clustered.client.internal.store.operations.codecs;

import org.ehcache.spi.serialization.Serializer;

/**
 * Base codec factory for all operations
 */
public abstract class OperationCodecFactory {

  public abstract <K, V> OperationCodec<K> getOperationCodec(Serializer<K> keySerializer, Serializer<V> valueSerializer);

  /**
   * Codec factory for put operation
   */
  public static class PutOperationCodecFactory extends OperationCodecFactory {

    public <K, V> OperationCodec<K> getOperationCodec(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
      return new PutOperationCodec<K, V>(keySerializer, valueSerializer);
    }
  }

  /**
   * Codec factory for put operation
   */
  public static class RemoveOperationCodecFactory extends OperationCodecFactory {

    public <K, V> OperationCodec<K> getOperationCodec(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
      return new RemoveOperationCodec<K>(keySerializer);
    }
  }

  /**
   * Codec factory for put operation
   */
  public static class PutIfAbsentOperationCodecFactory extends OperationCodecFactory {

    public <K, V> OperationCodec<K> getOperationCodec(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
      return new PutIfAbsentOperationCodec<K, V>(keySerializer, valueSerializer);
    }
  }
}