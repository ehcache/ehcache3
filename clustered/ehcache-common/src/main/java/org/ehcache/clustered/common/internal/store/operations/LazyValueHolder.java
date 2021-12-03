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

package org.ehcache.clustered.common.internal.store.operations;

import org.ehcache.clustered.common.internal.store.operations.codecs.CodecException;
import org.ehcache.spi.serialization.Serializer;

import java.nio.ByteBuffer;

class LazyValueHolder<V> {

  private V value;
  private ByteBuffer encodedValue;
  private final Serializer<V> valueSerializer;

  LazyValueHolder(final V value) {
    if(value == null) {
      throw new NullPointerException("Value can not be null");
    }
    this.value = value;
    this.encodedValue = null;
    this.valueSerializer = null;
  }

  LazyValueHolder(final ByteBuffer encodedValue, final Serializer<V> valueSerializer) {
    this.encodedValue = encodedValue;
    this.valueSerializer = valueSerializer;
    this.value = null;
  }

  public V getValue() {
    if(value == null) {
      try {
        value = this.valueSerializer.read(encodedValue);
      } catch (ClassNotFoundException e) {
        throw new CodecException(e);
      }
    }
    return value;
  }

  public ByteBuffer encode(Serializer<V> valueSerializer) {
    if(encodedValue == null) {
      this.encodedValue = valueSerializer.serialize(value);
    }
    return encodedValue.duplicate();
  }
}
