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

package org.ehcache.internal;

import org.ehcache.Ehcache;
import org.ehcache.internal.serialization.Serializer;

import java.io.IOException;
import java.io.NotSerializableException;
import java.nio.ByteBuffer;

/**
 * @author cdennis
 */
class SerializingCache<K, V> extends Ehcache<K, V> {

  private final Serializer<V> valueSerializer;
  private final HeapCache<K, ByteBuffer> cheat;

  public SerializingCache(Serializer<V> valueSerializer) {
    super(null);
    this.valueSerializer = valueSerializer;
    this.cheat = new HeapCache<K, ByteBuffer>();
  }

  @Override
  public V get(K key) {
    ByteBuffer binary = cheat.get(key);
    try {
      return valueSerializer.read(binary);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void remove(K key) {
    cheat.remove(key);
  }

  @Override
  public void put(K key, V value) {
    ByteBuffer binary;
    try {
      binary = valueSerializer.serialize(value);
    } catch (NotSerializableException e) {
      throw new IllegalArgumentException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    cheat.put(key, binary);
  }

  @Override
  public boolean containsKey(K key) {
    return cheat.containsKey(key);
  }
}
