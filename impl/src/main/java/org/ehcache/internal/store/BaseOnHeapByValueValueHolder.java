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
package org.ehcache.internal.store;

import org.ehcache.spi.cache.Store.ValueHolder;
import org.ehcache.spi.serialization.Serializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

abstract class BaseOnHeapByValueValueHolder<V> implements OnHeapValueHolder<V> {
  private final ByteBuffer buffer;
  private final Serializer<V> serializer;

  protected BaseOnHeapByValueValueHolder(V value, Serializer<V> serializer) {
    if (value == null) {
      throw new NullPointerException("null value");
    }
    if (serializer == null) {
      throw new NullPointerException("null serializer");
    }
    this.serializer = serializer;
    try {
      this.buffer = serializer.serialize(value);
    } catch (IOException e) {
      throw new RuntimeException(e); // todo: better exception, please
    }
  }
  
  @Override
  public final V value() {
    try {
      return serializer.read(buffer);
    } catch (IOException e) {
      throw new RuntimeException(e); // todo: better exception, please
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e); // todo: better exception, please
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof ValueHolder)) return false;
    try {
      return serializer.equals(((ValueHolder<V>)o).value(), buffer);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe); //todo: better exception, please
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException(cnfe); //todo: better exception, please
    }
  }

  @Override
  public int hashCode() {
    return buffer.hashCode();
  }
  
  @Override
  public long creationTime(TimeUnit unit) {
    throw new AssertionError();
  }

  @Override
  public float hitRate(TimeUnit unit) {
    throw new AssertionError();
  }

  @Override
  public long lastAccessTime(TimeUnit unit) {
    throw new AssertionError();
  }

  @Override
  public boolean isExpired(long now) {
    throw new AssertionError();
  }

  @Override
  public void setAccessTimeMillis(long accessTime) {
    throw new AssertionError();
  }

  @Override
  public void setExpireTimeMillis(long expireTime) {
    throw new AssertionError();
  }
}
