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

package org.ehcache.impl.internal.copy;

import org.ehcache.exceptions.SerializerException;
import org.ehcache.impl.internal.copy.ReadWriteCopier;
import org.ehcache.spi.serialization.Serializer;

/**
 * A helper copier implementation that performs the "copying" using serialization.
 *
 * @author Albin Suresh
 */
public final class SerializingCopier<T> extends ReadWriteCopier<T> {

  private final Serializer<T> serializer;

  public SerializingCopier(Serializer<T> serializer) {
    if (serializer == null) {
      throw new NullPointerException("A " + SerializingCopier.class.getName() + " instance requires a "
                                     + Serializer.class.getName() + " instance to copy!");
    }
    this.serializer = serializer;
  }

  @Override
  public T copy(final T obj) {
    try {
      return serializer.read(serializer.serialize(obj));
    } catch (ClassNotFoundException e) {
      throw new SerializerException("Copying failed.", e);
    }
  }

  public Serializer<T> getSerializer() {
    return serializer;
  }
}
