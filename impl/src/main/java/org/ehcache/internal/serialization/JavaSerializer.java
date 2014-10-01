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
package org.ehcache.internal.serialization;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

import org.ehcache.internal.util.ByteBufferInputStream;

/**
 *
 * @author cdennis
 */
public class JavaSerializer<T extends Serializable> implements Serializer<T> {

  public JavaSerializer() {
  }

  @Override
  public ByteBuffer serialize(T object) throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    try {
      ObjectOutputStream oout = new ObjectOutputStream(bout);
      oout.writeObject(object);
    } finally {
      bout.close();
    }
    return ByteBuffer.wrap(bout.toByteArray());
  }

  @Override
  public T read(ByteBuffer entry) throws IOException, ClassNotFoundException {
    ByteBufferInputStream bin = null;
    try {
      bin = new ByteBufferInputStream(entry);
      ObjectInputStream oin = new ObjectInputStream(bin);
      try {
        return (T)oin.readObject();
      } finally {
        oin.close();
      }
    } finally {
      if (bin != null) {
        bin.close();
      }
    }
  }

  @Override
  public boolean equals(T object, ByteBuffer binary) throws IOException, ClassNotFoundException {
    return object.equals(read(binary));
  }
}
