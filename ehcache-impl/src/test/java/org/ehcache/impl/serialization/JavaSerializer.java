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
package org.ehcache.impl.serialization;

import org.ehcache.spi.serialization.SerializerException;
import org.ehcache.core.util.ByteBufferInputStream;
import org.ehcache.spi.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author cdennis
 */
public class JavaSerializer<T> implements Serializer<T> {

  private final ClassLoader classLoader;

  public JavaSerializer(ClassLoader classLoader) {
    this.classLoader = classLoader;
  }

  @Override
  public ByteBuffer serialize(T object) {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    try {
      ObjectOutputStream oout = new ObjectOutputStream(bout);
      oout.writeObject(object);
    } catch (IOException e) {
      throw new SerializerException(e);
    } finally {
      try {
        bout.close();
      } catch (IOException e) {
        throw new AssertionError(e);
      }
    }
    return ByteBuffer.wrap(bout.toByteArray());
  }

  @SuppressWarnings("unchecked")
  @Override
  public T read(ByteBuffer entry) throws SerializerException, ClassNotFoundException {
    ByteBufferInputStream bin = new ByteBufferInputStream(entry);
    try {
      try (OIS ois = new OIS(bin, classLoader)) {
        return (T) ois.readObject();
      }
    } catch (IOException e) {
      throw new SerializerException(e);
    } finally {
      try {
        bin.close();
      } catch (IOException e) {
        throw new AssertionError(e);
      }
    }
  }

  @Override
  public boolean equals(T object, ByteBuffer binary) throws SerializerException, ClassNotFoundException {
    return object.equals(read(binary));
  }

  private static class OIS extends ObjectInputStream {

    private final ClassLoader classLoader;

    public OIS(InputStream in, ClassLoader classLoader) throws IOException {
      super(in);
      this.classLoader = classLoader;
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
      try {
        return Class.forName(desc.getName(), false, classLoader);
      } catch (ClassNotFoundException cnfe) {
        Class<?> primitive = primitiveClasses.get(desc.getName());
        if (primitive != null) {
          return primitive;
        }
        throw cnfe;
      }
    }

    @Override
    protected Class<?> resolveProxyClass(String[] interfaces) throws IOException, ClassNotFoundException {
      Class<?>[] interfaceClasses = new Class<?>[interfaces.length];
      for (int i = 0; i < interfaces.length; i++) {
        interfaceClasses[i] = Class.forName(interfaces[i], false, classLoader);
      }

      return Proxy.getProxyClass(classLoader, interfaceClasses);
    }

    private static final Map<String, Class<?>> primitiveClasses = new HashMap<>();
    static {
      primitiveClasses.put("boolean", boolean.class);
      primitiveClasses.put("byte", byte.class);
      primitiveClasses.put("char", char.class);
      primitiveClasses.put("double", double.class);
      primitiveClasses.put("float", float.class);
      primitiveClasses.put("int", int.class);
      primitiveClasses.put("long", long.class);
      primitiveClasses.put("short", short.class);
      primitiveClasses.put("void", void.class);
    }
  }

}
