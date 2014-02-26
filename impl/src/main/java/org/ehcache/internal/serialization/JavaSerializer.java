/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
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
    try (ByteBufferInputStream bin = new ByteBufferInputStream(entry)) {
      ObjectInputStream oin = new ObjectInputStream(bin);
      return (T)oin.readObject();
    }
  }

  @Override
  public boolean equals(T object, ByteBuffer binary) throws IOException, ClassNotFoundException {
    return object.equals(read(binary));
  }
}
