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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.ehcache.spi.serialization.SerializerException;
import org.ehcache.impl.internal.util.ByteBufferInputStream;
import org.ehcache.spi.serialization.Serializer;

/**
 * A trivially compressed Java serialization based serializer.
 * <p>
 * Class descriptors in the resultant bytes are encoded as integers.  Mappings
 * between the integer representation and the {@link ObjectStreamClass}, and the
 * {@code Class} and the integer representation are stored in a single on-heap
 * map.
 */
public class CompactJavaSerializer<T> implements Serializer<T>, Closeable {

  private final AtomicInteger nextStreamIndex = new AtomicInteger(0);

  private final ConcurrentMap<Integer, ObjectStreamClass> readLookup = new ConcurrentHashMap<Integer, ObjectStreamClass>();
  private final ConcurrentMap<SerializableDataKey, Integer> writeLookup = new ConcurrentHashMap<SerializableDataKey, Integer>();

  private final transient ClassLoader loader;

  /**
   * Constructor to enable this serializer as a transient one.
   *
   * @param loader the classloader to use
   *
   * @see Serializer
   */
  public CompactJavaSerializer(ClassLoader loader) {
    this.loader = loader;
  }

  CompactJavaSerializer(ClassLoader loader, Map<Integer, ObjectStreamClass> mappings) {
    this(loader);
    for (Entry<Integer, ObjectStreamClass> e : mappings.entrySet()) {
      Integer encoding = e.getKey();
      ObjectStreamClass disconnectedOsc = disconnect(e.getValue());
      readLookup.put(encoding, disconnectedOsc);
      if (writeLookup.putIfAbsent(new SerializableDataKey(disconnectedOsc, true), encoding) != null) {
        throw new AssertionError("Corrupted data " + mappings.toString());
      }
      if (nextStreamIndex.get() < encoding + 1) {
        nextStreamIndex.set(encoding + 1);
      }
    }
  }

  Map<Integer, ObjectStreamClass> getSerializationMappings() {
    return Collections.unmodifiableMap(new HashMap<Integer, ObjectStreamClass>(readLookup));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ByteBuffer serialize(T object) throws SerializerException {
    try {
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      ObjectOutputStream oout = getObjectOutputStream(bout);
      try {
        oout.writeObject(object);
      } finally {
        oout.close();
      }
      return ByteBuffer.wrap(bout.toByteArray());
    } catch (IOException e) {
      throw new SerializerException(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public T read(ByteBuffer binary) throws ClassNotFoundException, SerializerException {
    try {
      ObjectInputStream oin = getObjectInputStream(new ByteBufferInputStream(binary));
      try {
        return (T) oin.readObject();
      } finally {
        oin.close();
      }
    } catch (IOException e) {
      throw new SerializerException(e);
    }
  }

  private ObjectOutputStream getObjectOutputStream(OutputStream out) throws IOException {
    return new OOS(out);
  }

  private ObjectInputStream getObjectInputStream(InputStream input) throws IOException {
    return new OIS(input, loader);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(T object, ByteBuffer binary) throws ClassNotFoundException, SerializerException {
    return object.equals(read(binary));
  }

  private int getOrAddMapping(ObjectStreamClass desc) throws IOException {
    SerializableDataKey probe = new SerializableDataKey(desc, false);
    Integer rep = writeLookup.get(probe);
    if (rep == null) {
      ObjectStreamClass disconnected = disconnect(desc);
      SerializableDataKey key = new SerializableDataKey(disconnected, true);
      rep = nextStreamIndex.getAndIncrement();

      ObjectStreamClass existingOsc = readLookup.putIfAbsent(rep, disconnected);
      if (existingOsc == null) {
        Integer existingRep = writeLookup.putIfAbsent(key, rep);
        if (existingRep == null) {
          return rep;
        } else {
          /*
           * A racing thread established a mapping already.  We must clean up
           * our half complete mapping.
           */
          readLookup.remove(rep);
          return existingRep;
        }
      } else {
        //impossible as governed by AtomicInteger - excluding wrap-around == 2^32 types)
        throw new AssertionError();
      }
    } else {
      return rep;
    }
  }

  /**
   * Closes this serializer, clearing all known mappings.
   */
  @Override
  public void close() {
    readLookup.clear();
    writeLookup.clear();
  }

  class OOS extends ObjectOutputStream {

    OOS(OutputStream out) throws IOException {
      super(out);
    }

    @Override
    protected void writeClassDescriptor(final ObjectStreamClass desc) throws IOException {
      writeInt(getOrAddMapping(desc));
    }
  }

  class OIS extends ObjectInputStream {

    private final ClassLoader loader;

    OIS(InputStream in, ClassLoader loader) throws IOException {
      super(in);
      this.loader = loader;
    }

    @Override
    protected ObjectStreamClass readClassDescriptor() throws IOException, ClassNotFoundException {
      return readLookup.get(readInt());
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
      try {
        final ClassLoader cl = loader == null ? Thread.currentThread().getContextClassLoader() : loader;
        if (cl == null) {
          return super.resolveClass(desc);
        } else {
          try {
            return Class.forName(desc.getName(), false, cl);
          } catch (ClassNotFoundException e) {
            return super.resolveClass(desc);
          }
        }
      } catch (SecurityException ex) {
        return super.resolveClass(desc);
      }
    }
  }

  private static class SerializableDataKey {
    private final ObjectStreamClass osc;
    private final int hashCode;

    private transient WeakReference<Class<?>> klazz;

    SerializableDataKey(ObjectStreamClass desc, boolean store) {
      Class<?> forClass = desc.forClass();
      if (forClass != null) {
        if (store) {
          throw new AssertionError("Must not store ObjectStreamClass instances with strong references to classes");
        } else if (ObjectStreamClass.lookup(forClass) == desc) {
          this.klazz = new WeakReference<Class<?>>(forClass);
        }
      }
      this.hashCode = (3 * desc.getName().hashCode()) ^ (7 * (int) (desc.getSerialVersionUID() >>> 32))
          ^ (11 * (int) desc.getSerialVersionUID());
      this.osc = desc;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof SerializableDataKey) {
        return CompactJavaSerializer.equals(this, (SerializableDataKey) o);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    Class<?> forClass() {
      if (klazz == null) {
        return null;
      } else {
        return klazz.get();
      }
    }

    public void setClass(Class<?> clazz) {
      klazz = new WeakReference<Class<?>>(clazz);
    }

    ObjectStreamClass getObjectStreamClass() {
      return osc;
    }
  }

  private static boolean equals(SerializableDataKey k1, SerializableDataKey k2) {
    Class<?> k1Clazz = k1.forClass();
    Class<?> k2Clazz = k2.forClass();
    if (k1Clazz != null && k2Clazz != null) {
      return k1Clazz == k2Clazz;
    } else if (CompactJavaSerializer.equals(k1.getObjectStreamClass(), k2.getObjectStreamClass())) {
      if (k1Clazz != null) {
        k2.setClass(k1Clazz);
      } else if (k2Clazz != null) {
        k1.setClass(k2Clazz);
      }
      return true;
    } else {
      return false;
    }
  }

  private static boolean equals(ObjectStreamClass osc1, ObjectStreamClass osc2) {
    if (osc1 == osc2) {
      return true;
    } else if (osc1.getName().equals(osc2.getName()) && osc1.getSerialVersionUID() == osc2.getSerialVersionUID() && osc1.getFields().length == osc2.getFields().length) {
      try {
        return Arrays.equals(getSerializedForm(osc1), getSerializedForm(osc2));
      } catch (IOException e) {
        throw new AssertionError(e);
      }
    } else {
      return false;
    }
  }

  private static ObjectStreamClass disconnect(ObjectStreamClass desc) {
    try {
      ObjectInputStream oin = new ObjectInputStream(new ByteArrayInputStream(getSerializedForm(desc))) {

        @Override
        protected Class<?> resolveClass(ObjectStreamClass osc) {
          //Our stored OSC instances should not reference classes - doing so could cause perm-gen leaks
          return null;
        }
      };

      return (ObjectStreamClass) oin.readObject();
    } catch (ClassNotFoundException e) {
      throw new AssertionError(e);
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  private static byte[] getSerializedForm(ObjectStreamClass desc) throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    try {
      ObjectOutputStream oout = new ObjectOutputStream(bout);
      try {
        oout.writeObject(desc);
      } finally {
        oout.close();
      }
    } finally {
      bout.close();
    }
    return bout.toByteArray();
  }
}
