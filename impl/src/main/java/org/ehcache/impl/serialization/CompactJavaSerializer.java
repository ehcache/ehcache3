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
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.ehcache.spi.persistence.StateHolder;
import org.ehcache.spi.persistence.StateRepository;
import org.ehcache.spi.serialization.SerializerException;
import org.ehcache.core.util.ByteBufferInputStream;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.StatefulSerializer;

import static java.lang.Math.max;

/**
 * A trivially compressed Java serialization based serializer.
 * <p>
 * Class descriptors in the resultant bytes are encoded as integers.  Mappings
 * between the integer representation and the {@link ObjectStreamClass}, and the
 * {@code Class} and the integer representation are stored in a single on-heap
 * map.
 */
public class CompactJavaSerializer<T> implements StatefulSerializer<T> {

  private volatile StateHolder<Integer, ObjectStreamClass> persistentState;
  private final ConcurrentMap<Integer, ObjectStreamClass> readLookupCache = new ConcurrentHashMap<>();
  private final ConcurrentMap<SerializableDataKey, Integer> writeLookupCache = new ConcurrentHashMap<>();

  private final Lock lock = new ReentrantLock();
  private int nextStreamIndex = 0;
  private boolean potentiallyInconsistent;

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

  @SuppressWarnings("unchecked")
  public static <T> Class<? extends Serializer<T>> asTypedSerializer() {
    return (Class) CompactJavaSerializer.class;
  }

  @Override
  public void init(final StateRepository stateRepository) {
    this.persistentState = stateRepository.getPersistentStateHolder("CompactJavaSerializer-ObjectStreamClassIndex", Integer.class, ObjectStreamClass.class, c -> true, null);
    refreshMappingsFromStateRepository();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ByteBuffer serialize(T object) throws SerializerException {
    try {
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      try (ObjectOutputStream oout = getObjectOutputStream(bout)) {
        oout.writeObject(object);
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
      try (ObjectInputStream oin = getObjectInputStream(new ByteBufferInputStream(binary))) {
        @SuppressWarnings("unchecked")
        T value = (T) oin.readObject();
        return value;
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

  private int getOrAddMapping(ObjectStreamClass desc) {
    SerializableDataKey probe = new SerializableDataKey(desc, false);
    Integer rep = writeLookupCache.get(probe);
    if (rep == null) {
      return addMappingUnderLock(desc, probe);
    } else {
      return rep;
    }
  }

  private int addMappingUnderLock(ObjectStreamClass desc, SerializableDataKey probe) {
    lock.lock();
    try {
      if (potentiallyInconsistent) {
        refreshMappingsFromStateRepository();
        potentiallyInconsistent = false;
      }
      while (true) {
        Integer rep = writeLookupCache.get(probe);
        if (rep != null) {
          return rep;
        }
        rep = nextStreamIndex++;

        try {
          ObjectStreamClass disconnected = disconnect(desc);
          ObjectStreamClass existingOsc = persistentState.putIfAbsent(rep, disconnected);
          if (existingOsc == null) {
            cacheMapping(rep, disconnected);
            return rep;
          } else {
            cacheMapping(rep, disconnect(existingOsc));
          }
        } catch (Throwable t) {
          potentiallyInconsistent = true;
          throw t;
        }
      }
    } finally {
      lock.unlock();
    }
  }

  private void refreshMappingsFromStateRepository() {
    int highestIndex = -1;
    for (Entry<Integer, ObjectStreamClass> entry : persistentState.entrySet()) {
      Integer index = entry.getKey();
      cacheMapping(entry.getKey(), disconnect(entry.getValue()));
      highestIndex = max(highestIndex, index);
    }
    nextStreamIndex = highestIndex + 1;
  }

  private void cacheMapping(Integer index, ObjectStreamClass disconnectedOsc) {
    readLookupCache.merge(index, disconnectedOsc, (existing, update) -> {
      if (equals(existing, update)) {
        return existing;
      } else {
        throw new AssertionError("Corrupted data:\n"
            + "State Repository: " + persistentState + "\n"
            + "Local Write Lookup: " + writeLookupCache + "\n"
            + "Local Read Lookup: " + readLookupCache);
      }
    });
    writeLookupCache.merge(new SerializableDataKey(disconnectedOsc, true), index, (existing, update) -> {
      if (existing.equals(update)) {
        return existing;
      } else {
        throw new AssertionError("Corrupted data:\n"
            + "State Repository: " + persistentState + "\n"
            + "Local Write Lookup: " + writeLookupCache + "\n"
            + "Local Read Lookup: " + readLookupCache);
      }
    });
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
    protected ObjectStreamClass readClassDescriptor() throws IOException {
      int key = readInt();
      ObjectStreamClass objectStreamClass = readLookupCache.get(key);
      if (objectStreamClass == null) {
        objectStreamClass = persistentState.get(key);
        cacheMapping(key, disconnect(objectStreamClass));
      }
      return objectStreamClass;
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
          this.klazz = new WeakReference<>(forClass);
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
      klazz = new WeakReference<>(clazz);
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
    } catch (ClassNotFoundException | IOException e) {
      throw new AssertionError(e);
    }
  }

  private static byte[] getSerializedForm(ObjectStreamClass desc) throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    try {
      try (ObjectOutputStream oout = new ObjectOutputStream(bout)) {
        oout.writeObject(desc);
      }
    } finally {
      bout.close();
    }
    return bout.toByteArray();
  }
}
