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

package org.ehcache.impl.config.serializer;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.impl.config.copy.DefaultCopierConfiguration;
import org.ehcache.impl.config.persistence.CacheManagerPersistenceConfiguration;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.spi.serialization.SerializerException;
import org.ehcache.impl.copy.SerializingCopier;
import org.ehcache.impl.serialization.JavaSerializer;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.core.spi.service.FileBasedPersistenceContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.terracotta.org.junit.rules.TemporaryFolder;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * SerializerCountingTest
 */
public class SerializerCountingTest {

  private static final boolean PRINT_STACK_TRACES = false;

  private CacheManager cacheManager;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    cacheManager = newCacheManagerBuilder()
        .using(new DefaultSerializationProviderConfiguration().addSerializerFor(Serializable.class, (Class) CountingSerializer.class)
                                                              .addSerializerFor(Long.class, (Class) CountingSerializer.class)
                                                              .addSerializerFor(String.class, (Class) CountingSerializer.class))
        .with(new CacheManagerPersistenceConfiguration(folder.getRoot()))
        .build(true);
  }

  @After
  public void tearDown() {
    clearCounters();
    if (cacheManager != null) {
      cacheManager.close();
    }
  }

  @Test
  public void testOnHeapPutGet() {

    Cache<Long, String> cache = cacheManager.createCache("onHeap", newCacheConfigurationBuilder(Long.class, String.class, heap(10))
                .withService(new DefaultCopierConfiguration<>(SerializingCopier.<Long>asCopierClass(), DefaultCopierConfiguration.Type.KEY))
                .withService(new DefaultCopierConfiguration<>(SerializingCopier.<String>asCopierClass(), DefaultCopierConfiguration.Type.VALUE))
                .build());

    cache.put(42L, "TheAnswer!");
    assertCounters(2, 2, 0, 1, 0, 0);
    printSerializationCounters("Put OnHeap (create)");
    cache.get(42L);
    assertCounters(0, 0, 0, 0, 1, 0);
    printSerializationCounters("Get OnHeap");

    cache.put(42L, "Wrong ...");
    assertCounters(2, 2, 0, 1, 0, 0);
    printSerializationCounters("Put OnHeap (update)");
  }

  @Test
  public void testOffHeapPutGet() {
    Cache<Long, String> cache = cacheManager.createCache("offHeap", newCacheConfigurationBuilder(Long.class, String.class,
                                          newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).offheap(10, MemoryUnit.MB))
            .build()
    );

    cache.put(42L, "TheAnswer");
    assertCounters(1, 0, 0, 1, 0, 0);
    printSerializationCounters("Put Offheap");
    cache.get(42L);
    assertCounters(0, 0, 1, 0, 1, 0);
    printSerializationCounters("Get Offheap fault");
    cache.get(42L);
    assertCounters(0, 0, 0, 0, 0, 0);
    printSerializationCounters("Get Offheap faulted");

    cache.put(42L, "Wrong ...");
    assertCounters(1, 0, 2, 1, 0, 0);
    printSerializationCounters("Put OffHeap (update faulted)");
  }

  @Test
  public void testOffHeapOnHeapCopyPutGet() {
    Cache<Long, String> cache = cacheManager.createCache("offHeap", newCacheConfigurationBuilder(Long.class, String.class,
                                          newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).offheap(10, MemoryUnit.MB))
            .withService(new DefaultCopierConfiguration<>(SerializingCopier.<Long>asCopierClass(), DefaultCopierConfiguration.Type.KEY))
            .withService(new DefaultCopierConfiguration<>(SerializingCopier.<String>asCopierClass(), DefaultCopierConfiguration.Type.VALUE))
            .build()
    );

    cache.put(42L, "TheAnswer");
    assertCounters(2, 1, 0, 1, 0, 0);
    printSerializationCounters("Put OffheapOnHeapCopy");
    cache.get(42L);
    assertCounters(1, 1, 1, 0, 2, 0);
    printSerializationCounters("Get OffheapOnHeapCopy fault");
    cache.get(42L);
    assertCounters(0, 0, 0, 0, 1, 0);
    printSerializationCounters("Get OffheapOnHeapCopy faulted");

    cache.put(42L, "Wrong ...");
    assertCounters(3, 2, 2, 1, 0, 0);
    printSerializationCounters("Put OffheapOnHeapCopy (update faulted)");
  }

  @Test
  public void testDiskOffHeapOnHeapCopyPutGet() {
    Cache<Long, String> cache = cacheManager.createCache("offHeap", newCacheConfigurationBuilder(Long.class, String.class,
                  newResourcePoolsBuilder().heap(2, EntryUnit.ENTRIES).offheap(10, MemoryUnit.MB).disk(100, MemoryUnit.MB))
            .withService(new DefaultCopierConfiguration<>(SerializingCopier.<Long>asCopierClass(), DefaultCopierConfiguration.Type.KEY))
            .withService(new DefaultCopierConfiguration<>(SerializingCopier.<String>asCopierClass(), DefaultCopierConfiguration.Type.VALUE))
            .build()
    );


    cache.put(42L, "TheAnswer");
    assertCounters(3, 2, 0, 1, 0, 0);
    printSerializationCounters("Put DiskOffHeapOnHeapCopy");
    cache.get(42L);
    assertCounters(1, 1, 1, 0, 2, 0);
    printSerializationCounters("Get DiskOffHeapOnHeapCopy fault");
    cache.get(42L);
    assertCounters(0, 0, 0, 0, 1, 0);
    printSerializationCounters("Get DiskOffHeapOnHeapCopy faulted");

    cache.put(42L, "Wrong ...");
    assertCounters(3, 2, 2, 1, 0, 0);
    printSerializationCounters("Put DiskOffHeapOnHeapCopy (update faulted)");
  }

  private void printSerializationCounters(String operation) {
    System.out.println("Operation " + operation);
    System.out.println(format("Key Serialization - %d / Deserialization - %d / Equals - %d", CountingSerializer.keySerializeCounter.get(), CountingSerializer.keyDeserializeCounter.get(), CountingSerializer.keyEqualsCounter.get()));
    System.out.println(format("Value Serialization - %d / Deserialization - %d / Equals - %d", CountingSerializer.serializeCounter.get(), CountingSerializer.deserializeCounter.get(), CountingSerializer.equalsCounter.get()));
    clearCounters();
  }

  private void clearCounters() {
    CountingSerializer.serializeCounter.set(0);
    CountingSerializer.deserializeCounter.set(0);
    CountingSerializer.equalsCounter.set(0);
    CountingSerializer.keySerializeCounter.set(0);
    CountingSerializer.keyDeserializeCounter.set(0);
    CountingSerializer.keyEqualsCounter.set(0);
  }

  private void assertCounters(int keySerialization, int keyDeserialization, int keyEquals, int valueSerialization, int valueDeserialization, int valueEquals) {
    assertThat("Key Serialize", CountingSerializer.keySerializeCounter.get(), is(keySerialization));
    assertThat("Key Deserialize", CountingSerializer.keyDeserializeCounter.get(), is(keyDeserialization));
    assertThat("Key Equals", CountingSerializer.keyEqualsCounter.get(), is(keyEquals));
    assertThat("Value Serialize", CountingSerializer.serializeCounter.get(), is(valueSerialization));
    assertThat("Value Deserialize", CountingSerializer.deserializeCounter.get(), is(valueDeserialization));
    assertThat("Value Equals", CountingSerializer.equalsCounter.get(), is(valueEquals));
  }

  public static class CountingSerializer<T> implements Serializer<T> {

    static AtomicInteger serializeCounter = new AtomicInteger(0);
    static AtomicInteger deserializeCounter = new AtomicInteger(0);
    static AtomicInteger equalsCounter = new AtomicInteger(0);
    static AtomicInteger keySerializeCounter = new AtomicInteger(0);
    static AtomicInteger keyDeserializeCounter = new AtomicInteger(0);
    static AtomicInteger keyEqualsCounter = new AtomicInteger(0);

    private final JavaSerializer<T> serializer;

    public CountingSerializer(ClassLoader classLoader) {
      serializer = new JavaSerializer<>(classLoader);
    }

    public CountingSerializer(ClassLoader classLoader, FileBasedPersistenceContext persistenceContext) {
      serializer = new JavaSerializer<>(classLoader);
    }

    @Override
    public ByteBuffer serialize(T object) throws SerializerException {
      if (PRINT_STACK_TRACES) {
        new Exception().printStackTrace();
      }
      if (object.getClass().equals(String.class)) {
        serializeCounter.incrementAndGet();
      } else {
        keySerializeCounter.incrementAndGet();
      }
      return serializer.serialize(object);
    }

    @Override
    public T read(ByteBuffer binary) throws ClassNotFoundException, SerializerException {
      if (PRINT_STACK_TRACES) {
        new Exception().printStackTrace();
      }
      T value = serializer.read(binary);
      if (value.getClass().equals(String.class)) {
        deserializeCounter.incrementAndGet();
      } else {
        keyDeserializeCounter.incrementAndGet();
      }
      return value;
    }

    @Override
    public boolean equals(T object, ByteBuffer binary) throws ClassNotFoundException, SerializerException {
      if (PRINT_STACK_TRACES) {
        new Exception().printStackTrace();
      }
      if (object.getClass().equals(String.class)) {
        equalsCounter.incrementAndGet();
      } else {
        keyEqualsCounter.incrementAndGet();
      }
      return serializer.equals(object, binary);
    }
  }
}
