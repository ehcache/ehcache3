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

package org.ehcache.docs;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.CacheManagerBuilder;
import org.ehcache.Ehcache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.ResourceType;
import org.ehcache.config.SerializerConfiguration;
import org.ehcache.config.copy.CopierConfiguration;
import org.ehcache.config.copy.DefaultCopierConfiguration;
import org.ehcache.config.copy.DefaultCopyProviderConfiguration;
import org.ehcache.config.event.CacheEventListenerConfigurationBuilder;
import org.ehcache.config.loaderwriter.DefaultCacheLoaderWriterConfiguration;
import org.ehcache.config.persistence.CacheManagerPersistenceConfiguration;
import org.ehcache.config.serializer.DefaultSerializationProviderConfiguration;
import org.ehcache.config.serializer.DefaultSerializerConfiguration;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.config.writebehind.WriteBehindConfigurationBuilder;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventType;
import org.ehcache.exceptions.BulkCacheWritingException;
import org.ehcache.internal.copy.ReadWriteCopier;
import org.ehcache.internal.copy.SerializingCopier;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.serialization.Serializer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;

/**
 * Samples to get started with Ehcache 3
 *
 * If you add new examples, you should use tags to have them included in the README.adoc
 * You need to edit the README.adoc too to add  your new content.
 * The callouts are also used in docs/user/index.adoc
 */
@SuppressWarnings("unused")
public class GettingStarted {

  @Test
  public void cachemanagerExample() {
    // tag::cachemanagerExample[]
    CacheManager cacheManager
        = CacheManagerBuilder.newCacheManagerBuilder() // <1>
        .withCache("preConfigured",
            CacheConfigurationBuilder.newCacheConfigurationBuilder()
                .buildConfig(Long.class, String.class)) // <2>
        .build(false); // <3>
    cacheManager.init(); // <4>

    Cache<Long, String> preConfigured =
        cacheManager.getCache("preConfigured", Long.class, String.class); // <5>

    Cache<Long, String> myCache = cacheManager.createCache("myCache", // <6>
        CacheConfigurationBuilder.newCacheConfigurationBuilder().buildConfig(Long.class, String.class));

    myCache.put(1L, "da one!"); // <7>
    String value = myCache.get(1L); // <8>

    cacheManager.removeCache("preConfigured"); // <9>

    cacheManager.close(); // <10>
    // end::cachemanagerExample[]
  }

  @Test
  public void persistentCacheManager() throws Exception {
    // tag::persistentCacheManager[]
    PersistentCacheManager persistentCacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .with(new CacheManagerPersistenceConfiguration(new File(getStoragePath(), "myData"))) // <1>
        .withCache("persistent-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder()
            .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
                .heap(10, EntryUnit.ENTRIES)
                .disk(10L, MemoryUnit.MB, true)) // <2>
            .buildConfig(Long.class, String.class))
        .build(true);

    persistentCacheManager.close();
    // end::persistentCacheManager[]
  }

  @Test
  public void offheapCacheManager() {
    // tag::offheapCacheManager[]
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().withCache("tieredCache",
        CacheConfigurationBuilder.newCacheConfigurationBuilder()
            .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
                .heap(10, EntryUnit.ENTRIES)
                .offheap(10, MemoryUnit.MB)) // <1>
            .buildConfig(Long.class, String.class)).build(true);

    cacheManager.close();
    // end::offheapCacheManager[]
  }

  @Test
  public void threeTiersCacheManager() throws Exception {
    // tag::threeTiersCacheManager[]
    PersistentCacheManager persistentCacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .with(new CacheManagerPersistenceConfiguration(new File(getStoragePath(), "myData"))) // <1>
        .withCache("threeTieredCache",
            CacheConfigurationBuilder.newCacheConfigurationBuilder()
                .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
                        .heap(10, EntryUnit.ENTRIES) // <2>
                        .offheap(1, MemoryUnit.MB) // <3>
                        .disk(20, MemoryUnit.MB) // <4>
                )
                .buildConfig(Long.class, String.class)).build(true);

    persistentCacheManager.close();
    // end::threeTiersCacheManager[]
  }

  @Test
  public void defaultSerializers() throws Exception {
    // tag::defaultSerializers[]
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder
            .newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).offheap(1, MemoryUnit.MB).build())
        .buildConfig(Long.class, String.class);

    DefaultSerializationProviderConfiguration defaultSerializationProviderFactoryConfiguration =
        new DefaultSerializationProviderConfiguration()
            .addSerializerFor(String.class, StringSerializer.class);  // <1>
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("cache", cacheConfiguration)
        .using(defaultSerializationProviderFactoryConfiguration) // <2>
        .build(true);

    Cache<Long, String> cache = cacheManager.getCache("cache", Long.class, String.class);

    cache.put(1L, "one");
    assertThat(cache.get(1L), equalTo("one"));

    cacheManager.close();
    // end::defaultSerializers[]
  }

  @Test
  public void cacheSerializers() throws Exception {
    // tag::cacheSerializers[]
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .add(new DefaultSerializerConfiguration<Long>(LongSerializer.class,
            SerializerConfiguration.Type.KEY))  // <1>
        .add(new DefaultSerializerConfiguration<CharSequence>(CharSequenceSerializer.class,
            SerializerConfiguration.Type.VALUE))  // <2>
        .buildConfig(Long.class, String.class);

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("cache", cacheConfiguration)
        .build(true);

    Cache<Long, String> cache = cacheManager.getCache("cache", Long.class, String.class);

    cache.put(1L, "one");
    assertThat(cache.get(1L), equalTo("one"));

    cacheManager.close();
    // end::cacheSerializers[]
  }

  @Test
  public void testCacheEventListener() {
    // tag::cacheEventListener[]
    CacheEventListenerConfigurationBuilder cacheEventListenerConfiguration = CacheEventListenerConfigurationBuilder
        .newEventListenerConfiguration(ListenerObject.class, EventType.CREATED, EventType.UPDATED) // <1>
        .unordered().asynchronous(); // <2>
    
    final CacheManager manager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("foo",
            CacheConfigurationBuilder.newCacheConfigurationBuilder()
                .add(cacheEventListenerConfiguration) // <3>
                .buildConfig(String.class, String.class)).build(true);

    final Cache<String, String> cache = manager.getCache("foo", String.class, String.class);
    cache.put("Hello", "World"); // <4>
    cache.put("Hello", "Everyone"); // <5>
    cache.remove("Hello"); // <6>
    // end::cacheEventListener[]

    manager.close();
  }

  @Test
  public void writeThroughCache() throws ClassNotFoundException {
    
    // tag::writeThroughCache[]    
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().build(true);
    
    Class<CacheLoaderWriter<?, ?>> klazz = (Class<CacheLoaderWriter<?, ?>>)  (Class) (SampleLoaderWriter.class);
    
    final Cache<Long, String> writeThroughCache = cacheManager.createCache("writeThroughCache",
        CacheConfigurationBuilder.newCacheConfigurationBuilder()
            .add(new DefaultCacheLoaderWriterConfiguration(klazz, singletonMap(41L, "zero"))) // <1>
            .buildConfig(Long.class, String.class));
    
    assertThat(writeThroughCache.get(41L), is("zero"));
    writeThroughCache.put(42L, "one");
    assertThat(writeThroughCache.get(42L), equalTo("one"));
    
    cacheManager.close();
    // end::writeThroughCache[]
  }
  
  @Test
  public void writeBehindCache() throws ClassNotFoundException {
    
    // tag::writeBehindCache[]    
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().build(true);
    
    Class<CacheLoaderWriter<?, ?>> klazz = (Class<CacheLoaderWriter<?, ?>>) (Class) (SampleLoaderWriter.class);
    
    final Cache<Long, String> writeBehindCache = cacheManager.createCache("writeBehindCache",
        CacheConfigurationBuilder.newCacheConfigurationBuilder()
            .add(new DefaultCacheLoaderWriterConfiguration(klazz, singletonMap(41L, "zero"))) // <1>
            .add(WriteBehindConfigurationBuilder.newWriteBehindConfiguration() // <2>
                .queueSize(3)// <3>
                .concurrencyLevel(1) // <4>
                .batchSize(3) // <5>
                .enableCoalescing() // <6>
                .retry(2, 1) // <7>
                .rateLimit(2) // <8>
                .delay(1, 1)) // <9>
            .buildConfig(Long.class, String.class));
    
    assertThat(writeBehindCache.get(41L), is("zero"));
    writeBehindCache.put(42L, "one");
    writeBehindCache.put(43L, "two");
    writeBehindCache.put(42L, "This goes for the record");
    assertThat(writeBehindCache.get(42L), equalTo("This goes for the record"));
    
    cacheManager.close();
    // end::writeBehindCache[]  
  }

  @Test
  public void updateResourcesAtRuntime() throws InterruptedException {
    CacheEventListenerConfigurationBuilder cacheEventListenerConfiguration = CacheEventListenerConfigurationBuilder
        .newEventListenerConfiguration(ListenerObject.class, EventType.EVICTED).unordered().synchronous();

    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .add(cacheEventListenerConfiguration)
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
            .heap(10L, EntryUnit.ENTRIES).build()).buildConfig(Long.class, String.class);

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().withCache("cache", cacheConfiguration)
        .build(true);

    Cache<Long, String> cache = cacheManager.getCache("cache", Long.class, String.class);
    for(long i = 0; i < 20; i++ ){
      cache.put(i, "Hello World");
    }
    assertThat(ListenerObject.evicted, is(10));

    cache.clear();
    ListenerObject.resetEvictionCount();

    // tag::updateResourcesAtRuntime[]
    ResourcePools pools = ResourcePoolsBuilder.newResourcePoolsBuilder().heap(20L, EntryUnit.ENTRIES).build(); // <1>
    cache.getRuntimeConfiguration().updateResourcePools(pools); // <2>
    assertThat(cache.getRuntimeConfiguration().getResourcePools()
        .getPoolForResource(ResourceType.Core.HEAP).getSize(), is(20L));
    // end::updateResourcesAtRuntime[]
    
    for(long i = 0; i < 20; i++ ){
      cache.put(i, "Hello World");
    }
    assertThat(ListenerObject.evicted, is(0));

    cacheManager.close();
  }

  @Test
  public void cacheCopiers() throws Exception {
    // tag::cacheCopiers[]
    CacheConfiguration<Description, Person> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .add(new DefaultCopierConfiguration<Description>(DescriptionCopier.class,
            CopierConfiguration.Type.KEY))  // <1>
        .add(new DefaultCopierConfiguration<Person>(PersonCopier.class,
            CopierConfiguration.Type.VALUE))  // <2>
        .buildConfig(Description.class, Person.class);

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("cache", cacheConfiguration)
        .build(true);

    Cache<Description, Person> cache = cacheManager.getCache("cache", Description.class, Person.class);

    Description desc = new Description(1234, "foo");
    Person person = new Person("Bar", 24);
    cache.put(desc, person);
    assertThat(cache.get(desc), equalTo(person));

    cacheManager.close();
    // end::cacheCopiers[]
  }

  @Test
  public void cacheSerializingCopiers() throws Exception {
    // tag::cacheSerializingCopiers[]
    CacheConfiguration<Long, Person> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .add(new DefaultCopierConfiguration<Person>((Class)SerializingCopier.class,   //<1>
            CopierConfiguration.Type.VALUE))
        .buildConfig(Long.class, Person.class);
    // end::cacheSerializingCopiers[]

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("cache", cacheConfiguration)
        .build(true);

    Cache<Long, Person> cache = cacheManager.getCache("cache", Long.class, Person.class);

    Description desc = new Description(1234, "foo");
    Person person = new Person("Bar", 24);
    cache.put(1l, person);
    assertThat(cache.get(1l), equalTo(person));
    assertThat(cache.get(1l), not(sameInstance(person)));

    cacheManager.close();
  }

  @Test
  public void defaultCopiers() throws Exception {
    // tag::defaultCopiers[]
    CacheConfiguration<Description, Person> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .buildConfig(Description.class, Person.class);

    CacheConfiguration<Long, Person> anotherCacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .buildConfig(Long.class, Person.class);

    DefaultCopyProviderConfiguration defaultCopierConfig = new DefaultCopyProviderConfiguration()
        .addCopierFor(Description.class, DescriptionCopier.class)   //<1>
        .addCopierFor(Person.class, PersonCopier.class);

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .using(defaultCopierConfig)   //<2>
        .withCache("cache", cacheConfiguration)   //<3>
        .withCache("anotherCache", anotherCacheConfiguration)   //<4>
        .build(true);

    Cache<Description, Person> cache = cacheManager.getCache("cache", Description.class, Person.class);
    Cache<Long, Person> anotherCache = cacheManager.getCache("anotherCache", Long.class, Person.class);

    Description desc = new Description(1234, "foo");
    Person person = new Person("Bar", 24);
    cache.put(desc, person);
    assertThat(cache.get(desc), equalTo(person));

    anotherCache.put(1l, person);
    assertThat(anotherCache.get(1l), equalTo(person));

    cacheManager.close();
    // end::defaultCopiers[]
  }

  private static class Description {
    int id;
    String alias;

    Description(Description other) {
      this.id = other.id;
      this.alias = other.alias;
    }

    Description(int id, String alias) {
      this.id = id;
      this.alias = alias;
    }

    @Override
    public boolean equals(final Object other) {
      if(this == other) return true;
      if(other == null || this.getClass() != other.getClass()) return false;

      Description that = (Description)other;
      if(id != that.id) return false;
      if ((alias == null) ? (alias != null) : !alias.equals(that.alias)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      int result = 1;
      result = 31 * result + id;
      result = 31 * result + (alias == null ? 0 : alias.hashCode());
      return result;
    }
  }

  private static class Person implements Serializable {
    String name;
    int age;

    Person(Person other) {
      this.name = other.name;
      this.age = other.age;
    }

    Person(String name, int age) {
      this.name = name;
      this.age = age;
    }

    @Override
    public boolean equals(final Object other) {
      if(this == other) return true;
      if(other == null || this.getClass() != other.getClass()) return false;

      Person that = (Person)other;
      if(age != that.age) return false;
      if((name == null) ? (that.name != null) : !name.equals(that.name)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = 1;
      result = 31 * result + age;
      result = 31 * result + (name == null ? 0 : name.hashCode());
      return result;
    }
  }

  public static class DescriptionCopier extends ReadWriteCopier<Description> {

    @Override
    public Description copy(final Description obj) {
      return new Description(obj);
    }
  }

  public static class PersonCopier extends ReadWriteCopier<Person> {

    @Override
    public Person copy(final Person obj) {
      return new Person(obj);
    }
  }

  private String getStoragePath() throws URISyntaxException {
    return getClass().getClassLoader().getResource(".").toURI().getPath();
  }

  public static class ListenerObject implements CacheEventListener<Object, Object> {
    private static int evicted;
    @Override
    public void onEvent(CacheEvent<Object, Object> event) {
      Logger logger = LoggerFactory.getLogger(Ehcache.class + "-" + "GettingStarted");
      logger.info(event.getType().toString());
      if(event.getType() == EventType.EVICTED){
        evicted++;
      }
    }

    public static void resetEvictionCount() {
      evicted = 0;
    }
  }
  
  public static class SampleLoaderWriter<K, V> implements CacheLoaderWriter<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SampleLoaderWriter.class);
    
    private final Map<K, V> data = new HashMap<K, V>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    
    public SampleLoaderWriter(Map<K, V> initialData) {
      data.putAll(initialData);
    }

    @Override
    public V load(K key) throws Exception {
      lock.readLock().lock();
      try {
        return data.get(key);
      } finally {
        lock.readLock().unlock();
      }
    }

    @Override
    public Map<K, V> loadAll(Iterable<? extends K> keys) throws Exception {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void write(K key, V value) throws Exception {
      lock.writeLock().lock();
      try {
        data.put(key, value);
        LOGGER.info("Key - '{}', Value - '{}' successfully written", key, value);
      } finally {
        lock.writeLock().unlock();
      }
    }

    @Override
    public void writeAll(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) throws BulkCacheWritingException, Exception {
      lock.writeLock().lock();
      try {
        for (Map.Entry<? extends K, ? extends V> entry : entries) {
          data.put(entry.getKey(), entry.getValue());
          LOGGER.info("Key - '{}', Value - '{}' successfully written in batch", entry.getKey(), entry.getValue());
        }
      } finally {
        lock.writeLock().unlock();
      }
    }

    @Override
    public void delete(K key) throws Exception {
      lock.writeLock().lock();
      try {
        data.remove(key);
        LOGGER.info("Key - '{}' successfully deleted", key);
      } finally {
        lock.writeLock().unlock();
      }
    }

    @Override
    public void deleteAll(Iterable<? extends K> keys) throws BulkCacheWritingException, Exception {
      lock.writeLock().lock();
      try {
        for (K key : keys) {
          data.remove(key);
          LOGGER.info("Key - '{}' successfully deleted in batch", key);
        }
      } finally {
        lock.writeLock().unlock();
      }
    }
  }

  @Serializer.Persistent @Serializer.Transient
  public static class StringSerializer implements Serializer<String> {
    private static final Logger LOG = LoggerFactory.getLogger(StringSerializer.class);
    private static final Charset CHARSET = Charset.forName("US-ASCII");

    public StringSerializer(ClassLoader classLoader) {
    }

    @Override
    public ByteBuffer serialize(String object) {
      LOG.info("serializing {}", object);
      ByteBuffer byteBuffer = ByteBuffer.allocate(object.length());
      byteBuffer.put(object.getBytes(CHARSET)).flip();
      return byteBuffer;
    }

    @Override
    public String read(ByteBuffer binary) throws ClassNotFoundException {
      byte[] bytes = new byte[binary.remaining()];
      binary.get(bytes);
      String s = new String(bytes, CHARSET);
      LOG.info("deserialized {}", s);
      return s;
    }

    @Override
    public boolean equals(String object, ByteBuffer binary) throws ClassNotFoundException {
      return object.equals(read(binary));
    }

    @Override
    public void close() {
      //nothing
    }
  }

  @Serializer.Persistent @Serializer.Transient
  public static class LongSerializer implements Serializer<Long> {
    private static final Logger LOG = LoggerFactory.getLogger(LongSerializer.class);
    private static final Charset CHARSET = Charset.forName("US-ASCII");

    public LongSerializer(ClassLoader classLoader) {
    }

    @Override
    public ByteBuffer serialize(Long object) {
      LOG.info("serializing {}", object);
      ByteBuffer byteBuffer = ByteBuffer.allocate(8);
      byteBuffer.putLong(object).flip();
      return byteBuffer;
    }

    @Override
    public Long read(ByteBuffer binary) throws ClassNotFoundException {
      long l = binary.getLong();
      LOG.info("deserialized {}", l);
      return l;
    }

    @Override
    public boolean equals(Long object, ByteBuffer binary) throws ClassNotFoundException {
      return object.equals(read(binary));
    }

    @Override
    public void close() {
      //nothing
    }
  }

  @Serializer.Persistent @Serializer.Transient
  public static class CharSequenceSerializer implements Serializer<CharSequence> {
    private static final Logger LOG = LoggerFactory.getLogger(StringSerializer.class);
    private static final Charset CHARSET = Charset.forName("US-ASCII");

    public CharSequenceSerializer(ClassLoader classLoader) {
    }

    @Override
    public ByteBuffer serialize(CharSequence object) {
      LOG.info("serializing {}", object);
      ByteBuffer byteBuffer = ByteBuffer.allocate(object.length());
      byteBuffer.put(object.toString().getBytes(CHARSET)).flip();
      return byteBuffer;
    }

    @Override
    public CharSequence read(ByteBuffer binary) throws ClassNotFoundException {
      byte[] bytes = new byte[binary.remaining()];
      binary.get(bytes);
      String s = new String(bytes, CHARSET);
      LOG.info("deserialized {}", s);
      return s;
    }

    @Override
    public boolean equals(CharSequence object, ByteBuffer binary) throws ClassNotFoundException {
      return object.equals(read(binary));
    }

    @Override
    public void close() {
      //nothing
    }
  }

}
