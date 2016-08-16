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
import org.ehcache.PersistentCacheManager;
import org.ehcache.ValueSupplier;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceType;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.builders.WriteBehindConfigurationBuilder;
import org.ehcache.config.builders.CacheEventListenerConfigurationBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.docs.plugs.ListenerObject;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.impl.serialization.JavaSerializer;
import org.ehcache.impl.serialization.LongSerializer;
import org.ehcache.docs.plugs.OddKeysEvictionAdvisor;
import org.ehcache.docs.plugs.SampleLoaderWriter;
import org.ehcache.impl.serialization.StringSerializer;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;
import org.ehcache.impl.copy.ReadWriteCopier;
import org.junit.Test;
import org.terracotta.context.ContextElement;
import org.terracotta.context.TreeNode;
import org.terracotta.statistics.StatisticsManager;

import java.io.File;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, ResourcePoolsBuilder.heap(10))) // <2>
        .build(); // <3>
    cacheManager.init(); // <4>

    Cache<Long, String> preConfigured =
        cacheManager.getCache("preConfigured", Long.class, String.class); // <5>

    Cache<Long, String> myCache = cacheManager.createCache("myCache", // <6>
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, ResourcePoolsBuilder.heap(10)).build());

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
        .with(CacheManagerBuilder.persistence(getStoragePath() + File.separator + "myData")) // <1>
        .withCache("persistent-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder()
                .heap(10, EntryUnit.ENTRIES)
                .disk(10, MemoryUnit.MB, true)) // <2>
            )
        .build(true);

    persistentCacheManager.close();
    // end::persistentCacheManager[]
  }

  @Test
  public void offheapCacheManager() {
    // tag::offheapCacheManager[]
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().withCache("tieredCache",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder()
                .heap(10, EntryUnit.ENTRIES)
                .offheap(10, MemoryUnit.MB)) // <1>
            )
        .build(true);

    Cache<Long, String> tieredCache = cacheManager.getCache("tieredCache", Long.class, String.class);

    cacheManager.close();
    // end::offheapCacheManager[]
  }

  @Test
  public void threeTiersCacheManager() throws Exception {
    // tag::threeTiersCacheManager[]
    PersistentCacheManager persistentCacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .with(CacheManagerBuilder.persistence(getStoragePath() + File.separator + "myData")) // <1>
        .withCache("threeTieredCache",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .heap(10, EntryUnit.ENTRIES) // <2>
                    .offheap(1, MemoryUnit.MB) // <3>
                    .disk(20, MemoryUnit.MB) // <4>
                )
        ).build(true);

    Cache<Long, String> threeTieredCache = persistentCacheManager.getCache("threeTieredCache", Long.class, String.class);


    persistentCacheManager.close();
    // end::threeTiersCacheManager[]
  }

  @Test
  public void defaultSerializers() throws Exception {
    // tag::defaultSerializers[]
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
          ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES)
              .offheap(1, MemoryUnit.MB))
        .build();

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("cache", cacheConfiguration)
        .withSerializer(String.class, StringSerializer.class) // <1>
        .build(true);

    Cache<Long, String> cache = cacheManager.getCache("cache", Long.class, String.class);

    cache.put(1L, "one");
    assertThat(cache.get(1L), equalTo("one"));

    cacheManager.close();
    // end::defaultSerializers[]
  }

  @Test
  public void byteSizedTieredCache() {
    // tag::byteSizedTieredCache[]
    CacheConfiguration<Long, String> usesConfiguredInCacheConfig = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder()
                .heap(10, MemoryUnit.KB) // <1>
                .offheap(10, MemoryUnit.MB))
        .withSizeOfMaxObjectGraph(1000)
        .withSizeOfMaxObjectSize(1000, MemoryUnit.B) // <2>
        .build();

    CacheConfiguration<Long, String> usesDefaultSizeOfEngineConfig = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder()
                .heap(10, MemoryUnit.KB)
                .offheap(10, MemoryUnit.MB))
        .build();

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withDefaultSizeOfMaxObjectSize(500, MemoryUnit.B)
        .withDefaultSizeOfMaxObjectGraph(2000) // <3>
        .withCache("usesConfiguredInCache", usesConfiguredInCacheConfig)
        .withCache("usesDefaultSizeOfEngine", usesDefaultSizeOfEngineConfig)
        .build(true);

    Cache<Long, String> usesConfiguredInCache = cacheManager.getCache("usesConfiguredInCache", Long.class, String.class);

    usesConfiguredInCache.put(1L, "one");
    assertThat(usesConfiguredInCache.get(1L), equalTo("one"));

    Cache<Long, String> usesDefaultSizeOfEngine = cacheManager.getCache("usesDefaultSizeOfEngine", Long.class, String.class);

    usesDefaultSizeOfEngine.put(1L, "one");
    assertThat(usesDefaultSizeOfEngine.get(1L), equalTo("one"));

    cacheManager.close();
    // end::byteSizedTieredCache[]
  }

  @Test
  public void cacheSerializers() throws Exception {
    // tag::cacheSerializers[]
    CacheConfiguration<Long, Person> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, Person.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).offheap(10, MemoryUnit.MB))
        .withKeySerializer(new LongSerializer()) // <1>
        .withValueSerializer(new PersonSerializer()) // <2>
        .build();

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("cache", cacheConfiguration)
        .build(true);

    Cache<Long, Person> cache = cacheManager.getCache("cache", Long.class, Person.class);

    cache.put(1L, new Person("person one", 32));
    assertThat(cache.get(1L), equalTo(new Person("person one", 32)));

    cacheManager.close();
    // end::cacheSerializers[]
  }

  @Test
  public void testCacheEventListener() {
    // tag::cacheEventListener[]
    CacheEventListenerConfigurationBuilder cacheEventListenerConfiguration = CacheEventListenerConfigurationBuilder
        .newEventListenerConfiguration(new ListenerObject(), EventType.CREATED, EventType.UPDATED) // <1>
        .unordered().asynchronous(); // <2>

    final CacheManager manager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("foo",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class, ResourcePoolsBuilder.heap(10))
                .add(cacheEventListenerConfiguration) // <3>
        ).build(true);

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

    Cache<Long, String> writeThroughCache = cacheManager.createCache("writeThroughCache",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, ResourcePoolsBuilder.heap(10))
            .withLoaderWriter(new SampleLoaderWriter<Long, String>(singletonMap(41L, "zero"))) // <1>
            .build());

    assertThat(writeThroughCache.get(41L), is("zero")); // <2>
    writeThroughCache.put(42L, "one"); // <3>
    assertThat(writeThroughCache.get(42L), equalTo("one"));

    cacheManager.close();
    // end::writeThroughCache[]
  }

  @Test
  public void writeBehindCache() throws ClassNotFoundException {
    // tag::writeBehindCache[]
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().build(true);

    Cache<Long, String> writeBehindCache = cacheManager.createCache("writeBehindCache",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, ResourcePoolsBuilder.heap(10))
            .withLoaderWriter(new SampleLoaderWriter<Long, String>(singletonMap(41L, "zero"))) // <1>
            .add(WriteBehindConfigurationBuilder // <2>
                .newBatchedWriteBehindConfiguration(1, TimeUnit.SECONDS, 3)// <3>
                .queueSize(3)// <4>
                .concurrencyLevel(1) // <5>
                .enableCoalescing()) // <6>
            .build());

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
    ListenerObject listener = new ListenerObject();
    CacheEventListenerConfigurationBuilder cacheEventListenerConfiguration = CacheEventListenerConfigurationBuilder
        .newEventListenerConfiguration(listener, EventType.EVICTED).unordered().synchronous();

    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                                                                ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10L, EntryUnit.ENTRIES))
        .add(cacheEventListenerConfiguration)
        .build();

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().withCache("cache", cacheConfiguration)
        .build(true);

    Cache<Long, String> cache = cacheManager.getCache("cache", Long.class, String.class);
    for(long i = 0; i < 20; i++ ){
      cache.put(i, "Hello World");
    }
    assertThat(listener.evicted(), is(10));

    cache.clear();
    listener.resetEvictionCount();

    // tag::updateResourcesAtRuntime[]
    ResourcePools pools = ResourcePoolsBuilder.newResourcePoolsBuilder().heap(20L, EntryUnit.ENTRIES).build(); // <1>
    cache.getRuntimeConfiguration().updateResourcePools(pools); // <2>
    assertThat(cache.getRuntimeConfiguration().getResourcePools()
        .getPoolForResource(ResourceType.Core.HEAP).getSize(), is(20L));
    // end::updateResourcesAtRuntime[]

    for(long i = 0; i < 20; i++ ){
      cache.put(i, "Hello World");
    }
    assertThat(listener.evicted(), is(0));

    cacheManager.close();
  }

  @Test
  public void registerListenerAtRuntime() throws InterruptedException {
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
            ResourcePoolsBuilder.heap(10L)))
        .build(true);

    Cache<Long, String> cache = cacheManager.getCache("cache", Long.class, String.class);

    // tag::registerListenerAtRuntime[]
    ListenerObject listener = new ListenerObject(); // <1>
    cache.getRuntimeConfiguration().registerCacheEventListener(listener, EventOrdering.ORDERED,
        EventFiring.ASYNCHRONOUS, EnumSet.of(EventType.CREATED, EventType.REMOVED)); // <2>

    cache.put(1L, "one");
    cache.put(2L, "two");
    cache.remove(1L);
    cache.remove(2L);

    cache.getRuntimeConfiguration().deregisterCacheEventListener(listener); // <3>

    cache.put(1L, "one again");
    cache.remove(1L);
    // end::registerListenerAtRuntime[]

    cacheManager.close();
  }

  @Test
  public void configuringEventProcessing() {
    CacheEventListenerConfigurationBuilder cacheEventListenerConfiguration = CacheEventListenerConfigurationBuilder
        .newEventListenerConfiguration(ListenerObject.class, EventType.EVICTED).ordered().synchronous();
    // tag::configuringEventProcessingQueues[]
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                                                                                          ResourcePoolsBuilder.heap(5L))
        .withDispatcherConcurrency(10) // <1>
        .withEventListenersThreadPool("listeners-pool")
        .build();
    // end::configuringEventProcessingQueues[]
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().withCache("cache", cacheConfiguration)
        .build(true);
    cacheManager.close();
  }

  @Test
  public void cacheCopiers() throws Exception {
    // tag::cacheCopiers[]
    CacheConfiguration<Description, Person> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Description.class, Person.class,
                                                                                          ResourcePoolsBuilder.heap(10))
        .withKeyCopier(new DescriptionCopier()) // <1>
        .withValueCopier(new PersonCopier()) // <2>
        .build();

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
    CacheConfiguration<Long, Person> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, Person.class,
                                                                                          ResourcePoolsBuilder.heap(10))
        .withValueSerializingCopier() // <1>
        .build();
    // end::cacheSerializingCopiers[]

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("cache", cacheConfiguration)
        .build(true);

    Cache<Long, Person> cache = cacheManager.getCache("cache", Long.class, Person.class);

    Description desc = new Description(1234, "foo");
    Person person = new Person("Bar", 24);
    cache.put(1L, person);
    assertThat(cache.get(1L), equalTo(person));
    assertThat(cache.get(1L), not(sameInstance(person)));

    cacheManager.close();
  }

  @Test
  public void defaultCopiers() throws Exception {
    // tag::defaultCopiers[]
    CacheConfiguration<Description, Person> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Description.class, Person.class,
        ResourcePoolsBuilder.heap(10)).build();

    CacheConfiguration<Long, Person> anotherCacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, Person.class,
        ResourcePoolsBuilder.heap(10)).build();

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCopier(Description.class, DescriptionCopier.class) // <1>
        .withCopier(Person.class, PersonCopier.class)
        .withCache("cache", cacheConfiguration)  // <2>
        .withCache("anotherCache", anotherCacheConfiguration)  // <3>
        .build(true);

    Cache<Description, Person> cache = cacheManager.getCache("cache", Description.class, Person.class);
    Cache<Long, Person> anotherCache = cacheManager.getCache("anotherCache", Long.class, Person.class);

    Description desc = new Description(1234, "foo");
    Person person = new Person("Bar", 24);
    cache.put(desc, person);
    assertThat(cache.get(desc), equalTo(person));
    assertThat(cache.get(desc), is(not(sameInstance(person))));

    anotherCache.put(1L, person);
    assertThat(anotherCache.get(1L), equalTo(person));

    cacheManager.close();
    // end::defaultCopiers[]
  }

  @Test
  public void cacheServiceConfiguration() throws Exception {
    // tag::cacheServiceConfigurations[]
    CacheConfiguration<Description, Person> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Description.class, Person.class,
                                                                                              ResourcePoolsBuilder.heap(10))
        .withKeyCopier(DescriptionCopier.class) // <1>
        .withValueCopier(new PersonCopier()) // <2>
        .build();
    // end::cacheServiceConfigurations[]

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("cache", cacheConfiguration)
        .build(true);

    Cache<Description, Person> cache = cacheManager.getCache("cache", Description.class, Person.class);

    Description desc = new Description(1234, "foo");
    Person person = new Person("Bar", 24);
    cache.put(desc, person);
    assertThat(cache.get(desc), equalTo(person));

    cacheManager.close();
  }

  @Test
  public void cacheEvictionAdvisor() throws Exception {
    // tag::cacheEvictionAdvisor[]
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                                                                                        ResourcePoolsBuilder.heap(2L)) // <1>
        .withEvictionAdvisor(new OddKeysEvictionAdvisor<Long, String>()) // <2>
        .build();

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("cache", cacheConfiguration)
        .build(true);

    Cache<Long, String> cache = cacheManager.getCache("cache", Long.class, String.class);

    // Work with the cache
    cache.put(42L, "The Answer!");
    cache.put(41L, "The wrong Answer!");
    cache.put(39L, "The other wrong Answer!");

    cacheManager.close();
    // end::cacheEvictionAdvisor[]
  }

  @Test
  public void expiry() throws Exception {
    // tag::expiry[]
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
            ResourcePoolsBuilder.heap(100)) // <1>
        .withExpiry(Expirations.timeToLiveExpiration(Duration.of(20, TimeUnit.SECONDS))) // <2>
        .build();
    // end::expiry[]
  }

  @Test
  public void customExpiry() throws Exception {
    // tag::customExpiry[]
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
            ResourcePoolsBuilder.heap(100))
        .withExpiry(new CustomExpiry()) // <1>
        .build();
    // end::customExpiry[]
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

  static class PersonSerializer extends JavaSerializer<Person> {
    public PersonSerializer() {
      super(ClassLoader.getSystemClassLoader());
    }
  }

  private String getStoragePath() throws URISyntaxException {
    return getClass().getClassLoader().getResource(".").toURI().getPath();
  }

  public static class CustomExpiry implements Expiry<Long, String> {

    @Override
    public Duration getExpiryForCreation(Long key, String value) {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public Duration getExpiryForAccess(Long key, ValueSupplier<? extends String> value) {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public Duration getExpiryForUpdate(Long key, ValueSupplier<? extends String> oldValue, String newValue) {
      throw new UnsupportedOperationException("TODO Implement me!");
    }
  }

}
