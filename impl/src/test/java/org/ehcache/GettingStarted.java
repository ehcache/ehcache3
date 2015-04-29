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

package org.ehcache;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.event.CacheEventListenerBuilder;
import org.ehcache.config.event.DefaultCacheEventListenerConfiguration;
import org.ehcache.config.persistence.PersistenceConfiguration;
import org.ehcache.config.serializer.DefaultSerializationProviderFactoryConfiguration;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventType;
import org.ehcache.internal.store.heap.service.OnHeapStoreServiceConfig;
import org.ehcache.spi.serialization.Serializer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;

/**
 * @author Alex Snaps
 */
@SuppressWarnings("unused")
public class GettingStarted {

  /**
   * If you add new examples, you should use tags to have them included in the README.adoc
   * You need to edit the README.adoc too to add  your new content.
   * The callouts are also used in docs/user/index.adoc
   */

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
        cacheManager.getCache("preConfigured", Long.class, String.class); // <4>

    Cache<Long, String> myCache = cacheManager.createCache("myCache", // <5>
        CacheConfigurationBuilder.newCacheConfigurationBuilder().buildConfig(Long.class, String.class));

    myCache.put(1L, "da one!"); // <6>
    String value = myCache.get(1L); // <7>

    cacheManager.removeCache("preConfigured"); // <9>

    cacheManager.close(); // <9>
    // end::cachemanagerExample[]
  }

  @Test
  public void userManagedCacheExample() {
    // tag::userManagedCacheExample[]
    UserManagedCache<Long, String> userManagedCache =
        UserManagedCacheBuilder.newUserManagedCacheBuilder(Long.class, String.class,
            LoggerFactory.getLogger(Ehcache.class + "-" + "GettingStarted"))
            .build(false); // <1>
    userManagedCache.init(); // <2>

    userManagedCache.put(1L, "da one!"); // <3>

    userManagedCache.close(); // <4>
    // end::userManagedCacheExample[]
  }

  @Test
  public void persistentCacheManager() {
    // tag::persistentCacheManager[]
    PersistentCacheManager persistentCacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .with(new PersistenceConfiguration(new File(System.getProperty("java.io.tmpdir") + "/myData"))) // <1>
        .withCache("persistent-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder()
            .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
                .heap(10, EntryUnit.ENTRIES)
                .disk(100, EntryUnit.ENTRIES, true) // <2>
                .build())
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
                .offheap(10, MemoryUnit.MB) // <1>
                .build())
            .buildConfig(Long.class, String.class)).build(true);

    cacheManager.close();
    // end::offheapCacheManager[]
  }

  @Test
  public void testTieredStore() throws Exception {
    CacheConfiguration<Long, String> tieredCacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).disk(100, EntryUnit.ENTRIES).build())
        .buildConfig(Long.class, String.class);

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().withCache("tiered-cache", tieredCacheConfiguration).build(true);

    Cache<Long, String> tieredCache = cacheManager.getCache("tiered-cache", Long.class, String.class);

    tieredCache.put(1L, "one");

    assertThat(tieredCache.get(1L), equalTo("one")); // probably coming from disk
    assertThat(tieredCache.get(1L), equalTo("one")); // probably coming from heap

    cacheManager.close();
  }

  @Test
  public void testTieredOffHeapStore() throws Exception {
    CacheConfiguration<Long, String> tieredCacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).offheap(10, MemoryUnit.MB).build())
        .buildConfig(Long.class, String.class);

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().withCache("tieredCache", tieredCacheConfiguration).build(true);

    Cache<Long, String> tieredCache = cacheManager.getCache("tieredCache", Long.class, String.class);

    tieredCache.put(1L, "one");

    assertThat(tieredCache.get(1L), equalTo("one")); // probably coming from offheap
    assertThat(tieredCache.get(1L), equalTo("one")); // probably coming from heap

    cacheManager.close();
  }

  @Test
  public void testPersistentDiskCache() throws Exception {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).disk(100, EntryUnit.ENTRIES, true).build())
        .buildConfig(Long.class, String.class);

    PersistentCacheManager persistentCacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .with(new PersistenceConfiguration(new File(getClass().getClassLoader().getResource(".").toURI().getPath() + "/../../persistent-cache-data")))
        .withCache("persistent-cache", cacheConfiguration)
        .build(true);

    Cache<Long, String> cache = persistentCacheManager.getCache("persistent-cache", Long.class, String.class);

    // Comment the following line on subsequent run and see the test pass
    cache.put(42L, "That's the answer!");
    assertThat(cache.get(42L), is("That's the answer!"));

    // Uncomment the following line to nuke the disk store
//    persistentCacheManager.destroyCache("persistent-cache");

    persistentCacheManager.close();
  }

  @Test
  public void testStoreByValue() {
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().build(false);
    cacheManager.init();

    final Cache<Long, String> cache1 = cacheManager.createCache("cache1",
        CacheConfigurationBuilder.newCacheConfigurationBuilder().withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(1, EntryUnit.ENTRIES).build())
            .buildConfig(Long.class, String.class));
    performAssertions(cache1, true);

    final Cache<Long, String> cache2 = cacheManager.createCache("cache2",
        CacheConfigurationBuilder.newCacheConfigurationBuilder().addServiceConfig(new OnHeapStoreServiceConfig().storeByValue(true))
            .buildConfig(Long.class, String.class));
    performAssertions(cache2, false);

    final Cache<Long, String> cache3 = cacheManager.createCache("cache3",
        CacheConfigurationBuilder.newCacheConfigurationBuilder().addServiceConfig(new OnHeapStoreServiceConfig().storeByValue(false))
            .buildConfig(Long.class, String.class));
    performAssertions(cache3, true);

    cacheManager.close();
  }

  @Test
  public void testDefaultSerializer() throws Exception {
    CacheConfiguration<String, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .addServiceConfig(new OnHeapStoreServiceConfig().storeByValue(true))
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .buildConfig(String.class, String.class);

    DefaultSerializationProviderFactoryConfiguration service = new DefaultSerializationProviderFactoryConfiguration();
    service.addSerializerFor(String.class.getName(), StringSerializer.class);
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("cache", cacheConfiguration)
        .using(service)
        .build(true);

    Cache<String, String> cache = cacheManager.getCache("cache", String.class, String.class);

    cache.put("1", "one");
    assertThat(cache.get("1"), equalTo("one"));

    cacheManager.close();
  }

  @Test
  public void testCacheEventListener() {
    DefaultCacheEventListenerConfiguration cacheEventListenerConfiguration = CacheEventListenerBuilder
        .newEventListenerConfig(ListenerObject.class, EventType.CREATED, EventType.UPDATED)
        .unordered().asynchronous().build();
    
    final CacheManager manager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("foo",
            CacheConfigurationBuilder.newCacheConfigurationBuilder()
                .addServiceConfig(cacheEventListenerConfiguration)
                .buildConfig(String.class, String.class)).build(true);

    final Cache<String, String> cache = manager.getCache("foo", String.class, String.class);
    cache.put("Hello", "World");
    cache.put("Hello", "Everyone");
    cache.remove("Hello");

    manager.close();
  }

  private void performAssertions(Cache<Long, String> cache, boolean same) {
    cache.put(1L, "one");
    String s1 = cache.get(1L);
    String s2 = cache.get(1L);
    String s3 = cache.get(1L);

    assertThat(s1 == s2, is(same));
    assertThat(s2 == s3, is(same));
  }

  public static class ListenerObject implements CacheEventListener<Object, Object> {
    @Override
    public void onEvent(CacheEvent<Object, Object> event) {
      //noop
      Logger logger = LoggerFactory.getLogger(Ehcache.class + "-" + "GettingStarted");
      logger.info(event.getType().toString());
    }
  }

  public static class StringSerializer implements Serializer<String> {
    private static final Logger LOG = LoggerFactory.getLogger(StringSerializer.class);
    private static final Charset CHARSET = Charset.forName("US-ASCII");

    public StringSerializer(ClassLoader classLoader) {
    }

    @Override
    public ByteBuffer serialize(String object) throws IOException {
      LOG.info("serializing {}", object);
      ByteBuffer byteBuffer = ByteBuffer.allocate(object.length());
      byteBuffer.put(object.getBytes(CHARSET));
      return byteBuffer;
    }

    @Override
    public String read(ByteBuffer binary) throws IOException, ClassNotFoundException {
      byte[] bytes = new byte[binary.flip().remaining()];
      binary.get(bytes);
      String s = new String(bytes, CHARSET);
      LOG.info("deserialized {}", s);
      return s;
    }

    @Override
    public boolean equals(String object, ByteBuffer binary) throws IOException, ClassNotFoundException {
      return object.equals(read(binary));
    }
  }

}
