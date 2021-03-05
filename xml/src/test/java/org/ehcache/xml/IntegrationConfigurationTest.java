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

package org.ehcache.xml;

import com.pany.ehcache.integration.TestSecondCacheEventListener;
import com.pany.ehcache.integration.ThreadRememberingLoaderWriter;
import com.pany.ehcache.copier.Description;
import com.pany.ehcache.copier.Employee;
import com.pany.ehcache.copier.Person;
import com.pany.ehcache.integration.TestCacheEventListener;
import com.pany.ehcache.integration.TestCacheLoaderWriter;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.Configuration;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.impl.config.event.DefaultCacheEventDispatcherConfiguration;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.impl.config.loaderwriter.DefaultCacheLoaderWriterConfiguration;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.event.EventType;
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.Test;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.config.builders.WriteBehindConfigurationBuilder.newUnBatchedWriteBehindConfiguration;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Alex Snaps
 */
public class IntegrationConfigurationTest {

  @Test
  public void testSerializers() throws Exception {
    Configuration configuration = new XmlConfiguration(this.getClass().getResource("/configs/default-serializer.xml"));
    final CacheManager cacheManager = CacheManagerBuilder.newCacheManager(configuration);
    cacheManager.init();

    Cache<Long, Double> bar = cacheManager.getCache("bar", Long.class, Double.class);
    bar.put(1L, 1.0);
    assertThat(bar.get(1L), equalTo(1.0));

    Cache<String, String> baz = cacheManager.getCache("baz", String.class, String.class);
    baz.put("1", "one");
    assertThat(baz.get("1"), equalTo("one"));

    Cache<String, Object> bam = cacheManager.createCache("bam", newCacheConfigurationBuilder(String.class, Object.class, heap(10)).build());
    bam.put("1", "one");
    assertThat(bam.get("1"), equalTo((Object)"one"));

    cacheManager.close();
  }

  @Test
  public void testCopiers() throws Exception {
    Configuration configuration = new XmlConfiguration(this.getClass().getResource("/configs/cache-copiers.xml"));
    final CacheManager cacheManager = CacheManagerBuilder.newCacheManager(configuration);
    cacheManager.init();

    Cache<Description, Person> bar = cacheManager.getCache("bar", Description.class, Person.class);
    Description desc = new Description(1234, "foo");
    Person person = new Person("Bar", 24);
    bar.put(desc, person);
    assertEquals(person, bar.get(desc));
    assertNotSame(person, bar.get(desc));

    Cache<Long, Person> baz = cacheManager.getCache("baz", Long.class, Person.class);
    baz.put(1L, person);
    assertEquals(person, baz.get(1L));
    assertNotSame(person, baz.get(1L));

    Employee empl = new Employee(1234, "foo", 23);
    Cache<Long, Employee> bak = cacheManager.getCache("bak", Long.class, Employee.class);
    bak.put(1L, empl);
    assertSame(empl, bak.get(1L));
    cacheManager.close();
  }

  @Test
  public void testLoaderWriter() throws ClassNotFoundException, SAXException, InstantiationException,
      IOException, IllegalAccessException {
    Configuration configuration = new XmlConfiguration(this.getClass().getResource("/configs/cache-integration.xml"));
    assertThat(configuration.getCacheConfigurations().containsKey("bar"), is(true));
    final CacheManager cacheManager = CacheManagerBuilder.newCacheManager(configuration);
    cacheManager.init();
    final Cache<Number, String> cache = cacheManager.getCache("bar", Number.class, String.class);
    assertThat(cache, notNullValue());
    assertThat(cache.get(1), notNullValue());
    final Number key = new Long(42);
    cache.put(key, "Bye y'all!");
    assertThat(TestCacheLoaderWriter.lastWrittenKey, is(key));

    assertThat(configuration.getCacheConfigurations().containsKey("template1"), is(true));
    final Cache<Number, String> templateCache = cacheManager.getCache("template1", Number.class, String.class);
    assertThat(templateCache, notNullValue());
    assertThat(templateCache.get(1), notNullValue());
    final Number key1 = new Long(100);
    templateCache.put(key1, "Bye y'all!");
    assertThat(TestCacheLoaderWriter.lastWrittenKey, is(key1));
  }

  @Test
  public void testWriteBehind() throws ClassNotFoundException, InstantiationException, IllegalAccessException, SAXException, IOException, InterruptedException {

    Configuration configuration = new XmlConfiguration(this.getClass().getResource("/configs/writebehind-cache.xml"));
    assertThat(configuration.getCacheConfigurations().containsKey("bar"), is(true));
    final CacheManager cacheManager = CacheManagerBuilder.newCacheManager(configuration);
    cacheManager.init();
    final Cache<Number, String> cache = cacheManager.getCache("bar", Number.class, String.class);
    assertThat(cache, notNullValue());
    assertThat(cache.get(1), notNullValue());
    final Number key = 42L;
    TestCacheLoaderWriter.latch = new CountDownLatch(1);
    cache.put(key, "Bye y'all!");
    TestCacheLoaderWriter.latch.await(2, TimeUnit.SECONDS);
    assertThat(TestCacheLoaderWriter.lastWrittenKey, is(key));

    assertThat(configuration.getCacheConfigurations().containsKey("template1"), is(true));
    final Cache<Number, String> templateCache = cacheManager.getCache("template1", Number.class, String.class);
    assertThat(templateCache, notNullValue());
    assertThat(templateCache.get(1), notNullValue());
    final Number key1 = 100L;
    TestCacheLoaderWriter.latch = new CountDownLatch(2);
    templateCache.put(42L, "Howdy!");
    templateCache.put(key1, "Bye y'all!");
    TestCacheLoaderWriter.latch.await(2, TimeUnit.SECONDS);
    assertThat(TestCacheLoaderWriter.lastWrittenKey, is(key1));

  }

  @Test
  public void testCacheEventListener() throws Exception {
    Configuration configuration = new XmlConfiguration(this.getClass().getResource("/configs/ehcache-cacheEventListener.xml"));
    assertThat(configuration.getCacheConfigurations().containsKey("bar"), is(true));
    final CacheManager cacheManager = CacheManagerBuilder.newCacheManager(configuration);
    cacheManager.init();
    final Cache<Number, String> cache = cacheManager.getCache("bar", Number.class, String.class);
    cache.put(10, "dog");
    assertThat(TestCacheEventListener.FIRED_EVENT.getType(), is(EventType.CREATED));
    cache.put(10, "cat");
    assertThat(TestCacheEventListener.FIRED_EVENT.getType(), is(EventType.UPDATED));
    cache.remove(10);
    assertThat(TestCacheEventListener.FIRED_EVENT.getType(), is(EventType.REMOVED));
    cache.put(10, "dog");
    resetValues();
    assertThat(configuration.getCacheConfigurations().containsKey("template1"), is(true));
    final Cache<Number, String> templateCache = cacheManager.getCache("template1", Number.class, String.class);
    templateCache.put(10,"cat");
    assertThat(TestCacheEventListener.FIRED_EVENT, nullValue());
    templateCache.put(10, "dog");
    assertThat(TestCacheEventListener.FIRED_EVENT.getType(), is(EventType.UPDATED));
  }

  @Test
  public void testCacheEventListenerThreadPoolName() throws Exception {
    Configuration configuration = new XmlConfiguration(this.getClass().getResource("/configs/ehcache-cacheEventListener.xml"));
    CacheConfiguration<?, ?> template1 = configuration.getCacheConfigurations().get("template1");
    DefaultCacheEventDispatcherConfiguration eventDispatcherConfig = null;
    for (ServiceConfiguration<?, ?> serviceConfiguration : template1.getServiceConfigurations()) {
      if (serviceConfiguration instanceof DefaultCacheEventDispatcherConfiguration) {
        eventDispatcherConfig = (DefaultCacheEventDispatcherConfiguration) serviceConfiguration;
      }
    }
    assertThat(eventDispatcherConfig.getThreadPoolAlias(), is("listeners-pool"));
  }

  @Test
  public void testCacheEventListenerWithMultipleListener() throws Exception {
    Configuration configuration = new XmlConfiguration(this.getClass().getResource("/configs/ehcache-multipleCacheEventListener.xml"));
    assertThat(configuration.getCacheConfigurations().containsKey("bar"), is(true));
    final CacheManager cacheManager = CacheManagerBuilder.newCacheManager(configuration);
    cacheManager.init();
    final Cache<Number, String> cache = cacheManager.getCache("bar", Number.class, String.class);
    resetValues();
    cache.put(10, "dog");
    assertThat(TestCacheEventListener.FIRED_EVENT.getType(), is(EventType.CREATED));
    assertThat(TestSecondCacheEventListener.SECOND_LISTENER_FIRED_EVENT, is(nullValue()));
    resetValues();
    cache.put(10, "cat");
    assertThat(TestCacheEventListener.FIRED_EVENT.getType(), is(EventType.UPDATED));
    assertThat(TestSecondCacheEventListener.SECOND_LISTENER_FIRED_EVENT.getType(), is(EventType.UPDATED));
    resetValues();
    cache.remove(10);
    assertThat(TestCacheEventListener.FIRED_EVENT.getType(), is(EventType.REMOVED));
    assertThat(TestSecondCacheEventListener.SECOND_LISTENER_FIRED_EVENT.getType(), is(EventType.REMOVED));
  }

  @Test
  public void testThreadPools() throws Exception {
    Configuration configuration = new XmlConfiguration(this.getClass().getResource("/configs/thread-pools.xml"));
    final CacheManager cacheManager = CacheManagerBuilder.newCacheManager(configuration);
    cacheManager.init();
    try {
      Cache<String, String> cache = cacheManager.createCache("testThreadPools", newCacheConfigurationBuilder(String.class, String.class, heap(10))
              .withService(new DefaultCacheLoaderWriterConfiguration(ThreadRememberingLoaderWriter.class))
              .withService(newUnBatchedWriteBehindConfiguration().useThreadPool("small"))
              .build());

      cache.put("foo", "bar");

      ThreadRememberingLoaderWriter.USED.acquireUninterruptibly();

      assertThat(ThreadRememberingLoaderWriter.LAST_SEEN_THREAD.getName(), containsString("[small]"));
    } finally {
      cacheManager.close();
    }
  }

  @Test
  public void testThreadPoolsUsingDefaultPool() throws Exception {
    Configuration configuration = new XmlConfiguration(this.getClass().getResource("/configs/thread-pools.xml"));
    final CacheManager cacheManager = CacheManagerBuilder.newCacheManager(configuration);
    cacheManager.init();
    try {
      Cache<String, String> cache = cacheManager.createCache("testThreadPools", newCacheConfigurationBuilder(String.class, String.class, heap(10))
              .withService(new DefaultCacheLoaderWriterConfiguration(ThreadRememberingLoaderWriter.class))
              .withService(newUnBatchedWriteBehindConfiguration())
              .build());

      cache.put("foo", "bar");

      ThreadRememberingLoaderWriter.USED.acquireUninterruptibly();

      assertThat(ThreadRememberingLoaderWriter.LAST_SEEN_THREAD.getName(), containsString("[big]"));
    } finally {
      cacheManager.close();
    }
  }

  @Test
  public void testCacheWithSizeOfEngine() throws Exception {
    Configuration configuration = new XmlConfiguration(this.getClass().getResource("/configs/sizeof-engine.xml"));
    final CacheManager cacheManager = CacheManagerBuilder.newCacheManager(configuration);
    cacheManager.init();
    try {
      Cache<String, String> usesDefaultSizeOfEngine = cacheManager.getCache("usesDefaultSizeOfEngine", String.class, String.class);
      usesDefaultSizeOfEngine.put("foo", "bar");

      Cache<String, String> usesConfiguredInCache = cacheManager.getCache("usesConfiguredInCache", String.class, String.class);
      usesConfiguredInCache.put("foo", "bar");

      assertThat(usesDefaultSizeOfEngine.get("foo"), is("bar"));
      assertThat(usesConfiguredInCache.get("foo"), is("bar"));

      ResourcePools pools = ResourcePoolsBuilder.newResourcePoolsBuilder().heap(20L, EntryUnit.ENTRIES).build();
      try {
        usesDefaultSizeOfEngine.getRuntimeConfiguration().updateResourcePools(pools);
        fail();
      } catch (Exception ex) {
        assertThat(ex, instanceOf(IllegalArgumentException.class));
        assertThat(ex.getMessage(), equalTo("ResourcePool for heap with ResourceUnit 'entries' can not replace 'kB'"));
      }

      try {
        usesConfiguredInCache.getRuntimeConfiguration().updateResourcePools(pools);
        fail();
      } catch (Exception ex) {
        assertThat(ex, instanceOf(IllegalArgumentException.class));
        assertThat(ex.getMessage(), equalTo("ResourcePool for heap with ResourceUnit 'entries' can not replace 'kB'"));
      }

    } finally {
      cacheManager.close();
    }
  }

  private static void resetValues() {
    TestCacheEventListener.FIRED_EVENT = null;
    TestSecondCacheEventListener.SECOND_LISTENER_FIRED_EVENT= null;
  }

}
