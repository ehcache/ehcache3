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

package org.ehcache.config.xml;

import com.pany.ehcache.integration.TestCacheEventListener;
import com.pany.ehcache.integration.TestSecondCacheEventListener;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.CacheManagerBuilder;
import org.ehcache.config.Configuration;
import org.ehcache.event.EventType;
import org.junit.Test;
import org.xml.sax.SAXException;

import com.pany.ehcache.integration.TestCacheLoaderWriter;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.nullValue;

/**
 * @author Alex Snaps
 */
public class IntegrationConfigTest {

  @Test
  public void testName() throws Exception {
    Configuration configuration = new XmlConfiguration(this.getClass().getResource("/configs/default-serializer.xml"));
    final CacheManager cacheManager = CacheManagerBuilder.newCacheManager(configuration);
    cacheManager.init();

    Cache<String, String> bar = cacheManager.getCache("bar", String.class, String.class);

    bar.put("1", "one");

    bar.get("1");

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
    final Number key = new Long(42);
    TestCacheLoaderWriter.latch = new CountDownLatch(1);
    cache.put(key, "Bye y'all!");
    TestCacheLoaderWriter.latch.await(2, TimeUnit.SECONDS);
    assertThat(TestCacheLoaderWriter.lastWrittenKey, is(key));

    assertThat(configuration.getCacheConfigurations().containsKey("template1"), is(true));
    final Cache<Number, String> templateCache = cacheManager.getCache("template1", Number.class, String.class);
    assertThat(templateCache, notNullValue());
    assertThat(templateCache.get(1), notNullValue());
    final Number key1 = new Long(100);
    TestCacheLoaderWriter.latch = new CountDownLatch(1);
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

  private static void resetValues() {
    TestCacheEventListener.FIRED_EVENT = null;
    TestSecondCacheEventListener.SECOND_LISTENER_FIRED_EVENT= null;
  }
  
}
