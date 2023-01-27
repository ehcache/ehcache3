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
package org.ehcache.jsr107;

import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.core.EhcacheManager;
import org.ehcache.event.CacheEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.event.EventType;
import javax.cache.spi.CachingProvider;

import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.notNullValue;

/**
 * @author rism
 */
public class UnwrapTest {

  private CacheManager cacheManager;

  @Before
  public void setUp() {
    CachingProvider provider = Caching.getCachingProvider();
    cacheManager = provider.getCacheManager();
  }

  @After
  public void tearDown() {
    cacheManager.close();
  }

  @Test
  public void testCacheUnwrap() {
    MutableConfiguration<String, String> configuration = new MutableConfiguration<>();
    configuration.setTypes(String.class, String.class);
    Cache<String, String> cache = cacheManager.createCache("cache", configuration);
    assertThat(cache.unwrap(org.ehcache.Cache.class), is(instanceOf(org.ehcache.Cache.class)));
    assertThat(cache.unwrap(cache.getClass()), is(instanceOf(Eh107Cache.class)));
    cacheManager.close();
  }

  @Test
  public void testCacheManagerUnwrap() {
    assertThat(cacheManager.unwrap(org.ehcache.CacheManager.class), is(instanceOf(EhcacheManager.class)));
    assertThat(cacheManager.unwrap(cacheManager.getClass()), is(instanceOf(Eh107CacheManager.class)));
  }

  @Test
  public void testCacheEntryEventUnwrap() {
    MutableConfiguration<String, String> configuration = new MutableConfiguration<>();
    configuration.setTypes(String.class, String.class);
    Cache<String, String> cache = cacheManager.createCache("cache", configuration);
    org.ehcache.event.CacheEvent<String, String> ehEvent = new EhEvent();
    Eh107CacheEntryEvent<String, String> cacheEntryEvent = new Eh107CacheEntryEvent.NormalEvent<>(cache, EventType.CREATED, ehEvent, false);
    assertThat(cacheEntryEvent.unwrap(org.ehcache.event.CacheEvent.class), is(instanceOf(CacheEvent.class)));
    assertThat(cacheEntryEvent.unwrap(cacheEntryEvent.getClass()), is(instanceOf(Eh107CacheEntryEvent.NormalEvent.class)));
  }
  @Test
  public void testCacheVisibilityPostUnwrap() {

    CacheManager javaxCacheManager = Caching.getCachingProvider().getCacheManager();

    org.ehcache.CacheManager cacheManager = javaxCacheManager.unwrap(org.ehcache.CacheManager.class);
    CacheConfigurationBuilder<Integer, String> cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder(Integer.class, String.class, heap(5));
    cacheManager.createCache("jcache", cacheConfigurationBuilder);

    Cache<Integer, String> javaxCache = javaxCacheManager.getCache("jcache", Integer.class, String.class);
    assertThat(javaxCache, is(notNullValue()));

    CacheManager javaxCacheManager1 = javaxCacheManager.unwrap(javax.cache.CacheManager.class);
    Cache<Integer, String> javaxCache1 = javaxCacheManager1.getCache("jcache", Integer.class, String.class);
    assertThat(javaxCache1, is(notNullValue()));

    org.ehcache.Cache<Integer, String> cache = cacheManager.getCache("jcache", Integer.class, String.class);
    assertThat(cache, is(notNullValue()));

    cache.put(1, "one");
    assertThat(javaxCache.get(1), is("one"));
    assertThat(javaxCache1.get(1), is("one"));
  }
  private class EhEvent implements CacheEvent<String,String> {
    @Override
    public org.ehcache.event.EventType getType() {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public String getKey() {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public String getNewValue() {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public String getOldValue() {
      throw new UnsupportedOperationException("Implement me!");
    }

    @SuppressWarnings("deprecation")
    @Override
    public org.ehcache.Cache<String, String> getSource() {
      throw new UnsupportedOperationException("Implement me!");
    }
  }
}
