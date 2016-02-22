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

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;

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
    MutableConfiguration<String, String> configuration = new MutableConfiguration<String, String>();
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
    MutableConfiguration<String, String> configuration = new MutableConfiguration<String, String>();
    configuration.setTypes(String.class, String.class);
    Cache<String, String> cache = cacheManager.createCache("cache", configuration);
    org.ehcache.event.CacheEvent<String, String> ehEvent = new EhEvent();
    Eh107CacheEntryEvent<String, String> cacheEntryEvent = new Eh107CacheEntryEvent.NormalEvent<String, String>(cache, EventType.CREATED, ehEvent, false);
    assertThat(cacheEntryEvent.unwrap(org.ehcache.event.CacheEvent.class), is(instanceOf(CacheEvent.class)));
    assertThat(cacheEntryEvent.unwrap(cacheEntryEvent.getClass()), is(instanceOf(Eh107CacheEntryEvent.NormalEvent.class)));
  }

  @SuppressWarnings("unchecked")
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

    @Override
    public org.ehcache.Cache getSource() {
      throw new UnsupportedOperationException("Implement me!");
    }
  }
}
