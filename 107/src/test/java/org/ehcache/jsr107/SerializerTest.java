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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URISyntaxException;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.spi.CachingProvider;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * @author rism
 */

public class SerializerTest {
  private CacheManager cacheManager;

  @Before
  public void setUp() throws Exception {
    CachingProvider cachingProvider = Caching.getCachingProvider();
    cacheManager = cachingProvider.getCacheManager(getClass().getResource("/ehcache-107-serializer.xml")
        .toURI(), cachingProvider.getDefaultClassLoader());
  }

  @After
  public void tearDown() {
    cacheManager.close();
  }

  @Test
  public void testJSR107DefaultSerializer() throws URISyntaxException {
    Cache<Long, String> cache = cacheManager.getCache("cache", Long.class, String.class);
    cache.put(19L, "foo");
    assertThat(cache.get(19L), equalTo("foo"));
  }

  @Test
  public void testJSR107Serializer() {
    Cache<Long, String> cache1 = cacheManager.getCache("cache1", Long.class, String.class);
    cache1.put(17L, "foo");
    assertThat(cache1.get(17L), equalTo("foo"));
  }
}
