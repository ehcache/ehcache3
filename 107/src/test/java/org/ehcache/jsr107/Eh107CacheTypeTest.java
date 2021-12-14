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

import org.junit.Test;

import javax.cache.Caching;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.spi.CachingProvider;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class Eh107CacheTypeTest {

  @Test
  @SuppressWarnings("unchecked")
  public void testCompileTimeTypeSafety() throws Exception {
    CachingProvider provider = Caching.getCachingProvider();
    javax.cache.CacheManager cacheManager =
        provider.getCacheManager(this.getClass().getResource("/ehcache-107-types.xml").toURI(), getClass().getClassLoader());
    MutableConfiguration<Long, String> cache1Conf = new MutableConfiguration<Long, String>();
    javax.cache.Cache<Long, String> cache = cacheManager.createCache("cache1", cache1Conf);

    cache.put(1l, "one");
    cache.put(2l, "two");

    Configuration cache1CompleteConf = cache.getConfiguration(Configuration.class);
    //This ensures that we have compile time type safety, i.e when configuration does not have types defined but
    // what you get cache as should work.
    assertThat((Class<Object>)cache1CompleteConf.getKeyType(), is(equalTo(Object.class)));
    assertThat((Class<Object>)cache1CompleteConf.getValueType(), is(equalTo(Object.class)));

    assertThat(cache.get(1l), is(equalTo("one")));
    assertThat(cache.get(2l), is(equalTo("two")));


    javax.cache.Cache second = cacheManager.getCache("cache1");
    second.put("3","three");

    assertThat((String)second.get("3"), is(equalTo("three")));
    cacheManager.destroyCache("cache1");
    cacheManager.close();

  }


  @Test
  public void testRunTimeTypeSafety() throws Exception {
    CachingProvider provider = Caching.getCachingProvider();
    javax.cache.CacheManager cacheManager =
        provider.getCacheManager(this.getClass().getResource("/ehcache-107-types.xml").toURI(), getClass().getClassLoader());
    MutableConfiguration<Long, String> cache1Conf = new MutableConfiguration<Long, String>();
    cache1Conf.setTypes(Long.class, String.class);
    javax.cache.Cache<Long, String> cache = cacheManager.createCache("cache1", cache1Conf);

    @SuppressWarnings("unchecked")
    Configuration<Long, String> cache1CompleteConf = cache.getConfiguration(Configuration.class);

    assertThat(cache1CompleteConf.getKeyType(), is(equalTo(Long.class)));
    assertThat(cache1CompleteConf.getValueType(), is(equalTo(String.class)));

    try {
      cacheManager.getCache("cache1");
      fail("Caches with runtime types should throw illegal argument exception when different types are used in getcache");
    } catch (IllegalArgumentException e) {
      //Empty block as nothing is required to be tested
    } finally {
      cacheManager.destroyCache("cache1");
      cacheManager.close();
    }
  }

  @Test
  public void testTypeOverriding() throws Exception {
    CachingProvider provider = Caching.getCachingProvider();
    javax.cache.CacheManager cacheManager =
        provider.getCacheManager(this.getClass().getResource("/ehcache-107-types.xml").toURI(), getClass().getClassLoader());
    MutableConfiguration<Long, String> cache1Conf = new MutableConfiguration<Long, String>();
    cache1Conf.setTypes(Long.class, String.class);
    javax.cache.Cache<Long, String> cache = cacheManager.createCache("defaultCache", cache1Conf);
    @SuppressWarnings("unchecked")
    Configuration<Long, String> cache1CompleteConf = cache.getConfiguration(Configuration.class);
    assertThat(cache1CompleteConf.getKeyType(), is(equalTo(Long.class)));
    assertThat(cache1CompleteConf.getValueType(), is(equalTo(String.class)));
  }

  @Test
  public void testEhcacheCloseRemovesFromCacheManager() throws Exception {
    CachingProvider provider = Caching.getCachingProvider();
    javax.cache.CacheManager cacheManager =
        provider.getCacheManager(this.getClass()
            .getResource("/ehcache-107-types.xml")
            .toURI(), getClass().getClassLoader());
    MutableConfiguration<Long, String> cache1Conf = new MutableConfiguration<Long, String>();
    javax.cache.Cache<Long, String> cache = cacheManager.createCache("cache1", cache1Conf);
    cacheManager.unwrap(org.ehcache.CacheManager.class).removeCache(cache.getName());
    try {
      assertThat(cacheManager.getCache(cache.getName()), nullValue());
    } finally {
      cacheManager.close();
    }
  }

  @Test
  public void testCacheManagerCloseLenientToEhcacheClosed() throws Exception {
    CachingProvider provider = Caching.getCachingProvider();
    javax.cache.CacheManager cacheManager =
        provider.getCacheManager(this.getClass()
            .getResource("/ehcache-107-types.xml")
            .toURI(), getClass().getClassLoader());
    MutableConfiguration<Long, String> cache1Conf = new MutableConfiguration<Long, String>();
    javax.cache.Cache<Long, String> cache = cacheManager.createCache("cache1", cache1Conf);
    cacheManager.unwrap(org.ehcache.CacheManager.class).removeCache(cache.getName());
    cacheManager.close();
  }
}
