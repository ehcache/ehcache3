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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.Factory;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CompletionListenerFuture;
import javax.cache.spi.CachingProvider;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * SimpleEh107ConfigTest
 */
public class SimpleEh107ConfigTest {

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
  public void testExpiryConfiguration() {
    final AtomicBoolean expiryCreated = new AtomicBoolean(false);

    MutableConfiguration<String, String> configuration = new MutableConfiguration<String, String>();
    configuration.setTypes(String.class, String.class);
    configuration.setExpiryPolicyFactory(new Factory<ExpiryPolicy>() {
      @Override
      public ExpiryPolicy create() {
        expiryCreated.set(true);
        return new CreatedExpiryPolicy(Duration.FIVE_MINUTES);
      }
    });

    Cache<String, String> cache = cacheManager.createCache("cache", configuration);

    cache.putIfAbsent("42", "The Answer");
    cache.putIfAbsent("42", "Or not!?");

    assertThat(expiryCreated.get(), is(true));
  }

  @Test
  public void testLoaderConfiguration() throws Exception {
    final AtomicBoolean loaderCreated = new AtomicBoolean(false);

    MutableConfiguration<String, String> configuration = new MutableConfiguration<String, String>();
    configuration.setTypes(String.class, String.class).setReadThrough(true);
    configuration.setCacheLoaderFactory(new Factory<CacheLoader<String, String>>() {
      @Override
      public CacheLoader<String, String> create() {
        loaderCreated.set(true);
        return new TestCacheLoader();
      }
    });

    CachingProvider provider = Caching.getCachingProvider();
    CacheManager cacheManager = provider.getCacheManager();
    Cache<String, String> cache = cacheManager.createCache("cache", configuration);

    assertThat(loaderCreated.get(), is(true));

    cache.putIfAbsent("42", "The Answer");

    TestCacheLoader.seen.clear();
    CompletionListenerFuture future = new CompletionListenerFuture();
    cache.loadAll(Collections.singleton("42"), true, future);
    future.get();

    assertThat(TestCacheLoader.seen, contains("42"));
  }

  private static class TestCacheLoader implements CacheLoader<String, String> {

    static Set<String> seen = new HashSet<String>();

    @Override
    public String load(String key) throws CacheLoaderException {
      seen.add(key);
      return null;
    }

    @Override
    public Map<String, String> loadAll(Iterable<? extends String> keys) throws CacheLoaderException {
      for (String key : keys) {
        seen.add(key);
      }
      return Collections.emptyMap();
    }
  }
}
