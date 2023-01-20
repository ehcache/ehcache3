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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.net.URI;
import java.util.Properties;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.spi.CachingProvider;

import org.ehcache.config.Configuration;
import org.junit.Test;

import com.pany.domain.Customer;

public class EhCachingProviderTest {

  @Test
  public void testLoadsAsCachingProvider() {
    final CachingProvider provider = Caching.getCachingProvider();
    assertThat(provider, is(instanceOf(EhcacheCachingProvider.class)));
  }

  @Test
  public void testDefaultUriOverride() throws Exception {
    URI override = getClass().getResource("/ehcache-107.xml").toURI();

    Properties props = new Properties();
    props.put(DefaultConfigurationResolver.DEFAULT_CONFIG_PROPERTY_NAME, override);

    CacheManager cacheManager = Caching.getCachingProvider().getCacheManager(null, null, props);

    assertEquals(override, cacheManager.getURI());

    Caching.getCachingProvider().close();
  }

  @Test
  public void testCacheUsesCacheManagerClassLoaderForDefaultURI() {
    CachingProvider cachingProvider = Caching.getCachingProvider();
    LimitedClassLoader limitedClassLoader = new LimitedClassLoader(cachingProvider.getDefaultClassLoader());

    CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), limitedClassLoader);

    MutableConfiguration<Object, Object> configuration = new MutableConfiguration<>();
    Cache<Object, Object> cache = cacheManager.createCache("test", configuration);

    cache.put(1L, new Customer(1L));

    try {
      cache.get(1L);
      fail("Expected AssertionError");
    } catch (AssertionError e) {
      assertThat(e.getMessage(), is("No com.pany here"));
    }
  }

  @Test
  public void testClassLoadCount() throws Exception {
    EhcacheCachingProvider cachingProvider = (EhcacheCachingProvider)Caching.getCachingProvider();
    URI uri = cachingProvider.getDefaultURI();
    ClassLoader classLoader = cachingProvider.getDefaultClassLoader();
    CountingConfigSupplier configSupplier = new CountingConfigSupplier(uri, classLoader);

    assertEquals(configSupplier.configCount, 0);

    cachingProvider.getCacheManager(configSupplier, new Properties());

    assertEquals(configSupplier.configCount, 1);

    cachingProvider.getCacheManager(configSupplier, new Properties());

    assertEquals(configSupplier.configCount, 1);
  }

  private class LimitedClassLoader extends ClassLoader {

    private final ClassLoader delegate;

    private LimitedClassLoader(ClassLoader delegate) {
      this.delegate = delegate;
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
      if (name.startsWith("com.pany")) {
        throw new AssertionError("No com.pany here");
      }
      return delegate.loadClass(name);
    }
  }

  private static class CountingConfigSupplier extends EhcacheCachingProvider.ConfigSupplier {
    private int configCount = 0;

    public CountingConfigSupplier(URI uri, ClassLoader classLoader) {
      super(uri, classLoader);
    }

    @Override
    public Configuration getConfiguration() {
      configCount++;
      return super.getConfiguration();
    }
  }
}
