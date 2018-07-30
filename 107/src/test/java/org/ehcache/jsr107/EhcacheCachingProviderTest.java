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

import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.theInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.Collections;
import java.util.Properties;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.spi.CachingProvider;

import org.ehcache.config.Configuration;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.core.EhcacheManager;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.impl.internal.spi.serialization.DefaultSerializationProvider;
import org.ehcache.impl.internal.statistics.DefaultStatisticsService;
import org.ehcache.jsr107.config.Jsr107Configuration;
import org.ehcache.jsr107.internal.DefaultJsr107Service;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceProvider;
import org.junit.After;
import org.junit.Test;

import com.pany.domain.Customer;

public class EhcacheCachingProviderTest {

  @After
  public void after() {
    Caching.getCachingProvider().close();
  }

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

  @Test
  public void testCacheManagerUsingACacheManagerBuilder() throws Exception {
    EhcacheCachingProvider cachingProvider = (EhcacheCachingProvider)Caching.getCachingProvider();
    URI uri = cachingProvider.getDefaultURI();
    ClassLoader classLoader = cachingProvider.getDefaultClassLoader();

    Jsr107Service jsr107Service = new DefaultJsr107Service(null);
    DefaultStatisticsService statisticsService = new DefaultStatisticsService();
    Eh107CacheLoaderWriterProvider cacheLoaderWriterFactory = new Eh107CacheLoaderWriterProvider();
    SerializationProvider serializationProvider = new DefaultSerializationProvider(null);

    CacheManagerBuilder<org.ehcache.CacheManager> builder =
      newCacheManagerBuilder()
        .using(statisticsService) // both services should be used
        .using(jsr107Service)    // instead of the default ones
        .using(cacheLoaderWriterFactory)
        .using(serializationProvider);

    CacheManager cacheManager = cachingProvider.getCacheManager(uri, classLoader, builder);
    cacheManager.createCache("cache", new MutableConfiguration<String, String>());

    EhcacheManager internalCacheManager = cacheManager.unwrap(EhcacheManager.class);
    ServiceProvider<Service> serviceProvider = getServiceProvider(internalCacheManager);
    assertThat(serviceProvider.getService(StatisticsService.class), theInstance(statisticsService));
    assertThat(serviceProvider.getService(Jsr107Service.class), theInstance(jsr107Service));
    assertThat(serviceProvider.getService(Eh107CacheLoaderWriterProvider.class), theInstance(cacheLoaderWriterFactory));
    assertThat(serviceProvider.getService(SerializationProvider.class), theInstance(serializationProvider));
  }

  @SuppressWarnings("unchecked")
  private ServiceProvider<Service> getServiceProvider(EhcacheManager cacheManager) throws Exception {
    Field field = EhcacheManager.class.getDeclaredField("serviceLocator");
    field.setAccessible(true);
    return (ServiceProvider<Service>) field.get(cacheManager);
  }

  @Test
  public void testCacheManagerUsingACacheManagerBuilder_stillHaveDefaultServices() throws Exception {
    EhcacheCachingProvider cachingProvider = (EhcacheCachingProvider)Caching.getCachingProvider();
    URI uri = cachingProvider.getDefaultURI();
    ClassLoader classLoader = cachingProvider.getDefaultClassLoader();

    CacheManagerBuilder<org.ehcache.CacheManager> builder = newCacheManagerBuilder();

    CacheManager cacheManager = cachingProvider.getCacheManager(uri, classLoader, builder);
    cacheManager.createCache("cache", new MutableConfiguration<String, String>());

    EhcacheManager internalCacheManager = cacheManager.unwrap(EhcacheManager.class);
    ServiceProvider<Service> serviceProvider = getServiceProvider(internalCacheManager);
    assertThat(serviceProvider.getService(Jsr107Service.class), notNullValue());
  }

  @Test
  public void testCacheManagerUsingACacheManagerBuilderWithAJsr107Configuration_isUsingTheConfiguration() throws Exception {
    EhcacheCachingProvider cachingProvider = (EhcacheCachingProvider)Caching.getCachingProvider();
    URI uri = cachingProvider.getDefaultURI();
    ClassLoader classLoader = cachingProvider.getDefaultClassLoader();

    CacheManagerBuilder<org.ehcache.CacheManager> builder = newCacheManagerBuilder();
    builder = builder.using(new Jsr107Configuration(null, Collections.emptyMap(), false /* not the default */, null, null));

    CacheManager cacheManager = cachingProvider.getCacheManager(uri, classLoader, builder);
    cacheManager.createCache("cache", new MutableConfiguration<String, String>());

    EhcacheManager internalCacheManager = cacheManager.unwrap(EhcacheManager.class);
    ServiceProvider<Service> serviceProvider = getServiceProvider(internalCacheManager);
    assertEquals(serviceProvider.getService(Jsr107Service.class).jsr107CompliantAtomics(), false);
  }
}
