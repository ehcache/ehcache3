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

package org.ehcache.impl.internal.spi.loaderwriter;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.core.config.DefaultConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.impl.config.loaderwriter.DefaultCacheLoaderWriterConfiguration;
import org.ehcache.impl.config.loaderwriter.DefaultCacheLoaderWriterProviderConfiguration;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.service.ServiceConfiguration;
import org.hamcrest.CoreMatchers;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class DefaultCacheLoaderWriterProviderTest {

  @Test
  public void testCacheConfigUsage() {
    final CacheManager manager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("foo",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
                .withService(new DefaultCacheLoaderWriterConfiguration(MyLoader.class))
                .build()).build(true);
    final Object foo = manager.getCache("foo", Object.class, Object.class).get(new Object());
    assertThat(foo, is(MyLoader.object));
  }

  @Test
  public void testCacheManagerConfigUsage() {

    final CacheConfiguration<Object, Object> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
        .build();

    final Map<String, CacheConfiguration<?, ?>> caches = new HashMap<>();
    caches.put("foo", cacheConfiguration);
    final DefaultConfiguration configuration = new DefaultConfiguration(caches, null, new DefaultCacheLoaderWriterProviderConfiguration()
        .addLoaderFor("foo", MyLoader.class));
    final CacheManager manager = CacheManagerBuilder.newCacheManager(configuration);
    manager.init();
    final Object foo = manager.getCache("foo", Object.class, Object.class).get(new Object());
    assertThat(foo, is(MyLoader.object));
  }

  @Test
  public void testCacheConfigOverridesCacheManagerConfig() {
    final CacheConfiguration<Object, Object> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
        .withService(new DefaultCacheLoaderWriterConfiguration(MyOtherLoader.class))
        .build();

    final Map<String, CacheConfiguration<?, ?>> caches = new HashMap<>();
    caches.put("foo", cacheConfiguration);
    final DefaultConfiguration configuration = new DefaultConfiguration(caches, null, new DefaultCacheLoaderWriterProviderConfiguration()
        .addLoaderFor("foo", MyLoader.class));
    final CacheManager manager = CacheManagerBuilder.newCacheManager(configuration);
    manager.init();
    final Object foo = manager.getCache("foo", Object.class, Object.class).get(new Object());
    assertThat(foo, is(MyOtherLoader.object));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testAddingCacheLoaderWriterConfigurationAtCacheLevel() {
    CacheManagerBuilder<CacheManager> cacheManagerBuilder = CacheManagerBuilder.newCacheManagerBuilder();
    Class<CacheLoaderWriter<?, ?>> klazz = (Class<CacheLoaderWriter<?, ?>>) (Class) (MyLoader.class);
    CacheManager cacheManager = cacheManagerBuilder.build(true);
    final Cache<Long, String> cache = cacheManager.createCache("cache",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(100))
            .withService(new DefaultCacheLoaderWriterConfiguration(klazz))
            .build());
    Collection<ServiceConfiguration<?, ?>> serviceConfiguration = cache.getRuntimeConfiguration()
        .getServiceConfigurations();
    assertThat(serviceConfiguration, IsCollectionContaining.<ServiceConfiguration<?, ?>>hasItem(instanceOf(DefaultCacheLoaderWriterConfiguration.class)));
    cacheManager.close();
  }

  @Test
  public void testCreationConfigurationPreservedAfterStopStart() {
    DefaultCacheLoaderWriterProviderConfiguration configuration = new DefaultCacheLoaderWriterProviderConfiguration();
    configuration.addLoaderFor("cache", MyLoader.class);
    DefaultCacheLoaderWriterProvider loaderWriterProvider = new DefaultCacheLoaderWriterProvider(configuration);

    @SuppressWarnings("unchecked")
    ServiceProvider<Service> serviceProvider = mock(ServiceProvider.class);
    loaderWriterProvider.start(serviceProvider);
    @SuppressWarnings("unchecked")
    CacheConfiguration<Object, Object> cacheConfiguration = mock(CacheConfiguration.class);
    assertThat(loaderWriterProvider.createCacheLoaderWriter("cache", cacheConfiguration), CoreMatchers.instanceOf(MyLoader.class));

    loaderWriterProvider.stop();
    loaderWriterProvider.start(serviceProvider);

    assertThat(loaderWriterProvider.createCacheLoaderWriter("cache", cacheConfiguration), CoreMatchers.instanceOf(MyLoader.class));
  }

  public static class MyLoader implements CacheLoaderWriter<Object, Object> {

    private static final Object object = new Object() {
      @Override
      public String toString() {
        return MyLoader.class.getName() + "'s object";
      }
    };

    @Override
    public Object load(final Object key) {
      return object;
    }

    @Override
    public Map<Object, Object> loadAll(final Iterable<?> keys) {
      throw new UnsupportedOperationException("Implement me!");
    }

    private static Object lastWritten;

    @Override
    public void write(final Object key, final Object value) {
      lastWritten = value;
    }

    @Override
    public void writeAll(final Iterable<? extends Map.Entry<?, ?>> entries) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void delete(final Object key) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void deleteAll(final Iterable<?> keys) {
      throw new UnsupportedOperationException("Implement me!");
    }
  }

  public static class MyOtherLoader extends MyLoader {

    private static final Object object = new Object() {
      @Override
      public String toString() {
        return MyOtherLoader.class.getName() + "'s object";
      }
    };

    private static Object lastWritten;

    @Override
    public Object load(final Object key) {
      return object;
    }

    @Override
    public void write(final Object key, final Object value) {
      lastWritten = value;
    }

  }
}
