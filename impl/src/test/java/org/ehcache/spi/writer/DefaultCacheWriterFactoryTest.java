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

package org.ehcache.spi.writer;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.CacheManagerBuilder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.DefaultConfiguration;
import org.ehcache.config.writer.DefaultCacheWriterConfiguration;
import org.ehcache.config.writer.DefaultCacheWriterFactoryConfiguration;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Alex Snaps
 */
public class DefaultCacheWriterFactoryTest {
  @Test
  public void testCacheConfigUsage() {
    final CacheManager manager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("foo",
            CacheConfigurationBuilder.newCacheConfigurationBuilder()
                .addServiceConfig(new DefaultCacheWriterConfiguration(MyWriter.class))
                .buildConfig(Object.class, Object.class)).build();
    final Cache<Object, Object> cache = manager.getCache("foo", Object.class, Object.class);
    final Object value = new Object();
    cache.put(new Object(), value);
    assertThat(MyWriter.lastWritten, is(value));
  }

  @Test
  public void testCacheManagerConfigUsage() {

    final CacheConfiguration<Object, Object> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .buildConfig(Object.class, Object.class);

    final Map<String, CacheConfiguration<?, ?>> caches = new HashMap<String, CacheConfiguration<?, ?>>();
    caches.put("foo", cacheConfiguration);
    final DefaultConfiguration configuration = new DefaultConfiguration(caches, null, new DefaultCacheWriterFactoryConfiguration()
        .addWriterFor("foo", MyWriter.class));
    final CacheManager manager = CacheManagerBuilder.newCacheManager(configuration);
    final Cache<Object, Object> cache = manager.getCache("foo", Object.class, Object.class);
    final Object value = new Object();
    cache.put(new Object(), value);
    assertThat(MyWriter.lastWritten, is(value));
  }

  @Test
  public void testCacheConfigOverridesCacheManagerConfig() {
    final CacheConfiguration<Object, Object> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .addServiceConfig(new DefaultCacheWriterConfiguration(MyOtherWriter.class))
        .buildConfig(Object.class, Object.class);

    final Map<String, CacheConfiguration<?, ?>> caches = new HashMap<String, CacheConfiguration<?, ?>>();
    caches.put("foo", cacheConfiguration);
    final DefaultConfiguration configuration = new DefaultConfiguration(caches, null, new DefaultCacheWriterFactoryConfiguration()
        .addWriterFor("foo", MyWriter.class));
    final CacheManager manager = CacheManagerBuilder.newCacheManager(configuration);
    final Cache<Object, Object> cache = manager.getCache("foo", Object.class, Object.class);
    final Object value = new Object();
    cache.put(new Object(), value);
    assertThat(MyOtherWriter.lastWritten, is(value));
  }

  public static class MyWriter implements CacheWriter<Object, Object> {

    private static Object lastWritten;

    @Override
    public void write(final Object key, final Object value) throws Exception {
      MyWriter.lastWritten = value;
    }

    @Override
    public void writeAll(final Iterable<? extends Map.Entry<?, ?>> entries) throws Exception {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void delete(final Object key) throws Exception {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void deleteAll(final Iterable<?> keys) throws Exception {
      throw new UnsupportedOperationException("Implement me!");
    }
  }

  public static class MyOtherWriter extends MyWriter {

    private static Object lastWritten;

    @Override
    public void write(final Object key, final Object value) throws Exception {
      MyOtherWriter.lastWritten = value;
    }

  }
}
