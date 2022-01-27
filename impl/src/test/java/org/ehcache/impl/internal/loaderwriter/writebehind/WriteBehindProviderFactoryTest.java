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

package org.ehcache.impl.internal.loaderwriter.writebehind;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.impl.config.loaderwriter.DefaultCacheLoaderWriterConfiguration;
import org.ehcache.config.builders.WriteBehindConfigurationBuilder;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.WriteBehindConfiguration;
import org.ehcache.spi.service.ServiceConfiguration;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collection;
import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;

/**
 * @author rism
 */
public class WriteBehindProviderFactoryTest {

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  @SuppressWarnings("unchecked")
  @Test
  public void testAddingWriteBehindConfigurationAtCacheLevel() {
    CacheManagerBuilder<CacheManager> cacheManagerBuilder = CacheManagerBuilder.newCacheManagerBuilder();
    WriteBehindConfiguration<?> writeBehindConfiguration = WriteBehindConfigurationBuilder.newBatchedWriteBehindConfiguration(Long.MAX_VALUE, SECONDS, 1)
        .concurrencyLevel(3)
        .queueSize(10)
        .build();
    Class<CacheLoaderWriter<?, ?>> klazz = (Class<CacheLoaderWriter<?, ?>>) (Class) (SampleLoaderWriter.class);
    CacheManager cacheManager = cacheManagerBuilder.build(true);
    final Cache<Long, String> cache = cacheManager.createCache("cache",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(100))
            .withService(writeBehindConfiguration)
            .withService(new DefaultCacheLoaderWriterConfiguration(klazz))
            .build());
    Collection<ServiceConfiguration<?, ?>> serviceConfiguration = cache.getRuntimeConfiguration()
        .getServiceConfigurations();
    assertThat(serviceConfiguration, IsCollectionContaining.<ServiceConfiguration<?, ?>>hasItem(instanceOf(WriteBehindConfiguration.class)));
    cacheManager.close();
  }

  @Test
  public void testWriteBehindWithoutCacheLoaderWriter() {
    expectedEx.expect(NullPointerException.class);
    expectedEx.expectMessage("WriteBehind requires a non null CacheLoaderWriter");

    WriteBehindProviderFactory factory = new WriteBehindProviderFactory();
    factory.create(null).createWriteBehindLoaderWriter(null, null);
  }

  public static class SampleLoaderWriter<K, V> implements CacheLoaderWriter<K, V> {

    @Override
    public V load(K key) {
      throw new UnsupportedOperationException("Implement Me");
    }

    @Override
    public Map<K, V> loadAll(Iterable<? extends K> keys) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void write(K key, V value) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void writeAll(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void delete(K key) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void deleteAll(Iterable<? extends K> keys) {
      throw new UnsupportedOperationException("Implement me!");
    }
  }
}
