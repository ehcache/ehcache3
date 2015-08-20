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
package org.ehcache;

import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.loaderwriter.DefaultCacheLoaderWriterConfiguration;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.writebehind.WriteBehindConfigurationBuilder;
import org.ehcache.exceptions.StateTransitionException;
import org.ehcache.loaderwriter.writebehind.WriteBehindDecoratorLoaderWriterProviderFactory;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.WriteBehindConfiguration;
import org.ehcache.spi.service.ServiceUseConfiguration;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collection;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;

/**
 * @author rism
 */
public class WriteBehindDecoratorLoaderWriterProviderFactoryTest {
  
  @Rule
  public ExpectedException expectedEx = ExpectedException.none();
  
  @SuppressWarnings("unchecked")
  @Test
  public void testAddingWriteBehindConfigurationAtCacheLevel() {
    CacheManagerBuilder<CacheManager> cacheManagerBuilder = CacheManagerBuilder.newCacheManagerBuilder();
    WriteBehindConfiguration writeBehindConfiguration = WriteBehindConfigurationBuilder.newWriteBehindConfiguration()
        .concurrencyLevel(3).batchSize(1)
        .queueSize(10)
        .build();
    Class<CacheLoaderWriter<?, ?>> klazz = (Class<CacheLoaderWriter<?, ?>>) (Class) (SampleLoaderWriter.class);
    CacheManager cacheManager = cacheManagerBuilder.build(true);
    final Cache<Long, String> cache = cacheManager.createCache("cache",
        CacheConfigurationBuilder.newCacheConfigurationBuilder()
            .add(writeBehindConfiguration)
            .add(new DefaultCacheLoaderWriterConfiguration(klazz))
            .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
                .heap(100, EntryUnit.ENTRIES).build())
            .buildConfig(Long.class, String.class));
    Collection<ServiceUseConfiguration<?>> serviceConfiguration = cache.getRuntimeConfiguration()
        .getServiceConfigurations();
    assertThat(serviceConfiguration, IsCollectionContaining.<ServiceUseConfiguration<?>>hasItem(instanceOf(WriteBehindConfiguration.class)));
    cacheManager.close();
  }
  
  @Test
  public void testWriteBehindWithoutCacheLoaderWriter() {
    expectedEx.expect(NullPointerException.class);
    expectedEx.expectMessage("WriteBehind requires non null CacheLoaderWriter");

    WriteBehindDecoratorLoaderWriterProviderFactory factory = new WriteBehindDecoratorLoaderWriterProviderFactory();
    factory.create(null).createWriteBehindDecoratorLoaderWriter(null, null);
  }

  public static class SampleLoaderWriter<K, V> implements CacheLoaderWriter<K, V> {

    @Override
    public V load(K key) throws Exception {
      throw new UnsupportedOperationException("Implement Me");
    }

    @Override
    public Map<K, V> loadAll(Iterable<? extends K> keys) throws Exception {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void write(K key, V value) throws Exception {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void writeAll(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) throws Exception {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void delete(K key) throws Exception {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void deleteAll(Iterable<? extends K> keys) throws Exception {
      throw new UnsupportedOperationException("Implement me!");
    }
  }
}
