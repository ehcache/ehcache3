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
package org.ehcache.integration;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.core.internal.resilience.ThrowingResilienceStrategy;
import org.ehcache.impl.config.persistence.CacheManagerPersistenceConfiguration;
import org.ehcache.impl.serialization.PlainJavaSerializer;
import org.ehcache.spi.serialization.SerializerException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.terracotta.org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import static java.util.Collections.singleton;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.config.units.EntryUnit.ENTRIES;
import static org.ehcache.config.units.MemoryUnit.MB;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

@RunWith(Parameterized.class)
public class DeserializationOnRemovalTest {

  @Rule
  public final TemporaryFolder folder = new TemporaryFolder();

  private final ResourcePools resources;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
      //1 tier
      { newResourcePoolsBuilder().offheap(1, MB) },
      { newResourcePoolsBuilder().disk(1, MB) },

      //2 tiers
      { newResourcePoolsBuilder().heap(1, MB).offheap(2, MB) },
      { newResourcePoolsBuilder().heap(1, ENTRIES).offheap(2, MB) },
      { newResourcePoolsBuilder().heap(1, MB).disk(2, MB) },
      { newResourcePoolsBuilder().heap(1, ENTRIES).disk(2, MB) },

      //3 tiers
      { newResourcePoolsBuilder().heap(1, MB).offheap(2, MB).disk(3, MB) },
      { newResourcePoolsBuilder().heap(1, ENTRIES).offheap(2, MB).disk(3, MB) }
    });
  }

  public DeserializationOnRemovalTest(ResourcePoolsBuilder poolBuilder) {
    this.resources = poolBuilder.build();
  }

  @Test
  public void testForDeserializationOnRemove() throws IOException {
    try (CacheManager manager = CacheManagerBuilder.newCacheManagerBuilder()
      .with(new CacheManagerPersistenceConfiguration(folder.newFolder()))
      .withCache("test-cache", newCacheConfigurationBuilder(Long.class, String.class, resources)
        .withResilienceStrategy(new ThrowingResilienceStrategy<>())
        .withValueSerializer(new PlainJavaSerializer<String>(DeserializationOnRemovalTest.class.getClassLoader()) {
          @Override
          public String read(ByteBuffer entry) throws SerializerException {
            throw new AssertionError("No deserialization allowed");
          }
        })).build(true)) {

      Cache<Long, String> cache = manager.getCache("test-cache", Long.class, String.class);

      cache.put(1L, "one");
      cache.remove(1L);

      assertThat(cache.get(1L), is(nullValue()));
    }
  }

  @Test
  public void testForDeserializationOnRemoveAll() throws IOException {
    try (CacheManager manager = CacheManagerBuilder.newCacheManagerBuilder()
      .with(new CacheManagerPersistenceConfiguration(folder.newFolder()))
      .withCache("test-cache", newCacheConfigurationBuilder(Long.class, String.class, resources)
        .withResilienceStrategy(new ThrowingResilienceStrategy<>())
        .withValueSerializer(new PlainJavaSerializer<String>(DeserializationOnRemovalTest.class.getClassLoader()) {
          @Override
          public String read(ByteBuffer entry) throws SerializerException {
            throw new AssertionError("No deserialization allowed");
          }
        })).build(true)) {

      Cache<Long, String> cache = manager.getCache("test-cache", Long.class, String.class);

      cache.put(1L, "one");
      cache.removeAll(singleton(1L));

      assertThat(cache.get(1L), is(nullValue()));
    }

  }


}
