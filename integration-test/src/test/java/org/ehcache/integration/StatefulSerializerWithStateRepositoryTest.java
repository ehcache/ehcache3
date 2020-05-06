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
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.impl.serialization.CompactJavaSerializer;
import org.ehcache.integration.domain.Person;
import org.junit.Rule;
import org.junit.Test;
import org.terracotta.org.junit.rules.TemporaryFolder;

import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.persistence;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class StatefulSerializerWithStateRepositoryTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testStatefulSerializerWithDiskStateRepository() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> cmBuilder = newCacheManagerBuilder().with(persistence(temporaryFolder.newFolder()
        .getAbsolutePath()))
        .withCache("myCache", newCacheConfigurationBuilder(Long.class, Person.class, heap(10).disk(50, MemoryUnit.MB, true))
            .withValueSerializer(CompactJavaSerializer.asTypedSerializer()));
    PersistentCacheManager cacheManager = cmBuilder.build(true);

    Cache<Long, Person> myCache = cacheManager.getCache("myCache", Long.class, Person.class);

    myCache.put(42L, new Person("John", 42));
    myCache.put(35L, new Person("Marie", 35));

    cacheManager.close();

    cacheManager.init();

    myCache = cacheManager.getCache("myCache", Long.class, Person.class);

    assertThat(myCache.get(42L).getName(), is("John"));
  }

  @Test
  public void testStatefulSerializerWithDiskStateRepositoryDifferentPersistenceServices() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> cmBuilder = newCacheManagerBuilder().with(persistence(temporaryFolder.newFolder()
        .getAbsolutePath()))
        .withCache("myCache", newCacheConfigurationBuilder(Long.class, Person.class, heap(10).disk(50, MemoryUnit.MB, true))
            .withValueSerializer(CompactJavaSerializer.asTypedSerializer()));
    PersistentCacheManager cacheManager = cmBuilder.build(true);

    Cache<Long, Person> myCache = cacheManager.getCache("myCache", Long.class, Person.class);

    myCache.put(42L, new Person("John", 42));
    myCache.put(35L, new Person("Marie", 35));

    cacheManager.close();

    cacheManager = cmBuilder.build(true);

    myCache = cacheManager.getCache("myCache", Long.class, Person.class);

    assertThat(myCache.get(42L).getName(), is("John"));
  }
}
