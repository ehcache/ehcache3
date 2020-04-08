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

import org.ehcache.PersistentUserManagedCache;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.builders.UserManagedCacheBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.spi.service.LocalPersistenceService;
import org.ehcache.impl.config.persistence.DefaultPersistenceConfiguration;
import org.ehcache.impl.config.persistence.UserManagedPersistenceContext;
import org.ehcache.impl.persistence.DefaultLocalPersistenceService;
import org.junit.Rule;
import org.junit.Test;
import org.terracotta.org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.Serializable;

import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * PersistentUserManagedCacheTest
 */
public class PersistentUserManagedCacheTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void recoversWithSerializableType() throws Exception {
    File folder = temporaryFolder.newFolder("cache-persistence-store");
    {
      LocalPersistenceService persistenceService = new DefaultLocalPersistenceService(new DefaultPersistenceConfiguration(folder));
      PersistentUserManagedCache<Long, Foo> cache = UserManagedCacheBuilder.newUserManagedCacheBuilder(Long.class, Foo.class)
          .with(new UserManagedPersistenceContext<>("cache-name", persistenceService))
          .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
              .heap(10L, EntryUnit.ENTRIES)
              .disk(10L, MemoryUnit.MB, true))
          .build(true);
      cache.put(1L, new Foo(1));
      cache.close();
      persistenceService.stop();
    }

    {
      LocalPersistenceService persistenceService = new DefaultLocalPersistenceService(new DefaultPersistenceConfiguration(folder));
      PersistentUserManagedCache<Long, Foo> cache = UserManagedCacheBuilder.newUserManagedCacheBuilder(Long.class, Foo.class)
          .with(new UserManagedPersistenceContext<>("cache-name", persistenceService))
          .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
              .heap(10L, EntryUnit.ENTRIES)
              .disk(10L, MemoryUnit.MB, true))
          .build(true);
      //It fails here
      assertThat(cache.get(1L), notNullValue());
      cache.close();
      persistenceService.stop();
      cache.destroy();
    }
  }

  private static class Foo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int i;

    Foo(int i) {
      this.i = i;
    }
  }
}
