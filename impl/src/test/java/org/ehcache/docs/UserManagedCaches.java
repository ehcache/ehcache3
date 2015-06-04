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

package org.ehcache.docs;

import org.ehcache.PersistentUserManagedCache;
import org.ehcache.UserManagedCacheBuilder;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.persistence.DefaultPersistenceConfiguration;
import org.ehcache.config.persistence.PersistenceConfiguration;
import org.ehcache.config.persistence.UserManagedPersistenceContext;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.internal.persistence.DefaultLocalPersistenceService;
import org.ehcache.spi.service.LocalPersistenceService;
import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * UserManagedCaches
 */
public class UserManagedCaches {

  @Test
  public void userManagedDiskCache() throws Exception {
    // tag::persistentUserManagedCache[]
    LocalPersistenceService persistenceService = new DefaultLocalPersistenceService(
        new DefaultPersistenceConfiguration(new File(getStoragePath(), "myUserData"))); // <1>

    PersistentUserManagedCache<Long, String> cache = UserManagedCacheBuilder.newUserManagedCacheBuilder(Long.class, String.class)
        .with(new UserManagedPersistenceContext<Long, String>("name", persistenceService)) // <3>
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
            .heap(10L, EntryUnit.ENTRIES)
            .disk(10L, MemoryUnit.MB, true)) // <4>
        .build(true);

    // Work with the cache
    cache.put(42L, "The Answer!");
    assertThat(cache.get(42L), is("The Answer!"));

    cache.close(); // <5>
    cache.toMaintenance().destroy(); // <6>
    // end::persistentUserManagedCache[]
  }

  private String getStoragePath() throws URISyntaxException {
    return getClass().getClassLoader().getResource(".").toURI().getPath();
  }

}
