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

import org.ehcache.PersistentCacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.StateTransitionException;
import org.ehcache.impl.config.persistence.CacheManagerPersistenceConfiguration;
import org.junit.Test;

import java.io.File;
import java.io.Serializable;
import java.net.URISyntaxException;

import static junit.framework.TestCase.fail;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class PersistentCacheTest {

  @Test
  public void testRecoverPersistentCacheFailsWhenConfiguringIncompatibleClass() throws Exception {
    {
      PersistentCacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
          .with(new CacheManagerPersistenceConfiguration(new File(getStoragePath(), "testRecoverPersistentCacheFailsWhenConfiguringIncompatibleClass")))
          .withCache("persistentCache",
              CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                  newResourcePoolsBuilder()
                      .heap(1, MemoryUnit.MB)
                      .offheap(2, MemoryUnit.MB)
                      .disk(5, MemoryUnit.MB, true)
                  )
          ).build(true);


      cacheManager.close();
    }

    {
        PersistentCacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
            .with(new CacheManagerPersistenceConfiguration(new File(getStoragePath(), "testRecoverPersistentCacheFailsWhenConfiguringIncompatibleClass")))
            .withCache("persistentCache",
                CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, Serializable.class,
                    newResourcePoolsBuilder()
                        .heap(1, MemoryUnit.MB)
                        .offheap(2, MemoryUnit.MB)
                        .disk(5, MemoryUnit.MB, true)
                    )
            ).build();

      try {
        cacheManager.init();
        fail("expected StateTransitionException");
      } catch (StateTransitionException ste) {
        Throwable rootCause = findRootCause(ste);
        assertThat(rootCause, instanceOf(IllegalArgumentException.class));
        assertThat(rootCause.getMessage(), equalTo("Persisted value type 'java.lang.String' is not the same as the configured value type 'java.io.Serializable'"));
      }
    }
  }

  private Throwable findRootCause(Throwable t) {
    Throwable result = t;
    while (result.getCause() != null) {
      result = result.getCause();
    }
    return result;
  }

  @Test
  public void testRecoverPersistentCacheSucceedsWhenConfiguringArrayClass() throws Exception {
    {
      PersistentCacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
          .with(new CacheManagerPersistenceConfiguration(new File(getStoragePath(), "testRecoverPersistentCacheSucceedsWhenConfiguringArrayClass")))
          .withCache("persistentCache",
              CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, byte[].class,
                  newResourcePoolsBuilder()
                      .heap(1, MemoryUnit.MB)
                      .offheap(2, MemoryUnit.MB)
                      .disk(5, MemoryUnit.MB, true)
                  )
          ).build(true);


      cacheManager.close();
    }

    {
      PersistentCacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
          .with(new CacheManagerPersistenceConfiguration(new File(getStoragePath(), "testRecoverPersistentCacheSucceedsWhenConfiguringArrayClass")))
          .withCache("persistentCache",
              CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, byte[].class,
                  newResourcePoolsBuilder()
                      .heap(1, MemoryUnit.MB)
                      .offheap(2, MemoryUnit.MB)
                      .disk(5, MemoryUnit.MB, true)
                  )
          ).build(true);


      cacheManager.close();
    }
  }

  private String getStoragePath() throws URISyntaxException {
    return getClass().getClassLoader().getResource(".").toURI().getPath();
  }

}
