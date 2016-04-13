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

import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.StateTransitionException;
import org.junit.Test;

import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * TieringTest
 */
public class TieringTest {

  @Test
  public void testDiskTierWithoutPersistenceServiceFailsWithClearException() {
    try {
      newCacheManagerBuilder().withCache("failing", newCacheConfigurationBuilder(Long.class, String.class, newResourcePoolsBuilder()
          .heap(5, EntryUnit.ENTRIES)
          .disk(5, MemoryUnit.MB)).build()).build(true);
      fail("Should not initialize");
    } catch (StateTransitionException e) {
      assertThat(e.getCause().getCause().getMessage(), containsString("No service found for persistable resource: disk"));
    }
  }
}
