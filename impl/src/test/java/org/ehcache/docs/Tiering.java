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

import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 * Tiering
 */
public class Tiering {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testSingleTier() {
    // tag::offheapOnly[]
    CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, // <1>
      ResourcePoolsBuilder.newResourcePoolsBuilder().offheap(2, MemoryUnit.GB)).build(); // <2>
    // end::offheapOnly[]
  }

  @Test
  public void testPersistentDiskTier() throws Exception {
    // tag::diskPersistent[]
    CacheManagerBuilder.newCacheManagerBuilder()
      .with(CacheManagerBuilder.persistence(getFilePath())) // <1>
      .withCache("myCache",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
          ResourcePoolsBuilder.newResourcePoolsBuilder().disk(1, MemoryUnit.GB, true))); // <2>
    // end::diskPersistent[]
  }

  public String getFilePath() throws IOException {
    return folder.newFolder().getAbsolutePath();
  }
}
