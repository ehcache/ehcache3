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

package org.ehcache.clustered.client.docs;

import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.Test;

/**
 * Tiering
 */
public class Tiering {

  @Test
  public void testSingleTier() {
    // tag::clusteredOnly[]
    CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, // <1>
      ResourcePoolsBuilder.newResourcePoolsBuilder()
        .with(ClusteredResourcePoolBuilder.clusteredDedicated(2, MemoryUnit.GB))); // <2>
    // end::clusteredOnly[]
  }
}
