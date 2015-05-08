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
package org.ehcache.util;

import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.ResourceType;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author rism
 */
public class ResourcePoolsMergerTest {

  @Test
  public void testAddingNewTierWhileUpdating() {
    ResourcePools existing = ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10L, EntryUnit.ENTRIES).build();
    ResourcePools toBeUpdated = ResourcePoolsBuilder.newResourcePoolsBuilder().disk(50L, EntryUnit.ENTRIES).build();
    ResourcePoolMerger merger = new ResourcePoolMerger();
    try {
      merger.validateAndMerge(existing, toBeUpdated);
    } catch (IllegalArgumentException iae) {
      assertThat(iae.getMessage(), is("Pools to be updated cannot contain previously undefined resources pools"));
    }
  }

  @Test
  public void testUpdatingOffHeap() {
    ResourcePools existing = ResourcePoolsBuilder.newResourcePoolsBuilder().offheap(10L, MemoryUnit.MB).build();
    ResourcePools toBeUpdated = ResourcePoolsBuilder.newResourcePoolsBuilder().offheap(50L, MemoryUnit.MB).build();
    ResourcePoolMerger merger = new ResourcePoolMerger();
    try {
      merger.validateAndMerge(existing, toBeUpdated);
    } catch (UnsupportedOperationException uoe) {
      assertThat(uoe.getMessage(), is("Updating OFFHEAP resource is not supported"));
    }
  }

  @Test
  public void testUpdatingHeapSizeToLargerThanDisk() {
    ResourcePoolsBuilder existingPoolsBuilder = ResourcePoolsBuilder.newResourcePoolsBuilder();
    existingPoolsBuilder = existingPoolsBuilder.heap(20L, EntryUnit.ENTRIES).disk(50, EntryUnit.ENTRIES);
    ResourcePools existing = existingPoolsBuilder.build();

    ResourcePoolsBuilder toBeUpdatedPoolsBuilder = ResourcePoolsBuilder.newResourcePoolsBuilder();
    toBeUpdatedPoolsBuilder = toBeUpdatedPoolsBuilder.heap(60, EntryUnit.ENTRIES);
    ResourcePools toBeUpdated = toBeUpdatedPoolsBuilder.build();

    ResourcePoolMerger merger = new ResourcePoolMerger();
    try {
      merger.validateAndMerge(existing, toBeUpdated);
    } catch (IllegalArgumentException iae) {
      assertThat(iae.getMessage(), is("Updating resource pools leads authoritative tier being smaller than caching tier"));
    }
  }
}
