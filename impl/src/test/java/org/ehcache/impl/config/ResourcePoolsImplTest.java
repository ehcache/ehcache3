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
package org.ehcache.impl.config;

import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceType;
import org.ehcache.config.ResourceUnit;
import org.ehcache.config.SizedResourcePool;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Collection;

import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.ehcache.config.ResourceType.Core.HEAP;
import static org.ehcache.config.ResourceType.Core.OFFHEAP;
import static org.ehcache.config.units.EntryUnit.ENTRIES;
import static org.ehcache.config.units.MemoryUnit.KB;
import static org.ehcache.config.units.MemoryUnit.MB;
import static org.ehcache.impl.config.ResourcePoolsImpl.validateResourcePools;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 *
 * @author cdennis
 */
public class ResourcePoolsImplTest {

  private static class ArbitraryType implements ResourceType<SizedResourcePool> {
    private final int tierHeight;

    public ArbitraryType(int tierHeight) {
      this.tierHeight = tierHeight;
    }

    @Override
    public Class<SizedResourcePool> getResourcePoolClass() {
      return SizedResourcePool.class;
    }

    @Override
    public boolean isPersistable() {
      return false;
    }

    @Override
    public boolean requiresSerialization() {
      return false;
    }

    @Override
    public int getTierHeight() {
      return tierHeight;
    }

    @Override
    public String toString() {
      return "arbitrary";
    }
  }

  @Test
  public void testMismatchedUnits() {
    Collection<SizedResourcePoolImpl<SizedResourcePool>> pools = asList(
      new SizedResourcePoolImpl<>(HEAP, Integer.MAX_VALUE, ENTRIES, false),
      new SizedResourcePoolImpl<>(OFFHEAP, 10, MB, false));
    validateResourcePools(pools);
  }

  @Test
  public void testMatchingEqualUnitsWellTiered() {
    Collection<SizedResourcePoolImpl<SizedResourcePool>> pools = asList(
      new SizedResourcePoolImpl<>(HEAP, 9, MB, false),
      new SizedResourcePoolImpl<>(OFFHEAP, 10, MB, false));
    validateResourcePools(pools);
  }

  @Test
  public void testMatchingUnequalUnitsWellTiered() {
    Collection<SizedResourcePoolImpl<SizedResourcePool>> pools = asList(
      new SizedResourcePoolImpl<>(HEAP, 9, MB, false),
      new SizedResourcePoolImpl<>(OFFHEAP, 10240, KB, false));
    validateResourcePools(pools);
  }

  @Test
  public void testArbitraryPoolWellTieredHeap() {
    Collection<SizedResourcePoolImpl<SizedResourcePool>> pools = asList(
      new SizedResourcePoolImpl<>(HEAP, 9, MB, false),
      new SizedResourcePoolImpl<>(new ArbitraryType(HEAP.getTierHeight() - 1), 10, MB, false));
    validateResourcePools(pools);
  }

  @Test
  public void testArbitraryPoolWellTieredOffHeap() {
    Collection<SizedResourcePoolImpl<SizedResourcePool>> pools = asList(
      new SizedResourcePoolImpl<>(new ArbitraryType(OFFHEAP.getTierHeight() + 1), 9, MB, false),
      new SizedResourcePoolImpl<>(OFFHEAP, 10, MB, false));
    validateResourcePools(pools);
  }

  @Test
  public void testArbitraryPoolInversionHeap() {
    Collection<SizedResourcePoolImpl<SizedResourcePool>> pools = asList(
      new SizedResourcePoolImpl<>(HEAP, 10, MB, false),
      new SizedResourcePoolImpl<>(new ArbitraryType(HEAP.getTierHeight() - 1), 10, MB, false));
    try {
      validateResourcePools(pools);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("Tiering Inversion: 'Pool {10 MB heap}' is not smaller than 'Pool {10 MB arbitrary}'"));
    }
  }

  @Test
  public void testArbitraryPoolInversionOffHeap() {
    Collection<SizedResourcePoolImpl<SizedResourcePool>> pools = asList(
      new SizedResourcePoolImpl<>(new ArbitraryType(OFFHEAP.getTierHeight() + 1), 10, MB, false),
      new SizedResourcePoolImpl<>(OFFHEAP, 10, MB, false));
    try {
      validateResourcePools(pools);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("Tiering Inversion: 'Pool {10 MB arbitrary}' is not smaller than 'Pool {10 MB offheap}'"));
    }
  }

  @Test
  public void testArbitraryPoolAmbiguity() {
    Collection<SizedResourcePoolImpl<SizedResourcePool>> pools = asList(
      new SizedResourcePoolImpl<>(new ArbitraryType(OFFHEAP.getTierHeight()), 10, MB, false),
      new SizedResourcePoolImpl<>(OFFHEAP, 10, MB, false));
    try {
      validateResourcePools(pools);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("Tiering Ambiguity: 'Pool {10 MB arbitrary}' has the same tier height as 'Pool {10 MB offheap}'"));
    }
  }

  @Test
  public void testEntryResourceMatch() {
    Collection<SizedResourcePoolImpl<SizedResourcePool>> pools = asList(
      new SizedResourcePoolImpl<>(HEAP, 10, ENTRIES, false),
      new SizedResourcePoolImpl<>(OFFHEAP, 10, ENTRIES, false));
    try {
      validateResourcePools(pools);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("Tiering Inversion: 'Pool {10 entries heap}' is not smaller than 'Pool {10 entries offheap}'"));
    }
  }

  @Test
  public void testEntryResourceInversion() {
    Collection<SizedResourcePoolImpl<SizedResourcePool>> pools = asList(
      new SizedResourcePoolImpl<>(HEAP, 11, ENTRIES, false),
      new SizedResourcePoolImpl<>(OFFHEAP, 10, ENTRIES, false));
    try {
      validateResourcePools(pools);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("Tiering Inversion: 'Pool {11 entries heap}' is not smaller than 'Pool {10 entries offheap}'"));
    }
  }

  @Test
  public void testMemoryResourceEqualUnitMatch() {
    Collection<SizedResourcePoolImpl<SizedResourcePool>> pools = asList(
      new SizedResourcePoolImpl<>(HEAP, 10, MB, false),
      new SizedResourcePoolImpl<>(OFFHEAP, 10, MB, false));
    try {
      validateResourcePools(pools);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("Tiering Inversion: 'Pool {10 MB heap}' is not smaller than 'Pool {10 MB offheap}'"));
    }
  }

  @Test
  public void testMemoryResourceEqualUnitInversion() {
    Collection<SizedResourcePoolImpl<SizedResourcePool>> pools = asList(
      new SizedResourcePoolImpl<>(HEAP, 11, MB, false),
      new SizedResourcePoolImpl<>(OFFHEAP, 10, MB, false));
    try {
      validateResourcePools(pools);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("Tiering Inversion: 'Pool {11 MB heap}' is not smaller than 'Pool {10 MB offheap}'"));
    }
  }

  @Test
  public void testMemoryResourceUnequalUnitMatch() {
    Collection<SizedResourcePoolImpl<SizedResourcePool>> pools = asList(
      new SizedResourcePoolImpl<>(HEAP, 10240, KB, false),
      new SizedResourcePoolImpl<>(OFFHEAP, 10, MB, false));
    try {
      validateResourcePools(pools);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("Tiering Inversion: 'Pool {10240 kB heap}' is not smaller than 'Pool {10 MB offheap}'"));
    }
  }

  @Test
  public void testMemoryResourceUnequalUnitInversion() {
    Collection<SizedResourcePoolImpl<SizedResourcePool>> pools = asList(
      new SizedResourcePoolImpl<>(HEAP, 10241, KB, false),
      new SizedResourcePoolImpl<>(OFFHEAP, 10, MB, false));
    try {
      validateResourcePools(pools);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("Tiering Inversion: 'Pool {10241 kB heap}' is not smaller than 'Pool {10 MB offheap}'"));
    }
  }

  @Test
  public void testAddingNewTierWhileUpdating() {
    ResourcePools existing = resources(new SizedResourcePoolImpl<>(ResourceType.Core.HEAP, 10L, EntryUnit.ENTRIES, false));
    ResourcePools toBeUpdated = resources(new SizedResourcePoolImpl<>(ResourceType.Core.DISK, 10L, MemoryUnit.MB, false));
    try {
      existing.validateAndMerge(toBeUpdated);
      fail();
    } catch (IllegalArgumentException iae) {
      assertThat(iae.getMessage(), Matchers.is("Pools to be updated cannot contain previously undefined resources pools"));
    }
  }

  @Test
  public void testUpdatingOffHeap() {
    ResourcePools existing = resources(new SizedResourcePoolImpl<>(ResourceType.Core.OFFHEAP, 10L, MemoryUnit.MB, false));
    ResourcePools toBeUpdated = resources(new SizedResourcePoolImpl<>(ResourceType.Core.OFFHEAP, 50L, MemoryUnit.MB, false));
    try {
      existing.validateAndMerge(toBeUpdated);
      fail();
    } catch (UnsupportedOperationException uoe) {
      assertThat(uoe.getMessage(), Matchers.is("Updating OFFHEAP resource is not supported"));
    }
  }

  @Test
  public void testUpdatingDisk() {
    ResourcePools existing = resources(new SizedResourcePoolImpl<>(ResourceType.Core.DISK, 10L, MemoryUnit.MB, false));
    ResourcePools toBeUpdated = resources(new SizedResourcePoolImpl<>(ResourceType.Core.DISK, 50L, MemoryUnit.MB, false));
    try {
      existing.validateAndMerge(toBeUpdated);
      fail();
    } catch (UnsupportedOperationException uoe) {
      assertThat(uoe.getMessage(), Matchers.is("Updating DISK resource is not supported"));
    }
  }

  @Test
  public void testUpdateResourceUnitSuccess() {
    ResourcePools existing = resources(
      new SizedResourcePoolImpl<>(ResourceType.Core.HEAP, 200L, MemoryUnit.MB, false),
      new SizedResourcePoolImpl<>(ResourceType.Core.DISK, 4096L, MemoryUnit.MB, false)
    );
    ResourcePools toBeUpdated = resources(
      new SizedResourcePoolImpl<>(ResourceType.Core.HEAP, 2, MemoryUnit.GB, false)
    );

    existing = existing.validateAndMerge(toBeUpdated);
    assertThat(existing.getPoolForResource(ResourceType.Core.HEAP).getSize(), Matchers.is(2L));
    assertThat(existing.getPoolForResource(ResourceType.Core.HEAP).getUnit(), Matchers.<ResourceUnit>is(MemoryUnit.GB));
  }

  @Test
  public void testUpdateResourceUnitFailure() {
    ResourcePools existing = resources(
      new SizedResourcePoolImpl<>(ResourceType.Core.HEAP, 20L, MemoryUnit.MB, false),
      new SizedResourcePoolImpl<>(ResourceType.Core.DISK, 200, MemoryUnit.MB, false)
    );
    ResourcePools toBeUpdated = resources(
      new SizedResourcePoolImpl<>(ResourceType.Core.HEAP, 500, EntryUnit.ENTRIES, false)
    );

    try {
      existing = existing.validateAndMerge(toBeUpdated);
      fail();
    } catch (IllegalArgumentException uoe) {
      assertThat(uoe.getMessage(), Matchers.is("ResourcePool for heap with ResourceUnit 'entries' can not replace 'MB'"));
    }
    assertThat(existing.getPoolForResource(ResourceType.Core.HEAP).getSize(), Matchers.is(20L));
    assertThat(existing.getPoolForResource(ResourceType.Core.HEAP).getUnit(), Matchers.<ResourceUnit>is(MemoryUnit.MB));
  }

  private static ResourcePoolsImpl resources(ResourcePool ... resources) {
    return new ResourcePoolsImpl(stream(resources).collect(toMap(ResourcePool::getType, identity())));
  }
}
