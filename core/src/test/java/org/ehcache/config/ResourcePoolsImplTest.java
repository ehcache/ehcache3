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
package org.ehcache.config;

import static java.util.Arrays.asList;
import java.util.Collection;
import static org.ehcache.config.ResourcePoolsImpl.validateResourcePools;
import static org.ehcache.config.ResourceType.Core.HEAP;
import static org.ehcache.config.ResourceType.Core.OFFHEAP;
import static org.ehcache.config.units.EntryUnit.ENTRIES;
import static org.ehcache.config.units.MemoryUnit.KB;
import static org.ehcache.config.units.MemoryUnit.MB;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 *
 * @author cdennis
 */
public class ResourcePoolsImplTest {
  
  @Test
  public void testMismatchedUnits() {
    Collection<ResourcePoolImpl> pools = asList(
            new ResourcePoolImpl(HEAP, Integer.MAX_VALUE, ENTRIES, false),
            new ResourcePoolImpl(OFFHEAP, 10, MB, false));
    validateResourcePools(pools);
  }
  
  @Test
  public void testMatchingEqualUnitsWellTiered() {
    Collection<ResourcePoolImpl> pools = asList(
            new ResourcePoolImpl(HEAP, 9, MB, false),
            new ResourcePoolImpl(OFFHEAP, 10, MB, false));
    validateResourcePools(pools);
  }
  
  @Test
  public void testMatchingUnequalUnitsWellTiered() {
    Collection<ResourcePoolImpl> pools = asList(
            new ResourcePoolImpl(HEAP, 9, MB, false),
            new ResourcePoolImpl(OFFHEAP, 10240, KB, false));
    validateResourcePools(pools);
  }
  
  @Test
  public void testEntryResourceMatch() {
    Collection<ResourcePoolImpl> pools = asList(
            new ResourcePoolImpl(HEAP, 10, ENTRIES, false),
            new ResourcePoolImpl(OFFHEAP, 10, ENTRIES, false));
    try {
      validateResourcePools(pools);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("Tiering Inversion: 'Pool {10 entries heap}' is not smaller than 'Pool {10 entries offheap}'"));
    }
  }

  @Test
  public void testEntryResourceInversion() {
    Collection<ResourcePoolImpl> pools = asList(
            new ResourcePoolImpl(HEAP, 11, ENTRIES, false),
            new ResourcePoolImpl(OFFHEAP, 10, ENTRIES, false));
    try {
      validateResourcePools(pools);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("Tiering Inversion: 'Pool {11 entries heap}' is not smaller than 'Pool {10 entries offheap}'"));
    }
  }
  
  @Test
  public void testMemoryResourceEqualUnitMatch() {
    Collection<ResourcePoolImpl> pools = asList(
            new ResourcePoolImpl(HEAP, 10, MB, false),
            new ResourcePoolImpl(OFFHEAP, 10, MB, false));
    try {
      validateResourcePools(pools);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("Tiering Inversion: 'Pool {10 MB heap}' is not smaller than 'Pool {10 MB offheap}'"));
    }
  }

  @Test
  public void testMemoryResourceEqualUnitInversion() {
    Collection<ResourcePoolImpl> pools = asList(
            new ResourcePoolImpl(HEAP, 11, MB, false),
            new ResourcePoolImpl(OFFHEAP, 10, MB, false));
    try {
      validateResourcePools(pools);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("Tiering Inversion: 'Pool {11 MB heap}' is not smaller than 'Pool {10 MB offheap}'"));
    }
  }

  @Test
  public void testMemoryResourceUnequalUnitMatch() {
    Collection<ResourcePoolImpl> pools = asList(
            new ResourcePoolImpl(HEAP, 10240, KB, false),
            new ResourcePoolImpl(OFFHEAP, 10, MB, false));
    try {
      validateResourcePools(pools);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("Tiering Inversion: 'Pool {10240 kB heap}' is not smaller than 'Pool {10 MB offheap}'"));
    }
  }

  @Test
  public void testMemoryResourceUnequalUnitInversion() {
    Collection<ResourcePoolImpl> pools = asList(
            new ResourcePoolImpl(HEAP, 10241, KB, false),
            new ResourcePoolImpl(OFFHEAP, 10, MB, false));
    try {
      validateResourcePools(pools);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("Tiering Inversion: 'Pool {10241 kB heap}' is not smaller than 'Pool {10 MB offheap}'"));
    }
  }
}
