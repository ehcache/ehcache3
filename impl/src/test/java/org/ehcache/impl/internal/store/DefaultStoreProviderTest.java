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

package org.ehcache.impl.internal.store;

import org.ehcache.config.ResourceType;
import org.ehcache.core.spi.cache.Store;
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

/**
 * Basic tests for {@link DefaultStoreProvider}.
 */
public class DefaultStoreProviderTest {

  @Test
  public void testRank() throws Exception {
    DefaultStoreProvider provider = new DefaultStoreProvider();

    assertRank(provider, 0, ResourceType.Core.DISK);
    assertRank(provider, 0, ResourceType.Core.HEAP);
    assertRank(provider, 0, ResourceType.Core.OFFHEAP);
    assertRank(provider, 2, ResourceType.Core.OFFHEAP, ResourceType.Core.HEAP);
    assertRank(provider, 2, ResourceType.Core.DISK, ResourceType.Core.HEAP);
    assertRank(provider, 0, ResourceType.Core.DISK, ResourceType.Core.OFFHEAP);
    assertRank(provider, 3, ResourceType.Core.DISK, ResourceType.Core.OFFHEAP, ResourceType.Core.HEAP);

    final ResourceType unmatchedResourceType = new ResourceType() {
      @Override
      public boolean isPersistable() {
        return true;
      }

      @Override
      public boolean requiresSerialization() {
        return true;
      }
    };
    assertRank(provider, 0, unmatchedResourceType);
    assertRank(provider, 0, ResourceType.Core.DISK, ResourceType.Core.OFFHEAP, ResourceType.Core.HEAP, unmatchedResourceType);
  }

  private void assertRank(final Store.Provider provider, final int expectedRank, final ResourceType... resources) {
    assertThat(provider.rank(
        new HashSet<ResourceType>(Arrays.asList(resources)),
        Collections.<ServiceConfiguration<?>>emptyList()),
        is(expectedRank));
  }
}