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
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

/**
 * @author Clifford W. Johnson
 */
public class DefaultStoreProviderTest {

  @Test
  public void testRank() throws Exception {
    DefaultStoreProvider provider = new DefaultStoreProvider();

    assertThat(provider.rank(
        Collections.<ResourceType>singleton(ResourceType.Core.DISK), Collections.<ServiceConfiguration<?>>emptyList()),
        is(greaterThanOrEqualTo(1)));
    assertThat(provider.rank(
        Collections.<ResourceType>singleton(ResourceType.Core.HEAP), Collections.<ServiceConfiguration<?>>emptyList()),
        is(greaterThanOrEqualTo(1)));
    assertThat(provider.rank(
        Collections.<ResourceType>singleton(ResourceType.Core.OFFHEAP), Collections.<ServiceConfiguration<?>>emptyList()),
        is(greaterThanOrEqualTo(1)));
    assertThat(provider.rank(
        new HashSet<ResourceType>(
            Arrays.asList(ResourceType.Core.DISK, ResourceType.Core.OFFHEAP, ResourceType.Core.HEAP)),
        Collections.<ServiceConfiguration<?>>emptyList()),
        is(greaterThanOrEqualTo(1)));

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
    assertThat(provider.rank(
        Collections.singleton(unmatchedResourceType), Collections.<ServiceConfiguration<?>>emptyList()),
        is(0));

    assertThat(provider.rank(
        new HashSet<ResourceType>(
            Arrays.asList(ResourceType.Core.DISK, ResourceType.Core.OFFHEAP, ResourceType.Core.HEAP, unmatchedResourceType)),
        Collections.<ServiceConfiguration<?>>emptyList()),
        is(0));
  }
}