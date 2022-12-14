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

package org.ehcache.transactions.xa.internal;

import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceType;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 */
public class UnSupportedResourceTypeTest {

  @Test
  public void testUnSupportedResourceType() {
    XAStore.Provider provider = new XAStore.Provider();

    @SuppressWarnings("unchecked")
    Store.Configuration<Object, Object> configuration = mock(Store.Configuration.class);

    ResourcePools resourcePools = mock(ResourcePools.class);
    Set<ResourceType<?>> resourceTypes = Collections.singleton(new TestResourceType());

    when(configuration.getResourcePools()).thenReturn(resourcePools);
    when(resourcePools.getResourceTypeSet()).thenReturn(resourceTypes);

    try {
      provider.createStore(configuration, (ServiceConfiguration<?>) null);
      fail("IllegalStateException expected");
    } catch (IllegalStateException e) {

    }

  }


  private static class TestResourceType implements ResourceType<ResourcePool> {

    @Override
    public Class<ResourcePool> getResourcePoolClass() {
      return ResourcePool.class;
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
      return 0;
    }
  }
}
