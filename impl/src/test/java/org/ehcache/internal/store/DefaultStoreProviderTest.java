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
package org.ehcache.internal.store;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceType;
import org.ehcache.config.ResourceUnit;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class DefaultStoreProviderTest {
  
  @Rule
  public ExpectedException expectedEx = ExpectedException.none();
  
  
  @SuppressWarnings("unchecked")
  @Test
  public void testCreateStoreDiskPoolLessThanHeapPool() {
    expectedEx.expect(IllegalArgumentException.class);
    expectedEx.expectMessage("Disk pool size must be greater than Heap pool size");
    
    ResourcePools rPools = mock(ResourcePools.class);
    when(rPools.getPoolForResource(ResourceType.Core.HEAP)).thenReturn(new TestResourcePoolImpl(ResourceType.Core.HEAP, 10, EntryUnit.ENTRIES, false));
    when(rPools.getPoolForResource(ResourceType.Core.DISK)).thenReturn(new TestResourcePoolImpl(ResourceType.Core.DISK, 5, EntryUnit.ENTRIES, true));
    
    Store.Configuration<String, String> storeConf = mock(Store.Configuration.class);
    when(storeConf.getResourcePools()).thenReturn(rPools);

    DefaultStoreProvider storeProvider = new DefaultStoreProvider();
    storeProvider.createStore(storeConf, mock(ServiceConfiguration.class));
  }

  private class TestResourcePoolImpl implements ResourcePool {
    
    private ResourceType type;
    private long size;
    private ResourceUnit unit;
    private boolean isPersistent;
    
    public TestResourcePoolImpl(ResourceType type, long size, ResourceUnit unit, boolean isPersistent) {
      this.type = type;
      this.size = size; 
      this.unit = unit;
      this.isPersistent = isPersistent;
    }

    @Override
    public ResourceType getType() {
      return type;
    }

    @Override
    public long getSize() {
      return size;
    }

    @Override
    public ResourceUnit getUnit() {
      return unit;
    }

    @Override
    public boolean isPersistent() {
      return isPersistent;
    }
    
  }
  
}
