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

package org.ehcache.clustered.server.state;

import org.ehcache.clustered.server.KeySegmentMapper;
import org.ehcache.clustered.server.state.config.EhcacheStateServiceConfig;
import org.junit.Before;
import org.junit.Test;
import org.terracotta.entity.PlatformConfiguration;
import org.terracotta.entity.ServiceProviderCleanupException;
import org.terracotta.entity.ServiceProviderConfiguration;
import org.terracotta.offheapresource.OffHeapResources;
import org.terracotta.offheapresource.OffHeapResourcesProvider;
import org.terracotta.offheapresource.config.MemoryUnit;
import org.terracotta.offheapresource.config.OffheapResourcesType;
import org.terracotta.offheapresource.config.ResourceType;

import java.math.BigInteger;
import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class EhcacheStateServiceProviderTest {

  private static final KeySegmentMapper DEFAULT_MAPPER = new KeySegmentMapper(16);

  private PlatformConfiguration platformConfiguration;
  private ServiceProviderConfiguration serviceProviderConfiguration;

  @Before
  public void setUp() {
    ResourceType resource = new ResourceType();
    resource.setName("primary");
    resource.setUnit(MemoryUnit.MB);
    resource.setValue(BigInteger.valueOf(4L));
    OffheapResourcesType configuration = new OffheapResourcesType();
    configuration.getResource().add(resource);
    OffHeapResources offheapResources = new OffHeapResourcesProvider(configuration);

    platformConfiguration = new PlatformConfiguration() {
      @Override
      public String getServerName() {
        return "Server1";
      }

      @Override
      public <T> Collection<T> getExtendedConfiguration(Class<T> type) {
        if (OffHeapResources.class.isAssignableFrom(type)) {
          return Collections.singletonList(type.cast(offheapResources));
        }
        throw new UnsupportedOperationException("TODO Implement me!");
      }
    };

    serviceProviderConfiguration = mock(ServiceProviderConfiguration.class);
  }

  @Test
  public void testInitialize() {
    EhcacheStateServiceProvider serviceProvider = new EhcacheStateServiceProvider();
    assertTrue(serviceProvider.initialize(serviceProviderConfiguration, platformConfiguration));
  }

  @Test
  public void testGetService() {
    EhcacheStateServiceProvider serviceProvider = new EhcacheStateServiceProvider();
    serviceProvider.initialize(serviceProviderConfiguration, platformConfiguration);

    EhcacheStateService ehcacheStateService = serviceProvider.getService(1L, new EhcacheStateServiceConfig(null, DEFAULT_MAPPER));

    assertNotNull(ehcacheStateService);

    EhcacheStateService sameStateService = serviceProvider.getService(1L, new EhcacheStateServiceConfig(null, DEFAULT_MAPPER));

    assertSame(ehcacheStateService, sameStateService);

    EhcacheStateService anotherStateService = serviceProvider.getService(2L, new EhcacheStateServiceConfig(null, DEFAULT_MAPPER));

    assertNotNull(anotherStateService);
    assertNotSame(ehcacheStateService, anotherStateService);

  }

  @Test
  public void testClear() throws ServiceProviderCleanupException {
    EhcacheStateServiceProvider serviceProvider = new EhcacheStateServiceProvider();
    serviceProvider.initialize(serviceProviderConfiguration, platformConfiguration);

    EhcacheStateService ehcacheStateService = serviceProvider.getService(1L, new EhcacheStateServiceConfig(null, DEFAULT_MAPPER));
    EhcacheStateService anotherStateService = serviceProvider.getService(2L, new EhcacheStateServiceConfig(null, DEFAULT_MAPPER));

    serviceProvider.prepareForSynchronization();

    EhcacheStateService ehcacheStateServiceAfterClear = serviceProvider.getService(1L, new EhcacheStateServiceConfig(null, DEFAULT_MAPPER));
    EhcacheStateService anotherStateServiceAfterClear = serviceProvider.getService(2L, new EhcacheStateServiceConfig(null, DEFAULT_MAPPER));

    assertNotSame(ehcacheStateService, ehcacheStateServiceAfterClear);
    assertNotSame(anotherStateService, anotherStateServiceAfterClear);
  }

}
