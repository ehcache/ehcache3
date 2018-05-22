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

import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ClusterTierManagerConfiguration;
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

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EhcacheStateServiceProviderTest {

  private static final KeySegmentMapper DEFAULT_MAPPER = new KeySegmentMapper(16);

  private PlatformConfiguration platformConfiguration;
  private ServiceProviderConfiguration serviceProviderConfiguration;
  private ClusterTierManagerConfiguration tierManagerConfiguration;

  @Before
  public void setUp() {
    ResourceType resource = new ResourceType();
    resource.setName("primary");
    resource.setUnit(MemoryUnit.MB);
    resource.setValue(BigInteger.valueOf(4L));
    OffheapResourcesType configuration = new OffheapResourcesType();
    configuration.getResource().add(resource);
    OffHeapResources offheapResources = new OffHeapResourcesProvider(configuration);

    platformConfiguration = mock(PlatformConfiguration.class);
    when(platformConfiguration.getExtendedConfiguration(OffHeapResources.class)).thenReturn(Collections.singletonList(offheapResources));
    serviceProviderConfiguration = mock(ServiceProviderConfiguration.class);

    tierManagerConfiguration = new ClusterTierManagerConfiguration("identifier", new ServerSideConfiguration(emptyMap()));
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

    EhcacheStateService ehcacheStateService = serviceProvider.getService(1L, new EhcacheStateServiceConfig(tierManagerConfiguration, null, DEFAULT_MAPPER));

    assertNotNull(ehcacheStateService);

    EhcacheStateService sameStateService = serviceProvider.getService(1L, new EhcacheStateServiceConfig(tierManagerConfiguration, null, DEFAULT_MAPPER));

    assertSame(ehcacheStateService, sameStateService);

    ClusterTierManagerConfiguration otherConfiguration = new ClusterTierManagerConfiguration("otherIdentifier", new ServerSideConfiguration(emptyMap()));
    EhcacheStateService anotherStateService = serviceProvider.getService(2L, new EhcacheStateServiceConfig(otherConfiguration, null, DEFAULT_MAPPER));

    assertNotNull(anotherStateService);
    assertNotSame(ehcacheStateService, anotherStateService);

  }

  @Test
  public void testDestroyService() throws Exception {
    EhcacheStateServiceProvider serviceProvider = new EhcacheStateServiceProvider();
    serviceProvider.initialize(serviceProviderConfiguration, platformConfiguration);

    EhcacheStateServiceConfig configuration = new EhcacheStateServiceConfig(tierManagerConfiguration, null, DEFAULT_MAPPER);
    EhcacheStateService ehcacheStateService = serviceProvider.getService(1L, configuration);

    ehcacheStateService.destroy();

    assertThat(serviceProvider.getService(1L, configuration), not(sameInstance(ehcacheStateService)));
  }

  @Test
  public void testPrepareForSynchronization() throws ServiceProviderCleanupException {
    EhcacheStateServiceProvider serviceProvider = new EhcacheStateServiceProvider();
    serviceProvider.initialize(serviceProviderConfiguration, platformConfiguration);

    ClusterTierManagerConfiguration otherConfiguration = new ClusterTierManagerConfiguration("otherIdentifier", new ServerSideConfiguration(emptyMap()));

    EhcacheStateService ehcacheStateService = serviceProvider.getService(1L, new EhcacheStateServiceConfig(tierManagerConfiguration, null, DEFAULT_MAPPER));
    EhcacheStateService anotherStateService = serviceProvider.getService(2L, new EhcacheStateServiceConfig(otherConfiguration, null, DEFAULT_MAPPER));

    serviceProvider.prepareForSynchronization();

    EhcacheStateService ehcacheStateServiceAfterClear = serviceProvider.getService(1L, new EhcacheStateServiceConfig(tierManagerConfiguration, null, DEFAULT_MAPPER));
    EhcacheStateService anotherStateServiceAfterClear = serviceProvider.getService(2L, new EhcacheStateServiceConfig(otherConfiguration, null, DEFAULT_MAPPER));

    assertNotSame(ehcacheStateService, ehcacheStateServiceAfterClear);
    assertNotSame(anotherStateService, anotherStateServiceAfterClear);
  }

}
