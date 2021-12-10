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
package org.ehcache.management.providers.statistics;

import org.ehcache.core.Ehcache;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.core.spi.time.SystemTimeSource;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.impl.internal.statistics.DefaultStatisticsService;
import org.ehcache.management.ManagementRegistryServiceConfiguration;
import org.ehcache.management.providers.CacheBinding;
import org.ehcache.management.providers.ExposedCacheBinding;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Test;
import org.terracotta.management.model.capabilities.context.CapabilityContext;
import org.terracotta.management.model.capabilities.descriptors.Descriptor;
import org.terracotta.management.model.capabilities.descriptors.StatisticDescriptor;
import org.terracotta.management.model.context.Context;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EhcacheStatisticsProviderTest {

  ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
  StatisticsService statisticsService = new DefaultStatisticsService();
  Context cmContext_0 = Context.create("cacheManagerName", "cache-manager-0");
  ManagementRegistryServiceConfiguration cmConfig_0 = new DefaultManagementRegistryConfiguration()
      .setContext(cmContext_0);
  TimeSource timeSource = SystemTimeSource.INSTANCE;

  @After
  public void tearDown() throws Exception {
    executor.shutdown();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDescriptions() throws Exception {
    EhcacheStatisticsProvider ehcacheStatisticsProvider = new EhcacheStatisticsProvider(cmConfig_0, statisticsService, timeSource) {
      @Override
      protected ExposedCacheBinding wrap(CacheBinding cacheBinding) {
        StandardEhcacheStatistics mock = mock(StandardEhcacheStatistics.class);
        Collection<StatisticDescriptor> descriptors = new HashSet<>();
        descriptors.add(new StatisticDescriptor("aCounter", "COUNTER"));
        descriptors.add(new StatisticDescriptor("aDuration", "DURATION"));
        descriptors.add(new StatisticDescriptor("aSampledRate", "RATE"));
        when(mock.getDescriptors()).thenReturn(descriptors);
        return mock;
      }
    };

    ehcacheStatisticsProvider.register(new CacheBinding("cache-0", mock(Ehcache.class)));

    Collection<? extends Descriptor> descriptions = ehcacheStatisticsProvider.getDescriptors();
    assertThat(descriptions.size(), is(3));
    assertThat(descriptions, (Matcher) containsInAnyOrder(
        new StatisticDescriptor("aCounter", "COUNTER"),
        new StatisticDescriptor("aDuration", "DURATION"),
        new StatisticDescriptor("aSampledRate", "RATE")
    ));
  }

  @Test
  public void testCapabilityContext() throws Exception {
    EhcacheStatisticsProvider ehcacheStatisticsProvider = new EhcacheStatisticsProvider(cmConfig_0, statisticsService, timeSource) {
      @Override
      protected ExposedCacheBinding wrap(CacheBinding cacheBinding) {
        return mock(StandardEhcacheStatistics.class);
      }
    };


    ehcacheStatisticsProvider.register(new CacheBinding("cache-0", mock(Ehcache.class)));

    CapabilityContext capabilityContext = ehcacheStatisticsProvider.getCapabilityContext();

    assertThat(capabilityContext.getAttributes().size(), is(2));

    Iterator<CapabilityContext.Attribute> iterator = capabilityContext.getAttributes().iterator();
    CapabilityContext.Attribute next = iterator.next();
    assertThat(next.getName(), equalTo("cacheManagerName"));
    assertThat(next.isRequired(), is(true));
    next = iterator.next();
    assertThat(next.getName(), equalTo("cacheName"));
    assertThat(next.isRequired(), is(true));
  }

  @Test
  public void testCallAction() throws Exception {
    EhcacheStatisticsProvider ehcacheStatisticsProvider = new EhcacheStatisticsProvider(cmConfig_0, statisticsService, timeSource);

    try {
      ehcacheStatisticsProvider.callAction(null, null);
      fail("expected UnsupportedOperationException");
    } catch (UnsupportedOperationException uoe) {
      // expected
    }

  }


}
