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

import org.ehcache.Ehcache;
import org.ehcache.management.config.StatisticsProviderConfiguration;
import org.ehcache.management.registry.DefaultManagementRegistry;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.terracotta.management.capabilities.context.CapabilityContext;
import org.terracotta.management.capabilities.descriptors.Descriptor;
import org.terracotta.management.capabilities.descriptors.StatisticDescriptor;
import org.terracotta.management.stats.StatisticType;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Ludovic Orban
 */
public class EhcacheStatisticsProviderTest {


  @Test
  public void testDescriptions() throws Exception {
    StatisticsProviderConfiguration statisticsProviderConfiguration = DefaultManagementRegistry.DEFAULT_EHCACHE_STATISTICS_PROVIDER_CONFIGURATION;
    ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    EhcacheStatisticsProvider ehcacheStatisticsProvider = new EhcacheStatisticsProvider(statisticsProviderConfiguration, executor) {
      @Override
      EhcacheStatistics createStatistics(Ehcache ehcache) {
        EhcacheStatistics mock = mock(EhcacheStatistics.class);
        Set<Descriptor> descriptors = new HashSet<Descriptor>();
        descriptors.add(new StatisticDescriptor("aCounter", StatisticType.COUNTER));
        descriptors.add(new StatisticDescriptor("aDuration", StatisticType.DURATION));
        descriptors.add(new StatisticDescriptor("aSampledRate", StatisticType.SAMPLED_RATE));
        when(mock.capabilities()).thenReturn(descriptors);
        return mock;
      }
    };

    ehcacheStatisticsProvider.register(mock(Ehcache.class));

    Set<Descriptor> descriptions = ehcacheStatisticsProvider.descriptions();
    assertThat(descriptions.size(), is(3));
    assertThat(descriptions, (Matcher) containsInAnyOrder(
        new StatisticDescriptor("aCounter", StatisticType.COUNTER),
        new StatisticDescriptor("aDuration", StatisticType.DURATION),
        new StatisticDescriptor("aSampledRate", StatisticType.SAMPLED_RATE)
    ));

    executor.shutdown();
  }

  @Test
  public void testCapabilityContext() throws Exception {
    StatisticsProviderConfiguration statisticsProviderConfiguration = DefaultManagementRegistry.DEFAULT_EHCACHE_STATISTICS_PROVIDER_CONFIGURATION;
    ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    EhcacheStatisticsProvider ehcacheStatisticsProvider = new EhcacheStatisticsProvider(statisticsProviderConfiguration, executor) {
      @Override
      EhcacheStatistics createStatistics(Ehcache ehcache) {
        return mock(EhcacheStatistics.class);
      }
    };


    ehcacheStatisticsProvider.register(mock(Ehcache.class));

    CapabilityContext capabilityContext = ehcacheStatisticsProvider.capabilityContext();

    assertThat(capabilityContext.getAttributes().size(), is(2));

    Iterator<CapabilityContext.Attribute> iterator = capabilityContext.getAttributes().iterator();
    CapabilityContext.Attribute next = iterator.next();
    assertThat(next.getName(), equalTo("cacheManagerName"));
    assertThat(next.isRequired(), is(true));
    next = iterator.next();
    assertThat(next.getName(), equalTo("cacheName"));
    assertThat(next.isRequired(), is(true));

    executor.shutdown();
  }

  @Test
  public void testCallAction() throws Exception {
    StatisticsProviderConfiguration statisticsProviderConfiguration = DefaultManagementRegistry.DEFAULT_EHCACHE_STATISTICS_PROVIDER_CONFIGURATION;
    ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    EhcacheStatisticsProvider ehcacheStatisticsProvider = new EhcacheStatisticsProvider(statisticsProviderConfiguration, executor);

    try {
      ehcacheStatisticsProvider.callAction(null, null, null, null);
      fail("expected UnsupportedOperationException");
    } catch (UnsupportedOperationException uoe) {
      // expected
    }

    executor.shutdown();
  }


}
