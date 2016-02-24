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

package org.ehcache.clustered;

import org.ehcache.Cache;
import org.ehcache.clustered.config.ClusteringServiceConfiguration;
import org.ehcache.config.Configuration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.junit.Test;

import java.net.URI;
import java.util.Collections;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests basic functionality of {@link ClusteredCacheManagerBuilder}.
 *
 * @author Clifford W. Johnson
 */
public class ClusteredCacheManagerBuilderTest {

  @Test(expected = NullPointerException.class)
  public void testNewCacheManagerNullConfig() throws Exception {
    ClusteredCacheManagerBuilder.newCacheManager(null);
  }

  @Test
  public void testNewCacheManager() throws Exception {
    final ClusteringServiceConfiguration serviceConfiguration =
        new ClusteringServiceConfiguration(URI.create("http://localhost:9540"));
    final Configuration configuration = mock(Configuration.class);
    when(configuration.getServiceCreationConfigurations()).thenReturn(
        Collections.<ServiceCreationConfiguration<?>>singleton(serviceConfiguration));
    final ClusteredCacheManager cacheManager = ClusteredCacheManagerBuilder.newCacheManager(configuration);
    assertThat(cacheManager, is(not(nullValue())));
    assertThat(cacheManager, is(instanceOf(ClusteredCacheManager.class)));
  }

  @Test(expected = NullPointerException.class)
  public void testNewCacheManagerBuilderNullBuilder() throws Exception {
    ClusteredCacheManagerBuilder.newCacheManagerBuilder(null);
  }

  @Test
  public void testNewCacheManagerBuilder() throws Exception {
    final ClusteredCacheManagerBuilder builder =
        ClusteredCacheManagerBuilder.newCacheManagerBuilder(CacheManagerBuilder.newCacheManagerBuilder());
    assertThat(builder, is(not(nullValue())));
    assertThat(builder, is(instanceOf(ClusteredCacheManagerBuilder.class)));
  }

  @Test
  public void clusteredCacheManagerWithSimpleCache() throws Exception {
    final CacheManagerBuilder<ClusteredCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(new ClusteringServiceConfiguration(URI.create("http://localhost:9540")))
            .withCache("simple-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class)
                .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .heap(10, EntryUnit.ENTRIES))
                .build());

    assertThat(clusteredCacheManagerBuilder, is(not(nullValue())));
    assertThat(clusteredCacheManagerBuilder, is(instanceOf(ClusteredCacheManagerBuilder.class)));

    final ClusteredCacheManager cacheManager = clusteredCacheManagerBuilder.build(true);

    assertThat(cacheManager, is(not(nullValue())));

    final Cache<Long, String> cache = cacheManager.getCache("simple-cache", Long.class, String.class);
    assertThat(cache, is(not(nullValue())));

    cacheManager.close();
  }
}