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

package org.ehcache.clustered.client.internal.service;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionPropertyNames;
import org.terracotta.exception.ConnectionClosedException;

import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.Properties;

import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class ConnectionClosedTest {

  private static final URI CLUSTER_URI = URI.create("terracotta://connection.com:9540/timeout");

  @Parameters
  public static ResourcePoolsBuilder[] data() {
    return new ResourcePoolsBuilder[]{
            ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(clusteredDedicated("primary-server-resource", 2, MemoryUnit.MB)),
            ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .heap(10, EntryUnit.ENTRIES)
                    .with(clusteredDedicated("primary-server-resource", 2, MemoryUnit.MB))
    };
  }

  @Parameter
  public ResourcePoolsBuilder resourcePoolsBuilder;

  @Before
  public void definePassthroughServer() throws Exception {
    UnitTestConnectionService.add(CLUSTER_URI,
            new UnitTestConnectionService.PassthroughServerBuilder()
                    .resource("primary-server-resource", 64, MemoryUnit.MB)
                    .resource("secondary-server-resource", 64, MemoryUnit.MB)
                    .build());
  }

  @After
  public void removePassthroughServer() throws Exception {
    try {
      UnitTestConnectionService.remove(CLUSTER_URI);
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), is("Connection already closed"));
    }
  }

  @Test
  public void testCacheOperationThrowsAfterConnectionClosed() throws Exception {

    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
            newCacheManagerBuilder()
                    .with(cluster(CLUSTER_URI)
                            .timeouts(TimeoutsBuilder
                                    .timeouts()
                                    .connection(Duration.ofSeconds(20))
                                    .build())
                            .autoCreate())
                    .withCache("clustered-cache", newCacheConfigurationBuilder(Long.class, String.class,
                            resourcePoolsBuilder));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true);

    Cache<Long, String> cache = cacheManager.getCache("clustered-cache", Long.class, String.class);

    Collection<Properties> connectionProperties = UnitTestConnectionService.getConnectionProperties(CLUSTER_URI);

    assertThat(connectionProperties.size(), is(1));
    Properties properties = connectionProperties.iterator().next();

    assertThat(properties.getProperty(ConnectionPropertyNames.CONNECTION_TIMEOUT), is("20000"));

    cache.put(1L, "value");
    assertThat(cache.get(1L), is("value"));

    Collection<Connection> connections = UnitTestConnectionService.getConnections(CLUSTER_URI);

    assertThat(connections.size(), is(1));

    Connection connection = connections.iterator().next();

    connection.close();

    try {
      assertThat(cache.get(1L), is("value"));
      fail();
    } catch (Exception e) {
      assertThat(e.getCause().getCause(), instanceOf(ConnectionClosedException.class));
    }

  }


}
