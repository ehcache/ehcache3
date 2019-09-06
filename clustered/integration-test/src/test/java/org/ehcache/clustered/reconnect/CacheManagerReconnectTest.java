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
package org.ehcache.clustered.reconnect;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.ClusteredTests;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.impl.internal.resilience.RobustResilienceStrategy;
import org.ehcache.spi.resilience.RecoveryStore;
import org.ehcache.spi.resilience.ResilienceStrategy;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.ClassRule;
import org.junit.Test;
import org.terracotta.testing.rules.Cluster;

import java.io.File;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

public class CacheManagerReconnectTest extends ClusteredTests {
  public static final String RESOURCE_CONFIG =
    "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
      + "<ohr:offheap-resources>"
      + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">64</ohr:resource>"
      + "</ohr:offheap-resources>"
      + "</config>\n"

      + "<service xmlns:lease='http://www.terracotta.org/service/lease'>"
      + "<lease:connection-leasing>"
      + "<lease:lease-length unit='seconds'>30</lease:lease-length>"
      + "</lease:connection-leasing>"
      + "</service>";

  @ClassRule
  public static Cluster CLUSTER = newCluster(1)
    .in(new File("build/cluster"))
    .withServiceFragment(RESOURCE_CONFIG)
    .build();

  @Test
  public void cacheManagerCanReconnect() throws Exception {
    URI connectionURI = CLUSTER.getConnectionURI();

    try (PersistentCacheManager cacheManager = newCacheManagerBuilder()
      .with(cluster(connectionURI.resolve("/crud-cm"))
        .autoCreate(server -> server.defaultServerResource("primary-server-resource")))
      .build(true)) {

      @SuppressWarnings("unchecked")
      ResilienceStrategy<Long, String> resilienceStrategy = spy(new RobustResilienceStrategy<>(mock(RecoveryStore.class)));

      Cache<Long, String> cache = cacheManager.createCache("clustered-cache",
        newCacheConfigurationBuilder(Long.class, String.class, newResourcePoolsBuilder()
          .with(clusteredDedicated("primary-server-resource", 1, MemoryUnit.MB)))
          .withResilienceStrategy(resilienceStrategy)
          .build());

      cache.put(1L, "one");

      CLUSTER.getClusterControl().terminateAllServers();
      CLUSTER.getClusterControl().startAllServers();

      long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
      while (true) {
        assertThat(cache.get(1L), nullValue());
        try {
          verify(resilienceStrategy).getFailure(eq(1L), argThat(cause(hasMessage(is("Cache clustered-cache failed reconnecting to cluster")))));
          break;
        } catch (AssertionError e) {
          if (System.nanoTime() >= end) {
            throw e;
          } else {
            Thread.sleep(100);
          }
        }
      }
    }
  }

  private static <T extends Throwable> Matcher<T> cause(Matcher<? super T> matches) {
    return new TypeSafeMatcher<T>() {
      @Override
      protected boolean matchesSafely(Throwable item) {
        return matches.matches(item.getCause());
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("a throwable whose cause is ").appendDescriptionOf(matches);
      }
    };
  }
}
