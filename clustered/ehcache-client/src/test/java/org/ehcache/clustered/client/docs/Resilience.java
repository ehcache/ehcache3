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

package org.ehcache.clustered.client.docs;

import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.Timeouts;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.time.Duration;

public class Resilience {

  @Before
  public void resetPassthroughServer() throws Exception {
    UnitTestConnectionService.add("terracotta://localhost/my-application",
      new UnitTestConnectionService.PassthroughServerBuilder()
        .resource("primary-server-resource", 128, MemoryUnit.MB)
        .resource("secondary-server-resource", 96, MemoryUnit.MB)
        .build());
  }

  @After
  public void removePassthroughServer() throws Exception {
    UnitTestConnectionService.remove("terracotta://localhost/my-application");
  }

  @Test
  public void clusteredCacheManagerExample() throws Exception {
    // tag::timeoutsExample[]
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
      CacheManagerBuilder.newCacheManagerBuilder()
        .with(ClusteringServiceConfigurationBuilder.cluster(URI.create("terracotta://localhost/my-application"))
          .timeouts(TimeoutsBuilder.timeouts() // <1>
            .read(Duration.ofSeconds(10)) // <2>
            .write(Timeouts.DEFAULT_OPERATION_TIMEOUT) // <3>
            .connection(Timeouts.INFINITE_TIMEOUT)) // <4>
          .autoCreate(c -> c));
    // end::timeoutsExample[]
  }
}
