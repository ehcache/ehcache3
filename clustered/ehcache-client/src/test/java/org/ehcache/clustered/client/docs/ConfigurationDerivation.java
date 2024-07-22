/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

import org.ehcache.clustered.client.config.ClusteredResourceType;
import org.ehcache.clustered.client.config.ClusteredStoreConfiguration;
import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.config.Configuration;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.spi.service.ServiceUtils;
import org.junit.Test;

import java.net.URI;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class ConfigurationDerivation {

  @Test
  public void removingServices() {
    Configuration configuration = ConfigurationBuilder.newConfigurationBuilder()
      .withService(ClusteringServiceConfigurationBuilder.cluster(URI.create("terracotta://example.com/myCacheManager")))
      .withCache("cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.heap(1000).with(ClusteredResourcePoolBuilder.clusteredDedicated("offheap", 128, MemoryUnit.MB))))
      .build();

    //tag::removeService[]
    Configuration withoutClustering = configuration.derive()
      .updateCaches(cache -> cache // <1>
        .withoutServices(ClusteredStoreConfiguration.class) // <2>
        .updateResourcePools(existing -> {
          ResourcePoolsBuilder poolsBuilder = ResourcePoolsBuilder.newResourcePoolsBuilder(); // <3>
          for (ResourcePool pool : existing.getResourceTypeSet().stream() // <4>
            .filter(p -> !(p instanceof ClusteredResourceType)) // <5>
            .map(existing::getPoolForResource)
            .toArray(ResourcePool[]::new)) {
            poolsBuilder = poolsBuilder.with(pool); // <6>
          }
          return poolsBuilder.build();
        }))
      .withoutServices(ClusteringServiceConfiguration.class) // <7>
      .build();
    //end::removeService[]

    assertThat(withoutClustering.getServiceCreationConfigurations(), not(hasItem(
      instanceOf(ClusteringServiceConfiguration.class))));
  }

  @Test
  public void updateService() {
    Configuration configuration = ConfigurationBuilder.newConfigurationBuilder()
      .withCache("cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, ResourcePoolsBuilder.heap(10))
        .withService(new ClusteredStoreConfiguration(Consistency.STRONG)))
      .build();

    //tag::updateService[]
    Configuration changedConsistency = configuration.derive()
      .updateCache("cache", cache -> cache.updateServices(
        ClusteredStoreConfiguration.class,
        existing -> Consistency.EVENTUAL)
      )
      .build();
    //end::updateService[]

    assertThat(ServiceUtils.findSingletonAmongst(ClusteredStoreConfiguration.class,
      configuration.getCacheConfigurations().get("cache").getServiceConfigurations()).getConsistency(), is(Consistency.STRONG));

    assertThat(ServiceUtils.findSingletonAmongst(ClusteredStoreConfiguration.class,
      changedConsistency.getCacheConfigurations().get("cache").getServiceConfigurations()).getConsistency(), is(Consistency.EVENTUAL));
  }

}
