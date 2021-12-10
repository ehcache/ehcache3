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
package org.ehcache.clustered.client;

import org.ehcache.CacheManager;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.client.internal.UnitTestConnectionService.PassthroughServerBuilder;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLockClient;
import org.ehcache.clustered.client.service.ClientEntityFactory;
import org.ehcache.clustered.client.service.EntityBusyException;
import org.ehcache.clustered.client.service.EntityService;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.spi.service.ServiceProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.terracotta.exception.EntityAlreadyExistsException;

import java.net.URI;

import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class EntityServiceTest {

  private static final URI CLUSTER_URI = URI.create("terracotta://example.com:9540/EntityServiceTest");

  @Before
  public void definePassthroughServer() throws Exception {
    UnitTestConnectionService.add(CLUSTER_URI,
        new PassthroughServerBuilder()
            .resource("primary-server-resource", 4, MemoryUnit.MB)
            .build());
  }

  @After
  public void removePassthroughServer() throws Exception {
    UnitTestConnectionService.remove(CLUSTER_URI);
  }

  @Test @SuppressWarnings("try")
  public void test() throws Exception {
    ClusteredManagementService clusteredManagementService = new ClusteredManagementService();

    try (CacheManager cacheManager = newCacheManagerBuilder()
        .using(clusteredManagementService)
        .with(cluster(CLUSTER_URI).autoCreate(c -> c))
        .build(true)) {

      assertThat(clusteredManagementService.clientEntityFactory, is(notNullValue()));

      clusteredManagementService.clientEntityFactory.create();

      try {
        clusteredManagementService.clientEntityFactory.create();
        fail();
      } catch (Exception e) {
        assertThat(e, instanceOf(EntityAlreadyExistsException.class));
      }

      VoltronReadWriteLockClient entity = clusteredManagementService.clientEntityFactory.retrieve();
      assertThat(entity, is(notNullValue()));

      try {
        clusteredManagementService.clientEntityFactory.destroy();
        fail();
      } catch (Exception e) {
        assertThat(e, instanceOf(EntityBusyException.class));
      }

      entity.close();

      clusteredManagementService.clientEntityFactory.destroy();
    }
  }

  @ServiceDependencies(EntityService.class)
  public static class ClusteredManagementService implements Service {

    ClientEntityFactory<VoltronReadWriteLockClient, Void> clientEntityFactory;

    @Override
    public void start(ServiceProvider<Service> serviceProvider) {
      clientEntityFactory = serviceProvider.getService(EntityService.class).newClientEntityFactory("my-locker", VoltronReadWriteLockClient.class, 1L, null);
    }

    @Override
    public void stop() {
      clientEntityFactory = null;
    }
  }

}
