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

import org.ehcache.clustered.client.config.ClusteredResourcePool;
import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.internal.EhcacheClientEntity;
import org.ehcache.clustered.client.internal.service.ClusteredTierCreationException;
import org.ehcache.clustered.client.internal.service.ClusteringServiceFactory;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.exceptions.InvalidStoreException;
import org.ehcache.impl.serialization.CompactJavaSerializer;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.terracotta.testing.rules.BasicExternalCluster;
import org.terracotta.testing.rules.Cluster;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Collections;

import static org.ehcache.config.units.MemoryUnit.MB;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class BasicCacheLifeCyclePassiveReplicationTest {

  private static final String RESOURCE_CONFIG =
      "<service xmlns:ohr='http://www.terracotta.org/config/offheap-resource' id=\"resources\">"
      + "<ohr:offheap-resources>"
      + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">64</ohr:resource>"
      + "</ohr:offheap-resources>" +
      "</service>\n";

  @ClassRule
  public static Cluster CLUSTER =
      new BasicExternalCluster(new File("build/cluster"), 2, Collections.<File>emptyList(), "", RESOURCE_CONFIG, "");


  @BeforeClass
  public static void waitForActive() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
  }

  @Test
  public void testCreateCacheReplication() throws Exception {

    ClusteringServiceConfiguration configuration =
        ClusteringServiceConfigurationBuilder.cluster(CLUSTER.getConnectionURI())
            .autoCreate()
            .build();

    ClusteringService service = new ClusteringServiceFactory().create(configuration);

    service.start(null);

    EhcacheClientEntity clientEntity = getEntity(service);

    clientEntity.createCache("testCache", getServerStoreConfiguration("primary-server-resource"));

    CLUSTER.getClusterControl().terminateActive();

    try {
      clientEntity.createCache("testCache", getServerStoreConfiguration("primary-server-resource"));
      fail("ClusteredTierCreationException Expected.");
    } catch (ClusteredTierCreationException e) {
      assertThat(e.getCause(), instanceOf(InvalidStoreException.class));
      assertThat(e.getCause().getMessage(), is("Clustered tier 'testCache' already exists"));
    }

    service.stop();

  }

  private static EhcacheClientEntity getEntity(ClusteringService clusteringService) throws NoSuchFieldException, IllegalAccessException {
    Field entity = clusteringService.getClass().getDeclaredField("entity");
    entity.setAccessible(true);
    return (EhcacheClientEntity)entity.get(clusteringService);
  }

  private static ServerStoreConfiguration getServerStoreConfiguration(String resourceName) {
    ClusteredResourcePool resourcePool = ClusteredResourcePoolBuilder.clusteredDedicated(resourceName, 8, MB);
    return new ServerStoreConfiguration(resourcePool.getPoolAllocation(),
        String.class.getName(), String.class.getName(), null, null, CompactJavaSerializer.class.getName(), CompactJavaSerializer.class
        .getName(), Consistency.STRONG);
  }


}
