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

package org.ehcache.clustered.writebehind;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.terracotta.testing.rules.Cluster;

import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

public class BasicClusteredWriteBehindMultiClientTest extends WriteBehindTestBase {

  @ClassRule
  public static Cluster CLUSTER =
    newCluster().in(clusterPath()).withServiceFragment(RESOURCE_CONFIG).build();

  private PersistentCacheManager cacheManager1;
  private PersistentCacheManager cacheManager2;

  private Cache<Long, String> client1;
  private Cache<Long, String> client2;

  @Before
  public void setUp() throws Exception {
    super.setUp();

    CLUSTER.getClusterControl().startAllServers();

    cacheManager1 = createCacheManager(CLUSTER.getConnectionURI());
    cacheManager2 = createCacheManager(CLUSTER.getConnectionURI());

    client1 = cacheManager1.getCache(testName.getMethodName(), Long.class, String.class);
    client2 = cacheManager2.getCache(testName.getMethodName(), Long.class, String.class);
  }

  @After
  public void tearDown() throws Exception {
    if (cacheManager1 != null) {
      cacheManager1.close();
    }

    if (cacheManager2 != null) {
      cacheManager2.close();
      cacheManager2.destroy();
    }
  }

  @Test
  public void testWriteBehindMultipleClients() throws Exception {
    client1.put(KEY, "The one from client1");
    client2.put(KEY, "The one from client2");
    assertValue(client1, "The one from client2");
    client1.remove(KEY);
    client2.put(KEY, "The one from client2");
    client1.put(KEY, "The one from client1");
    assertValue(client2, "The one from client1");
    client2.remove(KEY);
    assertValue(client1, null);
    client1.put(KEY, "The one from client1");
    client1.put(KEY, "The one one from client1");
    client2.remove(KEY);
    client2.put(KEY, "The one from client2");
    client2.put(KEY, "The one one from client2");

    checkValueFromLoaderWriter(client1, "The one one from client2");
  }
}
