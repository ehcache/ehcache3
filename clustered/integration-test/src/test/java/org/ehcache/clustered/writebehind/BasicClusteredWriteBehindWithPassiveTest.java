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
import org.ehcache.clustered.util.ParallelTestCluster;
import org.ehcache.clustered.util.runners.Parallel;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

@RunWith(Parallel.class)
public class BasicClusteredWriteBehindWithPassiveTest extends WriteBehindTestBase {

  @ClassRule @Rule
  public static final ParallelTestCluster CLUSTER = new ParallelTestCluster(
      newCluster(2).in(clusterPath()).withServiceFragment(RESOURCE_CONFIG).build()
  );

  private PersistentCacheManager cacheManager;
  private Cache<Long, String> cache;

  @Before
  public void setUp() throws Exception {
    super.setUp();

    CLUSTER.getClusterControl().startAllServers();

    cacheManager = createCacheManager(CLUSTER.getConnectionURI());
    cache = cacheManager.getCache(testName.getMethodName(), Long.class, String.class);
  }

  @After
  public void tearDown() throws Exception {
    if (cacheManager != null) {
      cacheManager.close();
    }
  }

  @Test
  public void testBasicClusteredWriteBehind() throws Exception {
    for (int i = 0; i < 10; i++) {
      cache.put(KEY, String.valueOf(i));
    }

    assertValue(cache, "9");

    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
    CLUSTER.getClusterControl().terminateActive();

    assertValue(cache, "9");
    checkValueFromLoaderWriter(cache, String.valueOf(9));
  }

  @Test
  public void testClusteredWriteBehindCAS() throws Exception {
    cache.putIfAbsent(KEY, "First value");
    assertValue(cache,"First value");
    cache.putIfAbsent(KEY, "Second value");
    assertValue(cache, "First value");
    cache.put(KEY, "First value again");
    assertValue(cache, "First value again");
    cache.replace(KEY, "Replaced First value");
    assertValue(cache, "Replaced First value");
    cache.replace(KEY, "Replaced First value", "Replaced First value again");
    assertValue(cache, "Replaced First value again");
    cache.replace(KEY, "Replaced First", "Tried Replacing First value again");
    assertValue(cache, "Replaced First value again");
    cache.remove(KEY, "Replaced First value again");
    assertValue(cache, null);
    cache.replace(KEY, "Trying to replace value");
    assertValue(cache, null);
    cache.put(KEY, "new value");
    assertValue(cache, "new value");

    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
    CLUSTER.getClusterControl().terminateActive();

    assertValue(cache, "new value");
    checkValueFromLoaderWriter(cache,"new value");
  }
}
