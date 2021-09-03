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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.terracotta.testing.rules.Cluster;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

public class BasicClusteredWriteBehindTest extends WriteBehindTestBase {

  @Rule
  public Timeout timeout = new Timeout(1, TimeUnit.MINUTES);

  private boolean doThreadDump = true;

  @ClassRule
  public static Cluster CLUSTER =
      newCluster().in(new File("build/cluster")).withServiceFragment(RESOURCE_CONFIG).build();

  private PersistentCacheManager cacheManager;
  private Cache<Long, String> cache;

  @Before
  public void setUp() throws Exception {
    super.setUp();

    CLUSTER.getClusterControl().startAllServers();
    CLUSTER.getClusterControl().waitForActive();

    cacheManager = createCacheManager(CLUSTER.getConnectionURI());
    cache = cacheManager.getCache(CACHE_NAME, Long.class, String.class);
  }

  @After
  public void tearDown() throws Exception {
    if (doThreadDump) {
      System.out.println("Performing thread dump");
      ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
      ThreadInfo[] threadInfos = threadMXBean.dumpAllThreads(true, true);
      Arrays.stream(threadInfos).forEach(System.out::println);
    }

    if (cacheManager != null) {
      cacheManager.close();
      cacheManager.destroy();
    }
  }

  @Test
  public void testBasicClusteredWriteBehind() throws Exception {
    for (int i = 0; i < 10; i++) {
      cache.put(KEY, String.valueOf(i));
    }

    assertValue(cache, "9");

    checkValueFromLoaderWriter(cache, "9");

    doThreadDump = false;
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
    cache.remove(KEY);

    checkValueFromLoaderWriter(cache, null);

    doThreadDump = false;
  }

  @Test
  public void testClusteredWriteBehindLoading() throws Exception {
    cache.put(KEY, "Some value");
    checkValueFromLoaderWriter(cache, "Some value");
    cache.clear();

    assertThat(cache.get(KEY), notNullValue());

    doThreadDump = false;
  }
}
