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
package org.ehcache.clustered.management;

import java.util.concurrent.TimeoutException;
import org.ehcache.clustered.util.BeforeAll;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AfterFailoverManagementServiceTest extends ClusteringManagementServiceTest {

  private final static Logger LOGGER = LoggerFactory.getLogger(AfterFailoverManagementServiceTest.class);

  @BeforeAll
  @Override
  public void beforeAllTests() throws Exception {
    super.beforeAllTests();
    CLUSTER.getCluster().getClusterControl().terminateActive();
    CLUSTER.getCluster().getClusterControl().waitForActive();
    for (int i=0;i<3;i++) {
      try {
        CLUSTER.startCollectingServerEntityStats();
        return;
      } catch (TimeoutException to) {
        LOGGER.info("timeout initiating server entity stats on try count {}", i+1);
      }
    }
    throw new AssertionError("failed to start collecting server entity stats");
  }
}
