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

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.ehcache.clustered.client.internal.EhcacheClientEntityFactory;
import org.ehcache.clustered.client.internal.EhcacheEntityNotFoundException;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.ServerSideConfiguration.Pool;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.terracotta.connection.Connection;
import org.terracotta.exception.EntityAlreadyExistsException;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.testing.rules.BasicExternalCluster;
import org.terracotta.testing.rules.Cluster;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class EhcacheClientEntityFactoryIntegrationTest {

  private static final Map<String, Pool> EMPTY_RESOURCE_MAP = Collections.emptyMap();

  private static final String RESOURCE_CONFIG =
      "<service xmlns:ohr='http://www.terracotta.org/config/offheap-resource' id=\"resources\">"
          + "<ohr:offheap-resources>"
          + "<ohr:resource name=\"primary\" unit=\"MB\">64</ohr:resource>"
          + "</ohr:offheap-resources>" +
          "</service>\n";

  @ClassRule
  public static Cluster CLUSTER =
      new BasicExternalCluster(new File("build/cluster"), 1, Collections.<File>emptyList(), "", RESOURCE_CONFIG);
  private static Connection CONNECTION;

  @BeforeClass
  public static void waitForActive() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
    CONNECTION = CLUSTER.newConnection();
  }

  @AfterClass
  public static void closeConnection() throws IOException {
    CONNECTION.close();
  }

  @Test
  public void testCreate() throws Exception {
    EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(CONNECTION);

    factory.create("testCreate", new ServerSideConfiguration(null, EMPTY_RESOURCE_MAP));
  }

  @Test
  public void testCreateWhenExisting() throws Exception {
    EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(CONNECTION);
    factory.create("testCreateWhenExisting", new ServerSideConfiguration(null, EMPTY_RESOURCE_MAP));
    try {
      factory.create("testCreateWhenExisting",
          new ServerSideConfiguration(null, Collections.singletonMap("foo", new Pool("bar", 42L))));
      fail("Expected EntityAlreadyExistsException");
    } catch (EntityAlreadyExistsException e) {
      //expected
    }
  }

  @Test
  public void testRetrieveWithGoodConfig() throws Exception {
    EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(CONNECTION);
    factory.create("testRetrieveWithGoodConfig",
        new ServerSideConfiguration(null, Collections.singletonMap("foo", new Pool("primary", 43L))));
    assertThat(factory.retrieve("testRetrieveWithGoodConfig",
        new ServerSideConfiguration(null, Collections.singletonMap("foo", new Pool("primary", 43L)))), notNullValue());
  }

  @Test
  public void testRetrieveWithBadConfig() throws Exception {
    EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(CONNECTION);
    factory.create("testRetrieveWithBadConfig",
        new ServerSideConfiguration(null, Collections.singletonMap("foo", new Pool("primary", 42L))));
    try {
      factory.retrieve("testRetrieveWithBadConfig",
          new ServerSideConfiguration(null, Collections.singletonMap("bar", new Pool("primary", 42L))));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

  @Test
  public void testRetrieveWhenNotExisting() throws Exception {
    EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(CONNECTION);
    try {
      factory.retrieve("testRetrieveWhenNotExisting", null);
      fail("Expected EntityNotFoundException");
    } catch (EntityNotFoundException e) {
      //expected
    }
  }

  @Test
  @Ignore
  public void testDestroy() throws Exception {
    EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(CONNECTION);
    factory.create("testDestroy", null);
    factory.destroy("testDestroy");
  }

  @Test
  @Ignore
  public void testDestroyWhenNotExisting() throws Exception {
    EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(CONNECTION);
    try {
      factory.destroy("testDestroyWhenNotExisting");
      fail("Expected EntityNotFoundException");
    } catch (EhcacheEntityNotFoundException e) {
      //expected
    }
  }

  @Test
  public void testAbandonLeadershipWhenNotOwning() throws Exception {
    EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(CONNECTION);
    factory.abandonLeadership("testAbandonLeadershipWhenNotOwning");
  }

  @Test
  public void testAcquireLeadershipWhenAlone() throws Exception {
    EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(CONNECTION);
    assertThat(factory.acquireLeadership("testAcquireLeadershipWhenAlone"), is(true));
  }

  @Test
  public void testAcquireLeadershipWhenTaken() throws Exception {
    EhcacheClientEntityFactory factoryA = new EhcacheClientEntityFactory(CONNECTION);
    assertThat(factoryA.acquireLeadership("testAcquireLeadershipWhenTaken"), is(true));

    Connection clientB = CLUSTER.newConnection();
    try {
      EhcacheClientEntityFactory factoryB = new EhcacheClientEntityFactory(clientB);
      assertThat(factoryB.acquireLeadership("testAcquireLeadershipWhenTaken"), is(false));
    } finally {
      clientB.close();
    }
  }

  @Test
  public void testAcquireLeadershipAfterAbandoned() throws Exception {
    EhcacheClientEntityFactory factoryA = new EhcacheClientEntityFactory(CONNECTION);
    factoryA.acquireLeadership("testAcquireLeadershipAfterAbandoned");
    factoryA.abandonLeadership("testAcquireLeadershipAfterAbandoned");

    Connection clientB = CLUSTER.newConnection();
    try {
      EhcacheClientEntityFactory factoryB = new EhcacheClientEntityFactory(clientB);
      assertThat(factoryB.acquireLeadership("testAcquireLeadershipAfterAbandoned"), is(true));
    } finally {
      clientB.close();
    }
  }
}
