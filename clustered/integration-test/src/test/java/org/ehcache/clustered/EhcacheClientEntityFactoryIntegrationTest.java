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
import org.ehcache.clustered.client.internal.EhcacheClientEntityFactory;
import org.ehcache.clustered.common.ServerSideConfiguration;
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

  @ClassRule
  public static Cluster CLUSTER = new BasicExternalCluster(new File("build/cluster"), 1);

  @Test
  public void testCreate() throws Exception {
    Connection client = CLUSTER.newConnection();
    try {
      EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(client);

      assertThat(factory.create("testCreate", new ServerSideConfiguration(0)), notNullValue());
    } finally {
      client.close();
    }
  }

  @Test
  public void testCreateWhenExisting() throws Exception {
    Connection client = CLUSTER.newConnection();
    try {
      EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(client);
      factory.create("testCreateWhenExisting", new ServerSideConfiguration(0)).close();
      try {
        factory.create("testCreateWhenExisting", new ServerSideConfiguration(1));
        fail("Expected EntityAlreadyExistsException");
      } catch (EntityAlreadyExistsException e) {
        //expected
      }
    } finally {
      client.close();
    }
  }

  @Test
  public void testCreateOrRetrieve() throws Exception {
    Connection client = CLUSTER.newConnection();
    try {
      EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(client);
      assertThat(factory.createOrRetrieve("testCreateOrRetrieve", new ServerSideConfiguration(0)), notNullValue());
    } finally {
      client.close();
    }
  }

  @Test
  public void testCreateOrRetrieveWhenExisting() throws Exception {
    Connection client = CLUSTER.newConnection();
    try {
      EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(client);
      factory.create("testCreateOrRetrieveWhenExisting", new ServerSideConfiguration(0)).close();
      assertThat(factory.createOrRetrieve("testCreateOrRetrieveWhenExisting", new ServerSideConfiguration(0)), notNullValue());
    } finally {
      client.close();
    }
  }

  @Test
  public void testCreateOrRetrieveWhenExistingWithBadConfig() throws Exception {
    Connection client = CLUSTER.newConnection();
    try {
      EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(client);
      factory.create("testCreateOrRetrieveWhenExistingWithBadConfig", new ServerSideConfiguration(0)).close();
      try {
        factory.createOrRetrieve("testCreateOrRetrieveWhenExistingWithBadConfig", new ServerSideConfiguration(1));
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) {
        //expected
      }
    } finally {
      client.close();
    }
  }

  @Test
  public void testRetrieveWithGoodConfig() throws Exception {
    Connection client = CLUSTER.newConnection();
    try {
      EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(client);
      factory.create("testRetrieveWithGoodConfig", new ServerSideConfiguration(1)).close();
      assertThat(factory.retrieve("testRetrieveWithGoodConfig", new ServerSideConfiguration(2)), notNullValue());
    } finally {
      client.close();
    }
  }

  @Test
  public void testRetrieveWithBadConfig() throws Exception {
    Connection client = CLUSTER.newConnection();
    try {
      EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(client);
      factory.create("testRetrieveWithBadConfig", new ServerSideConfiguration(1)).close();
      try {
        factory.retrieve("testRetrieveWithBadConfig", new ServerSideConfiguration(3));
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) {
        //expected
      }
    } finally {
      client.close();
    }
  }

  @Test
  public void testRetrieveWhenNotExisting() throws Exception {
    Connection client = CLUSTER.newConnection();
    try {
      EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(client);
      try {
        factory.retrieve("testRetrieveWhenNotExisting", null);
        fail("Expected EntityNotFoundException");
      } catch (EntityNotFoundException e) {
        //expected
      }
    } finally {
      client.close();
    }
  }

  @Test
  @Ignore
  public void testDestroy() throws Exception {
    Connection client = CLUSTER.newConnection();
    try {
      EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(client);
      factory.create("testDestroy", null).close();
      factory.destroy("testDestroy");
    } finally {
      client.close();
    }
  }

  @Test
  @Ignore
  public void testDestroyWhenNotExisting() throws Exception {
    Connection client = CLUSTER.newConnection();
    try {
      EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(client);
      try {
        factory.destroy("testDestroyWhenNotExisting");
        fail("Expected EntityNotFoundException");
      } catch (EntityNotFoundException e) {
        //expected
      }
    } finally {
      client.close();
    }
  }

  @Test
  public void testAbandonLeadershipWhenNotOwning() throws Exception {
    Connection client = CLUSTER.newConnection();
    try {
      EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(client);
      factory.abandonLeadership("testAbandonLeadershipWhenNotOwning");
    } finally {
      client.close();
    }
  }

  @Test
  public void testAcquireLeadershipWhenAlone() throws Exception {
    Connection client = CLUSTER.newConnection();
    try {
      EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(client);
      assertThat(factory.acquireLeadership("testAcquireLeadershipWhenAlone"), is(true));
    } finally {
      client.close();
    }
  }

  @Test
  public void testAcquireLeadershipWhenTaken() throws Exception {
    Connection clientA = CLUSTER.newConnection();
    try {
      EhcacheClientEntityFactory factoryA = new EhcacheClientEntityFactory(clientA);
      assertThat(factoryA.acquireLeadership("testAcquireLeadershipWhenTaken"), is(true));

      Connection clientB = CLUSTER.newConnection();
      try {
        EhcacheClientEntityFactory factoryB = new EhcacheClientEntityFactory(clientB);
        assertThat(factoryB.acquireLeadership("testAcquireLeadershipWhenTaken"), is(false));
      } finally {
        clientB.close();
      }
    } finally {
      clientA.close();
    }
  }

  @Test
  public void testAcquireLeadershipAfterAbandoned() throws Exception {
    Connection clientA = CLUSTER.newConnection();
    try {
      EhcacheClientEntityFactory factoryA = new EhcacheClientEntityFactory(clientA);
      factoryA.acquireLeadership("testAcqruieLeadershipAfterAbandoned");
      factoryA.abandonLeadership("testAcqruieLeadershipAfterAbandoned");

      Connection clientB = CLUSTER.newConnection();
      try {
        EhcacheClientEntityFactory factoryB = new EhcacheClientEntityFactory(clientB);
        assertThat(factoryB.acquireLeadership("testAcqruieLeadershipAfterAbandoned"), is(true));
      } finally {
        clientB.close();
      }
    } finally {
      clientA.close();
    }
  }
}
