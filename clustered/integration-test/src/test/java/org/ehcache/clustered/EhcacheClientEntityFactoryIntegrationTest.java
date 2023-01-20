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

import org.ehcache.clustered.client.internal.EhcacheClientEntityFactory;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.junit.Ignore;
import org.junit.Test;
import org.terracotta.connection.Connection;
import org.terracotta.exception.EntityAlreadyExistsException;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.passthrough.PassthroughServer;

import static org.ehcache.clustered.TestUtils.createServer;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class EhcacheClientEntityFactoryIntegrationTest {

  @Test
  public void testCreate() throws Exception {
    PassthroughServer server = createServer();
    try {
      Connection client = server.connectNewClient();
      EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(client);

      assertThat(factory.create("test", new ServerSideConfiguration(0)), notNullValue());
    } finally {
      server.stop();
    }
  }

  @Test
  public void testCreateWhenExisting() throws Exception {
    PassthroughServer server = createServer();
    try {
      Connection client = server.connectNewClient();
      EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(client);
      factory.create("test", new ServerSideConfiguration(0)).close();
      try {
        factory.create("test", new ServerSideConfiguration(1));
        fail("Expected EntityAlreadyExistsException");
      } catch (EntityAlreadyExistsException e) {
        //expected
      }
    } finally {
      server.stop();
    }
  }

  @Test
  public void testCreateOrRetrieve() throws Exception {
    PassthroughServer server = createServer();
    try {
      Connection client = server.connectNewClient();
      EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(client);
      assertThat(factory.createOrRetrieve("test", new ServerSideConfiguration(0)), notNullValue());
    } finally {
      server.stop();
    }
  }

  @Test
  public void testCreateOrRetrieveWhenExisting() throws Exception {
    PassthroughServer server = createServer();
    try {
      Connection client = server.connectNewClient();
      EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(client);
      factory.create("test", new ServerSideConfiguration(0)).close();
      assertThat(factory.createOrRetrieve("test", new ServerSideConfiguration(0)), notNullValue());
    } finally {
      server.stop();
    }
  }

  @Test
  public void testCreateOrRetrieveWhenExistingWithBadConfig() throws Exception {
    PassthroughServer server = createServer();
    try {
      Connection client = server.connectNewClient();
      EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(client);
      factory.create("test", new ServerSideConfiguration(0)).close();
      try {
        factory.createOrRetrieve("test", new ServerSideConfiguration(1));
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) {
        //expected
      }
    } finally {
      server.stop();
    }
  }

  @Test
  public void testRetrieveWithGoodConfig() throws Exception {
    PassthroughServer server = createServer();
    try {
      Connection client = server.connectNewClient();
      EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(client);
      factory.create("test", new ServerSideConfiguration(1)).close();
      assertThat(factory.retrieve("test", new ServerSideConfiguration(2)), notNullValue());
    } finally {
      server.stop();
    }
  }

  @Test
  public void testRetrieveWithBadConfig() throws Exception {
    PassthroughServer server = createServer();
    try {
      Connection client = server.connectNewClient();
      EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(client);
      factory.create("test", new ServerSideConfiguration(1)).close();
      try {
        factory.retrieve("test", new ServerSideConfiguration(3));
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) {
        //expected
      }
    } finally {
      server.stop();
    }
  }

  @Test
  public void testRetrieveWhenNotExisting() throws Exception {
    PassthroughServer server = createServer();
    try {
      Connection client = server.connectNewClient();
      EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(client);
      try {
        factory.retrieve("test", null);
        fail("Expected EntityNotFoundException");
      } catch (EntityNotFoundException e) {
        //expected
      }
    } finally {
      server.stop();
    }
  }

  @Test
  @Ignore
  public void testDestroy() throws Exception {
    PassthroughServer server = createServer();
    try {
      Connection client = server.connectNewClient();
      EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(client);
      factory.create("test", null).close();
      factory.destroy("test");
    } finally {
      server.stop();
    }
  }

  @Test
  @Ignore
  public void testDestroyWhenNotExisting() throws Exception {
    PassthroughServer server = createServer();
    try {
      Connection client = server.connectNewClient();
      EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(client);
      try {
        factory.destroy("test");
        fail("Expected EntityNotFoundException");
      } catch (EntityNotFoundException e) {
        //expected
      }
    } finally {
      server.stop();
    }
  }

  @Test
  public void testAbandonLeadershipWhenNotOwning() throws Exception {
    PassthroughServer server = createServer();
    try {
      Connection client = server.connectNewClient();
      EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(client);
      factory.abandonLeadership("test");
    } finally {
      server.stop();
    }
  }

  @Test
  public void testAcquireLeadershipWhenAlone() throws Exception {
    PassthroughServer server = createServer();
    try {
      Connection client = server.connectNewClient();
      EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(client);
      assertThat(factory.acquireLeadership("test"), is(true));
    } finally {
      server.stop();
    }
  }

  @Test
  public void testAcquireLeadershipWhenTaken() throws Exception {
    PassthroughServer server = createServer();
    try {
      Connection clientA = server.connectNewClient();
      EhcacheClientEntityFactory factoryA = new EhcacheClientEntityFactory(clientA);
      assertThat(factoryA.acquireLeadership("test"), is(true));

      Connection clientB = server.connectNewClient();
      EhcacheClientEntityFactory factoryB = new EhcacheClientEntityFactory(clientB);
      assertThat(factoryB.acquireLeadership("test"), is(false));
    } finally {
      server.stop();
    }
  }

  @Test
  public void testAcqruieLeadershipAfterAbandoned() throws Exception {
    PassthroughServer server = createServer();
    try {
      Connection clientA = server.connectNewClient();
      EhcacheClientEntityFactory factoryA = new EhcacheClientEntityFactory(clientA);
      factoryA.acquireLeadership("test");
      factoryA.abandonLeadership("test");

      Connection clientB = server.connectNewClient();
      EhcacheClientEntityFactory factoryB = new EhcacheClientEntityFactory(clientB);
      assertThat(factoryB.acquireLeadership("test"), is(true));
    } finally {
      server.stop();
    }
  }
}
