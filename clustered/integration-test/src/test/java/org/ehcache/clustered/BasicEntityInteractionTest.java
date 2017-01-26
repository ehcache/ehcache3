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
import java.util.Collections;
import org.ehcache.clustered.client.internal.EhcacheClientEntity;
import org.ehcache.clustered.client.internal.InternalEhcacheClientEntity;
import org.ehcache.clustered.common.EhcacheEntityVersion;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ClusteredTierManagerConfiguration;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.terracotta.connection.Connection;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.exception.EntityAlreadyExistsException;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.testing.rules.BasicExternalCluster;
import org.terracotta.testing.rules.Cluster;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class BasicEntityInteractionTest {

  private static final String RESOURCE_CONFIG =
      "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
      + "<ohr:offheap-resources>"
      + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">4</ohr:resource>"
      + "</ohr:offheap-resources>" +
      "</config>\n";

  @ClassRule
  public static Cluster CLUSTER = new BasicExternalCluster(new File("build/cluster"), 1, Collections.<File>emptyList(), "", RESOURCE_CONFIG, "");
  private ClusteredTierManagerConfiguration blankConfiguration = new ClusteredTierManagerConfiguration("identifier", new ServerSideConfiguration(emptyMap()));

  @BeforeClass
  public static void waitForActive() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
  }

  @Rule
  public TestName testName= new TestName();

  @Test
  public void testAbsentEntityRetrievalFails() throws Throwable {
    Connection client = CLUSTER.newConnection();
    try {
      EntityRef<InternalEhcacheClientEntity, ClusteredTierManagerConfiguration> ref = getEntityRef(client);

      try {
        ref.fetchEntity();
        fail("Expected EntityNotFoundException");
      } catch (EntityNotFoundException e) {
        //expected
      }
    } finally {
      client.close();
    }
  }

  @Test
  public void testAbsentEntityCreationSucceeds() throws Throwable {
    Connection client = CLUSTER.newConnection();
    try {
      EntityRef<InternalEhcacheClientEntity, ClusteredTierManagerConfiguration> ref = getEntityRef(client);

      ref.create(blankConfiguration);
      assertThat(ref.fetchEntity(), not(Matchers.nullValue()));
    } finally {
      client.close();
    }
  }

  @Test
  public void testPresentEntityCreationFails() throws Throwable {
    Connection client = CLUSTER.newConnection();
    try {
      EntityRef<InternalEhcacheClientEntity, ClusteredTierManagerConfiguration> ref = getEntityRef(client);

      ref.create(blankConfiguration);
      try {
        try {
          ref.create(blankConfiguration);
          fail("Expected EntityAlreadyExistsException");
        } catch (EntityAlreadyExistsException e) {
          //expected
        }

        ClusteredTierManagerConfiguration otherConfiguration = new ClusteredTierManagerConfiguration("different", new ServerSideConfiguration(emptyMap()));
        try {
          ref.create(otherConfiguration);
          fail("Expected EntityAlreadyExistsException");
        } catch (EntityAlreadyExistsException e) {
          //expected
        }
      } finally {
        ref.destroy();
      }
    } finally {
      client.close();
    }
  }

  @Test
  public void testAbsentEntityDestroyFails() throws Throwable {
    Connection client = CLUSTER.newConnection();
    try {
      EntityRef<InternalEhcacheClientEntity, ClusteredTierManagerConfiguration> ref = getEntityRef(client);

      try {
        ref.destroy();
        fail("Expected EntityNotFoundException");
      } catch (EntityNotFoundException e) {
        //expected
      }
    } finally {
      client.close();
    }
  }

  @Test
  public void testPresentEntityDestroySucceeds() throws Throwable {
    Connection client = CLUSTER.newConnection();
    try {
      EntityRef<InternalEhcacheClientEntity, ClusteredTierManagerConfiguration> ref = getEntityRef(client);

      ref.create(blankConfiguration);
      ref.destroy();

      try {
        ref.fetchEntity();
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
  public void testPresentEntityDestroyBlockedByHeldReferenceSucceeds() throws Throwable {
    Connection client = CLUSTER.newConnection();
    try {
      EntityRef<InternalEhcacheClientEntity, ClusteredTierManagerConfiguration> ref = getEntityRef(client);

      ref.create(blankConfiguration);

      EhcacheClientEntity entity = ref.fetchEntity();
      try {
        ref.destroy();
      } finally {
        entity.close();
      }
    } finally {
      client.close();
    }
  }

  @Test
  public void testPresentEntityDestroyNotBlockedByReleasedReferenceSucceeds() throws Throwable {
    Connection client = CLUSTER.newConnection();
    try {
      EntityRef<InternalEhcacheClientEntity, ClusteredTierManagerConfiguration> ref = getEntityRef(client);

      ref.create(blankConfiguration);
      ref.fetchEntity().close();
      ref.destroy();
    } finally {
      client.close();
    }
  }

  @Test
  public void testDestroyedEntityAllowsRecreation() throws Throwable {
    Connection client = CLUSTER.newConnection();
    try {
      EntityRef<InternalEhcacheClientEntity, ClusteredTierManagerConfiguration> ref = getEntityRef(client);

      ref.create(blankConfiguration);
      ref.destroy();

      ref.create(blankConfiguration);
      assertThat(ref.fetchEntity(), not(nullValue()));
    } finally {
      client.close();
    }
  }

  private EntityRef<InternalEhcacheClientEntity, ClusteredTierManagerConfiguration> getEntityRef(Connection client) throws org.terracotta.exception.EntityNotProvidedException {
    return client.getEntityRef(InternalEhcacheClientEntity.class, EhcacheEntityVersion.ENTITY_VERSION, testName.getMethodName());
  }
}
