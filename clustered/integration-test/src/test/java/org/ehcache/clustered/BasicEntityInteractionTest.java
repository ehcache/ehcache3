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
import java.util.UUID;
import org.ehcache.clustered.client.internal.EhcacheClientEntity;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.terracotta.connection.Connection;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.exception.EntityAlreadyExistsException;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.testing.rules.BasicExternalCluster;
import org.terracotta.testing.rules.Cluster;

import static org.hamcrest.core.Is.is;
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

  @BeforeClass
  public static void waitForActive() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
  }

  @Test
  public void testAbsentEntityRetrievalFails() throws Throwable {
    Connection client = CLUSTER.newConnection();
    try {
      EntityRef<EhcacheClientEntity, UUID> ref = client.getEntityRef(EhcacheClientEntity.class, 1, "testAbsentEntityRetrievalFails");

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
      EntityRef<EhcacheClientEntity, UUID> ref = client.getEntityRef(EhcacheClientEntity.class, 1, "testAbsentEntityCreationSucceeds");

      UUID uuid = UUID.randomUUID();
      ref.create(uuid);
      try {
        EhcacheClientEntity entity = ref.fetchEntity();
        try {
          assertThat(entity.identity(), is(uuid));
        } finally {
          entity.close();
        }
      } finally {
        ref.destroy();
      }
    } finally {
      client.close();
    }
  }

  @Test
  public void testPresentEntityCreationFails() throws Throwable {
    Connection client = CLUSTER.newConnection();
    try {
      EntityRef<EhcacheClientEntity, UUID> ref = client.getEntityRef(EhcacheClientEntity.class, 1, "testPresentEntityCreationFails");

      UUID uuid = UUID.randomUUID();
      ref.create(uuid);
      try {
        try {
          ref.create(uuid);
          fail("Expected EntityAlreadyExistsException");
        } catch (EntityAlreadyExistsException e) {
          //expected
        }

        try {
          ref.create(UUID.randomUUID());
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
      EntityRef<EhcacheClientEntity, UUID> ref = client.getEntityRef(EhcacheClientEntity.class, 1, "testAbsentEntityDestroyFails");

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
      EntityRef<EhcacheClientEntity, UUID> ref = client.getEntityRef(EhcacheClientEntity.class, 1, "testPresentEntityDestroySucceeds");

      UUID uuid = UUID.randomUUID();
      ref.create(uuid);
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
      EntityRef<EhcacheClientEntity, UUID> ref = client.getEntityRef(EhcacheClientEntity.class, 1, "testPresentEntityDestroyBlockedByHeldReferenceSucceeds");

      UUID uuid = UUID.randomUUID();
      ref.create(uuid);

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
      EntityRef<EhcacheClientEntity, UUID> ref = client.getEntityRef(EhcacheClientEntity.class, 1, "testPresentEntityDestroyNotBlockedByReleasedReferenceSucceeds");

      UUID uuid = UUID.randomUUID();
      ref.create(uuid);
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
      EntityRef<EhcacheClientEntity, UUID> ref = client.getEntityRef(EhcacheClientEntity.class, 1, "testDestroyedEntityAllowsRecreation");

      ref.create(UUID.randomUUID());
      ref.destroy();

      UUID uuid = UUID.randomUUID();
      ref.create(uuid);
      try {
        EhcacheClientEntity entity = ref.fetchEntity();
        try {
          assertThat(entity.identity(), is(uuid));
        } finally {
          entity.close();
        }
      } finally {
        ref.destroy();
      }
    } finally {
      client.close();
    }
  }
}
