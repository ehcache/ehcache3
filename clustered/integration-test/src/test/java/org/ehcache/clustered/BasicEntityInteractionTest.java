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

import java.util.UUID;
import org.ehcache.clustered.client.internal.EhcacheClientEntity;
import org.junit.Ignore;
import org.junit.Test;
import org.terracotta.connection.Connection;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.exception.EntityAlreadyExistsException;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.passthrough.PassthroughServer;

import static org.ehcache.clustered.TestUtils.createServer;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class BasicEntityInteractionTest {

  @Test
  public void testAbsentEntityRetrievalFails() throws Throwable {
    PassthroughServer server = createServer();
    try {
      Connection client = server.connectNewClient();

      EntityRef<EhcacheClientEntity, UUID> ref = client.getEntityRef(EhcacheClientEntity.class, 1, "testEntity");

      try {
        ref.fetchEntity();
        fail("Expected EntityNotFoundException");
      } catch (EntityNotFoundException e) {
        //expected
      }
    } finally {
      server.stop();
    }
  }

  @Test
  public void testAbsentEntityCreationSucceeds() throws Throwable {
    PassthroughServer server = createServer();
    try {
      Connection client = server.connectNewClient();

      EntityRef<EhcacheClientEntity, UUID> ref = client.getEntityRef(EhcacheClientEntity.class, 1, "testEntity");

      UUID uuid = UUID.randomUUID();
      ref.create(uuid);

      EhcacheClientEntity entity = ref.fetchEntity();
      try {
        assertThat(entity.identity(), is(uuid));
      } finally {
        entity.close();
      }
    } finally {
      server.stop();
    }
  }

  @Test
  public void testPresentEntityCreationFails() throws Throwable {
    PassthroughServer server = createServer();
    try {
      Connection client = server.connectNewClient();

      EntityRef<EhcacheClientEntity, UUID> ref = client.getEntityRef(EhcacheClientEntity.class, 1, "testEntity");

      UUID uuid = UUID.randomUUID();
      ref.create(uuid);

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
      server.stop();
    }
  }

  @Test
  public void testAbsentEntityDestroyFails() throws Throwable {
    PassthroughServer server = createServer();
    try {
      Connection client = server.connectNewClient();

      EntityRef<EhcacheClientEntity, UUID> ref = client.getEntityRef(EhcacheClientEntity.class, 1, "testEntity");

      try {
        ref.destroy();
        fail("Expected EntityNotFoundException");
      } catch (EntityNotFoundException e) {
        //expected
      }
    } finally {
      server.stop();
    }
  }

  @Test
  public void testPresentEntityDestroySucceeds() throws Throwable {
    PassthroughServer server = createServer();
    try {
      Connection client = server.connectNewClient();

      EntityRef<EhcacheClientEntity, UUID> ref = client.getEntityRef(EhcacheClientEntity.class, 1, "testEntity");

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
      server.stop();
    }
  }

  @Test
  @Ignore
  public void testPresentEntityDestroyBlockedByHeldReferenceSucceeds() throws Throwable {
    PassthroughServer server = createServer();
    try {
      Connection client = server.connectNewClient();

      EntityRef<EhcacheClientEntity, UUID> ref = client.getEntityRef(EhcacheClientEntity.class, 1, "testEntity");

      UUID uuid = UUID.randomUUID();
      ref.create(uuid);

      EhcacheClientEntity entity = ref.fetchEntity();
      try {
        ref.destroy();
      } finally {
        entity.close();
      }
    } finally {
      server.stop();
    }
  }

  @Test
  public void testPresentEntityDestroyNotBlockedByReleasedReferenceSucceeds() throws Throwable {
    PassthroughServer server = createServer();
    try {
      Connection client = server.connectNewClient();

      EntityRef<EhcacheClientEntity, UUID> ref = client.getEntityRef(EhcacheClientEntity.class, 1, "testEntity");

      UUID uuid = UUID.randomUUID();
      ref.create(uuid);
      ref.fetchEntity().close();
      ref.destroy();
    } finally {
      server.stop();
    }
  }

  @Test
  public void testDestroyedEntityAllowsRecreation() throws Throwable {
    PassthroughServer server = createServer();
    try {
      Connection client = server.connectNewClient();

      EntityRef<EhcacheClientEntity, UUID> ref = client.getEntityRef(EhcacheClientEntity.class, 1, "testEntity");

      ref.create(UUID.randomUUID());
      ref.destroy();

      UUID uuid = UUID.randomUUID();
      ref.create(uuid);
      EhcacheClientEntity entity = ref.fetchEntity();
      try {
        assertThat(entity.identity(), is(uuid));
      } finally {
        entity.close();
      }
    } finally {
      server.stop();
    }
  }
}
