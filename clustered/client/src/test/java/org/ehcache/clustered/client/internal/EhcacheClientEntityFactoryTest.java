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

package org.ehcache.clustered.client.internal;

import java.util.UUID;

import org.ehcache.clustered.client.internal.service.ClusteredTierManagerConfigurationException;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.lock.LockMessaging.HoldType;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLockClient;
import org.junit.Test;
import org.terracotta.connection.Connection;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.exception.EntityAlreadyExistsException;
import org.terracotta.exception.EntityNotFoundException;

public class EhcacheClientEntityFactoryTest {

  @Test
  public void testCreate() throws Exception {
    EhcacheClientEntity entity = mock(EhcacheClientEntity.class);
    EntityRef<EhcacheClientEntity, Object> entityRef = mock(EntityRef.class);
    when(entityRef.fetchEntity()).thenReturn(entity);
    Connection connection = mock(Connection.class);
    when(connection.getEntityRef(eq(EhcacheClientEntity.class), anyInt(), anyString())).thenReturn(entityRef);

    addMockUnlockedLock(connection, "VoltronReadWriteLock-EhcacheClientEntityFactory-AccessLock-test");

    EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(connection);
    factory.create("test", null);
    verify(entityRef).create(any(UUID.class));
    verify(entity).configure(any(ServerSideConfiguration.class));
    verify(entity).close();
  }

  @Test
  public void testCreateBadConfig() throws Exception {
    EhcacheClientEntity entity = mock(EhcacheClientEntity.class);
    EntityRef<EhcacheClientEntity, Object> entityRef = mock(EntityRef.class);
    when(entityRef.fetchEntity()).thenReturn(entity);
    doThrow(ClusteredTierManagerConfigurationException.class).when(entity).configure(any(ServerSideConfiguration.class));
    Connection connection = mock(Connection.class);
    when(connection.getEntityRef(eq(EhcacheClientEntity.class), anyInt(), anyString())).thenReturn(entityRef);

    addMockUnlockedLock(connection, "VoltronReadWriteLock-EhcacheClientEntityFactory-AccessLock-test");

    EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(connection);
    try {
      factory.create("test", null);
      fail("Expecting EhcacheEntityCreationException");
    } catch (EhcacheEntityCreationException e) {
      // expected
    }
    verify(entityRef).create(any(UUID.class));
    verify(entity).configure(any(ServerSideConfiguration.class));
    verify(entity).close();
    verify(entityRef).destroy();
  }

  @Test
  public void testCreateWhenExisting() throws Exception {
    EntityRef<EhcacheClientEntity, Object> entityRef = mock(EntityRef.class);
    doThrow(EntityAlreadyExistsException.class).when(entityRef).create(any());
    Connection connection = mock(Connection.class);
    when(connection.getEntityRef(eq(EhcacheClientEntity.class), anyInt(), anyString())).thenReturn(entityRef);

    addMockUnlockedLock(connection, "VoltronReadWriteLock-EhcacheClientEntityFactory-AccessLock-test");

    EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(connection);
    try {
      factory.create("test", null);
      fail("Expected EntityAlreadyExistsException");
    } catch (EntityAlreadyExistsException e) {
      //expected
    }
  }

  @Test
  public void testRetrieve() throws Exception {
    EhcacheClientEntity entity = mock(EhcacheClientEntity.class);
    EntityRef<EhcacheClientEntity, Object> entityRef = mock(EntityRef.class);
    when(entityRef.fetchEntity()).thenReturn(entity);
    Connection connection = mock(Connection.class);
    when(connection.getEntityRef(eq(EhcacheClientEntity.class), anyInt(), anyString())).thenReturn(entityRef);

    addMockUnlockedLock(connection, "VoltronReadWriteLock-EhcacheClientEntityFactory-AccessLock-test");

    EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(connection);
    assertThat(factory.retrieve("test", null), is(entity));
    verify(entity).validate(any(ServerSideConfiguration.class));
    verify(entity, never()).close();
  }

  @Test
  public void testRetrieveFailedValidate() throws Exception {
    EhcacheClientEntity entity = mock(EhcacheClientEntity.class);
    EntityRef<EhcacheClientEntity, Object> entityRef = mock(EntityRef.class);
    when(entityRef.fetchEntity()).thenReturn(entity);
    doThrow(IllegalArgumentException.class).when(entity).validate(any(ServerSideConfiguration.class));
    Connection connection = mock(Connection.class);
    when(connection.getEntityRef(eq(EhcacheClientEntity.class), anyInt(), anyString())).thenReturn(entityRef);

    addMockUnlockedLock(connection, "VoltronReadWriteLock-EhcacheClientEntityFactory-AccessLock-test");

    EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(connection);
    try {
      factory.retrieve("test", null);
      fail("Expecting IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // expected
    }
    verify(entity).validate(any(ServerSideConfiguration.class));
    verify(entity).close();
  }

  @Test
  public void testRetrieveWhenNotExisting() throws Exception {
    EntityRef<EhcacheClientEntity, Object> entityRef = mock(EntityRef.class);
    when(entityRef.fetchEntity()).thenThrow(EntityNotFoundException.class);
    doThrow(EntityAlreadyExistsException.class).when(entityRef).create(any());
    Connection connection = mock(Connection.class);
    when(connection.getEntityRef(eq(EhcacheClientEntity.class), anyInt(), anyString())).thenReturn(entityRef);

    addMockUnlockedLock(connection, "VoltronReadWriteLock-EhcacheClientEntityFactory-AccessLock-test");

    EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(connection);
    try {
      factory.retrieve("test", null);
      fail("Expected EntityNotFoundException");
    } catch (EntityNotFoundException e) {
      //expected
    }
  }

  @Test
  public void testDestroy() throws Exception {
    EntityRef<EhcacheClientEntity, Object> entityRef = mock(EntityRef.class);
    doReturn(Boolean.TRUE).when(entityRef).destroy();
    Connection connection = mock(Connection.class);
    when(connection.getEntityRef(eq(EhcacheClientEntity.class), anyInt(), anyString())).thenReturn(entityRef);

    addMockUnlockedLock(connection, "VoltronReadWriteLock-EhcacheClientEntityFactory-AccessLock-test");

    EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(connection);
    factory.destroy("test");
    verify(entityRef).destroy();
  }

  @Test
  public void testDestroyWhenNotExisting() throws Exception {
    EntityRef<EhcacheClientEntity, Object> entityRef = mock(EntityRef.class);
    doThrow(EntityNotFoundException.class).when(entityRef).destroy();
    Connection connection = mock(Connection.class);
    when(connection.getEntityRef(eq(EhcacheClientEntity.class), anyInt(), anyString())).thenReturn(entityRef);

    addMockUnlockedLock(connection, "VoltronReadWriteLock-EhcacheClientEntityFactory-AccessLock-test");

    EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(connection);
    try {
      factory.destroy("test");
      fail("Expected EhcacheEntityNotFoundException");
    } catch (EhcacheEntityNotFoundException e) {
      //expected
    }
  }

  private static void addMockUnlockedLock(Connection connection, String lockname) throws Exception {
    addMockLock(connection, lockname, true);
  }

  private static void addMockLock(Connection connection, String lockname, boolean result, Boolean ... results) throws Exception {
    VoltronReadWriteLockClient lock = mock(VoltronReadWriteLockClient.class);
    when(lock.tryLock(any(HoldType.class))).thenReturn(result, results);
    EntityRef<VoltronReadWriteLockClient, Object> interlockRef = mock(EntityRef.class);
    when(connection.getEntityRef(eq(VoltronReadWriteLockClient.class), anyInt(), eq(lockname))).thenReturn(interlockRef);
    when(interlockRef.fetchEntity()).thenReturn(lock);
  }
}
