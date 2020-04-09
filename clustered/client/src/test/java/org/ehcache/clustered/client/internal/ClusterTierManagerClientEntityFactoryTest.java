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

import org.ehcache.clustered.common.internal.ClusterTierManagerConfiguration;
import org.ehcache.clustered.common.internal.lock.LockMessaging.HoldType;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLockClient;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.terracotta.connection.Connection;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import org.terracotta.connection.entity.Entity;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.exception.EntityAlreadyExistsException;
import org.terracotta.exception.EntityConfigurationException;
import org.terracotta.exception.EntityNotFoundException;

public class ClusterTierManagerClientEntityFactoryTest {

  @Mock
  private EntityRef<ClusterTierManagerClientEntity, Object, Void> entityRef;
  @Mock
  private ClusterTierManagerClientEntity entity;
  @Mock
  private Connection connection;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testCreate() throws Exception {
    when(getEntityRef(ClusterTierManagerClientEntity.class)).thenReturn(entityRef);

    addMockUnlockedLock(connection, "VoltronReadWriteLock-ClusterTierManagerClientEntityFactory-AccessLock-test");

    ClusterTierManagerClientEntityFactory factory = new ClusterTierManagerClientEntityFactory(connection, Runnable::run);
    factory.create("test", null);
    verify(entityRef).create(isA(ClusterTierManagerConfiguration.class));
    verifyNoMoreInteractions(entityRef);
  }

  @Test
  public void testCreateBadConfig() throws Exception {
    doThrow(EntityConfigurationException.class).when(entityRef).create(any(ClusterTierManagerConfiguration.class));
    when(getEntityRef(ClusterTierManagerClientEntity.class)).thenReturn(entityRef);
    addMockUnlockedLock(connection, "VoltronReadWriteLock-ClusterTierManagerClientEntityFactory-AccessLock-test");

    ClusterTierManagerClientEntityFactory factory = new ClusterTierManagerClientEntityFactory(connection, Runnable::run);
    try {
      factory.create("test", null);
      fail("Expecting ClusterTierManagerCreationException");
    } catch (ClusterTierManagerCreationException e) {
      // expected
    }
  }

  @Test
  public void testCreateWhenExisting() throws Exception {
    doThrow(EntityAlreadyExistsException.class).when(entityRef).create(any());
    when(getEntityRef(ClusterTierManagerClientEntity.class)).thenReturn(entityRef);

    addMockUnlockedLock(connection, "VoltronReadWriteLock-ClusterTierManagerClientEntityFactory-AccessLock-test");

    ClusterTierManagerClientEntityFactory factory = new ClusterTierManagerClientEntityFactory(connection, Runnable::run);
    try {
      factory.create("test", null);
      fail("Expected EntityAlreadyExistsException");
    } catch (EntityAlreadyExistsException e) {
      //expected
    }
  }

  @Test
  public void testRetrieve() throws Exception {
    when(entityRef.fetchEntity(null)).thenReturn(entity);
    when(getEntityRef(ClusterTierManagerClientEntity.class)).thenReturn(entityRef);

    addMockUnlockedLock(connection, "VoltronReadWriteLock-ClusterTierManagerClientEntityFactory-AccessLock-test");

    ClusterTierManagerClientEntityFactory factory = new ClusterTierManagerClientEntityFactory(connection, Runnable::run);
    assertThat(factory.retrieve("test", null), is(entity));
    verify(entity).validate(isNull());
    verify(entity, never()).close();
  }

  @Test
  public void testRetrieveFailedValidate() throws Exception {
    when(entityRef.fetchEntity(null)).thenReturn(entity);
    doThrow(IllegalArgumentException.class).when(entity).validate(isNull());
    when(getEntityRef(ClusterTierManagerClientEntity.class)).thenReturn(entityRef);

    addMockUnlockedLock(connection, "VoltronReadWriteLock-ClusterTierManagerClientEntityFactory-AccessLock-test");

    ClusterTierManagerClientEntityFactory factory = new ClusterTierManagerClientEntityFactory(connection, Runnable::run);
    try {
      factory.retrieve("test", null).close();
      fail("Expecting IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // expected
    }
    verify(entity).validate(isNull());
    verify(entity).close();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRetrieveWhenNotExisting() throws Exception {
    when(entityRef.fetchEntity(null)).thenThrow(EntityNotFoundException.class);
    doThrow(EntityAlreadyExistsException.class).when(entityRef).create(any());
    when(getEntityRef(ClusterTierManagerClientEntity.class)).thenReturn(entityRef);

    addMockUnlockedLock(connection, "VoltronReadWriteLock-ClusterTierManagerClientEntityFactory-AccessLock-test");

    ClusterTierManagerClientEntityFactory factory = new ClusterTierManagerClientEntityFactory(connection, Runnable::run);
    try {
      factory.retrieve("test", null).close();
      fail("Expected EntityNotFoundException");
    } catch (EntityNotFoundException e) {
      //expected
    }
  }

  @Test
  public void testDestroy() throws Exception {
    ClusterTierManagerClientEntity mockEntity = mock(ClusterTierManagerClientEntity.class);
    when(entityRef.fetchEntity(null)).thenReturn(mockEntity);
    doReturn(Boolean.TRUE).when(entityRef).destroy();
    when(getEntityRef(ClusterTierManagerClientEntity.class)).thenReturn(entityRef);

    addMockUnlockedLock(connection, "VoltronReadWriteLock-ClusterTierManagerClientEntityFactory-AccessLock-test");

    ClusterTierManagerClientEntityFactory factory = new ClusterTierManagerClientEntityFactory(connection, Runnable::run);
    factory.destroy("test");
    verify(entityRef).destroy();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDestroyWhenNotExisting() throws Exception {
    when(entityRef.fetchEntity(null)).thenThrow(EntityNotFoundException.class);
    doThrow(EntityNotFoundException.class).when(entityRef).destroy();
    when(getEntityRef(ClusterTierManagerClientEntity.class)).thenReturn(entityRef);

    addMockUnlockedLock(connection, "VoltronReadWriteLock-ClusterTierManagerClientEntityFactory-AccessLock-test");

    ClusterTierManagerClientEntityFactory factory = new ClusterTierManagerClientEntityFactory(connection, Runnable::run);
    factory.destroy("test");
    verify(entityRef).destroy();
  }

  private static void addMockUnlockedLock(Connection connection, String lockname) throws Exception {
    addMockLock(connection, lockname, true);
  }

  private static void addMockLock(Connection connection, String lockname, boolean result, Boolean ... results) throws Exception {
    VoltronReadWriteLockClient lock = mock(VoltronReadWriteLockClient.class);
    when(lock.tryLock(any(HoldType.class))).thenReturn(result, results);
    @SuppressWarnings("unchecked")
    EntityRef<VoltronReadWriteLockClient, Object, Object> interlockRef = mock(EntityRef.class);
    when(connection.getEntityRef(eq(VoltronReadWriteLockClient.class), anyLong(), eq(lockname))).thenReturn(interlockRef);
    when(interlockRef.fetchEntity(null)).thenReturn(lock);
  }

  private <E extends Entity> EntityRef<E, Object, Void> getEntityRef(Class<E> value) throws org.terracotta.exception.EntityNotProvidedException {
    return connection.getEntityRef(eq(value), anyLong(), anyString());
  }
}
