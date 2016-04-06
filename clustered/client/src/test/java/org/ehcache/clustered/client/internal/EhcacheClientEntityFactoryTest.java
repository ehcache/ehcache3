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
import java.util.concurrent.ExecutionException;
import static org.hamcrest.core.Is.is;

import org.junit.Test;
import org.terracotta.connection.Connection;
import org.terracotta.consensus.CoordinationService;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.terracotta.consensus.CoordinationService.ElectionTask;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import org.junit.Ignore;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
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

    EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(connection, winner());
    assertThat(factory.create("test", null), is(entity));
    verify(entityRef).create(any(UUID.class));
  }

  @Test
  public void testCreateWhenExisting() throws Exception {
    EntityRef<EhcacheClientEntity, Object> entityRef = mock(EntityRef.class);
    doThrow(EntityAlreadyExistsException.class).when(entityRef).create(any());
    Connection connection = mock(Connection.class);
    when(connection.getEntityRef(eq(EhcacheClientEntity.class), anyInt(), anyString())).thenReturn(entityRef);

    try {
      EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(connection, winner());
      factory.create("test", null);
      fail("Expected EntityAlreadyExistsException");
    } catch (EntityAlreadyExistsException e) {
      //expected
    }
  }

  @Test
  public void testCreateOrRetrieve() throws Exception {
    EhcacheClientEntity entity = mock(EhcacheClientEntity.class);
    EntityRef<EhcacheClientEntity, Object> entityRef = mock(EntityRef.class);
    when(entityRef.fetchEntity()).thenThrow(EntityNotFoundException.class).thenReturn(entity);
    Connection connection = mock(Connection.class);
    when(connection.getEntityRef(eq(EhcacheClientEntity.class), anyInt(), anyString())).thenReturn(entityRef);

    EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(connection, winner());
    assertThat(factory.createOrRetrieve("test", null), is(entity));
    verify(entityRef).create(any(UUID.class));
  }

  @Test
  public void testCreateOrRetrieveWhenExisting() throws Exception {
    EhcacheClientEntity entity = mock(EhcacheClientEntity.class);
    EntityRef<EhcacheClientEntity, Object> entityRef = mock(EntityRef.class);
    when(entityRef.fetchEntity()).thenReturn(entity);
    doThrow(EntityAlreadyExistsException.class).when(entityRef).create(any());
    Connection connection = mock(Connection.class);
    when(connection.getEntityRef(eq(EhcacheClientEntity.class), anyInt(), anyString())).thenReturn(entityRef);

    EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(connection, winner());
    assertThat(factory.createOrRetrieve("test", null), is(entity));
    verify(entityRef, never()).create(any(UUID.class));
  }

  @Test
  public void testRetrieve() throws Exception {
    EhcacheClientEntity entity = mock(EhcacheClientEntity.class);
    EntityRef<EhcacheClientEntity, Object> entityRef = mock(EntityRef.class);
    when(entityRef.fetchEntity()).thenReturn(entity);
    Connection connection = mock(Connection.class);
    when(connection.getEntityRef(eq(EhcacheClientEntity.class), anyInt(), anyString())).thenReturn(entityRef);

    EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(connection, null);
    assertThat(factory.retrieve("test", null), is(entity));
  }

  @Test
  public void testRetrieveWhenNotExisting() throws Exception {
    EntityRef<EhcacheClientEntity, Object> entityRef = mock(EntityRef.class);
    when(entityRef.fetchEntity()).thenThrow(EntityNotFoundException.class);
    doThrow(EntityAlreadyExistsException.class).when(entityRef).create(any());
    Connection connection = mock(Connection.class);
    when(connection.getEntityRef(eq(EhcacheClientEntity.class), anyInt(), anyString())).thenReturn(entityRef);

    EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(connection, null);
    try {
      factory.retrieve("test", null);
      fail("Expected EntityNotFoundException");
    } catch (EntityNotFoundException e) {
      //expected
    }
  }

  @Test
  @Ignore("Pending fix for Terracotta-OSS/terracotta-apis#27")
  public void testDestroy() throws Exception {
    EntityRef<EhcacheClientEntity, Object> entityRef = mock(EntityRef.class);
    Connection connection = mock(Connection.class);
    when(connection.getEntityRef(eq(EhcacheClientEntity.class), anyInt(), anyString())).thenReturn(entityRef);

    EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(connection, winner());
    factory.destroy("test");
    verify(entityRef).destroy();
  }

  @Test
  @Ignore("Pending fix for Terracotta-OSS/terracotta-apis#27")
  public void testDestroyAfterLosingElection() throws Exception {
    EntityRef<EhcacheClientEntity, Object> entityRef = mock(EntityRef.class);
    Connection connection = mock(Connection.class);
    when(connection.getEntityRef(eq(EhcacheClientEntity.class), anyInt(), anyString())).thenReturn(entityRef);

    EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(connection, loserThenWinner());
    factory.destroy("test");
    verify(entityRef).destroy();
  }

  @Test
  @Ignore("Pending fix for Terracotta-OSS/terracotta-apis#27")
  public void testDestroyWhenNotExisting() throws Exception {
    EntityRef<EhcacheClientEntity, Object> entityRef = mock(EntityRef.class);
    doThrow(EntityNotFoundException.class).when(entityRef).destroy();
    Connection connection = mock(Connection.class);
    when(connection.getEntityRef(eq(EhcacheClientEntity.class), anyInt(), anyString())).thenReturn(entityRef);

    EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(connection, winner());
    try {
      factory.destroy("test");
      fail("Expected EntityNotFoundException");
    } catch (EntityNotFoundException e) {
      //expected
    }
  }

  @Test
  @Ignore("Pending fix for Terracotta-OSS/terracotta-apis#27")
  public void testDestroyWhenNotExistingAfterLosingElection() throws Exception {
    EntityRef<EhcacheClientEntity, Object> entityRef = mock(EntityRef.class);
    doThrow(EntityNotFoundException.class).when(entityRef).destroy();
    Connection connection = mock(Connection.class);
    when(connection.getEntityRef(eq(EhcacheClientEntity.class), anyInt(), anyString())).thenReturn(entityRef);

    EhcacheClientEntityFactory factory = new EhcacheClientEntityFactory(connection, loserThenWinner());
    try {
      factory.destroy("test");
      fail("Expected EntityNotFoundException");
    } catch (EntityNotFoundException e) {
      //expected
    }
  }

  private static CoordinationService winner() throws ExecutionException {
    CoordinationService coordinator = mock(CoordinationService.class);
    when(coordinator.executeIfLeader(eq(EhcacheClientEntity.class), anyString(), any(ElectionTask.class))).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws ExecutionException {
        try {
          return ((ElectionTask) invocation.getArguments()[2]).call(true);
        } catch (Exception ex) {
          throw new ExecutionException(ex);
        }
      }
    }).thenReturn(null);
    return coordinator;
  }

  private static CoordinationService loserThenWinner() throws ExecutionException {
    CoordinationService coordinator = mock(CoordinationService.class);
    when(coordinator.executeIfLeader(eq(EhcacheClientEntity.class), anyString(), any(ElectionTask.class))).thenReturn(null).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws ExecutionException {
        try {
          return ((ElectionTask) invocation.getArguments()[2]).call(true);
        } catch (Exception ex) {
          throw new ExecutionException(ex);
        }
      }
    });
    return coordinator;
  }
}
