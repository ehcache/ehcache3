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

package org.ehcache.clustered.client.internal.service;

import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.client.internal.InternalClusterTierManagerClientEntity;
import org.ehcache.clustered.client.internal.MockConnectionService;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLockClient;
import org.ehcache.clustered.client.internal.store.InternalClusterTierClientEntity;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ClusterTierManagerConfiguration;
import org.ehcache.clustered.common.internal.exceptions.DestroyInProgressException;
import org.ehcache.clustered.common.internal.lock.LockMessaging;
import org.ehcache.spi.service.MaintainableService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.terracotta.connection.Connection;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.exception.EntityAlreadyExistsException;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.ehcache.clustered.common.EhcacheEntityVersion.ENTITY_VERSION;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * DefaultClusteringServiceDestroyTest
 */
public class DefaultClusteringServiceDestroyTest {

  @Mock
  private Connection connection;
  @Mock
  private EntityRef<InternalClusterTierManagerClientEntity, Object> managerEntityRef;
  @Mock
  private EntityRef<InternalClusterTierClientEntity, Object> tierEntityRef;
  @Mock
  private EntityRef<VoltronReadWriteLockClient, Object> lockEntityRef;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    MockConnectionService.mockConnection = connection;
  }

  @Test
  public void testDestroyAllFullyMocked() throws Exception {
    mockLockForWriteLockSuccess();

    when(connection.getEntityRef(same(InternalClusterTierManagerClientEntity.class), eq(ENTITY_VERSION), any())).thenReturn(managerEntityRef);
    InternalClusterTierManagerClientEntity managerEntity = mock(InternalClusterTierManagerClientEntity.class);
    when(managerEntityRef.fetchEntity()).thenReturn(managerEntity);

    Set<String> stores = new HashSet<>();
    stores.add("store1");
    stores.add("store2");
    when(managerEntity.prepareForDestroy()).thenReturn(stores);

    when(connection.getEntityRef(same(InternalClusterTierClientEntity.class), eq(ENTITY_VERSION), any())).thenReturn(tierEntityRef);

    when(tierEntityRef.destroy()).thenReturn(true);
    when(managerEntityRef.destroy()).thenReturn(true);

    DefaultClusteringService service = new DefaultClusteringService(new ClusteringServiceConfiguration(URI
      .create("mock://localhost/whatever")));
    service.startForMaintenance(null, MaintainableService.MaintenanceScope.CACHE_MANAGER);

    service.destroyAll();
    verify(managerEntity).prepareForDestroy();
    verify(tierEntityRef, times(2)).destroy();
    verify(managerEntityRef).destroy();
  }

  @Test
  public void testAutoCreateOnPartialDestroyState() throws Exception {
    ServerSideConfiguration serverConfig = new ServerSideConfiguration("default", emptyMap());

    mockLockForWriteLockSuccess();

    when(connection.getEntityRef(same(InternalClusterTierManagerClientEntity.class), eq(ENTITY_VERSION), any())).thenReturn(managerEntityRef);
    InternalClusterTierManagerClientEntity managerEntity = mock(InternalClusterTierManagerClientEntity.class);

    // ClusterTierManager exists
    doThrow(new EntityAlreadyExistsException("className", "entityName"))
      // Next time simulate creation
      .doNothing()
        .when(managerEntityRef).create(new ClusterTierManagerConfiguration("whatever", serverConfig));
    // And can be fetch
    when(managerEntityRef.fetchEntity()).thenReturn(managerEntity);
    // However validate indicates destroy in progress
    doThrow(new DestroyInProgressException("destroying"))
      // Next time validation succeeds
      .doNothing()
      .when(managerEntity).validate(serverConfig);

    Set<String> stores = new HashSet<>();
    stores.add("store1");
    stores.add("store2");
    when(managerEntity.prepareForDestroy()).thenReturn(stores);

    when(connection.getEntityRef(same(InternalClusterTierClientEntity.class), eq(ENTITY_VERSION), any())).thenReturn(tierEntityRef);

    when(tierEntityRef.destroy()).thenReturn(true);
    when(managerEntityRef.destroy()).thenReturn(true);

    DefaultClusteringService service = new DefaultClusteringService(new ClusteringServiceConfiguration(URI
      .create("mock://localhost/whatever"), true, serverConfig));
    service.start(null);

    verify(managerEntity).prepareForDestroy();
    verify(tierEntityRef, times(2)).destroy();
    verify(managerEntityRef).destroy();
  }

  @Test
  public void testFetchOnPartialDestroyState() throws Exception {
    mockLockForReadLockSuccess();

    when(connection.getEntityRef(same(InternalClusterTierManagerClientEntity.class), eq(ENTITY_VERSION), any())).thenReturn(managerEntityRef);
    InternalClusterTierManagerClientEntity managerEntity = mock(InternalClusterTierManagerClientEntity.class);

    // ClusterTierManager can be fetch
    when(managerEntityRef.fetchEntity()).thenReturn(managerEntity);
    // However validate indicates destroy in progress
    doThrow(new DestroyInProgressException("destroying")).when(managerEntity).validate(null);

    DefaultClusteringService service = new DefaultClusteringService(new ClusteringServiceConfiguration(URI
      .create("mock://localhost/whatever")));
    try {
      service.start(null);
      fail("IllegalStateException expected");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), containsString("does not exist"));
    }
  }

  @Test
  public void testDestroyOnPartialDestroyState() throws Exception {
    mockLockForWriteLockSuccess();

    when(connection.getEntityRef(same(InternalClusterTierManagerClientEntity.class), eq(ENTITY_VERSION), any())).thenReturn(managerEntityRef);
    InternalClusterTierManagerClientEntity managerEntity = mock(InternalClusterTierManagerClientEntity.class);
    when(managerEntityRef.fetchEntity()).thenReturn(managerEntity);
    doThrow(new DestroyInProgressException("destroying")).when(managerEntity).validate(any());

    Set<String> stores = new HashSet<>();
    stores.add("store1");
    stores.add("store2");
    when(managerEntity.prepareForDestroy()).thenReturn(stores);

    when(connection.getEntityRef(same(InternalClusterTierClientEntity.class), eq(ENTITY_VERSION), any())).thenReturn(tierEntityRef);

    when(tierEntityRef.destroy()).thenReturn(true);
    when(managerEntityRef.destroy()).thenReturn(true);

    DefaultClusteringService service = new DefaultClusteringService(new ClusteringServiceConfiguration(URI
      .create("mock://localhost/whatever")));
    service.startForMaintenance(null, MaintainableService.MaintenanceScope.CACHE_MANAGER);

    service.destroyAll();
    verify(managerEntity).prepareForDestroy();
    verify(tierEntityRef, times(2)).destroy();
    verify(managerEntityRef).destroy();
  }

  private void mockLockForWriteLockSuccess() throws org.terracotta.exception.EntityNotProvidedException, org.terracotta.exception.EntityNotFoundException, org.terracotta.exception.EntityVersionMismatchException {
    when(connection.getEntityRef(same(VoltronReadWriteLockClient.class), eq(1L), any())).thenReturn(lockEntityRef);
    VoltronReadWriteLockClient lockClient = mock(VoltronReadWriteLockClient.class);
    when(lockEntityRef.fetchEntity()).thenReturn(lockClient);

    when(lockClient.tryLock(LockMessaging.HoldType.WRITE)).thenReturn(true);
  }

  private void mockLockForReadLockSuccess() throws org.terracotta.exception.EntityNotProvidedException, org.terracotta.exception.EntityNotFoundException, org.terracotta.exception.EntityVersionMismatchException {
    when(connection.getEntityRef(same(VoltronReadWriteLockClient.class), eq(1L), any())).thenReturn(lockEntityRef);
    VoltronReadWriteLockClient lockClient = mock(VoltronReadWriteLockClient.class);
    when(lockEntityRef.fetchEntity()).thenReturn(lockClient);

    when(lockClient.tryLock(LockMessaging.HoldType.READ)).thenReturn(true);
  }

}
