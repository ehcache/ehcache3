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

import org.ehcache.clustered.client.internal.ClusterTierManagerClientEntity;
import org.ehcache.clustered.client.internal.MockConnectionService;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLockClient;
import org.ehcache.clustered.client.internal.store.InternalClusterTierClientEntity;
import org.ehcache.clustered.common.internal.ClusterTierManagerConfiguration;
import org.ehcache.clustered.common.internal.exceptions.DestroyInProgressException;
import org.ehcache.clustered.common.internal.lock.LockMessaging;
import org.ehcache.spi.service.MaintainableService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.terracotta.connection.Connection;
import org.terracotta.connection.entity.Entity;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.exception.EntityAlreadyExistsException;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.clustered.common.EhcacheEntityVersion.ENTITY_VERSION;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
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
  private EntityRef<ClusterTierManagerClientEntity, Object, Void> managerEntityRef;
  @Mock
  private EntityRef<InternalClusterTierClientEntity, Object, Void> tierEntityRef;
  @Mock
  private EntityRef<VoltronReadWriteLockClient, Object, Void> lockEntityRef;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    MockConnectionService.mockConnection = connection;
  }

  @Test
  public void testDestroyAllFullyMocked() throws Exception {
    mockLockForWriteLockSuccess();

    when(getEntityRef(ClusterTierManagerClientEntity.class)).thenReturn(managerEntityRef);
    ClusterTierManagerClientEntity managerEntity = mock(ClusterTierManagerClientEntity.class);
    when(managerEntityRef.fetchEntity(null)).thenReturn(managerEntity);

    Set<String> stores = new HashSet<>();
    stores.add("store1");
    stores.add("store2");
    when(managerEntity.prepareForDestroy()).thenReturn(stores);

    when(getEntityRef(InternalClusterTierClientEntity.class)).thenReturn(tierEntityRef);

    when(tierEntityRef.destroy()).thenReturn(true);
    when(managerEntityRef.destroy()).thenReturn(true);

    DefaultClusteringService service = new DefaultClusteringService(cluster((URI
      .create("mock://localhost/whatever"))).build());
    service.startForMaintenance(null, MaintainableService.MaintenanceScope.CACHE_MANAGER);

    service.destroyAll();
    verify(managerEntity).prepareForDestroy();
    verify(tierEntityRef, times(2)).destroy();
    verify(managerEntityRef).destroy();
  }

  @Test
  public void testAutoCreateOnPartialDestroyState() throws Exception {
    mockLockForWriteLockSuccess();

    when(getEntityRef(ClusterTierManagerClientEntity.class)).thenReturn(managerEntityRef);
    ClusterTierManagerClientEntity managerEntity = mock(ClusterTierManagerClientEntity.class);

    // ClusterTierManager exists
    doThrow(new EntityAlreadyExistsException("className", "entityName"))
      // Next time simulate creation
      .doNothing()
        .when(managerEntityRef).create(new ClusterTierManagerConfiguration("whatever", any()));
    // And can be fetch
    when(managerEntityRef.fetchEntity(null)).thenReturn(managerEntity);
    // However validate indicates destroy in progress
    doThrow(new DestroyInProgressException("destroying"))
      // Next time validation succeeds
      .doNothing()
      .when(managerEntity).validate(any());

    Set<String> stores = new HashSet<>();
    stores.add("store1");
    stores.add("store2");
    when(managerEntity.prepareForDestroy()).thenReturn(stores);

    when(getEntityRef(InternalClusterTierClientEntity.class)).thenReturn(tierEntityRef);

    when(tierEntityRef.destroy()).thenReturn(true);
    when(managerEntityRef.destroy()).thenReturn(true);

    DefaultClusteringService service = new DefaultClusteringService(cluster(URI
      .create("mock://localhost/whatever")).autoCreate(s -> s).build());
    service.start(null);

    verify(managerEntity).prepareForDestroy();
    verify(tierEntityRef, times(2)).destroy();
    verify(managerEntityRef).destroy();
  }

  @Test
  public void testFetchOnPartialDestroyState() throws Exception {
    mockLockForReadLockSuccess();

    when(getEntityRef(ClusterTierManagerClientEntity.class)).thenReturn(managerEntityRef);
    ClusterTierManagerClientEntity managerEntity = mock(ClusterTierManagerClientEntity.class);

    // ClusterTierManager can be fetch
    when(managerEntityRef.fetchEntity(null)).thenReturn(managerEntity);
    // However validate indicates destroy in progress
    doThrow(new DestroyInProgressException("destroying")).when(managerEntity).validate(null);

    DefaultClusteringService service = new DefaultClusteringService(cluster(URI
      .create("mock://localhost/whatever")).build());
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

    when(getEntityRef(ClusterTierManagerClientEntity.class)).thenReturn(managerEntityRef);
    ClusterTierManagerClientEntity managerEntity = mock(ClusterTierManagerClientEntity.class);
    when(managerEntityRef.fetchEntity(null)).thenReturn(managerEntity);
    doThrow(new DestroyInProgressException("destroying")).when(managerEntity).validate(any());

    Set<String> stores = new HashSet<>();
    stores.add("store1");
    stores.add("store2");
    when(managerEntity.prepareForDestroy()).thenReturn(stores);

    when(getEntityRef(InternalClusterTierClientEntity.class)).thenReturn(tierEntityRef);

    when(tierEntityRef.destroy()).thenReturn(true);
    when(managerEntityRef.destroy()).thenReturn(true);

    DefaultClusteringService service = new DefaultClusteringService(cluster(URI
      .create("mock://localhost/whatever")).build());
    service.startForMaintenance(null, MaintainableService.MaintenanceScope.CACHE_MANAGER);

    service.destroyAll();
    verify(managerEntity).prepareForDestroy();
    verify(tierEntityRef, times(2)).destroy();
    verify(managerEntityRef).destroy();
  }

  private void mockLockForWriteLockSuccess() throws org.terracotta.exception.EntityNotProvidedException, org.terracotta.exception.EntityNotFoundException, org.terracotta.exception.EntityVersionMismatchException {
    when(connection.<VoltronReadWriteLockClient, Object, Void>getEntityRef(same(VoltronReadWriteLockClient.class), eq(1L), any())).thenReturn(lockEntityRef);
    VoltronReadWriteLockClient lockClient = mock(VoltronReadWriteLockClient.class);
    when(lockEntityRef.fetchEntity(null)).thenReturn(lockClient);

    when(lockClient.tryLock(LockMessaging.HoldType.WRITE)).thenReturn(true);
  }

  private void mockLockForReadLockSuccess() throws org.terracotta.exception.EntityNotProvidedException, org.terracotta.exception.EntityNotFoundException, org.terracotta.exception.EntityVersionMismatchException {
    when(connection.<VoltronReadWriteLockClient, Object, Void>getEntityRef(same(VoltronReadWriteLockClient.class), eq(1L), any())).thenReturn(lockEntityRef);
    VoltronReadWriteLockClient lockClient = mock(VoltronReadWriteLockClient.class);
    when(lockEntityRef.fetchEntity(null)).thenReturn(lockClient);

    when(lockClient.tryLock(LockMessaging.HoldType.READ)).thenReturn(true);
  }

  private <E extends Entity> EntityRef<E, Object, Void> getEntityRef(Class<E> value) throws org.terracotta.exception.EntityNotProvidedException {
    return connection.getEntityRef(same(value), eq(ENTITY_VERSION), any());
  }
}
