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
package org.ehcache.impl.persistence;

import org.ehcache.CachePersistenceException;
import org.ehcache.config.ResourceType;
import org.ehcache.core.spi.service.LocalPersistenceService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.ehcache.test.MockitoUtil.uncheckedGenericMock;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Henri Tremblay
 */
@RunWith(Enclosed.class)
public class DefaultDiskResourceServiceTest {

  public static abstract class AbstractDefaultDiskResourceServiceTest {

    protected DefaultDiskResourceService service = new DefaultDiskResourceService();
    protected ServiceProvider<Service> serviceProvider = uncheckedGenericMock(ServiceProvider.class);

    @Before
    public void setup() {
      service.start(serviceProvider);
    }

    @After
    public void tearDown() {
      service.stop();
    }

  }

  public static class WithPersistenceService extends AbstractDefaultDiskResourceServiceTest {

    LocalPersistenceService persistenceService = mock(LocalPersistenceService.class);

    @Before
    public void setup() {
      when(serviceProvider.getService(LocalPersistenceService.class)).thenReturn(persistenceService);
      super.setup();
    }

    @Test
    public void testHandlesResourceType() {
      assertThat(service.handlesResourceType(ResourceType.Core.DISK)).isTrue();
    }

    @Test
    public void testDestroyAll() {
      service.destroyAll();
      verify(persistenceService, times(2)).destroyAll(DefaultDiskResourceService.PERSISTENCE_SPACE_OWNER);
    }

    @Test
    public void testDestroy() throws CachePersistenceException {
      service.destroy("test"); // should do nothing
    }

    // Some tests still missing here
  }

  public static class WithoutPersistenceService extends AbstractDefaultDiskResourceServiceTest {

    @Test
    public void testHandlesResourceType() {
      assertThat(service.handlesResourceType(ResourceType.Core.DISK)).isFalse();
    }

    @Test
    public void testDestroyAll() {
      service.destroyAll(); // should do nothing
    }

    @Test
    public void testDestroy() throws CachePersistenceException {
      service.destroy("test"); // should do nothing
    }

    @Test
    public void testCreatePersistenceContextWithin() throws CachePersistenceException {
      assertThatThrownBy(() -> service.createPersistenceContextWithin(null, "test"))
        .isInstanceOf(CachePersistenceException.class).withFailMessage("Unknown space: null");
    }

    @Test
    public void testGetPersistenceSpaceIdentifier() throws CachePersistenceException {
      assertThat(service.getPersistenceSpaceIdentifier("test", null)).isNull();
    }


    @Test
    public void testGetStateRepositoryWithin() throws CachePersistenceException {
      assertThatThrownBy(() -> service.getStateRepositoryWithin(null, "test"))
        .isInstanceOf(CachePersistenceException.class).withFailMessage("Unknown space: null");
    }

    @Test
    public void testReleasePersistenceSpaceIdentifier() throws CachePersistenceException {
      assertThatThrownBy(() -> service.getStateRepositoryWithin(null, "test"))
        .isInstanceOf(CachePersistenceException.class).withFailMessage("Unknown space: null");
    }

  }

}
