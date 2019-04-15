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
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Henri Tremblay
 */
@RunWith(Enclosed.class)
public class DefaultDiskResourceServiceTest {

  public static abstract class AbstractDefaultDiskResourceServiceTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    protected DefaultDiskResourceService service = new DefaultDiskResourceService();
    @SuppressWarnings("unchecked")
    protected ServiceProvider<Service> serviceProvider = mock(ServiceProvider.class);

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
      verify(persistenceService).destroyAll(DefaultDiskResourceService.PERSISTENCE_SPACE_OWNER);
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
      expectedException.expect(CachePersistenceException.class);
      expectedException.expectMessage("Unknown space: null");
      service.createPersistenceContextWithin(null, "test");
    }

    @Test
    public void testGetPersistenceSpaceIdentifier() throws CachePersistenceException {
      assertThat(service.getPersistenceSpaceIdentifier("test", null)).isNull();
    }


    @Test
    public void testGetStateRepositoryWithin() throws CachePersistenceException {
      expectedException.expect(CachePersistenceException.class);
      expectedException.expectMessage("Unknown space: null");
      assertThat(service.getStateRepositoryWithin(null, "test")).isNull();
    }

    @Test
    public void testReleasePersistenceSpaceIdentifier() throws CachePersistenceException {
      expectedException.expect(CachePersistenceException.class);
      expectedException.expectMessage("Unknown space: null");
      assertThat(service.getStateRepositoryWithin(null, "test")).isNull();
    }

  }

}
