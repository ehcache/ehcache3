/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

import org.ehcache.config.ResourceType;
import org.ehcache.core.spi.ServiceLocator;
import org.ehcache.core.spi.service.LocalPersistenceService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Henri Tremblay
 */
public class DefaultDiskResourceServiceTest {

    DefaultDiskResourceService service = new DefaultDiskResourceService();
    LocalPersistenceService persistenceService = mock(LocalPersistenceService.class);

    @Before
    public void setup() {
      ServiceLocator serviceLocator = ServiceLocator.dependencySet().with(service).with(persistenceService).build();
      service.start(serviceLocator);
    }

    @After
    public void tearDown() {
      service.stop();
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
    public void testDestroy() {
      service.destroy("test"); // should do nothing
    }

}
