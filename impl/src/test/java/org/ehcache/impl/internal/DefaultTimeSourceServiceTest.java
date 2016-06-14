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

package org.ehcache.impl.internal;

import org.ehcache.core.spi.time.SystemTimeSource;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.spi.time.TimeSourceService;
import org.ehcache.core.internal.service.ServiceLocator;
import org.ehcache.spi.service.ServiceDependencies;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

/**
 * DefaultTimeSourceServiceTest
 */
@ServiceDependencies(TimeSourceService.class)
public class DefaultTimeSourceServiceTest {

  @Test
  public void testResolvesDefaultTimeSource() {
    ServiceLocator serviceLocator = new ServiceLocator();
    serviceLocator.loadDependenciesOf(this.getClass());
    assertThat(serviceLocator.getService(TimeSourceService.class).getTimeSource(),
        sameInstance(SystemTimeSource.INSTANCE));
  }

  @Test
  public void testCanConfigureAlternateTimeSource() {
    TimeSource timeSource = mock(TimeSource.class);
    ServiceLocator serviceLocator = new ServiceLocator();
    TimeSourceService timeSourceService = serviceLocator.getOrCreateServiceFor(new TimeSourceConfiguration(timeSource));
    assertThat(timeSourceService.getTimeSource(), sameInstance(timeSource));
  }

}