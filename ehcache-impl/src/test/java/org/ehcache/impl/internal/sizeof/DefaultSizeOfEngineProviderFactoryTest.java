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
package org.ehcache.impl.internal.sizeof;

import org.ehcache.config.units.MemoryUnit;
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static java.lang.Integer.parseInt;
import static java.lang.System.getProperty;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assume.assumeThat;
import static org.mockito.Mockito.mock;
import static org.hamcrest.Matchers.instanceOf;

/**
 * @author Abhilash
 *
 */
@Deprecated
public class DefaultSizeOfEngineProviderFactoryTest {

  @BeforeClass
  public static void preconditions() {
    assumeThat(parseInt(getProperty("java.specification.version").split("\\.")[0]), is(lessThan(16)));
  }

  @Test
  public void testNullConfiguration() {
    DefaultSizeOfEngineProviderFactory factory = new DefaultSizeOfEngineProviderFactory();
    org.ehcache.core.spi.store.heap.SizeOfEngineProvider sizeOfEngineProvider = factory.create(null);
    org.ehcache.core.spi.store.heap.SizeOfEngine sizeOfEngine = sizeOfEngineProvider.createSizeOfEngine(MemoryUnit.B, mock(ServiceConfiguration.class));
    assertThat(sizeOfEngineProvider, notNullValue());
    assertThat(sizeOfEngine, notNullValue());
    assertThat(sizeOfEngine, instanceOf(DefaultSizeOfEngine.class));
  }

  @Test
  public void testNoopSizeOfEngineConfig() {
    DefaultSizeOfEngineProviderFactory factory = new DefaultSizeOfEngineProviderFactory();
    org.ehcache.core.spi.store.heap.SizeOfEngineProvider sizeOfEngineProvider = factory.create(null);
    org.ehcache.core.spi.store.heap.SizeOfEngine sizeOfEngine = sizeOfEngineProvider.createSizeOfEngine(null, mock(ServiceConfiguration.class));
    assertThat(sizeOfEngineProvider, notNullValue());
    assertThat(sizeOfEngine, notNullValue());
    assertThat(sizeOfEngine, instanceOf(NoopSizeOfEngine.class));
  }
}
