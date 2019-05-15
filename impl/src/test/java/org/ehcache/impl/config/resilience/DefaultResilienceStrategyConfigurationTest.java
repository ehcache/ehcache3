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
package org.ehcache.impl.config.resilience;

import org.ehcache.impl.internal.resilience.RobustResilienceStrategy;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.resilience.RecoveryStore;
import org.ehcache.spi.resilience.ResilienceStrategy;
import org.junit.Test;

import static org.ehcache.test.MockitoUtil.mock;
import static org.hamcrest.collection.IsArrayContainingInOrder.arrayContaining;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.nullValue;
import static org.hamcrest.core.IsSame.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class DefaultResilienceStrategyConfigurationTest {

  @Test
  public void testBindOnInstanceConfigurationReturnsSelf() {
    DefaultResilienceStrategyConfiguration configuration = new DefaultResilienceStrategyConfiguration((ResilienceStrategy<?, ?>) mock(ResilienceStrategy.class));
    assertThat(configuration.bind(null), sameInstance(configuration));
  }

  @Test
  public void testLoaderWriterBindOnInstanceConfigurationReturnsSelf() {
    DefaultResilienceStrategyConfiguration configuration = new DefaultResilienceStrategyConfiguration((ResilienceStrategy<?, ?>) mock(ResilienceStrategy.class));
    assertThat(configuration.bind(null, null), sameInstance(configuration));
  }

  @Test
  public void testBindOnRegularConfigurationAppendsParameters() {
    Object foo = new Object();
    DefaultResilienceStrategyConfiguration configuration = new DefaultResilienceStrategyConfiguration(RobustResilienceStrategy.class, foo);
    RecoveryStore<?> recoveryStore = mock(RecoveryStore.class);
    DefaultResilienceStrategyConfiguration bound = configuration.bind(recoveryStore);

    assertThat(bound.getArguments(), arrayContaining(foo, recoveryStore));
    assertThat(bound.getClazz(), sameInstance(RobustResilienceStrategy.class));
    assertThat(bound.getInstance(), nullValue());
  }

  @Test
  public void testLoaderWriterBindOnInstanceConfigurationAppendsParameters() {
    Object foo = new Object();
    DefaultResilienceStrategyConfiguration configuration = new DefaultResilienceStrategyConfiguration(RobustResilienceStrategy.class, foo);
    RecoveryStore<?> recoveryStore = mock(RecoveryStore.class);
    CacheLoaderWriter<?, ?> loaderWriter = mock(CacheLoaderWriter.class);
    DefaultResilienceStrategyConfiguration bound = configuration.bind(recoveryStore, loaderWriter);

    assertThat(bound.getArguments(), arrayContaining(foo, recoveryStore, loaderWriter));
    assertThat(bound.getClazz(), sameInstance(RobustResilienceStrategy.class));
    assertThat(bound.getInstance(), nullValue());
  }

  @Test
  public void testAlreadyBoundConfigurationCannotBeBound() {
    DefaultResilienceStrategyConfiguration configuration = new DefaultResilienceStrategyConfiguration(RobustResilienceStrategy.class);
    RecoveryStore<?> recoveryStore = mock(RecoveryStore.class);
    CacheLoaderWriter<?, ?> loaderWriter = mock(CacheLoaderWriter.class);
    DefaultResilienceStrategyConfiguration bound = configuration.bind(recoveryStore, loaderWriter);

    try {
      bound.bind(recoveryStore);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      //expected
    }
  }

  @Test
  public void testAlreadyBoundLoaderWriterConfigurationCannotBeBound() {
    DefaultResilienceStrategyConfiguration configuration = new DefaultResilienceStrategyConfiguration(RobustResilienceStrategy.class);
    RecoveryStore<?> recoveryStore = mock(RecoveryStore.class);
    DefaultResilienceStrategyConfiguration bound = configuration.bind(recoveryStore);

    try {
      bound.bind(recoveryStore);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      //expected
    }
  }

  @Test
  public void testDeriveDetachesCorrectly() {
    ResilienceStrategy<?, ?> resilienceStrategy = mock(ResilienceStrategy.class);
    DefaultResilienceStrategyConfiguration configuration = new DefaultResilienceStrategyConfiguration(resilienceStrategy);
    DefaultResilienceStrategyConfiguration derived = configuration.build(configuration.derive());

    assertThat(derived, is(not(sameInstance(configuration))));
    assertThat(derived.getInstance(), sameInstance(configuration.getInstance()));
  }
}
