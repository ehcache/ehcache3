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
package org.ehcache.impl.internal.spi.resilience;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.impl.config.resilience.DefaultResilienceStrategyProviderConfiguration;
import org.ehcache.spi.resilience.RecoveryStore;
import org.ehcache.spi.resilience.ResilienceStrategy;
import org.ehcache.spi.resilience.ResilienceStrategyProvider;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.junit.Test;

import static org.ehcache.test.MockitoUtil.uncheckedGenericMock;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class DefaultResilienceStrategyProviderFactoryTest {

  @Test
  public void testNullGivesValidFactory() {
    ResilienceStrategyProvider provider = new DefaultResilienceStrategyProviderFactory().create(null);
    assertThat(provider.createResilienceStrategy("test", uncheckedGenericMock(CacheConfiguration.class), uncheckedGenericMock(RecoveryStore.class)), notNullValue());
  }

  @Test
  public void testWrongConfigTypeFails() {
    try {
      new DefaultResilienceStrategyProviderFactory().create(uncheckedGenericMock(ServiceCreationConfiguration.class));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

  @Test
  public void testSpecifiedConfigIsPassed() {
    ResilienceStrategy<?, ?> resilienceStrategy = mock(ResilienceStrategy.class);

    DefaultResilienceStrategyProviderConfiguration configuration = new DefaultResilienceStrategyProviderConfiguration();
    configuration.setDefaultResilienceStrategy(resilienceStrategy);
    ResilienceStrategyProvider provider = new DefaultResilienceStrategyProviderFactory().create(configuration);

    assertThat(provider.createResilienceStrategy("foo", uncheckedGenericMock(CacheConfiguration.class), uncheckedGenericMock(RecoveryStore.class)), sameInstance(resilienceStrategy));
  }
}
