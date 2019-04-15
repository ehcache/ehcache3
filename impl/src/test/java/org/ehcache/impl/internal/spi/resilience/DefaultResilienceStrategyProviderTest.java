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
package org.ehcache.impl.internal.spi.resilience;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.impl.config.resilience.DefaultResilienceStrategyConfiguration;
import org.ehcache.impl.config.resilience.DefaultResilienceStrategyProviderConfiguration;
import org.ehcache.impl.internal.resilience.RobustResilienceStrategy;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.resilience.RecoveryStore;
import org.ehcache.spi.resilience.ResilienceStrategy;
import org.junit.Test;

import java.util.Collections;

import static org.ehcache.test.MockitoUtil.mock;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsSame.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

public class DefaultResilienceStrategyProviderTest {

  @Test
  public void testDefaultInstanceReturned() {
    ResilienceStrategy<?, ?> resilienceStrategy = mock(ResilienceStrategy.class);

    DefaultResilienceStrategyProviderConfiguration configuration = new DefaultResilienceStrategyProviderConfiguration();
    configuration.setDefaultResilienceStrategy(resilienceStrategy);

    DefaultResilienceStrategyProvider provider = new DefaultResilienceStrategyProvider(configuration);

    assertThat(provider.createResilienceStrategy("foo", mock(CacheConfiguration.class), mock(RecoveryStore.class)), sameInstance(resilienceStrategy));
  }

  @Test
  public void testDefaultLoaderWriterInstanceReturned() {
    ResilienceStrategy<?, ?> resilienceStrategy = mock(ResilienceStrategy.class);

    DefaultResilienceStrategyProviderConfiguration configuration = new DefaultResilienceStrategyProviderConfiguration();
    configuration.setDefaultLoaderWriterResilienceStrategy(resilienceStrategy);

    DefaultResilienceStrategyProvider provider = new DefaultResilienceStrategyProvider(configuration);

    assertThat(provider.createResilienceStrategy("foo", mock(CacheConfiguration.class), mock(RecoveryStore.class), mock(CacheLoaderWriter.class)), sameInstance(resilienceStrategy));
  }

  @Test
  public void testDefaultInstanceConstructed() {
    DefaultResilienceStrategyProviderConfiguration configuration = new DefaultResilienceStrategyProviderConfiguration();
    configuration.setDefaultResilienceStrategy(TestResilienceStrategy.class, "FooBar");

    DefaultResilienceStrategyProvider provider = new DefaultResilienceStrategyProvider(configuration);

    ResilienceStrategy<?, ?> resilienceStrategy = provider.createResilienceStrategy("foo", mock(CacheConfiguration.class), mock(RecoveryStore.class));
    assertThat(resilienceStrategy, instanceOf(TestResilienceStrategy.class));
    assertThat(((TestResilienceStrategy) resilienceStrategy).message, is("FooBar"));
  }

  @Test
  public void testDefaultLoaderWriterInstanceConstructed() {
    DefaultResilienceStrategyProviderConfiguration configuration = new DefaultResilienceStrategyProviderConfiguration();
    configuration.setDefaultLoaderWriterResilienceStrategy(TestResilienceStrategy.class, "FooBar");

    DefaultResilienceStrategyProvider provider = new DefaultResilienceStrategyProvider(configuration);

    ResilienceStrategy<?, ?> resilienceStrategy = provider.createResilienceStrategy("foo", mock(CacheConfiguration.class), mock(RecoveryStore.class), mock(CacheLoaderWriter.class));
    assertThat(resilienceStrategy, instanceOf(TestResilienceStrategy.class));
    assertThat(((TestResilienceStrategy) resilienceStrategy).message, is("FooBar"));
  }

  @Test
  public void testPreconfiguredInstanceReturned() {
    ResilienceStrategy<?, ?> resilienceStrategy = mock(ResilienceStrategy.class);

    DefaultResilienceStrategyProviderConfiguration configuration = new DefaultResilienceStrategyProviderConfiguration();
    configuration.addResilienceStrategyFor("foo", resilienceStrategy);

    DefaultResilienceStrategyProvider provider = new DefaultResilienceStrategyProvider(configuration);

    assertThat(provider.createResilienceStrategy("foo", mock(CacheConfiguration.class), mock(RecoveryStore.class)), sameInstance(resilienceStrategy));
  }

  @Test
  public void testPreconfiguredLoaderWriterInstanceReturned() {
    ResilienceStrategy<?, ?> resilienceStrategy = mock(ResilienceStrategy.class);

    DefaultResilienceStrategyProviderConfiguration configuration = new DefaultResilienceStrategyProviderConfiguration();
    configuration.addResilienceStrategyFor("foo", resilienceStrategy);

    DefaultResilienceStrategyProvider provider = new DefaultResilienceStrategyProvider(configuration);

    assertThat(provider.createResilienceStrategy("foo", mock(CacheConfiguration.class), mock(RecoveryStore.class), mock(CacheLoaderWriter.class)), sameInstance(resilienceStrategy));
  }

  @Test
  public void testPreconfiguredInstanceConstructed() {
    DefaultResilienceStrategyProviderConfiguration configuration = new DefaultResilienceStrategyProviderConfiguration();
    configuration.addResilienceStrategyFor("foo", TestResilienceStrategy.class, "FooBar");

    DefaultResilienceStrategyProvider provider = new DefaultResilienceStrategyProvider(configuration);

    ResilienceStrategy<?, ?> resilienceStrategy = provider.createResilienceStrategy("foo", mock(CacheConfiguration.class), mock(RecoveryStore.class));
    assertThat(resilienceStrategy, instanceOf(TestResilienceStrategy.class));
    assertThat(((TestResilienceStrategy) resilienceStrategy).message, is("FooBar"));
  }

  @Test
  public void testPreconfiguredLoaderWriterInstanceConstructed() {
    DefaultResilienceStrategyProviderConfiguration configuration = new DefaultResilienceStrategyProviderConfiguration();
    configuration.addResilienceStrategyFor("foo", TestResilienceStrategy.class, "FooBar");

    DefaultResilienceStrategyProvider provider = new DefaultResilienceStrategyProvider(configuration);

    ResilienceStrategy<?, ?> resilienceStrategy = provider.createResilienceStrategy("foo", mock(CacheConfiguration.class), mock(RecoveryStore.class), mock(CacheLoaderWriter.class));
    assertThat(resilienceStrategy, instanceOf(TestResilienceStrategy.class));
    assertThat(((TestResilienceStrategy) resilienceStrategy).message, is("FooBar"));
  }


  @Test
  public void testProvidedInstanceReturned() {
    ResilienceStrategy<?, ?> resilienceStrategy = mock(ResilienceStrategy.class);

    DefaultResilienceStrategyProviderConfiguration configuration = new DefaultResilienceStrategyProviderConfiguration();
    DefaultResilienceStrategyProvider provider = new DefaultResilienceStrategyProvider(configuration);

    CacheConfiguration<?, ?> cacheConfiguration = mock(CacheConfiguration.class);
    when(cacheConfiguration.getServiceConfigurations()).thenReturn(Collections.singleton(new DefaultResilienceStrategyConfiguration(resilienceStrategy)));

    assertThat(provider.createResilienceStrategy("foo", cacheConfiguration, mock(RecoveryStore.class)), sameInstance(resilienceStrategy));
  }

  @Test
  public void testProvidedLoaderWriterInstanceReturned() {
    ResilienceStrategy<?, ?> resilienceStrategy = mock(ResilienceStrategy.class);

    DefaultResilienceStrategyProviderConfiguration configuration = new DefaultResilienceStrategyProviderConfiguration();
    DefaultResilienceStrategyProvider provider = new DefaultResilienceStrategyProvider(configuration);

    CacheConfiguration<?, ?> cacheConfiguration = mock(CacheConfiguration.class);
    when(cacheConfiguration.getServiceConfigurations()).thenReturn(Collections.singleton(new DefaultResilienceStrategyConfiguration(resilienceStrategy)));

    assertThat(provider.createResilienceStrategy("foo", cacheConfiguration, mock(RecoveryStore.class), mock(CacheLoaderWriter.class)), sameInstance(resilienceStrategy));
  }

  @Test
  public void testProvidedInstanceConstructed() {
    DefaultResilienceStrategyProviderConfiguration configuration = new DefaultResilienceStrategyProviderConfiguration();
    DefaultResilienceStrategyProvider provider = new DefaultResilienceStrategyProvider(configuration);

    CacheConfiguration<?, ?> cacheConfiguration = mock(CacheConfiguration.class);
    when(cacheConfiguration.getServiceConfigurations()).thenReturn(Collections.singleton(new DefaultResilienceStrategyConfiguration(TestResilienceStrategy.class, "FooBar")));

    ResilienceStrategy<?, ?> resilienceStrategy = provider.createResilienceStrategy("foo", cacheConfiguration, mock(RecoveryStore.class));
    assertThat(resilienceStrategy, instanceOf(TestResilienceStrategy.class));
    assertThat(((TestResilienceStrategy) resilienceStrategy).message, is("FooBar"));
  }

  @Test
  public void testProvidedLoaderWriterInstanceConstructed() {
    DefaultResilienceStrategyProviderConfiguration configuration = new DefaultResilienceStrategyProviderConfiguration();
    DefaultResilienceStrategyProvider provider = new DefaultResilienceStrategyProvider(configuration);

    CacheConfiguration<?, ?> cacheConfiguration = mock(CacheConfiguration.class);
    when(cacheConfiguration.getServiceConfigurations()).thenReturn(Collections.singleton(new DefaultResilienceStrategyConfiguration(TestResilienceStrategy.class, "FooBar")));

    ResilienceStrategy<?, ?> resilienceStrategy = provider.createResilienceStrategy("foo", cacheConfiguration, mock(RecoveryStore.class), mock(CacheLoaderWriter.class));
    assertThat(resilienceStrategy, instanceOf(TestResilienceStrategy.class));
    assertThat(((TestResilienceStrategy) resilienceStrategy).message, is("FooBar"));
  }

  public static class TestResilienceStrategy<K, V> extends RobustResilienceStrategy<K, V> {

    public final String message;

    public TestResilienceStrategy(String message, RecoveryStore<K> store) {
      super(store);
      this.message = message;
    }

    public TestResilienceStrategy(String message, RecoveryStore<K> store, CacheLoaderWriter<K, V> loaderWriter) {
      super(store);
      this.message = message;
    }
  }
}
