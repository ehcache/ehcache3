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

import static org.ehcache.core.spi.ServiceLocatorUtils.withServiceLocator;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.ehcache.test.MockitoUtil.uncheckedGenericMock;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultResilienceStrategyProviderTest {

  @Test
  public void testDefaultInstanceReturned() throws Exception {
    ResilienceStrategy<?, ?> resilienceStrategy = mock(ResilienceStrategy.class);

    DefaultResilienceStrategyProviderConfiguration configuration = new DefaultResilienceStrategyProviderConfiguration();
    configuration.setDefaultResilienceStrategy(resilienceStrategy);

    withServiceLocator(new DefaultResilienceStrategyProvider(configuration), provider -> {
      assertThat(provider.createResilienceStrategy("foo", uncheckedGenericMock(CacheConfiguration.class), uncheckedGenericMock(RecoveryStore.class)), sameInstance(resilienceStrategy));
    });
  }

  @Test
  public void testDefaultLoaderWriterInstanceReturned() throws Exception {
    ResilienceStrategy<?, ?> resilienceStrategy = mock(ResilienceStrategy.class);

    DefaultResilienceStrategyProviderConfiguration configuration = new DefaultResilienceStrategyProviderConfiguration();
    configuration.setDefaultLoaderWriterResilienceStrategy(resilienceStrategy);

    withServiceLocator(new DefaultResilienceStrategyProvider(configuration), provider -> {
      assertThat(provider.createResilienceStrategy("foo", uncheckedGenericMock(CacheConfiguration.class), uncheckedGenericMock(RecoveryStore.class), uncheckedGenericMock(CacheLoaderWriter.class)), sameInstance(resilienceStrategy));
    });
  }

  @Test
  public void testDefaultInstanceConstructed() throws Exception {
    DefaultResilienceStrategyProviderConfiguration configuration = new DefaultResilienceStrategyProviderConfiguration();
    configuration.setDefaultResilienceStrategy(TestResilienceStrategy.class, "FooBar");

    withServiceLocator(new DefaultResilienceStrategyProvider(configuration), provider -> {
      ResilienceStrategy<?, ?> resilienceStrategy = provider.createResilienceStrategy("foo", uncheckedGenericMock(CacheConfiguration.class), uncheckedGenericMock(RecoveryStore.class));
      assertThat(resilienceStrategy, instanceOf(TestResilienceStrategy.class));
      assertThat(((TestResilienceStrategy) resilienceStrategy).message, is("FooBar"));
    });
  }

  @Test
  public void testDefaultLoaderWriterInstanceConstructed() throws Exception {
    DefaultResilienceStrategyProviderConfiguration configuration = new DefaultResilienceStrategyProviderConfiguration();
    configuration.setDefaultLoaderWriterResilienceStrategy(TestResilienceStrategy.class, "FooBar");

    withServiceLocator(new DefaultResilienceStrategyProvider(configuration), provider -> {
      ResilienceStrategy<?, ?> resilienceStrategy = provider.createResilienceStrategy("foo", uncheckedGenericMock(CacheConfiguration.class), uncheckedGenericMock(RecoveryStore.class), uncheckedGenericMock(CacheLoaderWriter.class));
      assertThat(resilienceStrategy, instanceOf(TestResilienceStrategy.class));
      assertThat(((TestResilienceStrategy) resilienceStrategy).message, is("FooBar"));
    });
  }

  @Test
  public void testPreconfiguredInstanceReturned() throws Exception {
    ResilienceStrategy<?, ?> resilienceStrategy = mock(ResilienceStrategy.class);

    DefaultResilienceStrategyProviderConfiguration configuration = new DefaultResilienceStrategyProviderConfiguration();
    configuration.addResilienceStrategyFor("foo", resilienceStrategy);

    withServiceLocator(new DefaultResilienceStrategyProvider(configuration), provider -> {
      assertThat(provider.createResilienceStrategy("foo", uncheckedGenericMock(CacheConfiguration.class), uncheckedGenericMock(RecoveryStore.class)), sameInstance(resilienceStrategy));
    });
  }

  @Test
  public void testPreconfiguredLoaderWriterInstanceReturned() throws Exception {
    ResilienceStrategy<?, ?> resilienceStrategy = mock(ResilienceStrategy.class);

    DefaultResilienceStrategyProviderConfiguration configuration = new DefaultResilienceStrategyProviderConfiguration();
    configuration.addResilienceStrategyFor("foo", resilienceStrategy);

    withServiceLocator(new DefaultResilienceStrategyProvider(configuration), provider -> {
      assertThat(provider.createResilienceStrategy("foo", uncheckedGenericMock(CacheConfiguration.class), uncheckedGenericMock(RecoveryStore.class), uncheckedGenericMock(CacheLoaderWriter.class)), sameInstance(resilienceStrategy));
    });
  }

  @Test
  public void testPreconfiguredInstanceConstructed() throws Exception {
    DefaultResilienceStrategyProviderConfiguration configuration = new DefaultResilienceStrategyProviderConfiguration();
    configuration.addResilienceStrategyFor("foo", TestResilienceStrategy.class, "FooBar");

    withServiceLocator(new DefaultResilienceStrategyProvider(configuration), provider -> {
      ResilienceStrategy<?, ?> resilienceStrategy = provider.createResilienceStrategy("foo", uncheckedGenericMock(CacheConfiguration.class), uncheckedGenericMock(RecoveryStore.class));
      assertThat(resilienceStrategy, instanceOf(TestResilienceStrategy.class));
      assertThat(((TestResilienceStrategy) resilienceStrategy).message, is("FooBar"));
    });
  }

  @Test
  public void testPreconfiguredLoaderWriterInstanceConstructed() throws Exception {
    DefaultResilienceStrategyProviderConfiguration configuration = new DefaultResilienceStrategyProviderConfiguration();
    configuration.addResilienceStrategyFor("foo", TestResilienceStrategy.class, "FooBar");

    withServiceLocator(new DefaultResilienceStrategyProvider(configuration), provider -> {
      ResilienceStrategy<?, ?> resilienceStrategy = provider.createResilienceStrategy("foo", uncheckedGenericMock(CacheConfiguration.class), uncheckedGenericMock(RecoveryStore.class), uncheckedGenericMock(CacheLoaderWriter.class));
      assertThat(resilienceStrategy, instanceOf(TestResilienceStrategy.class));
      assertThat(((TestResilienceStrategy) resilienceStrategy).message, is("FooBar"));
    });
  }


  @Test
  public void testProvidedInstanceReturned() throws Exception {
    ResilienceStrategy<?, ?> resilienceStrategy = mock(ResilienceStrategy.class);

    DefaultResilienceStrategyProviderConfiguration configuration = new DefaultResilienceStrategyProviderConfiguration();

    withServiceLocator(new DefaultResilienceStrategyProvider(configuration), provider -> {
      CacheConfiguration<?, ?> cacheConfiguration = mock(CacheConfiguration.class);
      when(cacheConfiguration.getServiceConfigurations()).thenReturn(Collections.singleton(new DefaultResilienceStrategyConfiguration(resilienceStrategy)));

      assertThat(provider.createResilienceStrategy("foo", cacheConfiguration, uncheckedGenericMock(RecoveryStore.class)), sameInstance(resilienceStrategy));
    });
  }

  @Test
  public void testProvidedLoaderWriterInstanceReturned() throws Exception {
    ResilienceStrategy<?, ?> resilienceStrategy = mock(ResilienceStrategy.class);

    DefaultResilienceStrategyProviderConfiguration configuration = new DefaultResilienceStrategyProviderConfiguration();

    withServiceLocator(new DefaultResilienceStrategyProvider(configuration), provider -> {
      CacheConfiguration<?, ?> cacheConfiguration = mock(CacheConfiguration.class);
      when(cacheConfiguration.getServiceConfigurations()).thenReturn(Collections.singleton(new DefaultResilienceStrategyConfiguration(resilienceStrategy)));

      assertThat(provider.createResilienceStrategy("foo", cacheConfiguration, uncheckedGenericMock(RecoveryStore.class), uncheckedGenericMock(CacheLoaderWriter.class)), sameInstance(resilienceStrategy));
    });
  }

  @Test
  public void testProvidedInstanceConstructed() throws Exception {
    DefaultResilienceStrategyProviderConfiguration configuration = new DefaultResilienceStrategyProviderConfiguration();

    withServiceLocator(new DefaultResilienceStrategyProvider(configuration), provider -> {
      CacheConfiguration<?, ?> cacheConfiguration = mock(CacheConfiguration.class);
      when(cacheConfiguration.getServiceConfigurations()).thenReturn(Collections.singleton(new DefaultResilienceStrategyConfiguration(TestResilienceStrategy.class, "FooBar")));

      ResilienceStrategy<?, ?> resilienceStrategy = provider.createResilienceStrategy("foo", cacheConfiguration, uncheckedGenericMock(RecoveryStore.class));
      assertThat(resilienceStrategy, instanceOf(TestResilienceStrategy.class));
      assertThat(((TestResilienceStrategy) resilienceStrategy).message, is("FooBar"));
    });
  }

  @Test
  public void testProvidedLoaderWriterInstanceConstructed() throws Exception {
    DefaultResilienceStrategyProviderConfiguration configuration = new DefaultResilienceStrategyProviderConfiguration();

    withServiceLocator(new DefaultResilienceStrategyProvider(configuration), provider -> {
      CacheConfiguration<?, ?> cacheConfiguration = mock(CacheConfiguration.class);
      when(cacheConfiguration.getServiceConfigurations()).thenReturn(Collections.singleton(new DefaultResilienceStrategyConfiguration(TestResilienceStrategy.class, "FooBar")));

      ResilienceStrategy<?, ?> resilienceStrategy = provider.createResilienceStrategy("foo", cacheConfiguration, uncheckedGenericMock(RecoveryStore.class), uncheckedGenericMock(CacheLoaderWriter.class));
      assertThat(resilienceStrategy, instanceOf(TestResilienceStrategy.class));
      assertThat(((TestResilienceStrategy) resilienceStrategy).message, is("FooBar"));
    });
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
