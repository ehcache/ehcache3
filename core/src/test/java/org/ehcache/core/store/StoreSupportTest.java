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

package org.ehcache.core.store;

import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;
import org.ehcache.core.spi.ServiceLocator;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static org.ehcache.core.spi.ServiceLocator.dependencySet;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.*;

/**
 * Tests functionality of {@link StoreSupport} methods.
 */
public class StoreSupportTest {

  private final ResourceType<ResourcePool> anyResourceType = new ResourceType<ResourcePool>() {
    @Override
    public Class<ResourcePool> getResourcePoolClass() {
      return ResourcePool.class;
    }
    @Override
    public boolean isPersistable() {
      return false;
    }
    @Override
    public boolean requiresSerialization() {
      return false;
    }
    @Override
    public int getTierHeight() {
      return 10;
    }
    @Override
    public String toString() {
      return "anyResourceType";
    }
  };

  @Test
  public void testSelectStoreProvider() throws Exception {

    final TestBaseProvider expectedProvider = new PrimaryProvider1();
    Collection<TestBaseProvider> storeProviders = asList(
        new SecondaryProvider1(),
        new ZeroProvider(),
        expectedProvider
    );

    final ServiceLocator serviceLocator = dependencySet().with(storeProviders).build();
    final Store.Provider selectedProvider = StoreSupport.selectStoreProvider(serviceLocator,
        Collections.<ResourceType<?>>singleton(anyResourceType),
        Collections.<ServiceConfiguration<?>>emptyList());

    assertThat(selectedProvider, is(Matchers.<Store.Provider>sameInstance(expectedProvider)));

    for (final TestBaseProvider provider : storeProviders) {
      assertThat(provider.rankAccessCount.get(), is(1));
    }
  }

  @Test
  public void testSelectStoreProviderMultiple() throws Exception {

    final TestBaseProvider expectedProvider = new PrimaryProvider1();
    final Collection<TestBaseProvider> storeProviders = asList(
        new SecondaryProvider1(),
        new ZeroProvider(),
        expectedProvider,
        new SecondaryProvider2(),
        new PrimaryProvider2()
    );

    final ServiceLocator serviceLocator = dependencySet().with(storeProviders).build();

    try {
      StoreSupport.selectStoreProvider(serviceLocator,
          Collections.<ResourceType<?>>singleton(anyResourceType),
          Collections.<ServiceConfiguration<?>>emptyList());
      fail();
    } catch (IllegalStateException e) {
      // expected
      assertThat(e.getMessage(), startsWith("Multiple Store.Providers "));
    }

    for (final TestBaseProvider provider : storeProviders) {
      assertThat(provider.rankAccessCount.get(), is(1));
    }
  }

  @Test
  public void testSelectStoreProviderNoProviders() throws Exception {
    try {
      StoreSupport.selectStoreProvider(dependencySet().build(),
          Collections.<ResourceType<?>>singleton(anyResourceType),
          Collections.<ServiceConfiguration<?>>emptyList());
      fail();
    } catch (IllegalStateException e) {
      // expected
      assertThat(e.getMessage(), startsWith("No Store.Provider "));
    }
  }

  @Test
  public void testSelectStoreProviderNoHits() throws Exception {

    final ResourceType<ResourcePool> otherResourceType = new ResourceType<ResourcePool>() {
      @Override
      public Class<ResourcePool> getResourcePoolClass() {
        return ResourcePool.class;
      }
      @Override
      public boolean isPersistable() {
        return true;
      }
      @Override
      public boolean requiresSerialization() {
        return true;
      }
      @Override
      public int getTierHeight() {
        return 10;
      }
    };

    final Collection<TestBaseProvider> storeProviders = asList(
        new SecondaryProvider1(),
        new ZeroProvider(),
        new PrimaryProvider1()
    );

    final ServiceLocator serviceLocator = dependencySet().with(storeProviders).build();
    try {
      StoreSupport.selectStoreProvider(serviceLocator,
          Collections.<ResourceType<?>>singleton(otherResourceType),
          Collections.<ServiceConfiguration<?>>emptyList());
      fail();
    } catch (IllegalStateException e) {
      // expected
      assertThat(e.getMessage(), startsWith("No Store.Provider "));
    }

    for (final TestBaseProvider provider : storeProviders) {
      assertThat(provider.rankAccessCount.get(), is(1));
    }
  }

  private final class ZeroProvider extends TestBaseProvider {
    public ZeroProvider() {
      super(0);
    }
  }

  private final class SecondaryProvider1 extends TestBaseProvider {
    public SecondaryProvider1() {
      super(50);
    }
  }

  private final class SecondaryProvider2 extends TestBaseProvider {
    public SecondaryProvider2() {
      super(50);
    }
  }

  private final class PrimaryProvider1 extends TestBaseProvider {
    public PrimaryProvider1() {
      super(100);
    }
  }

  private final class PrimaryProvider2 extends TestBaseProvider {
    public PrimaryProvider2() {
      super(100);
    }
  }

  private abstract class TestBaseProvider implements Store.Provider {
    final int rank;
    final AtomicInteger rankAccessCount = new AtomicInteger(0);

    public TestBaseProvider(final int rank) {
      this.rank = rank;
    }

    @Override
    public <K, V> Store<K, V> createStore(final Store.Configuration<K, V> storeConfig, final ServiceConfiguration<?>... serviceConfigs) {
      throw new UnsupportedOperationException("TestBaseProvider.createStore not implemented");
    }

    @Override
    public void releaseStore(final Store<?, ?> resource) {
      throw new UnsupportedOperationException("TestBaseProvider.releaseStore not implemented");
    }

    @Override
    public void initStore(final Store<?, ?> resource) {
      throw new UnsupportedOperationException("TestBaseProvider.initStore not implemented");
    }

    @Override
    public int rank(final Set<ResourceType<?>> resourceTypes, final Collection<ServiceConfiguration<?>> serviceConfigs) {
      assertThat(resourceTypes, is(not(nullValue())));
      assertThat(serviceConfigs, is(not(nullValue())));
      rankAccessCount.incrementAndGet();

      if (resourceTypes.contains(anyResourceType)) {
        return this.rank;
      }

      return 0;
    }

    @Override
    public void start(final ServiceProvider<Service> serviceProvider) {
      throw new UnsupportedOperationException("TestBaseProvider.start not implemented");
    }

    @Override
    public void stop() {
      throw new UnsupportedOperationException("TestBaseProvider.stop not implemented");
    }
  }
}
