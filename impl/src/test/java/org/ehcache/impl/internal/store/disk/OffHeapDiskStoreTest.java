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

package org.ehcache.impl.internal.store.disk;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.spi.service.CacheManagerProviderService;
import org.ehcache.core.statistics.DefaultStatisticsService;
import org.ehcache.core.store.StoreConfigurationImpl;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.core.statistics.LowerCachingTierOperationsOutcome;
import org.ehcache.CachePersistenceException;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.config.store.disk.OffHeapDiskStoreConfiguration;
import org.ehcache.impl.internal.store.offheap.portability.AssertingOffHeapValueHolderPortability;
import org.ehcache.impl.internal.store.offheap.portability.OffHeapValueHolderPortability;
import org.ehcache.impl.internal.events.TestStoreEventDispatcher;
import org.ehcache.impl.internal.executor.OnDemandExecutionService;
import org.ehcache.impl.internal.persistence.TestDiskResourceService;
import org.ehcache.impl.internal.store.offheap.AbstractOffHeapStore;
import org.ehcache.impl.internal.store.offheap.AbstractOffHeapStoreTest;
import org.ehcache.impl.internal.spi.serialization.DefaultSerializationProvider;
import org.ehcache.core.spi.time.SystemTimeSource;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.spi.ServiceLocator;
import org.ehcache.core.spi.store.Store;
import org.ehcache.impl.internal.util.UnmatchedResourceType;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.UnsupportedTypeException;
import org.ehcache.core.spi.service.FileBasedPersistenceContext;
import org.ehcache.spi.persistence.PersistableResourceService.PersistenceSpaceIdentifier;
import org.ehcache.test.MockitoUtil;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Answers;
import org.terracotta.context.query.Matcher;
import org.terracotta.context.query.Query;
import org.terracotta.context.query.QueryBuilder;
import org.terracotta.org.junit.rules.TemporaryFolder;
import org.terracotta.statistics.OperationStatistic;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.singleton;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.persistence;
import static org.ehcache.config.builders.ExpiryPolicyBuilder.noExpiration;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.config.units.MemoryUnit.MB;
import static org.ehcache.core.spi.ServiceLocator.dependencySet;
import static org.ehcache.impl.config.store.disk.OffHeapDiskStoreConfiguration.DEFAULT_DISK_SEGMENTS;
import static org.ehcache.impl.config.store.disk.OffHeapDiskStoreConfiguration.DEFAULT_WRITER_CONCURRENCY;
import static org.ehcache.impl.internal.spi.TestServiceProvider.providerContaining;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.terracotta.context.ContextManager.nodeFor;
import static org.terracotta.context.query.Matchers.attributes;
import static org.terracotta.context.query.Matchers.context;
import static org.terracotta.context.query.Matchers.hasAttribute;

public class OffHeapDiskStoreTest extends AbstractOffHeapStoreTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public final TestDiskResourceService diskResourceService = new TestDiskResourceService();

  @Test
  public void testRecovery() throws StoreAccessException, IOException {
    OffHeapDiskStore<String, String> offHeapDiskStore = createAndInitStore(SystemTimeSource.INSTANCE, noExpiration());
    try {
      offHeapDiskStore.put("key1", "value1");
      assertThat(offHeapDiskStore.get("key1"), notNullValue());

      OffHeapDiskStore.Provider.close(offHeapDiskStore);

      OffHeapDiskStore.Provider.init(offHeapDiskStore);
      assertThat(offHeapDiskStore.get("key1"), notNullValue());
    } finally {
      destroyStore(offHeapDiskStore);
    }
  }

  @Test
  public void testRecoveryFailureWhenValueTypeChangesToIncompatibleClass() throws Exception {
    OffHeapDiskStore.Provider provider = new OffHeapDiskStore.Provider();
    ServiceLocator serviceLocator = dependencySet().with(diskResourceService).with(provider)
      .with(mock(CacheManagerProviderService.class, Answers.RETURNS_DEEP_STUBS)).build();
    serviceLocator.startAllServices();

    CacheConfiguration<Long, String> cacheConfiguration = MockitoUtil.mock(CacheConfiguration.class);
    when(cacheConfiguration.getResourcePools()).thenReturn(newResourcePoolsBuilder().disk(1, MemoryUnit.MB, false).build());
    PersistenceSpaceIdentifier<?> space = diskResourceService.getPersistenceSpaceIdentifier("cache", cacheConfiguration);

    {
      @SuppressWarnings("unchecked")
      Store.Configuration<Long, String> storeConfig1 = MockitoUtil.mock(Store.Configuration.class);
      when(storeConfig1.getKeyType()).thenReturn(Long.class);
      when(storeConfig1.getValueType()).thenReturn(String.class);
      when(storeConfig1.getResourcePools()).thenReturn(ResourcePoolsBuilder.newResourcePoolsBuilder()
          .disk(10, MB)
          .build());
      when(storeConfig1.getDispatcherConcurrency()).thenReturn(1);

      OffHeapDiskStore<Long, String> offHeapDiskStore1 = provider.createStore(storeConfig1, space);
      provider.initStore(offHeapDiskStore1);

      destroyStore(offHeapDiskStore1);
    }

    {
      @SuppressWarnings("unchecked")
      Store.Configuration<Long, Serializable> storeConfig2 = MockitoUtil.mock(Store.Configuration.class);
      when(storeConfig2.getKeyType()).thenReturn(Long.class);
      when(storeConfig2.getValueType()).thenReturn(Serializable.class);
      when(storeConfig2.getResourcePools()).thenReturn(ResourcePoolsBuilder.newResourcePoolsBuilder()
          .disk(10, MB)
          .build());
      when(storeConfig2.getDispatcherConcurrency()).thenReturn(1);
      when(storeConfig2.getClassLoader()).thenReturn(ClassLoader.getSystemClassLoader());


      OffHeapDiskStore<Long, Serializable> offHeapDiskStore2 = provider.createStore(storeConfig2, space);
      try {
        provider.initStore(offHeapDiskStore2);
        fail("expected IllegalArgumentException");
      } catch (IllegalArgumentException e) {
        // expected
      }

      destroyStore(offHeapDiskStore2);
    }
  }

  @Test
  public void testRecoveryWithArrayType() throws Exception {
    OffHeapDiskStore.Provider provider = new OffHeapDiskStore.Provider();
    ServiceLocator serviceLocator = dependencySet().with(diskResourceService).with(provider)
      .with(mock(CacheManagerProviderService.class, Answers.RETURNS_DEEP_STUBS)).build();
    serviceLocator.startAllServices();

    CacheConfiguration<?, ?> cacheConfiguration = MockitoUtil.mock(CacheConfiguration.class);
    when(cacheConfiguration.getResourcePools()).thenReturn(newResourcePoolsBuilder().disk(1, MemoryUnit.MB, false).build());
    PersistenceSpaceIdentifier<?> space = diskResourceService.getPersistenceSpaceIdentifier("cache", cacheConfiguration);

    {
      @SuppressWarnings("unchecked")
      Store.Configuration<Long, Object[]> storeConfig1 = MockitoUtil.mock(Store.Configuration.class);
      when(storeConfig1.getKeyType()).thenReturn(Long.class);
      when(storeConfig1.getValueType()).thenReturn(Object[].class);
      when(storeConfig1.getResourcePools()).thenReturn(ResourcePoolsBuilder.newResourcePoolsBuilder()
          .disk(10, MB)
          .build());
      when(storeConfig1.getDispatcherConcurrency()).thenReturn(1);

      OffHeapDiskStore<Long, Object[]> offHeapDiskStore1 = provider.createStore(storeConfig1, space);
      provider.initStore(offHeapDiskStore1);

      destroyStore(offHeapDiskStore1);
    }

    {
      @SuppressWarnings("unchecked")
      Store.Configuration<Long, Object[]> storeConfig2 = MockitoUtil.mock(Store.Configuration.class);
      when(storeConfig2.getKeyType()).thenReturn(Long.class);
      when(storeConfig2.getValueType()).thenReturn(Object[].class);
      when(storeConfig2.getResourcePools()).thenReturn(ResourcePoolsBuilder.newResourcePoolsBuilder()
          .disk(10, MB)
          .build());
      when(storeConfig2.getDispatcherConcurrency()).thenReturn(1);
      when(storeConfig2.getClassLoader()).thenReturn(ClassLoader.getSystemClassLoader());


      OffHeapDiskStore<Long, Object[]> offHeapDiskStore2 = provider.createStore(storeConfig2, space);
      provider.initStore(offHeapDiskStore2);

      destroyStore(offHeapDiskStore2);
    }
  }

  @Test
  public void testProvidingOffHeapDiskStoreConfiguration() throws Exception {
    OffHeapDiskStore.Provider provider = new OffHeapDiskStore.Provider();
    ServiceLocator serviceLocator = dependencySet().with(diskResourceService).with(provider)
      .with(mock(CacheManagerProviderService.class, Answers.RETURNS_DEEP_STUBS)).build();
    serviceLocator.startAllServices();

    CacheConfiguration<?, ?> cacheConfiguration = MockitoUtil.mock(CacheConfiguration.class);
    when(cacheConfiguration.getResourcePools()).thenReturn(newResourcePoolsBuilder().disk(1, MemoryUnit.MB, false).build());
    PersistenceSpaceIdentifier<?> space = diskResourceService.getPersistenceSpaceIdentifier("cache", cacheConfiguration);

    @SuppressWarnings("unchecked")
    Store.Configuration<Long, Object[]> storeConfig1 = MockitoUtil.mock(Store.Configuration.class);
    when(storeConfig1.getKeyType()).thenReturn(Long.class);
    when(storeConfig1.getValueType()).thenReturn(Object[].class);
    when(storeConfig1.getResourcePools()).thenReturn(ResourcePoolsBuilder.newResourcePoolsBuilder()
      .disk(10, MB)
      .build());
    when(storeConfig1.getDispatcherConcurrency()).thenReturn(1);

    OffHeapDiskStore<Long, Object[]> offHeapDiskStore1 = provider.createStore(
            storeConfig1, space, new OffHeapDiskStoreConfiguration("pool", 2, 4));
    assertThat(offHeapDiskStore1.getThreadPoolAlias(), is("pool"));
    assertThat(offHeapDiskStore1.getWriterConcurrency(), is(2));
    assertThat(offHeapDiskStore1.getDiskSegments(), is(4));
  }

  @Override
  protected OffHeapDiskStore<String, String> createAndInitStore(final TimeSource timeSource, final ExpiryPolicy<? super String, ? super String> expiry) {
    try {
      SerializationProvider serializationProvider = new DefaultSerializationProvider(null);
      serializationProvider.start(providerContaining(diskResourceService));
      ClassLoader classLoader = getClass().getClassLoader();
      Serializer<String> keySerializer = serializationProvider.createKeySerializer(String.class, classLoader);
      Serializer<String> valueSerializer = serializationProvider.createValueSerializer(String.class, classLoader);
      StoreConfigurationImpl<String, String> storeConfiguration = new StoreConfigurationImpl<>(String.class, String.class,
        null, classLoader, expiry, null, 0, true, keySerializer, valueSerializer, null, false);
      OffHeapDiskStore<String, String> offHeapStore = new OffHeapDiskStore<String, String>(
        getPersistenceContext(),
        new OnDemandExecutionService(), null, DEFAULT_WRITER_CONCURRENCY, DEFAULT_DISK_SEGMENTS,
        storeConfiguration, timeSource,
        new TestStoreEventDispatcher<>(),
        MB.toBytes(1), new DefaultStatisticsService()) {
        @Override
        protected OffHeapValueHolderPortability<String> createValuePortability(Serializer<String> serializer) {
          return new AssertingOffHeapValueHolderPortability<>(serializer);
        }
      };
      OffHeapDiskStore.Provider.init(offHeapStore);
      return offHeapStore;
    } catch (UnsupportedTypeException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  protected OffHeapDiskStore<String, byte[]> createAndInitStore(TimeSource timeSource, ExpiryPolicy<? super String, ? super byte[]> expiry, EvictionAdvisor<? super String, ? super byte[]> evictionAdvisor) {
    try {
      SerializationProvider serializationProvider = new DefaultSerializationProvider(null);
      serializationProvider.start(providerContaining(diskResourceService));
      ClassLoader classLoader = getClass().getClassLoader();
      Serializer<String> keySerializer = serializationProvider.createKeySerializer(String.class, classLoader);
      Serializer<byte[]> valueSerializer = serializationProvider.createValueSerializer(byte[].class, classLoader);
      StoreConfigurationImpl<String, byte[]> storeConfiguration = new StoreConfigurationImpl<>(String.class, byte[].class,
        evictionAdvisor, getClass().getClassLoader(), expiry, null, 0, true, keySerializer, valueSerializer, null, false);
      OffHeapDiskStore<String, byte[]> offHeapStore = new OffHeapDiskStore<String, byte[]>(
        getPersistenceContext(),
        new OnDemandExecutionService(), null, DEFAULT_WRITER_CONCURRENCY, DEFAULT_DISK_SEGMENTS,
        storeConfiguration, timeSource,
        new TestStoreEventDispatcher<>(),
        MB.toBytes(1), new DefaultStatisticsService()) {
        @Override
        protected OffHeapValueHolderPortability<byte[]> createValuePortability(Serializer<byte[]> serializer) {
          return new AssertingOffHeapValueHolderPortability<>(serializer);
        }
      };
      OffHeapDiskStore.Provider.init(offHeapStore);
      return offHeapStore;
    } catch (UnsupportedTypeException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  protected void destroyStore(AbstractOffHeapStore<?, ?> store) {
    try {
      OffHeapDiskStore.Provider.close((OffHeapDiskStore<?, ?>) store);
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  @Test
  public void testStoreInitFailsWithoutLocalPersistenceService() throws Exception {
    OffHeapDiskStore.Provider provider = new OffHeapDiskStore.Provider();
    try {
      dependencySet().with(provider).build();
      fail("IllegalStateException expected");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), containsString("Failed to find provider with satisfied dependency set for interface" +
        " org.ehcache.core.spi.service.DiskResourceService"));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAuthoritativeRank() throws Exception {
    OffHeapDiskStore.Provider provider = new OffHeapDiskStore.Provider();
    assertThat(provider.rankAuthority(ResourceType.Core.DISK, EMPTY_LIST), is(1));
    assertThat(provider.rankAuthority(new UnmatchedResourceType(), EMPTY_LIST), is(0));
  }

  @Test
  public void testRank() throws Exception {
    OffHeapDiskStore.Provider provider = new OffHeapDiskStore.Provider();

    assertRank(provider, 1, ResourceType.Core.DISK);
    assertRank(provider, 0, ResourceType.Core.HEAP);
    assertRank(provider, 0, ResourceType.Core.OFFHEAP);
    assertRank(provider, 0, ResourceType.Core.DISK, ResourceType.Core.OFFHEAP);
    assertRank(provider, 0, ResourceType.Core.DISK, ResourceType.Core.HEAP);
    assertRank(provider, 0, ResourceType.Core.OFFHEAP, ResourceType.Core.HEAP);
    assertRank(provider, 0, ResourceType.Core.DISK, ResourceType.Core.OFFHEAP, ResourceType.Core.HEAP);

    assertRank(provider, 0, (ResourceType<ResourcePool>) new UnmatchedResourceType());
    assertRank(provider, 0, ResourceType.Core.DISK, new UnmatchedResourceType());
  }

  private void assertRank(final Store.Provider provider, final int expectedRank, final ResourceType<?>... resources) {
    assertThat(provider.rank(
      new HashSet<>(Arrays.asList(resources)),
        Collections.emptyList()),
        is(expectedRank));
  }

  private FileBasedPersistenceContext getPersistenceContext() {
    try {
      CacheConfiguration<?, ?> cacheConfiguration = MockitoUtil.mock(CacheConfiguration.class);
      when(cacheConfiguration.getResourcePools()).thenReturn(newResourcePoolsBuilder().disk(1, MB, false).build());
      PersistenceSpaceIdentifier<?> space = diskResourceService.getPersistenceSpaceIdentifier("cache", cacheConfiguration);
      return diskResourceService.createPersistenceContextWithin(space, "store");
    } catch (CachePersistenceException e) {
      throw new AssertionError(e);
    }
  }

  @Test
  public void diskStoreShrinkingTest() throws Exception {

    try (CacheManager manager = newCacheManagerBuilder()
      .with(persistence(temporaryFolder.newFolder("disk-stores").getAbsolutePath()))
      .build(true)) {

      CacheConfigurationBuilder<Long, CacheValue> cacheConfigurationBuilder = newCacheConfigurationBuilder(Long.class, CacheValue.class,
        heap(1000).offheap(10, MB).disk(20, MB))
        .withLoaderWriter(new CacheLoaderWriter<Long, CacheValue>() {
          @Override
          public CacheValue load(Long key) {
            return null;
          }

          @Override
          public Map<Long, CacheValue> loadAll(Iterable<? extends Long> keys) {
            return Collections.emptyMap();
          }

          @Override
          public void write(Long key, CacheValue value) {
          }

          @Override
          public void writeAll(Iterable<? extends Map.Entry<? extends Long, ? extends CacheValue>> entries) {
          }

          @Override
          public void delete(Long key) {
          }

          @Override
          public void deleteAll(Iterable<? extends Long> keys) {
          }
        });

      Cache<Long, CacheValue> cache = manager.createCache("test", cacheConfigurationBuilder);

      for (long i = 0; i < 100000; i++) {
        cache.put(i, new CacheValue((int) i));
      }

      Callable<Void> task = () -> {
        Random rndm = new Random();

        long start = System.nanoTime();
        while (System.nanoTime() < start + TimeUnit.SECONDS.toNanos(5)) {
          Long k = key(rndm);
          switch (rndm.nextInt(4)) {
            case 0: {
              CacheValue v = value(rndm);
              cache.putIfAbsent(k, v);
              break;
            }
            case 1: {
              CacheValue nv = value(rndm);
              CacheValue ov = value(rndm);
              cache.put(k, ov);
              cache.replace(k, nv);
              break;
            }
            case 2: {
              CacheValue nv = value(rndm);
              CacheValue ov = value(rndm);
              cache.put(k, ov);
              cache.replace(k, ov, nv);
              break;
            }
            case 3: {
              CacheValue v = value(rndm);
              cache.put(k, v);
              cache.remove(k, v);
              break;
            }
          }
        }
        return null;
      };

      ExecutorService executor = Executors.newCachedThreadPool();
      try {
        executor.invokeAll(Collections.nCopies(4, task));
      } finally {
        executor.shutdown();
      }

      Query invalidateAllQuery = QueryBuilder.queryBuilder()
        .descendants()
        .filter(context(attributes(hasAttribute("tags", new Matcher<Set<String>>() {
          @Override
          protected boolean matchesSafely(Set<String> object) {
            return object.contains("OffHeap");
          }
        }))))
        .filter(context(attributes(hasAttribute("name", "invalidateAll"))))
        .ensureUnique()
        .build();

      @SuppressWarnings("unchecked")
      OperationStatistic<LowerCachingTierOperationsOutcome.InvalidateAllOutcome> invalidateAll = (OperationStatistic<LowerCachingTierOperationsOutcome.InvalidateAllOutcome>) invalidateAllQuery
        .execute(singleton(nodeFor(cache)))
        .iterator()
        .next()
        .getContext()
        .attributes()
        .get("this");

      assertThat(invalidateAll.sum(), is(0L));
    }
  }

  private Long key(Random rndm) {
    return (long) rndm.nextInt(100000);
  }

  private CacheValue value(Random rndm) {
    return new CacheValue(rndm.nextInt(100000));
  }

  public static class CacheValue implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int value;
    private final byte[] padding;

    public CacheValue(int value) {
      this.value = value;
      this.padding = new byte[800];
    }

    @Override
    public int hashCode() {
      return value;
    }

    public boolean equals(Object o) {
      if (o instanceof CacheValue) {
        return value == ((CacheValue) o).value;
      } else {
        return false;
      }
    }
  }
}
