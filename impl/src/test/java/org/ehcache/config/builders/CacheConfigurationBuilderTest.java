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

package org.ehcache.config.builders;

import org.ehcache.config.*;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.spi.service.ServiceUtils;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.config.copy.DefaultCopierConfiguration;
import org.ehcache.impl.config.loaderwriter.DefaultCacheLoaderWriterConfiguration;
import org.ehcache.impl.config.resilience.DefaultResilienceStrategyConfiguration;
import org.ehcache.impl.config.serializer.DefaultSerializerConfiguration;
import org.ehcache.impl.config.store.disk.OffHeapDiskStoreConfiguration;
import org.ehcache.impl.config.store.heap.DefaultSizeOfEngineConfiguration;
import org.ehcache.impl.copy.SerializingCopier;
import org.ehcache.impl.internal.classes.ClassInstanceConfiguration;
import org.ehcache.impl.internal.resilience.RobustResilienceStrategy;
import org.ehcache.impl.serialization.JavaSerializer;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterConfiguration;
import org.ehcache.spi.resilience.RecoveryStore;
import org.ehcache.spi.resilience.ResilienceStrategy;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.test.MockitoUtil;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.core.IsSame;
import org.junit.Test;

import static java.util.function.UnaryOperator.identity;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.core.spi.service.ServiceUtils.findSingletonAmongst;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.ehcache.test.MockitoUtil.mock;
import static org.junit.Assert.fail;

public class CacheConfigurationBuilderTest {

  @Test
  public void testWithEvictionAdvisor() throws Exception {
    EvictionAdvisor<Object, Object> evictionAdvisor = (key, value) -> false;

    CacheConfiguration<Object, Object> cacheConfiguration = newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
        .withEvictionAdvisor(evictionAdvisor)
        .build();

    @SuppressWarnings("unchecked")
    Matcher<EvictionAdvisor<Object, Object>> evictionAdvisorMatcher = sameInstance(cacheConfiguration
      .getEvictionAdvisor());
    assertThat(evictionAdvisor, evictionAdvisorMatcher);
  }

  @Test
  public void testWithLoaderWriter() throws Exception {
    CacheLoaderWriter<Object, Object> loaderWriter = mock(CacheLoaderWriter.class);

    CacheConfiguration<Object, Object> cacheConfiguration = newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
        .withLoaderWriter(loaderWriter)
        .build();

    CacheLoaderWriterConfiguration<?> cacheLoaderWriterConfiguration = ServiceUtils.findSingletonAmongst(DefaultCacheLoaderWriterConfiguration.class, cacheConfiguration.getServiceConfigurations());
    Object instance = ((ClassInstanceConfiguration) cacheLoaderWriterConfiguration).getInstance();
    assertThat(instance, Matchers.sameInstance(loaderWriter));
  }

  @Test
  public void testWithoutLoaderWriter() {
    CacheConfiguration<Object, Object> cacheConfiguration = newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
      .withLoaderWriter(mock(CacheLoaderWriter.class))
      .withoutLoaderWriter()
      .build();

    assertThat(cacheConfiguration.getServiceConfigurations(), not(hasItem(instanceOf(DefaultCacheLoaderWriterConfiguration.class))));
  }

  @Test
  public void testWithKeySerializer() throws Exception {
    Serializer<Object> keySerializer = mock(Serializer.class);

    CacheConfiguration<Object, Object> cacheConfiguration = newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
        .withKeySerializer(keySerializer)
        .build();


    DefaultSerializerConfiguration<?> serializerConfiguration = ServiceUtils.findSingletonAmongst(DefaultSerializerConfiguration.class, cacheConfiguration.getServiceConfigurations());
    assertThat(serializerConfiguration.getType(), is(DefaultSerializerConfiguration.Type.KEY));
    Object instance = serializerConfiguration.getInstance();
    assertThat(instance, Matchers.sameInstance(keySerializer));
  }

  @Test
  public void testWithKeySerializerClass() throws Exception {
    @SuppressWarnings("unchecked")
    CacheConfiguration<Object, Object> cacheConfiguration = newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
      .withKeySerializer((Class) JavaSerializer.class)
      .build();


    DefaultSerializerConfiguration<?> serializerConfiguration = ServiceUtils.findSingletonAmongst(DefaultSerializerConfiguration.class, cacheConfiguration.getServiceConfigurations());
    assertThat(serializerConfiguration.getType(), is(DefaultSerializerConfiguration.Type.KEY));
    assertThat(serializerConfiguration.getClazz(), sameInstance(JavaSerializer.class));
  }

  @Test
  public void testWithoutKeySerializer() throws Exception {
    CacheConfiguration<Object, Object> cacheConfiguration = newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
      .withKeySerializer(MockitoUtil.<Serializer<Object>>mock(Serializer.class))
      .withDefaultKeySerializer()
      .build();

    assertThat(cacheConfiguration.getServiceConfigurations(), not(hasItem(instanceOf(DefaultSerializerConfiguration.class))));
  }

  @Test
  public void testWithValueSerializer() throws Exception {
    Serializer<Object> valueSerializer = mock(Serializer.class);

    CacheConfiguration<Object, Object> cacheConfiguration = newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
        .withValueSerializer(valueSerializer)
        .build();


    DefaultSerializerConfiguration<?> serializerConfiguration = ServiceUtils.findSingletonAmongst(DefaultSerializerConfiguration.class, cacheConfiguration.getServiceConfigurations());
    assertThat(serializerConfiguration.getType(), is(DefaultSerializerConfiguration.Type.VALUE));
    Object instance = ((ClassInstanceConfiguration) serializerConfiguration).getInstance();
    assertThat(instance, Matchers.sameInstance(valueSerializer));
  }

  @Test
  public void testWithValueSerializerClass() throws Exception {
    @SuppressWarnings("unchecked")
    CacheConfiguration<Object, Object> cacheConfiguration = newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
      .withValueSerializer((Class) JavaSerializer.class)
      .build();


    DefaultSerializerConfiguration<?> serializerConfiguration = ServiceUtils.findSingletonAmongst(DefaultSerializerConfiguration.class, cacheConfiguration.getServiceConfigurations());
    assertThat(serializerConfiguration.getType(), is(DefaultSerializerConfiguration.Type.VALUE));
    assertThat(serializerConfiguration.getClazz(), sameInstance(JavaSerializer.class));
  }

  @Test
  public void testWithoutValueSerializer() throws Exception {
    CacheConfiguration<Object, Object> cacheConfiguration = newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
      .withValueSerializer(MockitoUtil.<Serializer<Object>>mock(Serializer.class))
      .withDefaultValueSerializer()
      .build();

    assertThat(cacheConfiguration.getServiceConfigurations(), not(hasItem(instanceOf(DefaultSerializerConfiguration.class))));
  }

  @Test
  public void testWithKeyCopier() throws Exception {
    Copier<Object> keyCopier = mock(Copier.class);

    CacheConfiguration<Object, Object> cacheConfiguration = newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
        .withKeyCopier(keyCopier)
        .build();


    DefaultCopierConfiguration<?> copierConfiguration = ServiceUtils.findSingletonAmongst(DefaultCopierConfiguration.class, cacheConfiguration.getServiceConfigurations());
    assertThat(copierConfiguration.getType(), is(DefaultCopierConfiguration.Type.KEY));
    Object instance = copierConfiguration.getInstance();
    assertThat(instance, Matchers.sameInstance(keyCopier));
  }

  @Test
  public void testWithKeySerializingCopier() {
    CacheConfiguration<Object, Object> cacheConfiguration = newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
      .withKeySerializingCopier()
      .build();


    DefaultCopierConfiguration<?> copierConfiguration = ServiceUtils.findSingletonAmongst(DefaultCopierConfiguration.class, cacheConfiguration.getServiceConfigurations());
    assertThat(copierConfiguration.getType(), is(DefaultCopierConfiguration.Type.KEY));
    assertThat(copierConfiguration.getClazz(), Matchers.sameInstance(SerializingCopier.class));
  }

  @Test
  public void testWithoutKeyCopier() {
    CacheConfiguration<Object, Object> cacheConfiguration = newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
      .withKeyCopier(MockitoUtil.<Copier<Object>>mock(Copier.class))
      .withoutKeyCopier()
      .build();

    assertThat(cacheConfiguration.getServiceConfigurations(), not(hasItem(instanceOf(DefaultCopierConfiguration.class))));
  }

  @Test
  public void testWithValueCopier() throws Exception {
    Copier<Object> valueCopier = mock(Copier.class);

    CacheConfiguration<Object, Object> cacheConfiguration = newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
        .withValueCopier(valueCopier)
        .build();


    DefaultCopierConfiguration<?> copierConfiguration = ServiceUtils.findSingletonAmongst(DefaultCopierConfiguration.class, cacheConfiguration.getServiceConfigurations());
    assertThat(copierConfiguration.getType(), is(DefaultCopierConfiguration.Type.VALUE));
    Object instance = copierConfiguration.getInstance();
    assertThat(instance, Matchers.sameInstance(valueCopier));
  }

  @Test
  public void testWithValueSerializingCopier() {
    CacheConfiguration<Object, Object> cacheConfiguration = newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
      .withValueSerializingCopier()
      .build();


    DefaultCopierConfiguration<?> copierConfiguration = ServiceUtils.findSingletonAmongst(DefaultCopierConfiguration.class, cacheConfiguration.getServiceConfigurations());
    assertThat(copierConfiguration.getType(), is(DefaultCopierConfiguration.Type.VALUE));
    assertThat(copierConfiguration.getClazz(), Matchers.sameInstance(SerializingCopier.class));
  }

  @Test
  public void testWithoutValueCopier() {
    CacheConfiguration<Object, Object> cacheConfiguration = newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
      .withValueCopier(MockitoUtil.<Copier<Object>>mock(Copier.class))
      .withoutValueCopier()
      .build();

    assertThat(cacheConfiguration.getServiceConfigurations(), not(hasItem(instanceOf(DefaultCopierConfiguration.class))));
  }

  @Test
  public void testNothing() {
    final CacheConfigurationBuilder<Long, CharSequence> builder = newCacheConfigurationBuilder(Long.class, CharSequence.class, heap(10));

    final ExpiryPolicy<Object, Object> expiry = ExpiryPolicyBuilder.timeToIdleExpiration(ExpiryPolicy.INFINITE);

    builder
        .withEvictionAdvisor((key, value) -> value.charAt(0) == 'A')
        .withExpiry(expiry)
        .build();
  }

  @Test
  public void testOffheapGetsAddedToCacheConfiguration() {
    CacheConfigurationBuilder<Long, CharSequence> builder = newCacheConfigurationBuilder(Long.class, CharSequence.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES)
            .offheap(10, MemoryUnit.MB));

    final ExpiryPolicy<Object, Object> expiry = ExpiryPolicyBuilder.timeToIdleExpiration(ExpiryPolicy.INFINITE);

    CacheConfiguration<Long, CharSequence> config = builder
        .withEvictionAdvisor((key, value) -> value.charAt(0) == 'A')
        .withExpiry(expiry)
        .build();
    assertThat(config.getResourcePools().getPoolForResource(ResourceType.Core.OFFHEAP).getType(), Matchers.is(ResourceType.Core.OFFHEAP));
    assertThat(config.getResourcePools().getPoolForResource(ResourceType.Core.OFFHEAP).getUnit(), Matchers.is(MemoryUnit.MB));
  }

  @Test
  public void testSizeOf() {
    CacheConfigurationBuilder<String, String> builder = newCacheConfigurationBuilder(String.class, String.class, heap(10));

    builder = builder.withSizeOfMaxObjectSize(10, MemoryUnit.B).withSizeOfMaxObjectGraph(100);
    CacheConfiguration<String, String> configuration = builder.build();

    DefaultSizeOfEngineConfiguration sizeOfEngineConfiguration = ServiceUtils.findSingletonAmongst(DefaultSizeOfEngineConfiguration.class, configuration.getServiceConfigurations());
    assertThat(sizeOfEngineConfiguration, notNullValue());
    assertEquals(sizeOfEngineConfiguration.getMaxObjectSize(), 10);
    assertEquals(sizeOfEngineConfiguration.getUnit(), MemoryUnit.B);
    assertEquals(sizeOfEngineConfiguration.getMaxObjectGraphSize(), 100);

    builder = builder.withSizeOfMaxObjectGraph(1000);
    configuration = builder.build();

    sizeOfEngineConfiguration = ServiceUtils.findSingletonAmongst(DefaultSizeOfEngineConfiguration.class, configuration.getServiceConfigurations());
    assertEquals(sizeOfEngineConfiguration.getMaxObjectGraphSize(), 1000);

  }

  @Test
  public void testCopyingOfExistingConfiguration() {
    Class<Integer> keyClass = Integer.class;
    Class<String> valueClass = String.class;
    ClassLoader loader = mock(ClassLoader.class);
    @SuppressWarnings("unchecked")
    EvictionAdvisor<Integer, String> eviction = mock(EvictionAdvisor.class);
    @SuppressWarnings("unchecked")
    ExpiryPolicy<Integer, String> expiry = mock(ExpiryPolicy.class);
    ServiceConfiguration<?, ?> service = mock(ServiceConfiguration.class);

    CacheConfiguration<Integer, String> configuration = newCacheConfigurationBuilder(Integer.class, String.class, heap(10))
      .withClassLoader(loader)
      .withEvictionAdvisor(eviction)
      .withExpiry(expiry)
      .withService(service)
      .build();

    CacheConfiguration<Integer, String> copy = newCacheConfigurationBuilder(configuration).build();

    assertThat(copy.getKeyType(), equalTo(keyClass));
    assertThat(copy.getValueType(), equalTo(valueClass));
    assertThat(copy.getClassLoader(), equalTo(loader));

    assertThat(copy.getEvictionAdvisor(), IsSame.sameInstance(eviction));
    assertThat(copy.getExpiryPolicy(), IsSame.sameInstance(expiry));
    assertThat(copy.getServiceConfigurations(), contains(IsSame.sameInstance(service)));
  }

  @Test
  public void testWithResilienceStrategyInstance() throws Exception {
    ResilienceStrategy<Object, Object> resilienceStrategy = mock(ResilienceStrategy.class);

    CacheConfiguration<Object, Object> cacheConfiguration = newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
      .withResilienceStrategy(resilienceStrategy)
      .build();

    DefaultResilienceStrategyConfiguration resilienceStrategyConfiguration = ServiceUtils.findSingletonAmongst(DefaultResilienceStrategyConfiguration.class, cacheConfiguration.getServiceConfigurations());
    Object instance = resilienceStrategyConfiguration.getInstance();
    assertThat(instance, sameInstance(resilienceStrategy));
  }

  @Test
  public void testWithResilienceStrategyClass() throws Exception {
    CacheConfiguration<Object, Object> cacheConfiguration = newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
      .withResilienceStrategy(CustomResilience.class, "Hello World")
      .build();

    DefaultResilienceStrategyConfiguration resilienceStrategyConfiguration = ServiceUtils.findSingletonAmongst(DefaultResilienceStrategyConfiguration.class, cacheConfiguration.getServiceConfigurations());
    assertThat(resilienceStrategyConfiguration.getInstance(), nullValue());
    assertThat(resilienceStrategyConfiguration.getClazz(), sameInstance(CustomResilience.class));
    assertThat(resilienceStrategyConfiguration.getArguments(), arrayContaining("Hello World"));

  }

  @Test
  public void testWithDefaultResilienceStrategy() {
    CacheConfiguration<Object, Object> cacheConfiguration = newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
      .withResilienceStrategy(mock(ResilienceStrategy.class))
      .withDefaultResilienceStrategy()
      .build();

    assertThat(cacheConfiguration.getServiceConfigurations(), not(hasItem(instanceOf(DefaultResilienceStrategyConfiguration.class))));
  }

  @Test
  public void testWithServiceAddsNewConfiguration() {
    CacheConfigurationBuilder<Object, Object> oldBuilder = newCacheConfigurationBuilder(Object.class, Object.class, heap(10));

    ServiceConfiguration<?, ?> serviceConfiguration = new IncompatibleServiceConfig();

    CacheConfigurationBuilder<Object, Object> newBuilder = oldBuilder.withService(serviceConfiguration);

    assertThat(oldBuilder.build().getServiceConfigurations(), not(hasItem(sameInstance(serviceConfiguration))));
    assertThat(newBuilder.build().getServiceConfigurations(), hasItem(sameInstance(serviceConfiguration)));
  }

  @Test
  public void testIncompatibleServiceRemovesExistingConfiguration() {
    ServiceConfiguration<?, ?> oldConfig = new IncompatibleServiceConfig();
    ServiceConfiguration<?, ?> newConfig = new IncompatibleServiceConfig();

    CacheConfigurationBuilder<Object, Object> oldBuilder = newCacheConfigurationBuilder(Object.class, Object.class, heap(10)).withService(oldConfig);

    CacheConfigurationBuilder<Object, Object> newBuilder = oldBuilder.withService(newConfig);

    assertThat(oldBuilder.build().getServiceConfigurations(), both(hasItem(sameInstance(oldConfig))).and(not(hasItem(sameInstance(newConfig)))));
    assertThat(newBuilder.build().getServiceConfigurations(), both(hasItem(sameInstance(newConfig))).and(not(hasItem(sameInstance(oldConfig)))));
  }

  @Test
  public void testCompatibleServiceJoinsExistingConfiguration() {
    ServiceConfiguration<?, ?> oldConfig = new CompatibleServiceConfig();
    ServiceConfiguration<?, ?> newConfig = new CompatibleServiceConfig();

    CacheConfigurationBuilder<Object, Object> oldBuilder = newCacheConfigurationBuilder(Object.class, Object.class, heap(10)).withService(oldConfig);

    CacheConfigurationBuilder<Object, Object> newBuilder = oldBuilder.withService(newConfig);

    assertThat(oldBuilder.build().getServiceConfigurations(), both(hasItem(sameInstance(oldConfig))).and(not(hasItem(sameInstance(newConfig)))));
    assertThat(newBuilder.build().getServiceConfigurations(), both(hasItem(sameInstance(oldConfig))).and(hasItem(sameInstance(newConfig))));
  }

  @Test
  public void testUpdateServicesWithNoServicesThrows() {
    CacheConfigurationBuilder<Object, Object> oldBuilder = newCacheConfigurationBuilder(Object.class, Object.class, heap(10));
    try {
      oldBuilder.updateServices(IncompatibleServiceConfig.class, identity());
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      //expected
    }
  }

  @Test
  public void testUpdateServicesWithNullReturnThrows() {
    CacheConfigurationBuilder<Object, Object> oldBuilder = newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
      .withService(new IncompatibleServiceConfig());
    try {
      oldBuilder.updateServices(IncompatibleServiceConfig.class, c -> null);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }
  }


  @Test
  public void testUpdateServicesHitsAllServices() {
    CompatibleServiceConfig configA = new CompatibleServiceConfig();
    CompatibleServiceConfig configB = new CompatibleServiceConfig();
    CacheConfigurationBuilder<Object, Object> oldBuilder = newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
      .withService(configA)
      .withService(configB);

    CompatibleServiceConfig configA2 = new CompatibleServiceConfig();
    CompatibleServiceConfig configB2 = new CompatibleServiceConfig();
    CacheConfigurationBuilder<Object, Object> newBuilder = oldBuilder.updateServices(CompatibleServiceConfig.class, c -> {
      if (c == configA) {
        return configA2;
      } else if (c == configB) {
        return configB2;
      } else {
        throw new AssertionError();
      }
    });

    assertThat(oldBuilder.build().getServiceConfigurations(), hasItems(configA, configB));
    assertThat(newBuilder.build().getServiceConfigurations(), hasItems(configA2, configB2));
  }

  @Test
  public void testWithClassLoader() {
    ClassLoader classLoader = mock(ClassLoader.class);

    CacheConfiguration<Object, Object> cacheConfiguration = newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
      .withClassLoader(classLoader)
      .build();

    assertThat(cacheConfiguration.getClassLoader(), sameInstance(classLoader));
  }

  @Test
  public void testWithDefaultClassLoader() {
    CacheConfiguration<Object, Object> cacheConfiguration = newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
      .withClassLoader(mock(ClassLoader.class))
      .withDefaultClassLoader()
      .build();

    assertThat(cacheConfiguration.getClassLoader(), nullValue());
  }

  @Test
  public void testWithDiskStoreThreadPool() {
    CacheConfiguration<Object, Object> cacheConfiguration = newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
      .withDiskStoreThreadPool("banana", 42)
      .build();

    OffHeapDiskStoreConfiguration config = findSingletonAmongst(OffHeapDiskStoreConfiguration.class, cacheConfiguration.getServiceConfigurations());

    assertThat(config.getWriterConcurrency(), is(42));
    assertThat(config.getThreadPoolAlias(), is("banana"));
  }

  @Test
  public void testWithDefaultDiskStoreThreadPool() {
    CacheConfiguration<Object, Object> cacheConfiguration = newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
      .withDiskStoreThreadPool("banana", 42)
      .withDefaultDiskStoreThreadPool()
      .build();

    assertThat(cacheConfiguration.getServiceConfigurations(), not(hasItem(instanceOf(OffHeapDiskStoreConfiguration.class))));
  }

  @Test
  public void testWithSizeOfConfig() {
    CacheConfigurationBuilder<Object, Object> builder = newCacheConfigurationBuilder(Object.class, Object.class, heap(10));

    builder = builder.withSizeOfMaxObjectGraph(42L);
    {
      CacheConfiguration<Object, Object> cacheConfiguration = builder.build();
      DefaultSizeOfEngineConfiguration config = findSingletonAmongst(DefaultSizeOfEngineConfiguration.class, cacheConfiguration.getServiceConfigurations());
      assertThat(config.getMaxObjectGraphSize(), is(42L));
      assertThat(config.getMaxObjectSize(), is(Long.MAX_VALUE));
      assertThat(config.getUnit(), is(MemoryUnit.B));
    }

    builder = builder.withSizeOfMaxObjectSize(1024L, MemoryUnit.KB);
    {
      CacheConfiguration<Object, Object> cacheConfiguration = builder.build();
      DefaultSizeOfEngineConfiguration config = findSingletonAmongst(DefaultSizeOfEngineConfiguration.class, cacheConfiguration.getServiceConfigurations());
      assertThat(config.getMaxObjectGraphSize(), is(42L));
      assertThat(config.getMaxObjectSize(), is(1024L));
      assertThat(config.getUnit(), is(MemoryUnit.KB));
    }

    builder = builder.withSizeOfMaxObjectGraph(43L);
    {
      CacheConfiguration<Object, Object> cacheConfiguration = builder.build();
      DefaultSizeOfEngineConfiguration config = findSingletonAmongst(DefaultSizeOfEngineConfiguration.class, cacheConfiguration.getServiceConfigurations());
      assertThat(config.getMaxObjectGraphSize(), is(43L));
      assertThat(config.getMaxObjectSize(), is(1024L));
      assertThat(config.getUnit(), is(MemoryUnit.KB));
    }
  }

  @Test
  public void testWithDefaultSizeOfSettings() {
    CacheConfiguration<Object, Object> cacheConfiguration = newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
      .withSizeOfMaxObjectGraph(42L)
      .withSizeOfMaxObjectSize(1024L, MemoryUnit.KB)
      .withDefaultSizeOfSettings()
      .build();

    assertThat(cacheConfiguration.getServiceConfigurations(), not(hasItem(instanceOf(DefaultSizeOfEngineConfiguration.class))));
  }

  static class CustomResilience<K, V> extends RobustResilienceStrategy<K, V> {

    public CustomResilience(RecoveryStore<K> store, String blah) {
      super(store);
    }
  }

  static class IncompatibleServiceConfig implements ServiceConfiguration<Service, IncompatibleServiceConfig> {

    @Override
    public Class<Service> getServiceType() {
      return Service.class;
    }

    @Override
    public IncompatibleServiceConfig derive() throws UnsupportedOperationException {
      return this;
    }

    @Override
    public ServiceConfiguration<Service, ?> build(IncompatibleServiceConfig representation) throws UnsupportedOperationException {
      return representation;
    }
  }

  static class CompatibleServiceConfig implements ServiceConfiguration<Service, CompatibleServiceConfig> {
    @Override
    public CompatibleServiceConfig derive() throws UnsupportedOperationException {
      return this;
    }

    @Override
    public ServiceConfiguration<Service, ?> build(CompatibleServiceConfig representation) throws UnsupportedOperationException {
      return representation;
    }

    @Override
    public Class<Service> getServiceType() {
      return Service.class;
    }

    @Override
    public boolean compatibleWith(ServiceConfiguration<?, ?> other) {
      return true;
    }
  }
}
