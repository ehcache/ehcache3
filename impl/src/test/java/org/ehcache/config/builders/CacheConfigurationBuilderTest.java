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
import org.ehcache.impl.config.store.heap.DefaultSizeOfEngineConfiguration;
import org.ehcache.impl.internal.classes.ClassInstanceConfiguration;
import org.ehcache.impl.internal.resilience.RobustResilienceStrategy;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterConfiguration;
import org.ehcache.spi.resilience.RecoveryStore;
import org.ehcache.spi.resilience.ResilienceStrategy;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.SerializerException;
import org.ehcache.spi.service.ServiceConfiguration;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.core.IsSame;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.ehcache.test.MockitoUtil.mock;

public class CacheConfigurationBuilderTest {

  @Test
  public void testEvictionAdvisor() throws Exception {
    EvictionAdvisor<Object, Object> evictionAdvisor = (key, value) -> false;

    CacheConfiguration<Object, Object> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
        .withEvictionAdvisor(evictionAdvisor)
        .build();

    @SuppressWarnings("unchecked")
    Matcher<EvictionAdvisor<Object, Object>> evictionAdvisorMatcher = sameInstance(cacheConfiguration
      .getEvictionAdvisor());
    assertThat(evictionAdvisor, evictionAdvisorMatcher);
  }

  @Test
  public void testLoaderWriter() throws Exception {
    CacheLoaderWriter<Object, Object> loaderWriter = new CacheLoaderWriter<Object, Object>() {
      @Override
      public Object load(Object key) {
        return null;
      }

      @Override
      public void write(Object key, Object value) {

      }

      @Override
      public void delete(Object key) {

      }
    };

    CacheConfiguration<Object, Object> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
        .withLoaderWriter(loaderWriter)
        .build();

    CacheLoaderWriterConfiguration cacheLoaderWriterConfiguration = ServiceUtils.findSingletonAmongst(DefaultCacheLoaderWriterConfiguration.class, cacheConfiguration.getServiceConfigurations());
    Object instance = ((ClassInstanceConfiguration) cacheLoaderWriterConfiguration).getInstance();
    assertThat(instance, Matchers.sameInstance(loaderWriter));
  }

  @Test
  public void testKeySerializer() throws Exception {
    Serializer<Object> keySerializer = new Serializer<Object>() {
      @Override
      public ByteBuffer serialize(Object object) throws SerializerException {
        return null;
      }

      @Override
      public Object read(ByteBuffer binary) throws ClassNotFoundException, SerializerException {
        return null;
      }

      @Override
      public boolean equals(Object object, ByteBuffer binary) throws ClassNotFoundException, SerializerException {
        return false;
      }
    };

    CacheConfiguration<Object, Object> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
        .withKeySerializer(keySerializer)
        .build();


    DefaultSerializerConfiguration<?> serializerConfiguration = ServiceUtils.findSingletonAmongst(DefaultSerializerConfiguration.class, cacheConfiguration.getServiceConfigurations());
    assertThat(serializerConfiguration.getType(), is(DefaultSerializerConfiguration.Type.KEY));
    Object instance = serializerConfiguration.getInstance();
    assertThat(instance, Matchers.sameInstance(keySerializer));
  }

  @Test
  public void testValueSerializer() throws Exception {
    Serializer<Object> valueSerializer = new Serializer<Object>() {
      @Override
      public ByteBuffer serialize(Object object) throws SerializerException {
        return null;
      }

      @Override
      public Object read(ByteBuffer binary) throws ClassNotFoundException, SerializerException {
        return null;
      }

      @Override
      public boolean equals(Object object, ByteBuffer binary) throws ClassNotFoundException, SerializerException {
        return false;
      }
    };

    CacheConfiguration<Object, Object> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
        .withValueSerializer(valueSerializer)
        .build();


    DefaultSerializerConfiguration<?> serializerConfiguration = ServiceUtils.findSingletonAmongst(DefaultSerializerConfiguration.class, cacheConfiguration.getServiceConfigurations());
    assertThat(serializerConfiguration.getType(), is(DefaultSerializerConfiguration.Type.VALUE));
    Object instance = ((ClassInstanceConfiguration) serializerConfiguration).getInstance();
    assertThat(instance, Matchers.sameInstance(valueSerializer));
  }

  @Test
  public void testKeyCopier() throws Exception {
    Copier<Object> keyCopier = new Copier<Object>() {
      @Override
      public Long copyForRead(Object obj) {
        return null;
      }

      @Override
      public Long copyForWrite(Object obj) {
        return null;
      }
    };

    CacheConfiguration<Object, Object> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
        .withKeyCopier(keyCopier)
        .build();


    DefaultCopierConfiguration<?> copierConfiguration = ServiceUtils.findSingletonAmongst(DefaultCopierConfiguration.class, cacheConfiguration.getServiceConfigurations());
    assertThat(copierConfiguration.getType(), is(DefaultCopierConfiguration.Type.KEY));
    Object instance = copierConfiguration.getInstance();
    assertThat(instance, Matchers.sameInstance(keyCopier));
  }

  @Test
  public void testValueCopier() throws Exception {
    Copier<Object> valueCopier = new Copier<Object>() {
      @Override
      public Long copyForRead(Object obj) {
        return null;
      }

      @Override
      public Long copyForWrite(Object obj) {
        return null;
      }
    };

    CacheConfiguration<Object, Object> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
        .withValueCopier(valueCopier)
        .build();


    DefaultCopierConfiguration<?> copierConfiguration = ServiceUtils.findSingletonAmongst(DefaultCopierConfiguration.class, cacheConfiguration.getServiceConfigurations());
    assertThat(copierConfiguration.getType(), is(DefaultCopierConfiguration.Type.VALUE));
    Object instance = copierConfiguration.getInstance();
    assertThat(instance, Matchers.sameInstance(valueCopier));
  }

  @Test
  public void testNothing() {
    final CacheConfigurationBuilder<Long, CharSequence> builder = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, CharSequence.class, heap(10));

    final ExpiryPolicy<Object, Object> expiry = ExpiryPolicyBuilder.timeToIdleExpiration(ExpiryPolicy.INFINITE);

    builder
        .withEvictionAdvisor((key, value) -> value.charAt(0) == 'A')
        .withExpiry(expiry)
        .build();
  }

  @Test
  public void testOffheapGetsAddedToCacheConfiguration() {
    CacheConfigurationBuilder<Long, CharSequence> builder = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, CharSequence.class,
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
    CacheConfigurationBuilder<String, String> builder = CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class, heap(10));

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
    ServiceConfiguration<?> service = mock(ServiceConfiguration.class);

    CacheConfiguration<Integer, String> configuration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Integer.class, String.class, heap(10))
      .withClassLoader(loader)
      .withEvictionAdvisor(eviction)
      .withExpiry(expiry)
      .add(service)
      .build();

    CacheConfiguration<Integer, String> copy = CacheConfigurationBuilder.newCacheConfigurationBuilder(configuration).build();

    assertThat(copy.getKeyType(), equalTo(keyClass));
    assertThat(copy.getValueType(), equalTo(valueClass));
    assertThat(copy.getClassLoader(), equalTo(loader));

    assertThat(copy.getEvictionAdvisor(), IsSame.sameInstance(eviction));
    assertThat(copy.getExpiryPolicy(), IsSame.sameInstance(expiry));
    assertThat(copy.getServiceConfigurations(), contains(IsSame.sameInstance(service)));
  }

  @Test
  public void testResilienceStrategyInstance() throws Exception {
    ResilienceStrategy<Object, Object> resilienceStrategy = mock(ResilienceStrategy.class);

    CacheConfiguration<Object, Object> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
      .withResilienceStrategy(resilienceStrategy)
      .build();

    DefaultResilienceStrategyConfiguration resilienceStrategyConfiguration = ServiceUtils.findSingletonAmongst(DefaultResilienceStrategyConfiguration.class, cacheConfiguration.getServiceConfigurations());
    Object instance = resilienceStrategyConfiguration.getInstance();
    assertThat(instance, sameInstance(resilienceStrategy));
  }

  @Test
  public void testResilienceStrategyClass() throws Exception {
    CacheConfiguration<Object, Object> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
      .withResilienceStrategy(CustomResilience.class, "Hello World")
      .build();

    DefaultResilienceStrategyConfiguration resilienceStrategyConfiguration = ServiceUtils.findSingletonAmongst(DefaultResilienceStrategyConfiguration.class, cacheConfiguration.getServiceConfigurations());
    assertThat(resilienceStrategyConfiguration.getInstance(), nullValue());
    assertThat(resilienceStrategyConfiguration.getClazz(), sameInstance(CustomResilience.class));
    assertThat(resilienceStrategyConfiguration.getArguments(), arrayContaining("Hello World"));

  }

  static class CustomResilience<K, V> extends RobustResilienceStrategy<K, V> {

    public CustomResilience(RecoveryStore<K> store, String blah) {
      super(store);
    }
  }
}
