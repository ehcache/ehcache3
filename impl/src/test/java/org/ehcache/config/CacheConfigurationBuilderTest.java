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

package org.ehcache.config;

import org.ehcache.config.copy.CopierConfiguration;
import org.ehcache.config.loaderwriter.CacheLoaderWriterConfiguration;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.exceptions.BulkCacheWritingException;
import org.ehcache.exceptions.SerializerException;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.internal.classes.ClassInstanceConfiguration;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.serialization.Serializer;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;

public class CacheConfigurationBuilderTest {

  @Test
  public void testEvictionVeto() throws Exception {
    EvictionVeto<Object, Object> veto = new EvictionVeto<Object, Object>() {
      @Override
      public boolean vetoes(Object key, Object value) {
        return false;
      }
    };

    CacheConfiguration<Object, Object> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .evictionVeto(veto)
        .buildConfig(Object.class, Object.class);

    assertThat(veto, (Matcher) sameInstance(cacheConfiguration.getEvictionVeto()));
  }

  @Test
  public void testLoaderWriter() throws Exception {
    CacheLoaderWriter<Object, Object> loaderWriter = new CacheLoaderWriter<Object, Object>() {
      @Override
      public Object load(Object key) throws Exception {
        return null;
      }

      @Override
      public Map<Object, Object> loadAll(Iterable keys) throws Exception {
        return null;
      }

      @Override
      public void write(Object key, Object value) throws Exception {

      }

      @Override
      public void writeAll(Iterable iterable) throws BulkCacheWritingException, Exception {

      }

      @Override
      public void delete(Object key) throws Exception {

      }

      @Override
      public void deleteAll(Iterable keys) throws BulkCacheWritingException, Exception {

      }
    };

    CacheConfiguration<Object, Object> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withLoaderWriter(loaderWriter)
        .buildConfig(Object.class, Object.class);

    CacheLoaderWriterConfiguration cacheLoaderWriterConfiguration = ServiceLocator.findSingletonAmongst(CacheLoaderWriterConfiguration.class, cacheConfiguration.getServiceConfigurations());
    Object instance = ((ClassInstanceConfiguration) cacheLoaderWriterConfiguration).getInstance();
    assertThat(instance, Matchers.<Object>sameInstance(loaderWriter));
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

    CacheConfiguration<Object, Object> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withKeySerializer(keySerializer)
        .buildConfig(Object.class, Object.class);


    SerializerConfiguration serializerConfiguration = ServiceLocator.findSingletonAmongst(SerializerConfiguration.class, cacheConfiguration.getServiceConfigurations());
    assertThat(serializerConfiguration.getType(), is(SerializerConfiguration.Type.KEY));
    Object instance = ((ClassInstanceConfiguration) serializerConfiguration).getInstance();
    assertThat(instance, Matchers.<Object>sameInstance(keySerializer));
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

    CacheConfiguration<Object, Object> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withValueSerializer(valueSerializer)
        .buildConfig(Object.class, Object.class);


    SerializerConfiguration serializerConfiguration = ServiceLocator.findSingletonAmongst(SerializerConfiguration.class, cacheConfiguration.getServiceConfigurations());
    assertThat(serializerConfiguration.getType(), is(SerializerConfiguration.Type.VALUE));
    Object instance = ((ClassInstanceConfiguration) serializerConfiguration).getInstance();
    assertThat(instance, Matchers.<Object>sameInstance(valueSerializer));
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

    CacheConfiguration<Object, Object> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withKeyCopier(keyCopier)
        .buildConfig(Object.class, Object.class);


    CopierConfiguration copierConfiguration = ServiceLocator.findSingletonAmongst(CopierConfiguration.class, cacheConfiguration.getServiceConfigurations());
    assertThat(copierConfiguration.getType(), is(CopierConfiguration.Type.KEY));
    Object instance = ((ClassInstanceConfiguration) copierConfiguration).getInstance();
    assertThat(instance, Matchers.<Object>sameInstance(keyCopier));
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

    CacheConfiguration<Object, Object> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withValueCopier(valueCopier)
        .buildConfig(Object.class, Object.class);


    CopierConfiguration copierConfiguration = ServiceLocator.findSingletonAmongst(CopierConfiguration.class, cacheConfiguration.getServiceConfigurations());
    assertThat(copierConfiguration.getType(), is(CopierConfiguration.Type.VALUE));
    Object instance = ((ClassInstanceConfiguration) copierConfiguration).getInstance();
    assertThat(instance, Matchers.<Object>sameInstance(valueCopier));
  }

  @Test
  public void testNothing() {
    final CacheConfigurationBuilder<Long, CharSequence> builder = CacheConfigurationBuilder.newCacheConfigurationBuilder();
   
    final Expiry<Object, Object> expiry = Expirations.timeToIdleExpiration(Duration.FOREVER);

    builder
        .evictionVeto(new EvictionVeto<Long, String>() {
          @Override
          public boolean vetoes(Long key, String value) {
            return value.startsWith("A");
          }
        })
        .withExpiry(expiry)
        .buildConfig(Long.class, String.class);
    builder
        .buildConfig(Long.class, String.class, null);
    builder
        .buildConfig(Long.class, String.class, null);
  }

  @Test
  public void testOffheapGetsAddedToCacheConfiguration() {
    CacheConfigurationBuilder<Long, CharSequence> builder = CacheConfigurationBuilder.newCacheConfigurationBuilder();

    final Expiry<Object, Object> expiry = Expirations.timeToIdleExpiration(Duration.FOREVER);

    builder = builder.withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES)
        .offheap(10, MemoryUnit.MB));
    CacheConfiguration config = builder
        .evictionVeto(new EvictionVeto<Long, String>() {
          @Override
          public boolean vetoes(Long key, String value) {
            return value.startsWith("A");
          }
        })
        .withExpiry(expiry)
        .buildConfig(Long.class, String.class);
    assertThat(config.getResourcePools().getPoolForResource(ResourceType.Core.OFFHEAP).getType(), Matchers.<ResourceType>is(ResourceType.Core.OFFHEAP));
    assertThat(config.getResourcePools().getPoolForResource(ResourceType.Core.OFFHEAP).getUnit(), Matchers.<ResourceUnit>is(MemoryUnit.MB));
  }
}
