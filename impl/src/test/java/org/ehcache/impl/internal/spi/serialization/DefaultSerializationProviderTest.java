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
package org.ehcache.impl.internal.spi.serialization;

import org.ehcache.core.config.serializer.SerializerConfiguration;
import org.ehcache.impl.config.serializer.DefaultSerializationProviderConfiguration;
import org.ehcache.impl.config.serializer.DefaultSerializerConfiguration;
import org.ehcache.impl.serialization.CompactJavaSerializer;
import org.ehcache.impl.serialization.IntegerSerializer;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.UnsupportedTypeException;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.ClassLoader.getSystemClassLoader;
import static org.ehcache.impl.internal.spi.TestServiceProvider.providerContaining;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Ludovic Orban
 */
public class DefaultSerializationProviderTest {

  @Test
  public void testCreateSerializerNoConfig() throws Exception {
    DefaultSerializationProviderConfiguration dspfConfig = new DefaultSerializationProviderConfiguration();
    DefaultSerializationProvider dsp = new DefaultSerializationProvider(dspfConfig);
    dsp.start(providerContaining());

    assertThat(dsp.createValueSerializer(HashMap.class, ClassLoader.getSystemClassLoader()), instanceOf(CompactJavaSerializer.class));
    try {
      dsp.createValueSerializer(Object.class, ClassLoader.getSystemClassLoader());
      fail("expected UnsupportedTypeException");
    } catch (UnsupportedTypeException ute) {
      // expected
    }
  }

  @Test
  public void testCreateSerializerWithConfig() throws Exception {
    DefaultSerializationProviderConfiguration dspfConfig = new DefaultSerializationProviderConfiguration();
    DefaultSerializationProvider dsp = new DefaultSerializationProvider(dspfConfig);
    dsp.start(providerContaining());

    DefaultSerializerConfiguration dspConfig = new DefaultSerializerConfiguration((Class) TestSerializer.class, DefaultSerializerConfiguration.Type.VALUE);

    assertThat(dsp.createValueSerializer(String.class, ClassLoader.getSystemClassLoader(), dspConfig), instanceOf(TestSerializer.class));
    assertThat(dsp.createValueSerializer(Object.class, ClassLoader.getSystemClassLoader(), dspConfig), instanceOf(TestSerializer.class));
  }

  @Test
  public void testCreateSerializerWithFactoryConfig() throws Exception {
    DefaultSerializationProviderConfiguration dspfConfig = new DefaultSerializationProviderConfiguration();
    dspfConfig.addSerializerFor(Long.class, (Class) TestSerializer.class);
    DefaultSerializationProvider dsp = new DefaultSerializationProvider(dspfConfig);
    dsp.start(providerContaining());

    assertThat(dsp.createValueSerializer(Long.class, ClassLoader.getSystemClassLoader()), instanceOf(TestSerializer.class));
    assertThat(dsp.createValueSerializer(HashMap.class, ClassLoader.getSystemClassLoader()), instanceOf(CompactJavaSerializer.class));
  }

  @Test
  public void testCreateTransientSerializers() throws Exception {
    DefaultSerializationProviderConfiguration dspfConfig = new DefaultSerializationProviderConfiguration();
    dspfConfig.addSerializerFor(String.class, (Class) TestSerializer.class);

    DefaultSerializationProvider dsp = new DefaultSerializationProvider(dspfConfig);
    dsp.start(providerContaining());

    assertThat(dsp.createKeySerializer(String.class, getSystemClassLoader()), instanceOf(TestSerializer.class));
    assertThat(dsp.createKeySerializer(Serializable.class, getSystemClassLoader()), instanceOf(CompactJavaSerializer.class));
    assertThat(dsp.createKeySerializer(Integer.class, getSystemClassLoader()), instanceOf(IntegerSerializer.class));
  }

  @Test
  public void tesCreateTransientSerializersWithOverriddenSerializableType() throws Exception {
    DefaultSerializationProviderConfiguration dspfConfig = new DefaultSerializationProviderConfiguration();
    dspfConfig.addSerializerFor(Serializable.class, (Class) TestSerializer.class);

    DefaultSerializationProvider dsp = new DefaultSerializationProvider(dspfConfig);
    dsp.start(providerContaining());

    assertThat(dsp.createKeySerializer(HashMap.class, getSystemClassLoader()), instanceOf(TestSerializer.class));
    assertThat(dsp.createKeySerializer(Serializable.class, getSystemClassLoader()), instanceOf(TestSerializer.class));
    assertThat(dsp.createKeySerializer(Integer.class, getSystemClassLoader()), instanceOf(IntegerSerializer.class));
  }

  @Test
  public void testRemembersCreationConfigurationAfterStopStart() throws UnsupportedTypeException {
    DefaultSerializationProviderConfiguration configuration = new DefaultSerializationProviderConfiguration();
    configuration.addSerializerFor(String.class, (Class) TestSerializer.class);
    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(configuration);
    serializationProvider.start(mock(ServiceProvider.class));
    assertThat(serializationProvider.createKeySerializer(String.class, getSystemClassLoader()), instanceOf(TestSerializer.class));
    serializationProvider.stop();
    serializationProvider.start(mock(ServiceProvider.class));
    assertThat(serializationProvider.createKeySerializer(String.class, getSystemClassLoader()), instanceOf(TestSerializer.class));
  }

  @Test
  public void testReleaseSerializerWithCloseableSerializer() throws Exception {
    DefaultSerializationProvider provider = new DefaultSerializationProvider(null);
    CompactJavaSerializer<?> serializer = mock(CompactJavaSerializer.class);
    provider.providedVsCount.put(serializer, new AtomicInteger(1));

    provider.releaseSerializer(serializer);
    verify(serializer).close();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testReleaseSerializerByAnotherProvider() throws Exception {
    DefaultSerializationProvider provider = new DefaultSerializationProvider(null);
    Serializer<?> serializer = mock(Serializer.class);
    provider.releaseSerializer(serializer);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testReleaseSameInstanceMultipleTimesThrows() throws Exception {
    DefaultSerializationProvider provider = new DefaultSerializationProvider(null);
    Serializer<?> serializer = mock(Serializer.class);
    provider.providedVsCount.put(serializer, new AtomicInteger(1));

    provider.releaseSerializer(serializer);
    assertThat(provider.providedVsCount.get("foo"), Matchers.nullValue());
    provider.releaseSerializer(serializer);
  }

  @Test
  public void testCreateKeySerializerWithActualInstanceInServiceConfig() throws Exception {
    DefaultSerializationProvider provider = new DefaultSerializationProvider(null);
    TestSerializer serializer = mock(TestSerializer.class);
    DefaultSerializerConfiguration config = new DefaultSerializerConfiguration(serializer, SerializerConfiguration.Type.KEY);
    Serializer<?> created = provider.createKeySerializer(TestSerializer.class, getSystemClassLoader(), config);
    assertSame(serializer, created);
  }

  @Test
  public void testSameInstanceRetrievedMultipleTimesUpdatesTheProvidedCount() throws Exception {
    DefaultSerializationProvider provider = new DefaultSerializationProvider(null);
    TestSerializer serializer = mock(TestSerializer.class);
    DefaultSerializerConfiguration config = new DefaultSerializerConfiguration(serializer, SerializerConfiguration.Type.KEY);
    
    Serializer<?> created = provider.createKeySerializer(TestSerializer.class, getSystemClassLoader(), config);
    assertSame(serializer, created);
    assertThat(provider.providedVsCount.get(created).get(), is(1));
    
    created = provider.createKeySerializer(TestSerializer.class, getSystemClassLoader(), config);
    assertSame(serializer, created);
    assertThat(provider.providedVsCount.get(created).get(), is(2));
  }

  public static class TestSerializer<T> implements Serializer<T> {
    public TestSerializer(ClassLoader classLoader) {
    }
    @Override
    public ByteBuffer serialize(T object) {
      return null;
    }
    @Override
    public T read(ByteBuffer binary) {
      return null;
    }
    @Override
    public boolean equals(T object, ByteBuffer binary) {
      return false;
    }
  }

}
