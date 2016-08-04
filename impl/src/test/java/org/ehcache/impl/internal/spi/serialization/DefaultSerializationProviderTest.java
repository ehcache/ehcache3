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

import org.ehcache.CachePersistenceException;
import org.ehcache.core.spi.service.FileBasedPersistenceContext;
import org.ehcache.core.spi.service.LocalPersistenceService;
import org.ehcache.impl.config.serializer.DefaultSerializationProviderConfiguration;
import org.ehcache.impl.config.serializer.DefaultSerializerConfiguration;
import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.ehcache.impl.serialization.ByteArraySerializer;
import org.ehcache.impl.serialization.CharSerializer;
import org.ehcache.impl.serialization.CompactJavaSerializer;
import org.ehcache.impl.serialization.CompactPersistentJavaSerializer;
import org.ehcache.impl.serialization.DoubleSerializer;
import org.ehcache.impl.serialization.FloatSerializer;
import org.ehcache.impl.serialization.IntegerSerializer;
import org.ehcache.impl.serialization.LongSerializer;
import org.ehcache.impl.serialization.PlainJavaSerializer;
import org.ehcache.impl.serialization.StringSerializer;
import org.ehcache.spi.persistence.PersistableResourceService;
import org.ehcache.spi.persistence.StateRepository;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.SerializerException;
import org.ehcache.spi.serialization.UnsupportedTypeException;
import org.ehcache.spi.service.ServiceProvider;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Ludovic Orban
 */
public class DefaultSerializationProviderTest {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

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
  public void testReleaseSerializerWithProvidedCloseableSerializerDoesNotClose() throws Exception {
    DefaultSerializationProvider provider = new DefaultSerializationProvider(null);
    CloseableSerializer closeableSerializer = new CloseableSerializer();
    provider.providedVsCount.put(closeableSerializer, new AtomicInteger(1));

    provider.releaseSerializer(closeableSerializer);
    assertThat(closeableSerializer.closed, is(false));
  }

  @Test
  public void testReleaseSerializerWithInstantiatedCloseableSerializerDoesClose() throws Exception {
    DefaultSerializerConfiguration config = new DefaultSerializerConfiguration(CloseableSerializer.class, DefaultSerializerConfiguration.Type.KEY);
    DefaultSerializationProvider provider = new DefaultSerializationProvider(null);
    Serializer serializer = provider.createKeySerializer(String.class, getSystemClassLoader(), config);

    provider.releaseSerializer(serializer);
    assertTrue(((CloseableSerializer)serializer).closed);
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
    DefaultSerializerConfiguration config = new DefaultSerializerConfiguration(serializer, DefaultSerializerConfiguration.Type.KEY);
    Serializer<?> created = provider.createKeySerializer(TestSerializer.class, getSystemClassLoader(), config);
    assertSame(serializer, created);
  }

  @Test
  public void testSameInstanceRetrievedMultipleTimesUpdatesTheProvidedCount() throws Exception {
    DefaultSerializationProvider provider = new DefaultSerializationProvider(null);
    TestSerializer serializer = mock(TestSerializer.class);
    DefaultSerializerConfiguration config = new DefaultSerializerConfiguration(serializer, DefaultSerializerConfiguration.Type.KEY);

    Serializer<?> created = provider.createKeySerializer(TestSerializer.class, getSystemClassLoader(), config);
    assertSame(serializer, created);
    assertThat(provider.providedVsCount.get(created).get(), is(1));

    created = provider.createKeySerializer(TestSerializer.class, getSystemClassLoader(), config);
    assertSame(serializer, created);
    assertThat(provider.providedVsCount.get(created).get(), is(2));
  }

  @Test
  public void testDefaultSerializableSerializer() throws Exception {
    DefaultSerializationProvider provider = getStartedProvider();
    Serializer<Serializable> keySerializer = provider.createKeySerializer(Serializable.class, getSystemClassLoader());
    assertThat(keySerializer, instanceOf(CompactJavaSerializer.class));

    keySerializer = provider.createKeySerializer(Serializable.class, getSystemClassLoader(), getPersistenceSpaceIdentifierMock());
    assertThat(keySerializer, instanceOf(CompactJavaSerializer.class));
  }

  @Test
  public void testDefaultStringSerializer() throws Exception {
    DefaultSerializationProvider provider = getStartedProvider();
    Serializer<String> keySerializer = provider.createKeySerializer(String.class, getSystemClassLoader());
    assertThat(keySerializer, instanceOf(StringSerializer.class));

    keySerializer = provider.createKeySerializer(String.class, getSystemClassLoader(), getPersistenceSpaceIdentifierMock());
    assertThat(keySerializer, instanceOf(StringSerializer.class));
  }

  @Test
  public void testDefaultIntegerSerializer() throws Exception {
    DefaultSerializationProvider provider = getStartedProvider();
    Serializer<Integer> keySerializer = provider.createKeySerializer(Integer.class, getSystemClassLoader());
    assertThat(keySerializer, instanceOf(IntegerSerializer.class));

    keySerializer = provider.createKeySerializer(Integer.class, getSystemClassLoader(), getPersistenceSpaceIdentifierMock());
    assertThat(keySerializer, instanceOf(IntegerSerializer.class));
  }

  @Test
  public void testDefaultLongSerializer() throws Exception {
    DefaultSerializationProvider provider = getStartedProvider();
    Serializer<Long> keySerializer = provider.createKeySerializer(Long.class, getSystemClassLoader());
    assertThat(keySerializer, instanceOf(LongSerializer.class));

    keySerializer = provider.createKeySerializer(Long.class, getSystemClassLoader(), getPersistenceSpaceIdentifierMock());
    assertThat(keySerializer, instanceOf(LongSerializer.class));
  }

  @Test
  public void testDefaultCharSerializer() throws Exception {
    DefaultSerializationProvider provider = getStartedProvider();
    Serializer<Character> keySerializer = provider.createKeySerializer(Character.class, getSystemClassLoader());
    assertThat(keySerializer, instanceOf(CharSerializer.class));

    keySerializer = provider.createKeySerializer(Character.class, getSystemClassLoader(), getPersistenceSpaceIdentifierMock());
    assertThat(keySerializer, instanceOf(CharSerializer.class));
  }

  @Test
  public void testDefaultDoubleSerializer() throws Exception {
    DefaultSerializationProvider provider = getStartedProvider();
    Serializer<Double> keySerializer = provider.createKeySerializer(Double.class, getSystemClassLoader());
    assertThat(keySerializer, instanceOf(DoubleSerializer.class));

    keySerializer = provider.createKeySerializer(Double.class, getSystemClassLoader(), getPersistenceSpaceIdentifierMock());
    assertThat(keySerializer, instanceOf(DoubleSerializer.class));
  }

  @Test
  public void testDefaultFloatSerializer() throws Exception {
    DefaultSerializationProvider provider = getStartedProvider();
    Serializer<Float> keySerializer = provider.createKeySerializer(Float.class, getSystemClassLoader());
    assertThat(keySerializer, instanceOf(FloatSerializer.class));

    keySerializer = provider.createKeySerializer(Float.class, getSystemClassLoader(), getPersistenceSpaceIdentifierMock());
    assertThat(keySerializer, instanceOf(FloatSerializer.class));
  }

  @Test
  public void testDefaultByteArraySerializer() throws Exception {
    DefaultSerializationProvider provider = getStartedProvider();
    Serializer<byte[]> keySerializer = provider.createKeySerializer(byte[].class, getSystemClassLoader());
    assertThat(keySerializer, instanceOf(ByteArraySerializer.class));

    keySerializer = provider.createKeySerializer(byte[].class, getSystemClassLoader(), getPersistenceSpaceIdentifierMock());
    assertThat(keySerializer, instanceOf(ByteArraySerializer.class));
  }

  private PersistableResourceService.PersistenceSpaceIdentifier getPersistenceSpaceIdentifierMock() {
    PersistableResourceService.PersistenceSpaceIdentifier spaceIdentifier = mock(LocalPersistenceService.PersistenceSpaceIdentifier.class);
    when(spaceIdentifier.getServiceType()).thenReturn(LocalPersistenceService.class);
    return spaceIdentifier;
  }

  private DefaultSerializationProvider getStartedProvider() throws CachePersistenceException {
    DefaultSerializationProvider defaultProvider = new DefaultSerializationProvider(null);

    ServiceProvider serviceProvider = mock(ServiceProvider.class);
    LocalPersistenceService persistenceService = mock(LocalPersistenceService.class);
    StateRepository stateRepository = mock(StateRepository.class);
    when(stateRepository.getPersistentConcurrentMap(any(String.class), any(Class.class), any(Class.class))).thenReturn(new ConcurrentHashMap());
    when(persistenceService.getStateRepositoryWithin(any(PersistableResourceService.PersistenceSpaceIdentifier.class), any(String.class))).thenReturn(stateRepository);
    when(persistenceService.createPersistenceContextWithin(any(PersistableResourceService.PersistenceSpaceIdentifier.class), anyString()))
          .thenReturn(new FileBasedPersistenceContext() {
            @Override
            public File getDirectory() {
              try {
                return tempFolder.newFolder();
              } catch (IOException e) {
                fail("unable to create persistence ");
                return null;
              }
            }
          });
    when(serviceProvider.getService(LocalPersistenceService.class)).thenReturn(persistenceService);
    defaultProvider.start(serviceProvider);
    return defaultProvider;
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

  public static class CloseableSerializer<T> implements Serializer, Closeable {

    boolean closed = false;

    public CloseableSerializer() {

    }

    public CloseableSerializer(ClassLoader classLoader) {

    }

    @Override
    public void close() throws IOException {
      closed = true;
    }

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
  }
}
