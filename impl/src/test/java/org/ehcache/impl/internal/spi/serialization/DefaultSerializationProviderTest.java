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
import org.ehcache.core.spi.service.DiskResourceService;
import org.ehcache.core.spi.service.FileBasedPersistenceContext;
import org.ehcache.impl.config.serializer.DefaultSerializationProviderConfiguration;
import org.ehcache.impl.config.serializer.DefaultSerializerConfiguration;
import org.ehcache.impl.serialization.ByteArraySerializer;
import org.ehcache.impl.serialization.CharSerializer;
import org.ehcache.impl.serialization.CompactJavaSerializer;
import org.ehcache.impl.serialization.DoubleSerializer;
import org.ehcache.impl.serialization.FloatSerializer;
import org.ehcache.impl.serialization.IntegerSerializer;
import org.ehcache.impl.serialization.LongSerializer;
import org.ehcache.impl.serialization.StringSerializer;
import org.ehcache.spi.persistence.PersistableResourceService;
import org.ehcache.spi.persistence.StateRepository;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.SerializerException;
import org.ehcache.spi.serialization.StatefulSerializer;
import org.ehcache.spi.serialization.UnsupportedTypeException;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceProvider;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.terracotta.org.junit.rules.TemporaryFolder;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.ClassLoader.getSystemClassLoader;
import static org.ehcache.impl.internal.spi.TestServiceProvider.providerContaining;
import static org.ehcache.test.MockitoUtil.mock;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * @author Ludovic Orban
 */
public class DefaultSerializationProviderTest {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

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

    DefaultSerializerConfiguration<?> dspConfig = new DefaultSerializerConfiguration<>(getSerializerClass(), DefaultSerializerConfiguration.Type.VALUE);

    assertThat(dsp.createValueSerializer(String.class, ClassLoader.getSystemClassLoader(), dspConfig), instanceOf(TestSerializer.class));
    assertThat(dsp.createValueSerializer(Object.class, ClassLoader.getSystemClassLoader(), dspConfig), instanceOf(TestSerializer.class));
  }

  @Test
  public void testCreateSerializerWithFactoryConfig() throws Exception {
    DefaultSerializationProviderConfiguration dspfConfig = new DefaultSerializationProviderConfiguration();
    Class<Serializer<Long>> serializerClass = getSerializerClass();
    dspfConfig.addSerializerFor(Long.class, serializerClass);
    DefaultSerializationProvider dsp = new DefaultSerializationProvider(dspfConfig);
    dsp.start(providerContaining());

    assertThat(dsp.createValueSerializer(Long.class, ClassLoader.getSystemClassLoader()), instanceOf(TestSerializer.class));
    assertThat(dsp.createValueSerializer(HashMap.class, ClassLoader.getSystemClassLoader()), instanceOf(CompactJavaSerializer.class));
  }

  @SuppressWarnings("unchecked")
  private <T> Class<Serializer<T>> getSerializerClass() {
    return (Class) TestSerializer.class;
  }

  @Test
  public void testCreateTransientSerializers() throws Exception {
    DefaultSerializationProviderConfiguration dspfConfig = new DefaultSerializationProviderConfiguration();
    Class<Serializer<String>> serializerClass = getSerializerClass();
    dspfConfig.addSerializerFor(String.class, serializerClass);

    DefaultSerializationProvider dsp = new DefaultSerializationProvider(dspfConfig);
    dsp.start(providerContaining());

    assertThat(dsp.createKeySerializer(String.class, getSystemClassLoader()), instanceOf(TestSerializer.class));
    assertThat(dsp.createKeySerializer(Serializable.class, getSystemClassLoader()), instanceOf(CompactJavaSerializer.class));
    assertThat(dsp.createKeySerializer(Integer.class, getSystemClassLoader()), instanceOf(IntegerSerializer.class));
  }

  @Test
  public void tesCreateTransientSerializersWithOverriddenSerializableType() throws Exception {
    DefaultSerializationProviderConfiguration dspfConfig = new DefaultSerializationProviderConfiguration();
    Class<Serializer<Serializable>> serializerClass = getSerializerClass();
    dspfConfig.addSerializerFor(Serializable.class, serializerClass);

    DefaultSerializationProvider dsp = new DefaultSerializationProvider(dspfConfig);
    dsp.start(providerContaining());

    assertThat(dsp.createKeySerializer(HashMap.class, getSystemClassLoader()), instanceOf(TestSerializer.class));
    assertThat(dsp.createKeySerializer(Serializable.class, getSystemClassLoader()), instanceOf(TestSerializer.class));
    assertThat(dsp.createKeySerializer(Integer.class, getSystemClassLoader()), instanceOf(IntegerSerializer.class));
  }

  @Test
  public void testRemembersCreationConfigurationAfterStopStart() throws UnsupportedTypeException {
    DefaultSerializationProviderConfiguration configuration = new DefaultSerializationProviderConfiguration();
    Class<Serializer<String>> serializerClass = getSerializerClass();
    configuration.addSerializerFor(String.class, serializerClass);
    DefaultSerializationProvider serializationProvider = new DefaultSerializationProvider(configuration);
    @SuppressWarnings("unchecked")
    ServiceProvider<Service> serviceProvider = mock(ServiceProvider.class);
    serializationProvider.start(serviceProvider);
    assertThat(serializationProvider.createKeySerializer(String.class, getSystemClassLoader()), instanceOf(TestSerializer.class));
    serializationProvider.stop();
    serializationProvider.start(serviceProvider);
    assertThat(serializationProvider.createKeySerializer(String.class, getSystemClassLoader()), instanceOf(TestSerializer.class));
  }

  @Test
  public void testReleaseSerializerWithProvidedCloseableSerializerDoesNotClose() throws Exception {
    DefaultSerializationProvider provider = new DefaultSerializationProvider(null);
    CloseableSerializer<?> closeableSerializer = new CloseableSerializer<>();
    provider.providedVsCount.put(closeableSerializer, new AtomicInteger(1));

    provider.releaseSerializer(closeableSerializer);
    assertThat(closeableSerializer.closed, is(false));
  }

  @Test
  public void testReleaseSerializerWithInstantiatedCloseableSerializerDoesClose() throws Exception {
    @SuppressWarnings("unchecked")
    Class<? extends Serializer<String>> serializerClass = (Class) CloseableSerializer.class;
    DefaultSerializerConfiguration<String> config = new DefaultSerializerConfiguration<>(serializerClass, DefaultSerializerConfiguration.Type.KEY);
    DefaultSerializationProvider provider = new DefaultSerializationProvider(null);
    Serializer<?> serializer = provider.createKeySerializer(String.class, getSystemClassLoader(), config);

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
    @SuppressWarnings("unchecked")
    TestSerializer<String> serializer = mock(TestSerializer.class);
    DefaultSerializerConfiguration<String> config = new DefaultSerializerConfiguration<>(serializer, DefaultSerializerConfiguration.Type.KEY);
    Serializer<?> created = provider.createKeySerializer(TestSerializer.class, getSystemClassLoader(), config);
    assertSame(serializer, created);
  }

  @Test
  public void testSameInstanceRetrievedMultipleTimesUpdatesTheProvidedCount() throws Exception {
    DefaultSerializationProvider provider = new DefaultSerializationProvider(null);
    @SuppressWarnings("unchecked")
    TestSerializer<String> serializer = mock(TestSerializer.class);
    DefaultSerializerConfiguration<String> config = new DefaultSerializerConfiguration<>(serializer, DefaultSerializerConfiguration.Type.KEY);

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

  @Test
  public void testCreateTransientSerializerWithoutConstructor() throws Exception {
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("does not have a constructor that takes in a ClassLoader.");
    DefaultSerializationProvider provider = new DefaultSerializationProvider(null);
    provider.start(providerContaining());

    @SuppressWarnings("unchecked")
    Class<Serializer<Object>> serializerClass = (Class) BaseSerializer.class;
    DefaultSerializerConfiguration<Object> configuration = new DefaultSerializerConfiguration<>(serializerClass, DefaultSerializerConfiguration.Type.VALUE);
    provider.createValueSerializer(Object.class, ClassLoader.getSystemClassLoader(), configuration);
  }

  @Test
  public void testCreatePersistentSerializerWithoutConstructor() throws Exception {
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("does not have a constructor that takes in a ClassLoader.");
    DefaultSerializationProvider provider = new DefaultSerializationProvider(null);
    provider.start(providerContaining());

    @SuppressWarnings("unchecked")
    Class<Serializer<Object>> serializerClass = (Class) BaseSerializer.class;
    DefaultSerializerConfiguration<Object> configuration = new DefaultSerializerConfiguration<>(serializerClass, DefaultSerializerConfiguration.Type.VALUE);
    provider.createValueSerializer(Object.class, ClassLoader.getSystemClassLoader(), configuration, getPersistenceSpaceIdentifierMock());
  }

  @Test
  public void testCreateTransientStatefulSerializerWithoutConstructor() throws Exception {
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("does not have a constructor that takes in a ClassLoader.");
    DefaultSerializationProvider provider = new DefaultSerializationProvider(null);
    provider.start(providerContaining());

    @SuppressWarnings("unchecked")
    Class<Serializer<Object>> serializerClass = (Class) StatefulBaseSerializer.class;
    DefaultSerializerConfiguration<Object> configuration = new DefaultSerializerConfiguration<>(serializerClass, DefaultSerializerConfiguration.Type.VALUE);
    provider.createValueSerializer(Object.class, ClassLoader.getSystemClassLoader(), configuration);
  }

  @Test
  public void testCreatePersistentStatefulSerializerWithoutConstructor() throws Exception {
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("does not have a constructor that takes in a ClassLoader.");
    DefaultSerializationProvider provider = new DefaultSerializationProvider(null);
    provider.start(providerContaining());

    @SuppressWarnings("unchecked")
    Class<Serializer<Object>> serializerClass = (Class) StatefulBaseSerializer.class;
    DefaultSerializerConfiguration<Object> configuration = new DefaultSerializerConfiguration<>(serializerClass, DefaultSerializerConfiguration.Type.VALUE);
    provider.createValueSerializer(Object.class, ClassLoader.getSystemClassLoader(), configuration, getPersistenceSpaceIdentifierMock());
  }

  @Test
  public void testCreateTransientMinimalSerializer() throws Exception {
    DefaultSerializationProvider provider = new DefaultSerializationProvider(null);
    provider.start(providerContaining());

    MinimalSerializer.baseConstructorInvoked = false;
    @SuppressWarnings("unchecked")
    Class<Serializer<Object>> serializerClass = (Class) MinimalSerializer.class;
    DefaultSerializerConfiguration<Object> configuration = new DefaultSerializerConfiguration<>(serializerClass, DefaultSerializerConfiguration.Type.VALUE);
    Serializer<Object> valueSerializer =
      provider.createValueSerializer(Object.class, ClassLoader.getSystemClassLoader(), configuration);
    assertThat(valueSerializer, instanceOf(MinimalSerializer.class));
    assertThat(MinimalSerializer.baseConstructorInvoked, is(true));
  }

  @Test
  public void testCreatePersistentMinimalSerializer() throws Exception {
    DefaultSerializationProvider provider = new DefaultSerializationProvider(null);
    provider.start(providerContaining());

    MinimalSerializer.baseConstructorInvoked = false;
    @SuppressWarnings("unchecked")
    Class<Serializer<Object>> serializerClass = (Class) MinimalSerializer.class;
    DefaultSerializerConfiguration<Object> configuration = new DefaultSerializerConfiguration<>(serializerClass, DefaultSerializerConfiguration.Type.VALUE);
    Serializer<Object> valueSerializer =
      provider.createValueSerializer(Object.class, ClassLoader.getSystemClassLoader(), configuration, getPersistenceSpaceIdentifierMock());
    assertThat(valueSerializer, instanceOf(MinimalSerializer.class));
    assertThat(MinimalSerializer.baseConstructorInvoked, is(true));
  }

  @Test
  public void testTransientMinimalStatefulSerializer() throws Exception {
    DefaultSerializationProvider provider = new DefaultSerializationProvider(null);
    provider.start(providerContaining());

    MinimalStatefulSerializer.baseConstructorInvoked = false;
    @SuppressWarnings("unchecked")
    Class<Serializer<Object>> serializerClass = (Class) MinimalStatefulSerializer.class;
    DefaultSerializerConfiguration<Object> configuration = new DefaultSerializerConfiguration<>(serializerClass, DefaultSerializerConfiguration.Type.VALUE);
    Serializer<Object> valueSerializer =
      provider.createValueSerializer(Object.class, ClassLoader.getSystemClassLoader(), configuration);
    assertThat(valueSerializer, instanceOf(MinimalStatefulSerializer.class));
    assertThat(MinimalStatefulSerializer.baseConstructorInvoked, is(true));
  }

  @Test
  public void testPersistentMinimalStatefulSerializer() throws Exception {
    DefaultSerializationProvider provider = new DefaultSerializationProvider(null);
    provider.start(providerContaining());

    MinimalStatefulSerializer.baseConstructorInvoked = false;
    @SuppressWarnings("unchecked")
    Class<Serializer<Object>> serializerClass = (Class) MinimalStatefulSerializer.class;
    DefaultSerializerConfiguration<Object> configuration = new DefaultSerializerConfiguration<>(serializerClass, DefaultSerializerConfiguration.Type.VALUE);
    Serializer<Object> valueSerializer =
      provider.createValueSerializer(Object.class, ClassLoader.getSystemClassLoader(), configuration, getPersistenceSpaceIdentifierMock());
    assertThat(valueSerializer, instanceOf(MinimalStatefulSerializer.class));
    assertThat(MinimalStatefulSerializer.baseConstructorInvoked, is(true));
  }

  @Test
  public void testTransientLegacySerializer() throws Exception {
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("does not have a constructor that takes in a ClassLoader.");
    DefaultSerializationProvider provider = new DefaultSerializationProvider(null);
    provider.start(providerContaining());

    @SuppressWarnings("unchecked")
    Class<Serializer<Object>> serializerClass = (Class) LegacySerializer.class;
    DefaultSerializerConfiguration<Object> configuration = new DefaultSerializerConfiguration<>(serializerClass, DefaultSerializerConfiguration.Type.VALUE);
    provider.createValueSerializer(Object.class, ClassLoader.getSystemClassLoader(), configuration);
  }

  @Test
  public void testPersistentLegacySerializer() throws Exception {
    DefaultSerializationProvider provider = getStartedProvider();

    LegacySerializer.legacyConstructorInvoked = false;
    @SuppressWarnings("unchecked")
    Class<Serializer<Object>> serializerClass = (Class) LegacySerializer.class;
    DefaultSerializerConfiguration<Object> configuration = new DefaultSerializerConfiguration<>(serializerClass, DefaultSerializerConfiguration.Type.VALUE);
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("does not have a constructor that takes in a ClassLoader.");
    Serializer<Object> valueSerializer =
      provider.createValueSerializer(Object.class, ClassLoader.getSystemClassLoader(), configuration, getPersistenceSpaceIdentifierMock());
    assertThat(valueSerializer, instanceOf(LegacySerializer.class));
    assertThat(LegacySerializer.legacyConstructorInvoked, is(true));
  }

  @Test
  public void testTransientLegacyComboSerializer() throws Exception {
    DefaultSerializationProvider provider = new DefaultSerializationProvider(null);
    provider.start(providerContaining());

    LegacyComboSerializer.baseConstructorInvoked = false;
    LegacyComboSerializer.legacyConstructorInvoked = false;
    @SuppressWarnings("unchecked")
    Class<Serializer<Object>> serializerClass = (Class) LegacyComboSerializer.class;
    DefaultSerializerConfiguration<Object> configuration = new DefaultSerializerConfiguration<>(serializerClass, DefaultSerializerConfiguration.Type.VALUE);
    Serializer<Object> valueSerializer =
      provider.createValueSerializer(Object.class, ClassLoader.getSystemClassLoader(), configuration);
    assertThat(valueSerializer, instanceOf(LegacyComboSerializer.class));
    assertThat(LegacyComboSerializer.baseConstructorInvoked, is(true));
    assertThat(LegacyComboSerializer.legacyConstructorInvoked, is(false));
  }

  @Test
  public void testPersistentLegacyComboSerializer() throws Exception {
    DefaultSerializationProvider provider = getStartedProvider();

    LegacyComboSerializer.baseConstructorInvoked = false;
    LegacyComboSerializer.legacyConstructorInvoked = false;
    @SuppressWarnings("unchecked")
    Class<Serializer<Object>> serializerClass = (Class) LegacyComboSerializer.class;
    DefaultSerializerConfiguration<Object> configuration = new DefaultSerializerConfiguration<>(serializerClass, DefaultSerializerConfiguration.Type.VALUE);
    Serializer<Object> valueSerializer =
      provider.createValueSerializer(Object.class, ClassLoader.getSystemClassLoader(), configuration, getPersistenceSpaceIdentifierMock());
    assertThat(valueSerializer, instanceOf(LegacyComboSerializer.class));
    assertThat(LegacyComboSerializer.baseConstructorInvoked, is(true));
    assertThat(LegacyComboSerializer.legacyConstructorInvoked, is(false));
  }

  @Test
  public void testCreateTransientStatefulLegacySerializer() throws Exception {
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("does not have a constructor that takes in a ClassLoader.");
    DefaultSerializationProvider provider = new DefaultSerializationProvider(null);
    provider.start(providerContaining());

    @SuppressWarnings("unchecked")
    Class<Serializer<Object>> serializerClass = (Class) StatefulLegacySerializer.class;
    DefaultSerializerConfiguration<Object> configuration = new DefaultSerializerConfiguration<>(serializerClass, DefaultSerializerConfiguration.Type.VALUE);
    provider.createValueSerializer(Object.class, ClassLoader.getSystemClassLoader(), configuration);
  }

  @Test
  public void testCreatePersistentStatefulLegacySerializer() throws Exception {
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("does not have a constructor that takes in a ClassLoader.");
    DefaultSerializationProvider provider = new DefaultSerializationProvider(null);
    provider.start(providerContaining());

    @SuppressWarnings("unchecked")
    Class<Serializer<Object>> serializerClass = (Class) StatefulLegacySerializer.class;
    DefaultSerializerConfiguration<Object> configuration = new DefaultSerializerConfiguration<>(serializerClass, DefaultSerializerConfiguration.Type.VALUE);
    provider.createValueSerializer(Object.class, ClassLoader.getSystemClassLoader(), configuration, getPersistenceSpaceIdentifierMock());
  }

  @Test
  public void testTransientStatefulLegacyComboSerializer() throws Exception {
    DefaultSerializationProvider provider = new DefaultSerializationProvider(null);
    provider.start(providerContaining());

    StatefulLegacyComboSerializer.baseConstructorInvoked = false;
    StatefulLegacyComboSerializer.legacyConstructorInvoked = false;
    @SuppressWarnings("unchecked")
    Class<Serializer<Object>> serializerClass = (Class) StatefulLegacyComboSerializer.class;
    DefaultSerializerConfiguration<Object> configuration = new DefaultSerializerConfiguration<>(serializerClass, DefaultSerializerConfiguration.Type.VALUE);
    Serializer<Object> valueSerializer =
      provider.createValueSerializer(Object.class, ClassLoader.getSystemClassLoader(), configuration);
    assertThat(valueSerializer, instanceOf(StatefulLegacyComboSerializer.class));
    assertThat(StatefulLegacyComboSerializer.baseConstructorInvoked, is(true));
    assertThat(StatefulLegacyComboSerializer.legacyConstructorInvoked, is(false));
  }

  @Test
  public void testPersistentStatefulLegacyComboSerializer() throws Exception {
    DefaultSerializationProvider provider = getStartedProvider();

    StatefulLegacyComboSerializer.baseConstructorInvoked = false;
    StatefulLegacyComboSerializer.legacyConstructorInvoked = false;
    @SuppressWarnings("unchecked")
    Class<Serializer<Object>> serializerClass = (Class) StatefulLegacyComboSerializer.class;
    DefaultSerializerConfiguration<Object> configuration = new DefaultSerializerConfiguration<>(serializerClass, DefaultSerializerConfiguration.Type.VALUE);
    Serializer<Object> valueSerializer =
      provider.createValueSerializer(Object.class, ClassLoader.getSystemClassLoader(), configuration, getPersistenceSpaceIdentifierMock());
    assertThat(valueSerializer, instanceOf(StatefulLegacyComboSerializer.class));
    assertThat(StatefulLegacyComboSerializer.baseConstructorInvoked, is(true));
    assertThat(StatefulLegacyComboSerializer.legacyConstructorInvoked, is(false));
  }

  private PersistableResourceService.PersistenceSpaceIdentifier<?> getPersistenceSpaceIdentifierMock() {
    PersistableResourceService.PersistenceSpaceIdentifier<DiskResourceService> spaceIdentifier = mock(DiskResourceService.PersistenceSpaceIdentifier.class);
    when(spaceIdentifier.getServiceType()).thenReturn(DiskResourceService.class);
    return spaceIdentifier;
  }

  private DefaultSerializationProvider getStartedProvider() throws CachePersistenceException {
    DefaultSerializationProvider defaultProvider = new DefaultSerializationProvider(null);

    @SuppressWarnings("unchecked")
    ServiceProvider<Service> serviceProvider = mock(ServiceProvider.class);
    DiskResourceService diskResourceService = mock(DiskResourceService.class);
    when(diskResourceService.createPersistenceContextWithin(any(PersistableResourceService.PersistenceSpaceIdentifier.class), anyString()))
          .thenReturn(() -> {
            try {
              return tempFolder.newFolder();
            } catch (IOException e) {
              fail("unable to create persistence ");
              return null;
            }
          });
    when(serviceProvider.getService(DiskResourceService.class)).thenReturn(diskResourceService);
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

  public static class CloseableSerializer<T> implements Serializer<T>, Closeable {

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
    public T read(ByteBuffer binary) throws ClassNotFoundException, SerializerException {
      return null;
    }

    @Override
    public boolean equals(Object object, ByteBuffer binary) throws ClassNotFoundException, SerializerException {
      return false;
    }
  }

  public static class BaseSerializer<T> implements Serializer<T> {

    @Override
    public ByteBuffer serialize(final T object) throws SerializerException {
      return null;
    }

    @Override
    public T read(final ByteBuffer binary) throws ClassNotFoundException, SerializerException {
      return null;
    }

    @Override
    public boolean equals(final T object, final ByteBuffer binary) throws ClassNotFoundException, SerializerException {
      return false;
    }
  }

  public static class MinimalSerializer<T> extends BaseSerializer<T> {

    private static boolean baseConstructorInvoked = false;

    public MinimalSerializer(ClassLoader loader) {
      baseConstructorInvoked = true;
    }

  }

  //Stateful but no constructor
  public static class StatefulBaseSerializer<T> extends BaseSerializer<T> implements StatefulSerializer<T> {

    @Override
    public void init(final StateRepository stateRepository) {
    }
  }

  public static class MinimalStatefulSerializer<T> extends BaseSerializer<T> implements StatefulSerializer<T> {

    private static boolean baseConstructorInvoked = false;

    public MinimalStatefulSerializer(ClassLoader loader) {
      baseConstructorInvoked = true;
    }

    @Override
    public void init(final StateRepository stateRepository) {
    }
  }

  public static class LegacySerializer<T> extends BaseSerializer<T> {

    private static boolean legacyConstructorInvoked = false;

    public LegacySerializer(ClassLoader loader, FileBasedPersistenceContext context) {
      legacyConstructorInvoked = true;
    }
  }

  public static class LegacyComboSerializer<T> extends BaseSerializer<T> {

    private static boolean baseConstructorInvoked = false;
    private static boolean legacyConstructorInvoked = false;

    public LegacyComboSerializer(ClassLoader loader) {
      baseConstructorInvoked = true;
    }

    public LegacyComboSerializer(ClassLoader loader, FileBasedPersistenceContext context) {
      legacyConstructorInvoked = true;
    }
  }

  public static class StatefulLegacySerializer<T> extends StatefulBaseSerializer<T> {

    private static boolean legacyConstructorInvoked = false;

    public StatefulLegacySerializer(ClassLoader loader, FileBasedPersistenceContext context) {
      legacyConstructorInvoked = true;
    }
  }

  public static class StatefulLegacyComboSerializer<T> extends BaseSerializer<T> implements StatefulSerializer<T> {

    private static boolean baseConstructorInvoked = false;
    private static boolean legacyConstructorInvoked = false;

    public StatefulLegacyComboSerializer(final ClassLoader loader) {
      baseConstructorInvoked = true;
    }

    public StatefulLegacyComboSerializer(ClassLoader loader, FileBasedPersistenceContext context) {
      legacyConstructorInvoked = true;
    }

    @Override
    public void init(final StateRepository stateRepository) {
    }
  }
}
