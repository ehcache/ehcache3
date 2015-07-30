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
package org.ehcache.spi.serialization;

import org.ehcache.config.SerializerConfiguration;
import org.ehcache.config.serializer.DefaultSerializerConfiguration;
import org.ehcache.config.serializer.DefaultSerializationProviderConfiguration;
import org.ehcache.internal.classes.ClassInstanceProvider;
import org.ehcache.internal.serialization.JavaSerializer;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.fail;

/**
 * @author Ludovic Orban
 */
public class DefaultSerializationProviderTest {

  @Test
  public void testCreateSerializerNoConfig() throws Exception {
    DefaultSerializationProviderConfiguration dspfConfig = new DefaultSerializationProviderConfiguration();
    DefaultSerializationProvider dsp = new DefaultSerializationProvider(dspfConfig);
    dsp.start(null);

    assertThat(dsp.createValueSerializer(String.class, ClassLoader.getSystemClassLoader()), instanceOf(JavaSerializer.class));
    try {
      dsp.createValueSerializer(Object.class, ClassLoader.getSystemClassLoader());
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // expected
    }
  }

  @Test
  public void testRefusesSerializerConfigAtCreation() {
    DefaultSerializationProviderFactory factory = new DefaultSerializationProviderFactory();

    try {
      factory.create(new DefaultSerializerConfiguration<String>((Class)JavaSerializer.class, SerializerConfiguration.Type.KEY));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), Is.is("DefaultCacheLoaderWriterConfiguration must not be provided at CacheManager level"));
    }
  }

  @Test
  public void testCreateSerializerWithConfig() throws Exception {
    DefaultSerializationProviderConfiguration dspfConfig = new DefaultSerializationProviderConfiguration();
    DefaultSerializationProvider dsp = new DefaultSerializationProvider(dspfConfig);
    dsp.start(null);

    DefaultSerializerConfiguration dspConfig = new DefaultSerializerConfiguration((Class) TestSerializer.class, DefaultSerializerConfiguration.Type.VALUE);

    assertThat(dsp.createValueSerializer(String.class, ClassLoader.getSystemClassLoader(), dspConfig), instanceOf(TestSerializer.class));
    assertThat(dsp.createValueSerializer(Object.class, ClassLoader.getSystemClassLoader(), dspConfig), instanceOf(TestSerializer.class));
  }

  @Test
  public void testCreateSerializerWithFactoryConfig() throws Exception {
    DefaultSerializationProviderConfiguration dspfConfig = new DefaultSerializationProviderConfiguration();
    dspfConfig.addSerializerFor(Number.class, (Class) TestSerializer.class);
    DefaultSerializationProvider dsp = new DefaultSerializationProvider(dspfConfig);
    dsp.start(null);

    assertThat(dsp.createValueSerializer(Long.class, ClassLoader.getSystemClassLoader()), instanceOf(TestSerializer.class));
    assertThat(dsp.createValueSerializer(String.class, ClassLoader.getSystemClassLoader()), instanceOf(JavaSerializer.class));
  }

  @Test
  public void testGetPreconfigured() throws Exception {
    DefaultSerializationProviderConfiguration dspfConfig = new DefaultSerializationProviderConfiguration();
    dspfConfig.addSerializerFor(String.class, (Class) TestSerializer.class);

    DefaultSerializationProvider dsp = new DefaultSerializationProvider(dspfConfig);
    dsp.start(null);

    ClassInstanceProvider.ConstructorArgument arg = new ClassInstanceProvider.ConstructorArgument<ClassLoader>(ClassLoader.class, ClassLoader.getSystemClassLoader());

    assertThat(dsp.getPreconfigured("java.lang.String", arg), equalTo((Class) TestSerializer.class));
    assertThat(dsp.getPreconfigured("java.io.Serializable", arg), equalTo((Class) JavaSerializer.class));
    assertThat(dsp.getPreconfigured("java.lang.Integer", arg), equalTo((Class) JavaSerializer.class));
    assertThat(dsp.getPreconfigured("java.lang.Object", arg), is(nullValue()));
  }

  @Test
  public void testGetPreconfiguredWithOverriddenSerializableType() throws Exception {
    DefaultSerializationProviderConfiguration dspfConfig = new DefaultSerializationProviderConfiguration();
    dspfConfig.addSerializerFor(Serializable.class, (Class) TestSerializer.class);

    DefaultSerializationProvider dsp = new DefaultSerializationProvider(dspfConfig);
    dsp.start(null);

    ClassInstanceProvider.ConstructorArgument arg = new ClassInstanceProvider.ConstructorArgument<ClassLoader>(ClassLoader.class, ClassLoader.getSystemClassLoader());

    assertThat(dsp.getPreconfigured("java.lang.String", arg), equalTo((Class) TestSerializer.class));
    assertThat(dsp.getPreconfigured("java.io.Serializable", arg), equalTo((Class) TestSerializer.class));
    assertThat(dsp.getPreconfigured("java.lang.Integer", arg), equalTo((Class) TestSerializer.class));
  }

  public static class TestSerializer<T> implements Serializer<T> {
    public TestSerializer(ClassLoader classLoader) {
    }
    @Override
    public ByteBuffer serialize(T object) throws IOException {
      return null;
    }
    @Override
    public T read(ByteBuffer binary) throws IOException, ClassNotFoundException {
      return null;
    }
    @Override
    public boolean equals(T object, ByteBuffer binary) throws IOException, ClassNotFoundException {
      return false;
    }
  }

}
