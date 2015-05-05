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

import org.ehcache.config.serializer.DefaultSerializationProviderConfiguration;
import org.ehcache.config.serializer.DefaultSerializationProviderFactoryConfiguration;
import org.ehcache.internal.serialization.JavaSerializer;
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
    DefaultSerializationProvider dsp = new DefaultSerializationProvider();
    DefaultSerializationProviderFactoryConfiguration dspfConfig = new DefaultSerializationProviderFactoryConfiguration();
    dsp.start(dspfConfig, null);

    assertThat(dsp.createSerializer(String.class, ClassLoader.getSystemClassLoader()), instanceOf(JavaSerializer.class));
    try {
      dsp.createSerializer(Object.class, ClassLoader.getSystemClassLoader());
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // expected
    }
  }

  @Test
  public void testCreateSerializerWithConfig() throws Exception {
    DefaultSerializationProvider dsp = new DefaultSerializationProvider();
    DefaultSerializationProviderFactoryConfiguration dspfConfig = new DefaultSerializationProviderFactoryConfiguration();
    dsp.start(dspfConfig, null);

    DefaultSerializationProviderConfiguration dspConfig = new DefaultSerializationProviderConfiguration((Class) TestSerializer.class, DefaultSerializationProviderConfiguration.Type.KEY);

    assertThat(dsp.createSerializer(String.class, ClassLoader.getSystemClassLoader(), dspConfig), instanceOf(TestSerializer.class));
    assertThat(dsp.createSerializer(Object.class, ClassLoader.getSystemClassLoader(), dspConfig), instanceOf(TestSerializer.class));
  }

  @Test
  public void testCreateSerializerWithFactoryConfig() throws Exception {
    DefaultSerializationProvider dsp = new DefaultSerializationProvider();
    DefaultSerializationProviderFactoryConfiguration dspfConfig = new DefaultSerializationProviderFactoryConfiguration();
    dspfConfig.addSerializerFor(Number.class, (Class) TestSerializer.class);
    dsp.start(dspfConfig, null);

    assertThat(dsp.createSerializer(Long.class, ClassLoader.getSystemClassLoader()), instanceOf(TestSerializer.class));
    assertThat(dsp.createSerializer(String.class, ClassLoader.getSystemClassLoader()), instanceOf(JavaSerializer.class));
  }

  @Test
  public void testGetPreconfigured() throws Exception {
    DefaultSerializationProvider dsp = new DefaultSerializationProvider();

    DefaultSerializationProviderFactoryConfiguration dspfConfig = new DefaultSerializationProviderFactoryConfiguration();
    dspfConfig.addSerializerFor(String.class, (Class) TestSerializer.class);
    dsp.start(dspfConfig, null);

    assertThat(dsp.getPreconfigured("java.lang.String"), equalTo((Class) TestSerializer.class));
    assertThat(dsp.getPreconfigured("java.io.Serializable"), equalTo((Class) JavaSerializer.class));
    assertThat(dsp.getPreconfigured("java.lang.Integer"), equalTo((Class) JavaSerializer.class));
    assertThat(dsp.getPreconfigured("java.lang.Object"), is(nullValue()));
  }

  @Test
  public void testGetPreconfiguredWithOverriddenSerializableType() throws Exception {
    DefaultSerializationProvider dsp = new DefaultSerializationProvider();

    DefaultSerializationProviderFactoryConfiguration dspfConfig = new DefaultSerializationProviderFactoryConfiguration();
    dspfConfig.addSerializerFor(Serializable.class, (Class) TestSerializer.class);
    dsp.start(dspfConfig, null);

    assertThat(dsp.getPreconfigured("java.lang.String"), equalTo((Class) TestSerializer.class));
    assertThat(dsp.getPreconfigured("java.io.Serializable"), equalTo((Class) TestSerializer.class));
    assertThat(dsp.getPreconfigured("java.lang.Integer"), equalTo((Class) TestSerializer.class));
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
