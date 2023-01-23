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

package org.ehcache.impl.config.serializer;

import org.ehcache.impl.serialization.JavaSerializer;
import org.ehcache.spi.persistence.StateRepository;
import org.ehcache.spi.serialization.SerializerException;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.core.spi.service.FileBasedPersistenceContext;
import org.ehcache.spi.serialization.StatefulSerializer;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

public class DefaultSerializationProviderConfigurationTest {

  @Test
  public void testAddSerializerFor() throws Exception {
    DefaultSerializationProviderConfiguration config = new DefaultSerializationProviderConfiguration();
    config.addSerializerFor(Long.class, MinimalSerializer.class);

    assertSame(MinimalSerializer.class, config.getDefaultSerializers().get(Long.class));
  }

  @Test
  public void testAddSerializerForDuplicateThrows() throws Exception {
    DefaultSerializationProviderConfiguration config = new DefaultSerializationProviderConfiguration();
    config.addSerializerFor(Long.class, MinimalSerializer.class);
    IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> config.addSerializerFor(Long.class, MinimalSerializer.class));
    assertThat(thrown, hasProperty("message", startsWith("Duplicate serializer for class")));
  }

  @Test
  public void testAddSerializerForConstructorless() throws Exception {
    DefaultSerializationProviderConfiguration config = new DefaultSerializationProviderConfiguration();
    IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> config.addSerializerFor(Long.class, UnusableSerializer.class));
    assertThat(thrown, hasProperty("message", endsWith("does not have a constructor that takes in a ClassLoader.")));
  }

  @Test
  public void testAddSerializerForStatefulSerializer() throws Exception {
    DefaultSerializationProviderConfiguration config = new DefaultSerializationProviderConfiguration();
    config.addSerializerFor(Long.class, MinimalStatefulSerializer.class);
    assertSame(MinimalStatefulSerializer.class, config.getDefaultSerializers().get(Long.class));
  }

  @Test
  public void testAddSerializerForStatefulConstructorless() throws Exception {
    DefaultSerializationProviderConfiguration config = new DefaultSerializationProviderConfiguration();
    IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> config.addSerializerFor(Long.class, UnusableStatefulSerializer.class));
    assertThat(thrown, hasProperty("message", endsWith("does not have a constructor that takes in a ClassLoader.")));
  }

  @Test
  public void testAddSerializerForLegacySerializer() throws Exception {
    DefaultSerializationProviderConfiguration config = new DefaultSerializationProviderConfiguration();
    IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> config.addSerializerFor(Long.class, LegacySerializer.class));
    assertThat(thrown, hasProperty("message", endsWith("does not have a constructor that takes in a ClassLoader.")));
  }

  @Test @SuppressWarnings("unchecked")
  public void testDeriveDetachesCorrectly() {
    DefaultSerializationProviderConfiguration configuration = new DefaultSerializationProviderConfiguration();
    configuration.addSerializerFor(String.class, (Class) JavaSerializer.class);

    DefaultSerializationProviderConfiguration derived = configuration.build(configuration.derive());

    assertThat(derived, is(not(sameInstance(configuration))));
    assertThat(derived.getDefaultSerializers(), is(not(sameInstance(configuration.getDefaultSerializers()))));
    assertThat(derived.getDefaultSerializers().get(String.class), sameInstance(JavaSerializer.class));
  }

  private static class MinimalSerializer implements Serializer<Long> {

    public MinimalSerializer(ClassLoader loader) {
    }

    @Override
    public ByteBuffer serialize(final Long object) throws SerializerException {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public Long read(final ByteBuffer binary) throws ClassNotFoundException, SerializerException {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public boolean equals(final Long object, final ByteBuffer binary) throws ClassNotFoundException, SerializerException {
      throw new UnsupportedOperationException("Implement me!");
    }
  }

  private static class LegacySerializer implements Serializer<Long> {

    public LegacySerializer(ClassLoader loader, FileBasedPersistenceContext context) {
    }

    @Override
    public ByteBuffer serialize(final Long object) throws SerializerException {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public Long read(final ByteBuffer binary) throws ClassNotFoundException, SerializerException {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public boolean equals(final Long object, final ByteBuffer binary) throws ClassNotFoundException, SerializerException {
      throw new UnsupportedOperationException("Implement me!");
    }
  }

  private static class UnusableSerializer implements Serializer<Long> {

    @Override
    public ByteBuffer serialize(final Long object) throws SerializerException {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public Long read(final ByteBuffer binary) throws ClassNotFoundException, SerializerException {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public boolean equals(final Long object, final ByteBuffer binary) throws ClassNotFoundException, SerializerException {
      throw new UnsupportedOperationException("Implement me!");
    }
  }

  private static class MinimalStatefulSerializer implements StatefulSerializer<Long> {

    public MinimalStatefulSerializer(ClassLoader loader) {
    }

    @Override
    public void init(final StateRepository stateRepository) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public ByteBuffer serialize(final Long object) throws SerializerException {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public Long read(final ByteBuffer binary) throws ClassNotFoundException, SerializerException {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public boolean equals(final Long object, final ByteBuffer binary) throws ClassNotFoundException, SerializerException {
      throw new UnsupportedOperationException("Implement me!");
    }
  }

  private static class UnusableStatefulSerializer implements StatefulSerializer<Long> {

    @Override
    public void init(final StateRepository stateRepository) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public ByteBuffer serialize(final Long object) throws SerializerException {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public Long read(final ByteBuffer binary) throws ClassNotFoundException, SerializerException {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public boolean equals(final Long object, final ByteBuffer binary) throws ClassNotFoundException, SerializerException {
      throw new UnsupportedOperationException("Implement me!");
    }
  }
}
