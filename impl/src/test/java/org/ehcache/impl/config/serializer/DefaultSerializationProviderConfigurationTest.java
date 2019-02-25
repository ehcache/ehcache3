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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsSame.sameInstance;
import static org.junit.Assert.*;

public class DefaultSerializationProviderConfigurationTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

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
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Duplicate serializer for class");
    config.addSerializerFor(Long.class, MinimalSerializer.class);
  }

  @Test
  public void testAddSerializerForConstructorless() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("does not have a constructor that takes in a ClassLoader.");
    DefaultSerializationProviderConfiguration config = new DefaultSerializationProviderConfiguration();
    config.addSerializerFor(Long.class, UnusableSerializer.class);
  }

  @Test
  public void testAddSerializerForStatefulSerializer() throws Exception {
    DefaultSerializationProviderConfiguration config = new DefaultSerializationProviderConfiguration();
    config.addSerializerFor(Long.class, MinimalStatefulSerializer.class);
    assertSame(MinimalStatefulSerializer.class, config.getDefaultSerializers().get(Long.class));
  }

  @Test
  public void testAddSerializerForStatefulConstructorless() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("does not have a constructor that takes in a ClassLoader.");
    DefaultSerializationProviderConfiguration config = new DefaultSerializationProviderConfiguration();
    config.addSerializerFor(Long.class, UnusableStatefulSerializer.class);
  }

  @Test
  public void testAddSerializerForLegacySerializer() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("does not have a constructor that takes in a ClassLoader.");
    DefaultSerializationProviderConfiguration config = new DefaultSerializationProviderConfiguration();
    config.addSerializerFor(Long.class, LegacySerializer.class);
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
