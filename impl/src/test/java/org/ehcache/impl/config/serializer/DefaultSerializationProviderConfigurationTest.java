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

import org.ehcache.spi.persistence.StateRepository;
import org.ehcache.spi.serialization.SerializerException;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.core.spi.service.FileBasedPersistenceContext;
import org.ehcache.spi.serialization.StatefulSerializer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class DefaultSerializationProviderConfigurationTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testAddSerializerForTransient() throws Exception {
    DefaultSerializationProviderConfiguration config = new DefaultSerializationProviderConfiguration();
    config.addSerializerFor(Long.class, TransientSerializer.class);

    assertSame(TransientSerializer.class, config.getPersistentSerializers().get(Long.class));
    assertSame(TransientSerializer.class, config.getTransientSerializers().get(Long.class));
  }

  @Test
  public void testAddSerializerForPersistent() throws Exception {
    DefaultSerializationProviderConfiguration config = new DefaultSerializationProviderConfiguration();
    config.addSerializerFor(Long.class, PersistentSerializer.class);

    assertTrue(config.getTransientSerializers().isEmpty());
    assertSame(PersistentSerializer.class, config.getPersistentSerializers().get(Long.class));
  }

  @Test
  public void testAddSerializerForTransientPersistentLegacyCombo() throws Exception {
    DefaultSerializationProviderConfiguration config = new DefaultSerializationProviderConfiguration();
    config.addSerializerFor(Long.class, LegacyComboSerializer.class);

    assertSame(LegacyComboSerializer.class, config.getPersistentSerializers().get(Long.class));
    assertSame(LegacyComboSerializer.class, config.getTransientSerializers().get(Long.class));
  }

  @Test
  public void testAddSerializerForTransientPersistentCombo() throws Exception {
    DefaultSerializationProviderConfiguration config = new DefaultSerializationProviderConfiguration();
    config.addSerializerFor(Long.class, ComboSerializer.class);

    assertSame(ComboSerializer.class, config.getPersistentSerializers().get(Long.class));
    assertSame(ComboSerializer.class, config.getTransientSerializers().get(Long.class));
  }

  @Test
  public void testAddSerializerForConstructorless() throws Exception {
    expectedException.expectMessage("does not meet the constructor requirements for either transient or persistent caches");
    DefaultSerializationProviderConfiguration config = new DefaultSerializationProviderConfiguration();
    config.addSerializerFor(Long.class, UnusableSerializer.class);
  }

  @Test
  public void testAddSerializerForStatefulOnly() throws Exception {
    expectedException.expectMessage("does not meet the constructor requirements for either transient or persistent caches");
    DefaultSerializationProviderConfiguration config = new DefaultSerializationProviderConfiguration();
    config.addSerializerFor(Long.class, YetAnotherUnusableSerializer.class);
  }

  private static class TransientSerializer implements Serializer<Long> {

    public TransientSerializer(ClassLoader loader) {
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

  private static class PersistentSerializer implements Serializer<Long> {

    public PersistentSerializer(ClassLoader loader, FileBasedPersistenceContext context) {
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

  private static class LegacyComboSerializer implements Serializer<Long> {

    public LegacyComboSerializer(ClassLoader loader) {
    }

    public LegacyComboSerializer(ClassLoader loader, FileBasedPersistenceContext context) {
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

  private static class ComboSerializer implements StatefulSerializer<Long> {

    public ComboSerializer(ClassLoader loader) {
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

  private static class AnotherUnusableSerializer implements StatefulSerializer<Long> {

    public AnotherUnusableSerializer(ClassLoader loader, FileBasedPersistenceContext context) {
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

  private static class YetAnotherUnusableSerializer implements StatefulSerializer<Long> {

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
