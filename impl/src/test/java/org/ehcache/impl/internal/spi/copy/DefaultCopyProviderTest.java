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

package org.ehcache.impl.internal.spi.copy;

import org.ehcache.impl.config.copy.DefaultCopierConfiguration;
import org.ehcache.impl.copy.IdentityCopier;
import org.ehcache.impl.copy.ReadWriteCopier;
import org.ehcache.impl.copy.SerializingCopier;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.serialization.Serializer;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Created by alsu on 20/08/15.
 */
public class DefaultCopyProviderTest {

  @Test
  public void testCreateKeyCopierWithCustomCopierConfig() {
    DefaultCopyProvider provider = new DefaultCopyProvider(null);

    @SuppressWarnings("unchecked")
    DefaultCopierConfiguration<Long> config = new DefaultCopierConfiguration<Long>(
        (Class)TestCopier.class, DefaultCopierConfiguration.Type.KEY);

    assertThat(provider.createKeyCopier(Long.class, null, config), instanceOf(TestCopier.class));
  }

  @Test
  public void testCreateKeyCopierWithoutConfig() {
    DefaultCopyProvider provider = new DefaultCopyProvider(null);

    assertThat(provider.createKeyCopier(Long.class, null), instanceOf(IdentityCopier.class));
  }

  @Test
  public void testCreateKeyCopierWithSerializer() {
    DefaultCopyProvider copyProvider = new DefaultCopyProvider(null);
    DefaultCopierConfiguration<Long> config = new DefaultCopierConfiguration<Long>(
        SerializingCopier.<Long>asCopierClass(), DefaultCopierConfiguration.Type.KEY);

    @SuppressWarnings("unchecked")
    Serializer<Long> serializer = mock(Serializer.class);
    assertThat(copyProvider.createKeyCopier(Long.class, serializer, config), instanceOf(SerializingCopier.class));
  }

  @Test
  public void testCreateValueCopierWithCustomCopierConfig() {
    DefaultCopyProvider provider = new DefaultCopyProvider(null);

    @SuppressWarnings("unchecked")
    DefaultCopierConfiguration<Long> config = new DefaultCopierConfiguration<Long>(
        (Class)TestCopier.class, DefaultCopierConfiguration.Type.VALUE);

    assertThat(provider.createValueCopier(Long.class, null, config), instanceOf(TestCopier.class));
  }

  @Test
  public void testCreateValueCopierWithoutConfig() {
    DefaultCopyProvider provider = new DefaultCopyProvider(null);

    assertThat(provider.createValueCopier(Long.class, null), instanceOf(IdentityCopier.class));
  }

  @Test
  public void testCreateValueCopierWithSerializer() {
    DefaultCopyProvider copyProvider = new DefaultCopyProvider(null);
    DefaultCopierConfiguration<Long> config = new DefaultCopierConfiguration<Long>(
        SerializingCopier.<Long>asCopierClass(), DefaultCopierConfiguration.Type.VALUE);

    @SuppressWarnings("unchecked")
    Serializer<Long> serializer = mock(Serializer.class);
    assertThat(copyProvider.createValueCopier(Long.class, serializer, config), instanceOf(SerializingCopier.class));
  }

  @Test
  public void testUserProvidedCloseableCopierInstanceDoesNotCloseOnRelease() throws Exception {
    DefaultCopyProvider copyProvider = new DefaultCopyProvider(null);
    TestCloseableCopier<Long> testCloseableCopier = new TestCloseableCopier<Long>();
    DefaultCopierConfiguration<Long> config = new DefaultCopierConfiguration<Long>(testCloseableCopier, DefaultCopierConfiguration.Type.KEY);

    @SuppressWarnings("unchecked")
    Serializer<Long> serializer = mock(Serializer.class);
    assertThat(copyProvider.createKeyCopier(Long.class, serializer, config), sameInstance((Copier)testCloseableCopier));

    copyProvider.releaseCopier(testCloseableCopier);

    assertFalse(testCloseableCopier.isInvoked());

  }

  private static class TestCloseableCopier<T> extends ReadWriteCopier<T> implements Closeable {

    private boolean invoked = false;

    @Override
    public T copy(final T obj) {
      return obj;
    }

    @Override
    public void close() throws IOException {
      invoked = true;
    }

    boolean isInvoked() {
      return invoked;
    }
  }

  public static class TestCopier<T> extends ReadWriteCopier<T> {

    @Override
    public T copy(final T obj) {
      return obj;
    }
  }
}
