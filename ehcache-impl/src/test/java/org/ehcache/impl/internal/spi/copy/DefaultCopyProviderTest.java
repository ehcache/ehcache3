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

import static org.ehcache.core.spi.ServiceLocatorUtils.withServiceLocator;
import static org.ehcache.test.MockitoUtil.uncheckedGenericMock;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertFalse;

/**
 * Created by alsu on 20/08/15.
 */
public class DefaultCopyProviderTest {

  @Test
  public void testCreateKeyCopierWithCustomCopierConfig() throws Exception {
    withServiceLocator(new DefaultCopyProvider(null), provider -> {
      @SuppressWarnings("unchecked")
      DefaultCopierConfiguration<Long> config = new DefaultCopierConfiguration<Long>(
        (Class)TestCopier.class, DefaultCopierConfiguration.Type.KEY);

      assertThat(provider.createKeyCopier(Long.class, null, config), instanceOf(TestCopier.class));
    });
  }

  @Test
  public void testCreateKeyCopierWithoutConfig() throws Exception {
    withServiceLocator(new DefaultCopyProvider(null), provider -> {

      assertThat(provider.createKeyCopier(Long.class, null), instanceOf(IdentityCopier.class));
    });
  }

  @Test
  public void testCreateKeyCopierWithSerializer() throws Exception {
    withServiceLocator(new DefaultCopyProvider(null), provider -> {
      DefaultCopierConfiguration<Long> config = new DefaultCopierConfiguration<>(
        SerializingCopier.<Long>asCopierClass(), DefaultCopierConfiguration.Type.KEY);

      Serializer<Long> serializer = uncheckedGenericMock(Serializer.class);
      assertThat(provider.createKeyCopier(Long.class, serializer, config), instanceOf(SerializingCopier.class));
    });
  }

  @Test
  public void testCreateValueCopierWithCustomCopierConfig() throws Exception {
    withServiceLocator(new DefaultCopyProvider(null), provider -> {
      @SuppressWarnings("unchecked")
      DefaultCopierConfiguration<Long> config = new DefaultCopierConfiguration<Long>(
        (Class) TestCopier.class, DefaultCopierConfiguration.Type.VALUE);

      assertThat(provider.createValueCopier(Long.class, null, config), instanceOf(TestCopier.class));
    });
  }

  @Test
  public void testCreateValueCopierWithoutConfig() throws Exception {
    withServiceLocator(new DefaultCopyProvider(null), provider -> {
      assertThat(provider.createValueCopier(Long.class, null), instanceOf(IdentityCopier.class));
    });
  }

  @Test
  public void testCreateValueCopierWithSerializer() throws Exception {
    withServiceLocator(new DefaultCopyProvider(null), provider -> {
      DefaultCopierConfiguration<Long> config = new DefaultCopierConfiguration<>(
        SerializingCopier.<Long>asCopierClass(), DefaultCopierConfiguration.Type.VALUE);

      Serializer<Long> serializer = uncheckedGenericMock(Serializer.class);
      assertThat(provider.createValueCopier(Long.class, serializer, config), instanceOf(SerializingCopier.class));
    });

  }

  @Test
  public void testUserProvidedCloseableCopierInstanceDoesNotCloseOnRelease() throws Exception {
    withServiceLocator(new DefaultCopyProvider(null), provider -> {
      TestCloseableCopier<Long> testCloseableCopier = new TestCloseableCopier<>();
      DefaultCopierConfiguration<Long> config = new DefaultCopierConfiguration<>(testCloseableCopier, DefaultCopierConfiguration.Type.KEY);

      Serializer<Long> serializer = uncheckedGenericMock(Serializer.class);
      assertThat(provider.createKeyCopier(Long.class, serializer, config), sameInstance((Copier) testCloseableCopier));

      provider.releaseCopier(testCloseableCopier);

      assertFalse(testCloseableCopier.isInvoked());
    });
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
