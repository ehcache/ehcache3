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
package org.ehcache.jsr107;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListener;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.creation.MockSettingsImpl;

public class CacheResourcesTest {

  private static final RuntimeException RE = new RuntimeException();

  private Config config;

  @Before
  public void setUp() throws Exception {
    config = new Config();
  }

  @After
  public void tearDown() throws Exception {
    config.verifyAllClosed();
  }

  @Test
  public void testCacheLoaderInitThrowException() throws Exception {
    config.cacheLoaderThrowException = true;
    newCacheResourcesExpectCacheException(config);
  }

  @Test
  public void testCacheWriterInitThrowException() throws Exception {
    config.cacheWriterThrowException = true;
    newCacheResourcesExpectCacheException(config);
  }

  @Test
  public void testExpiryPolicyInitThrowException() throws Exception {
    config.expiryPolicyThrowException = true;
    newCacheResourcesExpectCacheException(config);
  }

  @Test
  public void testCacheEntryListenerThrowException() throws Exception {
    config.entryListenerThrowException = true;
    newCacheResourcesExpectCacheException(config);
  }

  @Test
  public void testCacheEntryFilterThrowException() throws Exception {
    config.entryFilterThrowException = true;
    newCacheResourcesExpectCacheException(config);
  }

  @Test
  public void testNoExceptions() {
    // test that construction succeeds
    CacheResources<Object, Object> cacheResources = new CacheResources<Object, Object>("cache", config);
    config.verifyAllFactoriesAccessed(); 
    
    MultiCacheException mce = new MultiCacheException();
    cacheResources.closeResources(mce);
    assertThat(mce.getThrowables(), everyItem(is((Throwable) RE)));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testRegisterDeregisterAfterClose() {
    CacheResources<Object, Object> cacheResources = new CacheResources<Object, Object>("cache", config);
    cacheResources.closeResources(new MultiCacheException());

    try {
      cacheResources.registerCacheEntryListener(mock(CacheEntryListenerConfiguration.class));
      fail();
    } catch (IllegalStateException ise) {
      // expected
    }

    try {
      cacheResources.deregisterCacheEntryListener(mock(CacheEntryListenerConfiguration.class));
      fail();
    } catch (IllegalStateException ise) {
      // expected
    }
  }

  private static void newCacheResourcesExpectCacheException(CompleteConfiguration<Object, Object> config) {
    try {
      new CacheResources<Object, Object>("cache", config);
      fail();
    } catch (MultiCacheException mce) {
      // expected the static instance is present, anything else is some unexpected failure
      if (!mce.getThrowables().contains(RE)) {
        throw mce;
      }
    }
  }

  @SuppressWarnings({ "serial" })
  private static class Config implements CompleteConfiguration<Object, Object> {
    boolean cacheWriterThrowException = false;
    boolean cacheLoaderThrowException = false;
    boolean expiryPolicyThrowException = false;
    boolean entryFilterThrowException = false;
    boolean entryListenerThrowException = false;

    private Factory<CacheLoader<Object, Object>> cacheLoaderFactory;
    private Factory<CacheWriter<? super Object, ? super Object>> cacheWriterFactory;
    private Factory<ExpiryPolicy> expiryPolicyFactory;
    private Factory<CacheEntryListener<? super Object, ? super Object>> cacheEntryListenerFactory;
    private Factory<CacheEntryEventFilter<? super Object, ? super Object>> cacheEntryEventFilterFactory;

    @Override
    public Class<Object> getKeyType() {
      throw new AssertionError();
    }

    @Override
    public Class<Object> getValueType() {
      throw new AssertionError();
    }

    @Override
    public boolean isStoreByValue() {
      throw new AssertionError();
    }

    @Override
    public boolean isReadThrough() {
      throw new AssertionError();
    }

    @Override
    public boolean isWriteThrough() {
      throw new AssertionError();
    }

    @Override
    public boolean isStatisticsEnabled() {
      throw new AssertionError();
    }

    @Override
    public boolean isManagementEnabled() {
      throw new AssertionError();
    }

    @Override
    public Iterable<CacheEntryListenerConfiguration<Object, Object>> getCacheEntryListenerConfigurations() {
      CacheEntryListenerConfiguration<Object, Object> listenerConfig = new CacheEntryListenerConfiguration<Object, Object>() {

        @Override
        public boolean isSynchronous() {
          throw new AssertionError();
        }

        @Override
        public boolean isOldValueRequired() {
          throw new AssertionError();
        }

        @Override
        public Factory<CacheEntryListener<? super Object, ? super Object>> getCacheEntryListenerFactory() {
          if (cacheEntryListenerFactory == null) {
            cacheEntryListenerFactory = new Factory<CacheEntryListener<? super Object, ? super Object>>(
                CacheEntryListener.class, entryListenerThrowException);
          }
          return cacheEntryListenerFactory;
        }

        @Override
        public Factory<CacheEntryEventFilter<? super Object, ? super Object>> getCacheEntryEventFilterFactory() {
          if (cacheEntryEventFilterFactory == null) {
            cacheEntryEventFilterFactory = new Factory<CacheEntryEventFilter<? super Object, ? super Object>>(
                CacheEntryEventFilter.class, entryFilterThrowException);
          }
          return cacheEntryEventFilterFactory;
        }
      };

      return Collections.singleton(listenerConfig);
    }

    @Override
    public Factory<CacheLoader<Object, Object>> getCacheLoaderFactory() {
      if (cacheLoaderFactory == null) {
        cacheLoaderFactory = new Factory<CacheLoader<Object, Object>>(CacheLoader.class, cacheLoaderThrowException);
      }
      return cacheLoaderFactory;
    }

    @Override
    public Factory<CacheWriter<? super Object, ? super Object>> getCacheWriterFactory() {
      if (cacheWriterFactory == null) {
        cacheWriterFactory = new Factory<CacheWriter<? super Object, ? super Object>>(CacheWriter.class,
            cacheWriterThrowException);
      }
      return cacheWriterFactory;
    }

    @Override
    public Factory<ExpiryPolicy> getExpiryPolicyFactory() {
      if (expiryPolicyFactory == null) {
        expiryPolicyFactory = new Factory<ExpiryPolicy>(ExpiryPolicy.class, expiryPolicyThrowException);
      }
      return expiryPolicyFactory;
    }

    void verifyAllClosed() throws IOException {
      for (Factory<?> f : getFactories()) {
        verifyInstanceClosedIfNonNull(f);
      }
    }

    void verifyAllFactoriesAccessed() {
      for (Factory<?> f : getFactories()) {
        if (f == null) {
          throw new AssertionError();
        }
      }
    }

    Iterable<Factory<?>> getFactories() {
      List<Factory<?>> factories = new ArrayList<Factory<?>>();

      try {
        for (Field f : getClass().getDeclaredFields()) {
          f.setAccessible(true);
          if (f.getType().isAssignableFrom(Factory.class)) {
            factories.add((Factory<?>) f.get(this));
          }
        }

        if (factories.isEmpty()) {
          throw new AssertionError();
        }

        return factories;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private static void verifyInstanceClosedIfNonNull(Factory<?> factory) throws IOException {
      if (factory != null) {
        if (factory.instance != null) {
          verify((Closeable) factory.instance).close();
        }
      }
    }
  }

  @SuppressWarnings({ "serial", "unchecked" })
  private static class Factory<T> implements javax.cache.configuration.Factory<T> {

    private final Class<?> clazz;
    private final boolean throwException;
    private Closeable instance;

    Factory(Class<?> clazz, boolean throwException) {
      this.clazz = clazz;
      this.throwException = throwException;
    }

    @Override
    public synchronized T create() {
      if (throwException) {
        throw RE;
      }

      if (instance == null) {
        instance = (Closeable) mock(clazz, new MockSettingsImpl<Object>().extraInterfaces(Closeable.class));
        try {
          doThrow(RE).when(instance).close();
        } catch (IOException e) {
          throw new AssertionError();
        }
        return (T) instance;
      }

      // for this test if we're calling create() more than once we're doing something wrong
      throw new AssertionError();
    }
  }

}
